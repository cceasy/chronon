package ai.chronon.spark.batch

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, JoinOps, MetadataOps, SourceOps}
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api.{Accuracy, DataModel, DateRange, MetaData, PartitionRange, PartitionSpec}
import ai.chronon.planner.{JoinBootstrapNode, JoinDerivationNode, JoinMergeNode, JoinPartNode, SourceWithFilterNode}
import ai.chronon.spark.JoinUtils
import ai.chronon.spark.catalog.TableUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Seq

/** ModularMonolith orchestrates the join pipeline using discrete job classes from an api.Join object:
  * SourceJob -> JoinBootstrapJob -> JoinPartJob(s) -> MergeJob -> JoinDerivationJob
  *
  * Each job handles its own internal range shifting logic (e.g., for EVENTS + SNAPSHOT cases).
  */
class ModularMonolith(join: api.Join, dateRange: DateRange)(implicit tableUtils: TableUtils) {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val partitionSpec: api.PartitionSpec = tableUtils.partitionSpec

  // decompose into smaller jobs and run them in topological order
  def run(): Unit = {
    logger.info(s"Starting ModularMonolith pipeline for join: ${join.metaData.name}")

    // Step 1: Run SourceJob to compute the left source table
    runSourceJob()

    // Step 2: Run JoinBootstrapJob if there are bootstrap parts or external parts
    if (join.bootstrapParts != null && join.onlineExternalParts != null) {
      runBootstrapJob()
    }

    // Step 3: Run JoinPartJob for each join part
    runJoinPartJobs()

    // Step 4: Run MergeJob to combine all parts
    runMergeJob()

    // Step 5: Run JoinDerivationJob if there are derivations
    if (join.isSetDerivations && !join.derivations.isEmpty) {
      runDerivationJob()
    }

    logger.info(s"Completed ModularMonolith pipeline for join: ${join.metaData.name}")
  }

  // materialize left side
  private def runSourceJob(): Unit = {
    logger.info("Running SourceJob for left source")

    val sourceOutputTable = JoinUtils.computeFullLeftSourceTableName(join)
    val sourceParts = sourceOutputTable.split("\\.", 2)
    val sourceNamespace = sourceParts(0)
    val sourceName = sourceParts(1)

    val sourceMetaData = new MetaData()
      .setName(sourceName)
      .setOutputNamespace(sourceNamespace)

    val sourceNode = new SourceWithFilterNode()
      .setSource(join.left)
      .setExcludeKeys(join.skewKeys)

    StepRunner(dateRange, sourceMetaData) { stepRange =>
      val sourceJob = new SourceJob(sourceNode, sourceMetaData, stepRange)
      sourceJob.run()
    }

    logger.info(s"SourceJob completed, output table: $sourceOutputTable")
  }

  private def runBootstrapJob(): String = {
    logger.info("Running JoinBootstrapJob")

    val bootstrapOutputTable = join.metaData.bootstrapTable
    val bootstrapParts = bootstrapOutputTable.split("\\.", 2)
    val bootstrapNamespace = bootstrapParts(0)
    val bootstrapName = bootstrapParts(1)

    val bootstrapMetaData = new MetaData()
      .setName(bootstrapName)
      .setOutputNamespace(bootstrapNamespace)

    val bootstrapNode = new JoinBootstrapNode()
      .setJoin(join)

    StepRunner(dateRange, bootstrapMetaData) { stepRange =>
      val bootstrapJob = new JoinBootstrapJob(bootstrapNode, bootstrapMetaData, stepRange)
      bootstrapJob.run()
    }

    logger.info(s"JoinBootstrapJob completed, output table: $bootstrapOutputTable")
    bootstrapOutputTable
  }

  // Note: Range shifting for EVENTS+SNAPSHOT cases is handled internally by JoinPartJob and mergeJob
  // TODO: we will remove this internal shifting
  private def runJoinPartJobs(): Unit = {
    logger.info(s"Running JoinPartJobs for ${join.joinParts.size()} join parts")

    val sourceOutputTable = JoinUtils.computeFullLeftSourceTableName(join)
    val outputNamespace = join.metaData.outputNamespace

    join.joinParts.toScala.foreach { joinPart =>
      val joinPartGroupByName = joinPart.groupBy.metaData.name
      logger.info(s"Running JoinPartJob for: $joinPartGroupByName")

      val partTableName = ai.chronon.api.planner.RelevantLeftForJoinPart.partTableName(join, joinPart)
      val partFullTableName = ai.chronon.api.planner.RelevantLeftForJoinPart.fullPartTableName(join, joinPart)

      val partMetaData = new MetaData()
        .setName(partTableName)
        .setOutputNamespace(outputNamespace)

      val joinPartNode = new JoinPartNode()
        .setLeftSourceTable(sourceOutputTable)
        .setLeftDataModel(join.left.dataModel)
        .setJoinPart(joinPart)
        .setSkewKeys(join.skewKeys)

      val shiftedRange =
        if (join.left.dataModel == DataModel.EVENTS && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
          val spec = PartitionSpec.daily
          new DateRange()
            .setStartDate(spec.before(dateRange.startDate))
            .setEndDate(spec.before(dateRange.endDate))
        } else {
          dateRange
        }

      StepRunner(shiftedRange, partMetaData) { stepRange =>
        val joinPartJob = new JoinPartJob(joinPartNode, partMetaData, stepRange, alignOutput = true)
        joinPartJob.run(None) // Run without context for now
      }
      logger.info(s"JoinPartJob completed for: $joinPartGroupByName, output table: $partFullTableName")
    }
  }

  // combine all parts and bootstraps
  // Note: Range shifting for reading EVENTS+SNAPSHOT parts is handled internally by MergeJob
  // TODO: we need to get rid of the shifting
  private def runMergeJob(): Unit = {
    logger.info("Running MergeJob to merge all join parts")

    val mergeOutputTable = join.metaData.outputTable
    val mergeMetaData = new MetaData()
      .setName(join.metaData.name)
      .setOutputNamespace(join.metaData.outputNamespace)

    val mergeNode = new JoinMergeNode()
      .setJoin(join)
      .setProductionJoin(null) // No production join for basic case

    StepRunner(dateRange, mergeMetaData) { stepRange =>
      val mergeJob = new MergeJob(mergeNode, mergeMetaData, stepRange, join.joinParts.toScala.toSeq)
      mergeJob.run()
    }

    logger.info(s"MergeJob completed, output table: $mergeOutputTable")
  }

  private def runDerivationJob(): Unit = {
    logger.info("Running JoinDerivationJob to apply derivations")

    // Derivation output follows naming convention: <join_cleanname>_derived
    val derivationMetaData = new MetaData()
      .setName(s"${join.metaData.cleanName}_derived")
      .setOutputNamespace(join.metaData.outputNamespace)

    val derivationNode = new JoinDerivationNode()
      .setJoin(join)

    StepRunner(dateRange, derivationMetaData) { stepRange =>
      val derivationJob = new JoinDerivationJob(derivationNode, derivationMetaData, stepRange)
      derivationJob.run()
    }
    val derivationOutputTable = derivationMetaData.outputTable
    logger.info(s"JoinDerivationJob completed, output table: $derivationOutputTable")
  }
}

object ModularMonolith {

  def run(join: api.Join, dateRange: DateRange)(implicit tableUtils: TableUtils): Unit = {
    new ModularMonolith(join, dateRange).run()
  }
}
