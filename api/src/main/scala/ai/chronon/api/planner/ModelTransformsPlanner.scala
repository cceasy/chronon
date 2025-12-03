package ai.chronon.api.planner

import ai.chronon.api.{ModelTransforms, PartitionSpec}
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.ScalaJavaConversions.IterableOps
import ai.chronon.api.planner.TableDependencies.{fromSource, fromTable}
import ai.chronon.planner.{ConfPlan, ModelTransformsBackfillNode, ModelTransformsUploadNode, Node}
import ai.chronon.planner

import scala.collection.JavaConverters._

class ModelTransformsPlanner(modelTransforms: ModelTransforms)(implicit outputPartitionSpec: PartitionSpec)
    extends ConfPlanner[ModelTransforms](modelTransforms)(outputPartitionSpec) {

  private def eraseExecutionInfo: ModelTransforms = {
    val result = modelTransforms.deepCopy()
    result.metaData.unsetExecutionInfo()
    result
  }

  private def semanticModelTransforms(modelTransforms: ModelTransforms): ModelTransforms = {
    val semantic = modelTransforms.deepCopy()
    semantic.unsetMetaData()
    semantic
  }

  def backfillNode: Node = {
    val tableDeps =
      Option(modelTransforms.sources)
        .map(_.toScala.toSeq)
        .getOrElse(Seq.empty)
        .flatMap { source =>
          if (source.isSetJoinSource) {
            // For join sources, depend on the join's output table
            val upstreamJoin = source.getJoinSource.getJoin
            val upstreamJoinOutputTable = upstreamJoin.metaData.outputTable
            Some(fromTable(upstreamJoinOutputTable, source.getJoinSource.query))
          } else {
            fromSource(source)
          }
        }

    val metaData =
      MetaDataUtils.layer(
        modelTransforms.metaData,
        "model_transforms_backfill",
        modelTransforms.metaData.name + "__model_transforms_backfill",
        tableDeps,
        outputTableOverride = Some(modelTransforms.metaData.outputTable)
      )

    val node = new ModelTransformsBackfillNode().setModelTransforms(modelTransforms)

    val copy = semanticModelTransforms(modelTransforms)

    toNode(metaData, _.setModelTransformsBackfill(node), copy)
  }

  def uploadNode: Node = {
    val stepDays = 1 // Default step days for metadata upload

    // Create table dependencies only for JoinSource sources - we ensure join metadata is uploaded before proceeding
    val allDeps = TableDependencies.fromJoinSources(modelTransforms.sources)

    val metaData =
      MetaDataUtils.layer(
        modelTransforms.metaData,
        "model_transforms_upload",
        modelTransforms.metaData.name + "__model_transforms_upload",
        allDeps,
        Some(stepDays)
      )

    val node = new ModelTransformsUploadNode().setModelTransforms(eraseExecutionInfo)

    val copy = semanticModelTransforms(modelTransforms)

    toNode(metaData, _.setModelTransformsUpload(node), copy)
  }

  override def buildPlan: ConfPlan = {
    val upload = uploadNode
    val backfill = backfillNode

    val terminalNodeNames = Map(
      planner.Mode.BACKFILL -> backfill.metaData.name,
      planner.Mode.DEPLOY -> upload.metaData.name
    )

    new ConfPlan()
      .setNodes(Seq(upload, backfill).asJava)
      .setTerminalNodeNames(terminalNodeNames.asJava)
  }
}

object ModelTransformsPlanner {
  def apply(modelTransforms: ModelTransforms)(implicit outputPartitionSpec: PartitionSpec): ModelTransformsPlanner =
    new ModelTransformsPlanner(modelTransforms)(outputPartitionSpec)
}
