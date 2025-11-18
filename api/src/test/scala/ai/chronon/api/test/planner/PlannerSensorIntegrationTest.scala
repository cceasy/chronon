package ai.chronon.api.test.planner

import ai.chronon.api._
import ai.chronon.api.planner.{GroupByPlanner, LocalRunner, MonolithJoinPlanner, StagingQueryPlanner}
import ai.chronon.planner.Mode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.collection.JavaConverters._

class PlannerSensorIntegrationTest extends AnyFlatSpec with Matchers {

  private implicit val testPartitionSpec: PartitionSpec = PartitionSpec.daily

  "GroupByPlanner" should "create external sensor nodes for table dependencies" in {
    val gbRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/group_bys").getPath)
    val gbConfs = LocalRunner.parseConfs[GroupBy](gbRootDir.toString)

    gbConfs.foreach { groupBy =>
      val planner = GroupByPlanner(groupBy)
      val plan = planner.buildPlan

      // Find sensor nodes in the plan
      val sensorNodes = plan.nodes.asScala.filter(_.content.isSetExternalSourceSensor)

      if (
        groupBy.metaData.executionInfo.tableDependencies != null && !groupBy.metaData.executionInfo.tableDependencies.isEmpty
      ) {
        sensorNodes should not be empty

        sensorNodes.foreach { node =>
          val sensor = node.content.getExternalSourceSensor

          // Verify sensor structure
          sensor should not be null
          sensor.getSourceTableDependency.tableInfo.table should not be empty
          sensor.metaData should not be null

          // Verify sensor metadata follows expected naming pattern
          sensor.metaData.name should include("__sensor")
          sensor.metaData.name should include(groupBy.metaData.name)

          // Verify sensor has no table dependencies (sensors are leaf nodes)
          sensor.metaData.executionInfo.tableDependencies should be(empty)

          // Verify sensor output table matches source name
          sensor.metaData.executionInfo.outputTableInfo.table should equal(
            sensor.getSourceTableDependency.tableInfo.table)
        }

        // Verify sensor count matches table dependency count
        val tableDepsCount = groupBy.metaData.executionInfo.tableDependencies.size()
        sensorNodes should have size tableDepsCount
      }
    }
  }

  "StagingQueryPlanner" should "create external sensor nodes for table dependencies" in {
    val stagingQueryRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/staging_queries").getPath)
    val stagingQueryConfs = LocalRunner.parseConfs[StagingQuery](stagingQueryRootDir.toString)

    stagingQueryConfs.foreach { stagingQuery =>
      val planner = StagingQueryPlanner(stagingQuery)
      val plan = planner.buildPlan

      // Find sensor nodes in the plan
      val sensorNodes = plan.nodes.asScala.filter(_.content.isSetExternalSourceSensor)

      if (
        stagingQuery.metaData.executionInfo.tableDependencies != null && !stagingQuery.metaData.executionInfo.tableDependencies.isEmpty
      ) {
        sensorNodes should not be empty

        sensorNodes.foreach { node =>
          val sensor = node.content.getExternalSourceSensor

          // Verify sensor structure
          sensor should not be null
          sensor.getSourceTableDependency.tableInfo.table should not be empty
          sensor.metaData should not be null

          // Verify sensor metadata follows expected naming pattern
          sensor.metaData.name should include("__sensor")
          sensor.metaData.name should include(stagingQuery.metaData.name)

          // Verify sensor has no table dependencies
          sensor.metaData.executionInfo.tableDependencies should be(empty)

          // Verify sensor output table matches source name
          sensor.metaData.executionInfo.outputTableInfo.table should equal(
            sensor.getSourceTableDependency.tableInfo.table)
        }
      }
    }
  }

  "MonolithJoinPlanner" should "create external sensor nodes for table dependencies" in {
    val joinRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/joins").getPath)
    val joinConfs = LocalRunner.parseConfs[Join](joinRootDir.toString)

    joinConfs.foreach { join =>
      val planner = MonolithJoinPlanner(join)
      val plan = planner.buildPlan

      // Find sensor nodes in the plan
      val sensorNodes = plan.nodes.asScala.filter(_.content.isSetExternalSourceSensor)

      if (
        join.metaData.executionInfo.tableDependencies != null && !join.metaData.executionInfo.tableDependencies.isEmpty
      ) {
        sensorNodes should not be empty

        sensorNodes.foreach { node =>
          val sensor = node.content.getExternalSourceSensor

          // Verify sensor structure
          sensor should not be null
          sensor.getSourceTableDependency.tableInfo.table should not be empty
          sensor.metaData should not be null

          // Verify sensor metadata follows expected naming pattern
          sensor.metaData.name should include("__sensor")
          sensor.metaData.name should include(join.metaData.name)

          // Verify sensor has no table dependencies
          sensor.metaData.executionInfo.tableDependencies should be(empty)

          // Verify sensor output table matches source name
          sensor.metaData.executionInfo.outputTableInfo.table should equal(
            sensor.getSourceTableDependency.tableInfo.table)
        }

        // Verify sensor count matches table dependency count
        val tableDepsCount = join.metaData.executionInfo.tableDependencies.size()
        sensorNodes should have size tableDepsCount
      }
    }
  }

  "All planners" should "create consistent sensor metadata" in {
    val gbRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/group_bys").getPath)
    val stagingQueryRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/staging_queries").getPath)
    val joinRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/joins").getPath)

    val gbConfs = LocalRunner.parseConfs[GroupBy](gbRootDir.toString)
    val stagingQueryConfs = LocalRunner.parseConfs[StagingQuery](stagingQueryRootDir.toString)
    val joinConfs = LocalRunner.parseConfs[Join](joinRootDir.toString)

    val allPlans = Seq(
      ("GroupBy", gbConfs.map(GroupByPlanner(_).buildPlan)),
      ("StagingQuery", stagingQueryConfs.map(StagingQueryPlanner(_).buildPlan)),
      ("Join", joinConfs.map(MonolithJoinPlanner(_).buildPlan))
    )

    allPlans.foreach { case (plannerType, plans) =>
      plans.foreach { plan =>
        val sensorNodes = plan.nodes.asScala.filter(_.content.isSetExternalSourceSensor)

        sensorNodes.foreach { node =>
          val sensor = node.content.getExternalSourceSensor

          withClue(s"$plannerType planner sensor validation: ") {
            // Verify sensor follows consistent structure
            sensor.metaData.name should fullyMatch regex ".*__sensor"
            sensor.metaData.executionInfo should not be null
            sensor.metaData.executionInfo.outputTableInfo should not be null
            sensor.metaData.executionInfo.outputTableInfo.table should not be empty

            // Verify sensor output table matches source name
            sensor.metaData.executionInfo.outputTableInfo.table should equal(
              sensor.getSourceTableDependency.tableInfo.table)

            // Verify sensor has empty table dependencies
            Option(sensor.metaData.executionInfo.tableDependencies).foreach { deps =>
              deps should be(empty)
            }
          }
        }
      }
    }
  }

  "Sensor nodes" should "be included in terminal node plans" in {
    val gbRootDir = Paths.get(getClass.getClassLoader.getResource("canary/compiled/group_bys").getPath)
    val gbConfs = LocalRunner.parseConfs[GroupBy](gbRootDir.toString)

    gbConfs.foreach { groupBy =>
      val planner = GroupByPlanner(groupBy)
      val plan = planner.buildPlan

      // Verify plan structure includes sensor nodes
      val allNodes = plan.nodes.asScala
      val sensorNodes = allNodes.filter(_.content.isSetExternalSourceSensor)
      val backfillNodes = allNodes.filter(_.content.isSetGroupByBackfill)
      val deployNodes =
        allNodes.filter(node => node.content.isSetGroupByUploadToKV || node.content.isSetGroupByStreaming)

      // Verify sensors exist if there are table dependencies
      if (
        groupBy.metaData.executionInfo.tableDependencies != null && !groupBy.metaData.executionInfo.tableDependencies.isEmpty
      ) {
        sensorNodes should not be empty

        // Verify sensors are properly integrated into the execution plan
        allNodes should contain allElementsOf sensorNodes
        backfillNodes should not be empty
        deployNodes should not be empty
      }

      // Verify terminal nodes are set up correctly
      plan.terminalNodeNames should not be null
      plan.terminalNodeNames.containsKey(Mode.BACKFILL) should be(true)
      plan.terminalNodeNames.containsKey(Mode.DEPLOY) should be(true)
    }
  }
}
