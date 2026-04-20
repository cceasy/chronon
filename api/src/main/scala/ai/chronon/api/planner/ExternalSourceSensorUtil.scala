package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.{MetaData, PartitionSpec}
import ai.chronon.planner.ExternalSourceSensorNode

import scala.collection.JavaConverters._

object ExternalSourceSensorUtil {

  // Sensors only run lightweight partition-check queries — override resource-heavy
  // configs inherited from downstream nodes with minimal values.
  private val SensorResourceOverrides: Map[String, String] = Map(
    "spark.driver.memory" -> "1g",
    "spark.driver.cores" -> "4",
    "spark.executor.memory" -> "1g",
    "spark.executor.cores" -> "4",
    "spark.executor.instances" -> "1",
    "spark.default.parallelism" -> "4",
    "spark.sql.shuffle.partitions" -> "4"
  )

  def semanticExternalSourceSensor(sensorNode: ExternalSourceSensorNode): ExternalSourceSensorNode = {
    val semanticSensor = sensorNode.deepCopy()
    semanticSensor.unsetMetaData()
    semanticSensor
  }

  def sensorNodes(metaData: MetaData)(implicit spec: PartitionSpec): Seq[ExternalSourceSensorNode] = {

    metaData.executionInfo.tableDependencies.asScala
      .map((td) => {
        val tdSpec = td.tableInfo.partitionSpec(spec)
        val sensorMd = MetaDataUtils.layer(
          metaData,
          "sensor",
          f"${td.tableInfo.table}__sensor",
          Seq(), // No table dependencies for sensors
          outputTableOverride =
            Option(td.tableInfo.table) // The input table and the output table are the same for sensors.
        )(tdSpec)

        applySensorResourceOverrides(sensorMd)

        val retryInterval = 15 // minutes
        val retryCount = 96
        new ExternalSourceSensorNode()
          .setSourceTableDependency(td)
          .setMetaData(sensorMd)
          .setRetryCount(retryCount)
          .setRetryIntervalMin(retryInterval)
      })
      .toList
  }

  private def applySensorResourceOverrides(metaData: MetaData): Unit = {
    val execInfo = Option(metaData.executionInfo).getOrElse(return
    )
    val conf = Option(execInfo.conf).getOrElse(return
    )
    val common = Option(conf.common).getOrElse(return
    )
    SensorResourceOverrides.foreach { case (k, v) =>
      common.put(k, v)
    }
  }

}
