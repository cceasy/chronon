package ai.chronon.api.planner

import ai.chronon.api.Extensions._
import ai.chronon.api.{MetaData, PartitionSpec}
import ai.chronon.planner.ExternalSourceSensorNode

import scala.collection.JavaConverters._

object ExternalSourceSensorUtil {

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
          "backfill",
          metaData.name + "__" + td.tableInfo.table + "__sensor__backfill",
          Seq(), // No table dependencies for sensors
          outputTableOverride =
            Option(td.tableInfo.table) // The input table and the output table are the same for sensors.
        )(tdSpec)
        new ExternalSourceSensorNode()
          .setSourceName(td.tableInfo.table)
          .setMetaData(sensorMd)
      })
      .toList
  }

}
