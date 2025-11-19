package ai.chronon.spark.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

case object Iceberg extends Format {

  override def tableProperties: Map[String, String] = {
    Map(
      "commit.retry.num-retries" -> "20", // default = 4
      "commit.retry.min-wait-ms" -> (10 * 1000).toString,
      "commit.retry.max-wait-ms" -> (600 * 1000).toString,
      "commit.status-check.num-retries" -> "20", // default = 3
      "commit.status-check.min-wait-ms" -> (10 * 1000).toString, // default = 1000
      "commit.status-check.max-wait-ms" -> (600 * 1000).toString,
      "write.merge.isolation-level" -> "snapshot"
    )
  }

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] = {

    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    getIcebergPartitions(tableName, partitionColumn)
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val partitionsDf = sparkSession.read
      .format("iceberg")
      .load(s"${tableName}.partitions")

    // Get the partition struct schema to understand what columns are partitioned
    val index = partitionsDf.schema.fieldIndex("partition")
    val partitionStruct = partitionsDf.schema(index).dataType.asInstanceOf[StructType]
    val partitionColumnNames = partitionStruct.fieldNames

    // Collect all partition rows and convert each partition struct to a Map[String, String]
    val partitionRows = partitionsDf.select(col("partition")).collect()

    partitionRows
      .map { row =>
        val partitionData = row.getStruct(0)
        partitionColumnNames.flatMap { colName =>
          Option(partitionData.getAs[Any](colName)).map(colName -> _.toString)
        }.toMap
      }
      .toList
      .distinct
  }

  private def getIcebergPartitions(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): List[String] = {

    val partitionsDf = sparkSession.read
      .format("iceberg")
      .load(s"${tableName}.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")
    if (partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("hr")) {
      // Hour filter is currently buggy in iceberg. https://github.com/apache/iceberg/issues/4718
      // so we collect and then filter.
      partitionsDf
        .select(col(s"partition.$partitionColumn"), col("partition.hr"))
        .collect()
        .filter(_.get(1) == null)
        .map(_.getString(0))
        .toList

    } else {

      partitionsDf
        .select(col(s"partition.$partitionColumn").cast("string"))
        .collect()
        .map(_.getString(0))
        .toList
    }
  }

  override def supportSubPartitionsFilter: Boolean = false
}
