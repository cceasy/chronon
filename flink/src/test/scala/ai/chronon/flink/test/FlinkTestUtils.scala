package ai.chronon.flink.test

import ai.chronon.api.Accuracy
import ai.chronon.api.Builders
import ai.chronon.api.GroupBy
import ai.chronon.api.GroupByServingInfo
import ai.chronon.api.Operation
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.TimeUnit
import ai.chronon.api.Window
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.source.FlinkSource
import ai.chronon.flink.{AsyncKVStoreWriter, SparkExpressionEvalFn}
import ai.chronon.flink.types.WriteResponse
import ai.chronon.online.Api
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.KVStore
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.spark.sql.types.StructType
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.when
import org.mockito.Mockito.withSettings
import org.scalatestplus.mockito.MockitoSugar.mock

import java.time.Duration
import java.util
import java.util.Collections
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future

case class E2ETestEvent(id: String, int_val: Int, double_val: Double, created: Long)
case class E2ETestMutationEvent(id: String,
                                int_val: Int,
                                double_val: Double,
                                created: Long,
                                mutationTime: Long,
                                isBefore: Boolean)

abstract class BaseWatermarkedE2EEventSource[T](mockEvents: Seq[T], sparkExprEvalFn: SparkExpressionEvalFn[T])
    extends FlinkSource[ProjectedEvent] {

  def watermarkStrategy: WatermarkStrategy[T]

  implicit val parallelism: Int = 1

  override def getDataStream(topic: String, groupName: String)(
      env: StreamExecutionEnvironment,
      parallelism: Int): SingleOutputStreamOperator[ProjectedEvent] = {
    env
      .fromCollection(mockEvents.toJava)
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .flatMap(sparkExprEvalFn)
      .map(e => ProjectedEvent(e, System.currentTimeMillis()))
  }
}

class WatermarkedE2EEventSource(mockEvents: Seq[E2ETestEvent], sparkExprEvalFn: SparkExpressionEvalFn[E2ETestEvent])
    extends BaseWatermarkedE2EEventSource[E2ETestEvent](mockEvents, sparkExprEvalFn) {
  def watermarkStrategy: WatermarkStrategy[E2ETestEvent] =
    WatermarkStrategy
      .forBoundedOutOfOrderness[E2ETestEvent](Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[E2ETestEvent] {
        override def extractTimestamp(event: E2ETestEvent, previousElementTimestamp: Long): Long =
          event.created
      })
}

class WatermarkedE2ETestMutationEventSource(mockEvents: Seq[E2ETestMutationEvent],
                                            sparkExprEvalFn: SparkExpressionEvalFn[E2ETestMutationEvent])
    extends BaseWatermarkedE2EEventSource[E2ETestMutationEvent](mockEvents, sparkExprEvalFn) {
  def watermarkStrategy: WatermarkStrategy[E2ETestMutationEvent] =
    WatermarkStrategy
      .forBoundedOutOfOrderness[E2ETestMutationEvent](Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[E2ETestMutationEvent] {
        override def extractTimestamp(event: E2ETestMutationEvent, previousElementTimestamp: Long): Long =
          event.created
      })
}

class MockAsyncKVStoreWriter(mockResults: Seq[Boolean], onlineImpl: Api, featureGroup: String)
    extends AsyncKVStoreWriter(onlineImpl, featureGroup) {
  override def getKVStore: KVStore = {
    implicit val ec: ExecutionContextExecutor = ExecutionContext.global
    val mockKvStore = mock[KVStore](withSettings().serializable())
    when(mockKvStore.multiPut(ArgumentMatchers.any[Seq[KVStore.PutRequest]]))
      .thenReturn(Future(mockResults))
    mockKvStore
  }
}

class CollectSink extends SinkFunction[WriteResponse] {
  override def invoke(value: WriteResponse, context: SinkFunction.Context): Unit = {
    CollectSink.values.add(value)
  }
}

object CollectSink {
  // must be static
  val values: util.List[WriteResponse] = Collections.synchronizedList(new util.ArrayList())
}

object FlinkTestUtils {
  def makeTestGroupByServingInfoParsed(groupBy: GroupBy,
                                       inputSchema: StructType,
                                       outputSchema: StructType): GroupByServingInfoParsed = {
    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(groupBy)

    // Set input avro schema for groupByServingInfo
    groupByServingInfo.setInputAvroSchema(
      inputSchema.toAvroSchema("Input").toString(true)
    )

    // Set key avro schema for groupByServingInfo
    groupByServingInfo.setKeyAvroSchema(
      StructType(
        groupBy.keyColumns.toScala.map { keyCol =>
          val keyColStructType = outputSchema.fields.find(field => field.name == keyCol)
          keyColStructType match {
            case Some(col) => col
            case None =>
              throw new IllegalArgumentException(s"Missing key col from output schema: $keyCol")
          }
        }
      ).toAvroSchema("Key")
        .toString(true)
    )

    // Set value avro schema for groupByServingInfo
    val aggInputColNames = groupBy.aggregations.toScala.map(_.inputColumn).toList
    groupByServingInfo.setSelectedAvroSchema(
      StructType(outputSchema.fields.filter(field => aggInputColNames.contains(field.name)))
        .toAvroSchema("Value")
        .toString(true)
    )
    new GroupByServingInfoParsed(groupByServingInfo)
  }

  def makeGroupBy(keyColumns: Seq[String], filters: Seq[String] = Seq.empty): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.my_stream_raw",
          topic = "events.my_stream",
          query = Builders.Query(
            selects = Map(
              "id" -> "id",
              "int_val" -> "int_val",
              "double_val" -> "double_val"
            ),
            wheres = filters,
            timeColumn = "created",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = keyColumns,
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "double_val",
          windows = Seq(
            new Window(1, TimeUnit.DAYS)
          )
        )
      ),
      metaData = Builders.MetaData(
        name = "e2e-count"
      ),
      accuracy = Accuracy.TEMPORAL
    )

  def makeEntityGroupBy(keyColumns: Seq[String], filters: Seq[String] = Seq.empty): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.entities(
          snapshotTable = "events.my_stream_raw",
          mutationTopic = "events.my_stream",
          query = Builders.Query(
            selects = Map(
              "id" -> "id",
              "int_val" -> "int_val",
              "double_val" -> "double_val"
            ),
            wheres = filters,
            timeColumn = "created",
            mutationTimeColumn = "mutationTime",
            reversalColumn = "isBefore",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = keyColumns,
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "double_val",
          windows = Seq(
            new Window(1, TimeUnit.DAYS)
          )
        )
      ),
      metaData = Builders.MetaData(
        name = "e2e-count-entity"
      ),
      accuracy = Accuracy.TEMPORAL
    )
}
