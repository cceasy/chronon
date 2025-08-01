package ai.chronon.flink.deser

import ai.chronon.api.StructType
import ai.chronon.flink.FlinkKafkaItemEventDriver
import ai.chronon.flink.deser.MockCustomSchemaProvider.schemaMap
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroConversions, AvroSerDe, Mutation, SerDe}
import org.apache.avro.Schema
import scala.collection.mutable

// Configured in topic config in this fashion:
// kafka://my-test-topic/provider_class=ai.chronon.flink.deser.MockCustomSchemaProvider/schema_name=item_event
object CustomSchemaSerDe {
  val ProviderClass = "provider_class"
  val SchemaName = "schema_name"
}

/** Mock custom schema provider that vends out a custom hardcoded event schema
  */
class MockCustomSchemaProvider(topicInfo: TopicInfo) extends SerDe {
  private val schemaName = topicInfo.params.getOrElse(CustomSchemaSerDe.SchemaName, "item_event")
  require(schemaMap.contains(schemaName), s"Schema name must in ${schemaMap.keySet}, but got $schemaName")
  private val avroSchema = schemaMap(schemaName)

  lazy val chrononSchema: StructType = AvroConversions.toChrononSchema(avroSchema).asInstanceOf[StructType]

  lazy val avroSerDe = new AvroSerDe(avroSchema)

  override def schema: StructType = chrononSchema

  override def fromBytes(messageBytes: Array[Byte]): Mutation = {
    avroSerDe.fromBytes(messageBytes)
  }
}

object MockCustomSchemaProvider {
  val schemaMap: mutable.Map[String, Schema] = mutable.Map.empty

  {
    schemaMap("item_event") = FlinkKafkaItemEventDriver.avroSchema
    schemaMap("pa_fraud") = MyAvroSchemas.paFraud
  }

}
