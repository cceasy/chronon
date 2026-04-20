package ai.chronon.flink.test.deser

import ai.chronon.api.{IntType, StringType}
import ai.chronon.flink.deser.ApicurioSchemaSerDe
import ai.chronon.flink.deser.ApicurioSchemaSerDe._
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.AvroCodec
import com.google.protobuf.DynamicMessage
import io.apicurio.registry.rest.client.RegistryClient
import io.apicurio.registry.rest.client.exception.ArtifactNotFoundException
import io.apicurio.registry.rest.v2.beans.{ArtifactMetaData, Error => ApicurioError}
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{ByteArrayInputStream, InputStream}

class MockApicurioSchemaSerDe(topicInfo: TopicInfo, mockClient: RegistryClient)
    extends ApicurioSchemaSerDe(topicInfo) {
  override def buildRegistryClient(registryUrl: String): RegistryClient = mockClient
}

class ApicurioSchemaSerDeSpec extends AnyFlatSpec {

  private val avroSchemaStr =
    """{ "type": "record", "name": "TestRecord",
      |  "fields": [
      |    { "type": "string", "name": "field1" },
      |    { "type": "int",    "name": "field2" }
      |  ]
      |}""".stripMargin

  private def makeMetadata(globalId: Long, artifactType: String): ArtifactMetaData = {
    val meta = new ArtifactMetaData()
    meta.setGlobalId(globalId)
    meta.setType(artifactType)
    meta
  }

  private def streamOf(s: String): InputStream = new ByteArrayInputStream(s.getBytes("UTF-8"))

  it should "fail if registry_host is not provided" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map.empty)
    val mockClient = Mockito.mock(classOf[RegistryClient])
    assertThrows[IllegalArgumentException] {
      new MockApicurioSchemaSerDe(topicInfo, mockClient)
    }
  }

  it should "fail if the artifact is not found" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map(RegistryHostKey -> "localhost"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString()))
      .thenThrow(new ArtifactNotFoundException(new ApicurioError()))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    assertThrows[IllegalArgumentException] {
      serDe.schema
    }
  }

  it should "fail if an unexpected exception occurs when fetching metadata" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map(RegistryHostKey -> "localhost"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString()))
      .thenThrow(new RuntimeException("connection refused"))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    assertThrows[IllegalArgumentException] {
      serDe.schema
    }
  }

  it should "succeed with an Avro schema and derive correct field types" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map(RegistryHostKey -> "localhost"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData("default", "test-topic")).thenReturn(makeMetadata(1L, "AVRO"))
    when(mockClient.getContentByGlobalId(1L)).thenReturn(streamOf(avroSchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "field1" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "field2" && f.fieldType == IntType))
  }

  it should "use the topic name as artifact_id by default" in {
    val topicInfo = TopicInfo("my-events-topic", "kafka", Map(RegistryHostKey -> "localhost"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    // Expect default group="default", artifact="my-events-topic"
    when(mockClient.getArtifactMetaData("default", "my-events-topic")).thenReturn(makeMetadata(2L, "AVRO"))
    when(mockClient.getContentByGlobalId(2L)).thenReturn(streamOf(avroSchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    assert(serDe.schema != null)
  }

  it should "use a custom artifact_id when provided" in {
    val topicInfo = TopicInfo(
      "test-topic", "kafka",
      Map(RegistryHostKey -> "localhost", ArtifactIdKey -> "my-custom-schema"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData("default", "my-custom-schema")).thenReturn(makeMetadata(3L, "AVRO"))
    when(mockClient.getContentByGlobalId(3L)).thenReturn(streamOf(avroSchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    assert(serDe.schema != null)
  }

  it should "use a custom group_id when provided" in {
    val topicInfo = TopicInfo(
      "test-topic", "kafka",
      Map(RegistryHostKey -> "localhost", GroupIdKey -> "my-group", ArtifactIdKey -> "my-schema"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData("my-group", "my-schema")).thenReturn(makeMetadata(4L, "AVRO"))
    when(mockClient.getContentByGlobalId(4L)).thenReturn(streamOf(avroSchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    assert(serDe.schema != null)
  }

  it should "fail on an unsupported schema type" in {
    val topicInfo = TopicInfo("test-topic", "kafka", Map(RegistryHostKey -> "localhost"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(5L, "OPENAPI"))
    when(mockClient.getContentByGlobalId(5L)).thenReturn(streamOf("{}"))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    assertThrows[IllegalArgumentException] {
      serDe.schema
    }
  }

  // ============== Avro deserialization ==============

  it should "deserialize an Avro message with wire_format=none" in {
    val topicInfo = TopicInfo(
      "test-topic", "kafka",
      Map(RegistryHostKey -> "localhost", WireFormatKey -> "none"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(10L, "AVRO"))
    when(mockClient.getContentByGlobalId(10L)).thenReturn(streamOf(avroSchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val avroCodec = AvroCodec.of(avroSchemaStr)
    val record = new org.apache.avro.generic.GenericData.Record(avroCodec.schema)
    record.put("field1", "hello")
    record.put("field2", 42)
    val bytes = avroCodec.encodeBinary(record)

    val mutation = serDe.fromBytes(bytes)
    assert(mutation.after != null)
    assert(mutation.after(0) == "hello")
    assert(mutation.after(1) == 42)
  }

  it should "strip the 9-byte Apicurio wire header (wire_format=apicurio)" in {
    val topicInfo = TopicInfo(
      "test-topic", "kafka",
      Map(RegistryHostKey -> "localhost", WireFormatKey -> "apicurio"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(11L, "AVRO"))
    when(mockClient.getContentByGlobalId(11L)).thenReturn(streamOf(avroSchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val avroCodec = AvroCodec.of(avroSchemaStr)
    val record = new org.apache.avro.generic.GenericData.Record(avroCodec.schema)
    record.put("field1", "world")
    record.put("field2", 99)
    val payload = avroCodec.encodeBinary(record)
    // Prepend: 1 magic byte + 8-byte global ID
    val wireBytes = Array[Byte](0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0B) ++ payload

    val mutation = serDe.fromBytes(wireBytes)
    assert(mutation.after != null)
    assert(mutation.after(0) == "world")
    assert(mutation.after(1) == 99)
  }

  it should "strip the 5-byte Confluent wire header (wire_format=confluent)" in {
    val topicInfo = TopicInfo(
      "test-topic", "kafka",
      Map(RegistryHostKey -> "localhost", WireFormatKey -> "confluent"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(12L, "AVRO"))
    when(mockClient.getContentByGlobalId(12L)).thenReturn(streamOf(avroSchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val avroCodec = AvroCodec.of(avroSchemaStr)
    val record = new org.apache.avro.generic.GenericData.Record(avroCodec.schema)
    record.put("field1", "confluent")
    record.put("field2", 7)
    val payload = avroCodec.encodeBinary(record)
    // Prepend: 1 magic byte + 4-byte schema ID
    val wireBytes = Array[Byte](0x00, 0x00, 0x00, 0x00, 0x0C) ++ payload

    val mutation = serDe.fromBytes(wireBytes)
    assert(mutation.after != null)
    assert(mutation.after(0) == "confluent")
    assert(mutation.after(1) == 7)
  }

  // ============== JSON Schema tests ==============

  it should "succeed with a JSON schema and derive correct field types" in {
    val jsonSchemaStr =
      """{
        |  "title": "login_event",
        |  "type": "object",
        |  "properties": {
        |    "user_id": { "type": "string" },
        |    "ts":      { "type": "integer" },
        |    "success": { "type": "boolean" }
        |  }
        |}""".stripMargin
    val topicInfo = TopicInfo(
      "test-topic-json", "kafka",
      Map(RegistryHostKey -> "localhost", WireFormatKey -> "none"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(40L, "JSON"))
    when(mockClient.getContentByGlobalId(40L)).thenReturn(streamOf(jsonSchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 3)
    assert(schema.fields.exists(_.name == "user_id"))
    assert(schema.fields.exists(_.name == "ts"))
    assert(schema.fields.exists(_.name == "success"))
  }

  it should "deserialize a JSON message" in {
    val jsonSchemaStr =
      """{
        |  "title": "login_event",
        |  "type": "object",
        |  "properties": {
        |    "user_id": { "type": "string" },
        |    "ts":      { "type": "integer" },
        |    "score":   { "type": "number" }
        |  }
        |}""".stripMargin
    val topicInfo = TopicInfo(
      "test-topic-json-deser", "kafka",
      Map(RegistryHostKey -> "localhost", WireFormatKey -> "none"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(41L, "JSON"))
    when(mockClient.getContentByGlobalId(41L)).thenReturn(streamOf(jsonSchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val message = """{"user_id": "abc123", "ts": 1700000000000, "score": 0.95}"""
    val mutation = serDe.fromBytes(message.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    assert(mutation.after != null)
    assert(mutation.after(0) == "abc123")
  }

  // ============== Proto3 tests ==============

  it should "succeed with a proto3 schema and derive correct field types" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestProto3 {
        |  string name = 1;
        |  int32 age = 2;
        |}""".stripMargin
    val topicInfo = TopicInfo("test-topic-proto3", "kafka", Map(RegistryHostKey -> "localhost"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(20L, "PROTOBUF"))
    when(mockClient.getContentByGlobalId(20L)).thenReturn(streamOf(proto3SchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "deserialize a proto3 message" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message User {
        |  string username = 1;
        |  int32 user_id = 2;
        |}""".stripMargin
    val topicInfo = TopicInfo(
      "test-topic-proto3-deser", "kafka",
      Map(RegistryHostKey -> "localhost", WireFormatKey -> "none"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(21L, "PROTOBUF"))
    when(mockClient.getContentByGlobalId(21L)).thenReturn(streamOf(proto3SchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val descriptor = new ProtobufSchema(proto3SchemaStr).toDescriptor()
    val message = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("username"), "alice")
      .setField(descriptor.findFieldByName("user_id"), 42)
      .build()

    val mutation = serDe.fromBytes(message.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "alice")
    assert(mutation.after(1) == 42)
  }

  it should "treat proto3 default values as null when proto3_default_as_null=true" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestDefaults {
        |  string text = 1;
        |  int32 number = 2;
        |}""".stripMargin
    val topicInfo = TopicInfo(
      "test-proto3-defaults", "kafka",
      Map(RegistryHostKey -> "localhost", WireFormatKey -> "none", Proto3DefaultAsNullKey -> "true"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(22L, "PROTOBUF"))
    when(mockClient.getContentByGlobalId(22L)).thenReturn(streamOf(proto3SchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val descriptor = new ProtobufSchema(proto3SchemaStr).toDescriptor()
    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutation = serDe.fromBytes(emptyMessage.toByteArray)
    assert(mutation.after(0) == null)
    assert(mutation.after(1) == null)
  }

  it should "return proto3 default values when proto3_default_as_null=false" in {
    val proto3SchemaStr =
      """syntax = "proto3";
        |message TestDefaults {
        |  string text = 1;
        |  int32 number = 2;
        |}""".stripMargin
    val topicInfo = TopicInfo(
      "test-proto3-defaults", "kafka",
      Map(RegistryHostKey -> "localhost", WireFormatKey -> "none", Proto3DefaultAsNullKey -> "false"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(23L, "PROTOBUF"))
    when(mockClient.getContentByGlobalId(23L)).thenReturn(streamOf(proto3SchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val descriptor = new ProtobufSchema(proto3SchemaStr).toDescriptor()
    val emptyMessage = DynamicMessage.newBuilder(descriptor).build()

    val mutation = serDe.fromBytes(emptyMessage.toByteArray)
    assert(mutation.after(0) == "")
    assert(mutation.after(1) == 0)
  }

  // ============== Proto2 tests ==============

  it should "succeed with a proto2 schema and derive correct field types" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message TestProto2 {
        |  required string name = 1;
        |  optional int32 age = 2;
        |}""".stripMargin
    val topicInfo = TopicInfo("test-topic-proto2", "kafka", Map(RegistryHostKey -> "localhost"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(30L, "PROTOBUF"))
    when(mockClient.getContentByGlobalId(30L)).thenReturn(streamOf(proto2SchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val schema = serDe.schema
    assert(schema != null)
    assert(schema.fields.length == 2)
    assert(schema.fields.exists(f => f.name == "name" && f.fieldType == StringType))
    assert(schema.fields.exists(f => f.name == "age" && f.fieldType == IntType))
  }

  it should "return null for unset proto2 optional fields" in {
    val proto2SchemaStr =
      """syntax = "proto2";
        |message Person {
        |  required string name = 1;
        |  optional int32 id = 2;
        |}""".stripMargin
    val topicInfo = TopicInfo(
      "test-topic-proto2-deser", "kafka",
      Map(RegistryHostKey -> "localhost", WireFormatKey -> "none"))
    val mockClient = Mockito.mock(classOf[RegistryClient])
    when(mockClient.getArtifactMetaData(anyString(), anyString())).thenReturn(makeMetadata(31L, "PROTOBUF"))
    when(mockClient.getContentByGlobalId(31L)).thenReturn(streamOf(proto2SchemaStr))

    val serDe = new MockApicurioSchemaSerDe(topicInfo, mockClient)
    val descriptor = new ProtobufSchema(proto2SchemaStr).toDescriptor()
    val messageWithOnlyRequired = DynamicMessage
      .newBuilder(descriptor)
      .setField(descriptor.findFieldByName("name"), "bob")
      .build()

    val mutation = serDe.fromBytes(messageWithOnlyRequired.toByteArray)
    assert(mutation.after != null)
    assert(mutation.after(0) == "bob")
    assert(mutation.after(1) == null)
  }
}
