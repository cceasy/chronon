package ai.chronon.flink.deser

import ai.chronon.api.StructType
import ai.chronon.online.TopicInfo
import ai.chronon.online.serde.{AvroCodec, AvroSerDe, JsonSchemaSerDe, Mutation, ProtobufSerDe, SerDe}
import io.apicurio.registry.rest.client.{RegistryClient, RegistryClientFactory}
import io.apicurio.registry.rest.client.exception.ArtifactNotFoundException
import io.apicurio.registry.types.ArtifactType
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema

import scala.io.{Codec, Source}

/** SerDe that fetches schemas from an Apicurio Schema Registry and auto-detects the format (Avro, Protobuf, or JSON).
  *
  * Can be configured as:
  *   topic = "kafka://topic-name/serde=apicurio_registry/registry_host=host/[registry_port=port]/[registry_scheme=http]/[group_id=default]/[artifact_id=topic-name]/[wire_format=apicurio]/[proto3_default_as_null=false]"
  *
  * Port, scheme, group_id, artifact_id, wire_format and proto3_default_as_null are optional.
  * Scheme defaults to http. Group ID defaults to "default" (Apicurio's default group).
  * Artifact ID defaults to the topic name. Wire format defaults to "apicurio".
  *
  * Wire format options:
  *   - "apicurio": strip 1 magic byte + 8-byte global ID prefix (Apicurio default)
  *   - "confluent": strip 1 magic byte + 4-byte schema ID prefix (Confluent-compatible mode)
  *   - "none": pass bytes through as-is (also appropriate for JSON, which has no wire header)
  */
class ApicurioSchemaSerDe(topicInfo: TopicInfo) extends SerDe {
  import ApicurioSchemaSerDe._

  private val registryHost: String =
    topicInfo.params.getOrElse(RegistryHostKey, throw new IllegalArgumentException(s"$RegistryHostKey not set"))

  private val registryPort: Option[String] = topicInfo.params.get(RegistryPortKey)

  private val registryScheme: String = topicInfo.params.getOrElse(RegistrySchemeKey, "http")

  private val groupId: String = topicInfo.params.getOrElse(GroupIdKey, "default")

  private val artifactId: String = topicInfo.params.getOrElse(ArtifactIdKey, topicInfo.name)

  private val wireFormat: String = topicInfo.params.getOrElse(WireFormatKey, "apicurio")

  private val proto3DefaultAsNull: Boolean =
    topicInfo.params.getOrElse(Proto3DefaultAsNullKey, "false").toBoolean

  protected[flink] def buildRegistryClient(registryUrl: String): RegistryClient =
    RegistryClientFactory.create(registryUrl)

  private lazy val delegate: SerDe = buildSerDe()

  private def buildSerDe(): SerDe = {
    val registryUrl = registryPort match {
      case Some(port) => s"$registryScheme://$registryHost:$port/apis/registry/v2"
      case None       => s"$registryScheme://$registryHost/apis/registry/v2"
    }

    val client = buildRegistryClient(registryUrl)
    try {
      val metadata =
        try {
          client.getArtifactMetaData(groupId, artifactId)
        } catch {
          case e: ArtifactNotFoundException =>
            throw new IllegalArgumentException(
              s"Artifact not found in Apicurio registry - group: $groupId, artifact: $artifactId",
              e)
          case e: Exception =>
            throw new IllegalArgumentException(
              s"Error retrieving artifact metadata from Apicurio registry - group: $groupId, artifact: $artifactId",
              e)
        }

      val schemaContent =
        scala.util
          .Using(client.getContentByGlobalId(metadata.getGlobalId)) { stream =>
            Source.fromInputStream(stream)(Codec.UTF8).mkString
          }
          .recover { case e: Exception =>
            throw new IllegalArgumentException(
              s"Error retrieving schema content from Apicurio registry - globalId: ${metadata.getGlobalId}",
              e)
          }
          .get

      metadata.getType match {
        case ArtifactType.AVRO =>
          val avroSchema = AvroCodec.of(schemaContent).schema
          new AvroSerDe(avroSchema)
        case ArtifactType.PROTOBUF =>
          val protobufSchema = new ProtobufSchema(schemaContent)
          val descriptor = protobufSchema.toDescriptor()
          new ProtobufSerDe(descriptor, proto3DefaultAsNull)
        case ArtifactType.JSON =>
          new JsonSchemaSerDe(schemaContent, artifactId)
        case other =>
          throw new IllegalArgumentException(
            s"Unsupported schema type: $other. Supported types are AVRO, PROTOBUF, and JSON.")
      }
    } finally {
      client.close()
    }
  }

  override def schema: StructType = delegate.schema

  override def fromBytes(bytes: Array[Byte]): Mutation = {
    val payload = wireFormat match {
      case "apicurio"  => bytes.drop(9) // 1 magic byte + 8-byte global ID
      case "confluent" => bytes.drop(5) // 1 magic byte + 4-byte schema ID
      case _           => bytes // "none" or unrecognised value → pass through
    }
    delegate.fromBytes(payload)
  }
}

object ApicurioSchemaSerDe {
  val RegistryHostKey = "registry_host"
  val RegistryPortKey = "registry_port"
  val RegistrySchemeKey = "registry_scheme"
  val GroupIdKey = "group_id"
  val ArtifactIdKey = "artifact_id"
  val WireFormatKey = "wire_format"
  val Proto3DefaultAsNullKey = "proto3_default_as_null"
}
