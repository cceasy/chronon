package ai.chronon.online

import ai.chronon.online.KVStore.PutRequest
import sttp.client3._
import sttp.model.StatusCode

import java.util.Base64
import scala.concurrent.Future

// Hacky test kv store that we use to send objects to the in-memory KV store that lives in a different JVM (e.g spark -> hub)
class HTTPKVStore(host: String = "localhost", port: Int = 9000) extends KVStore with Serializable {

  val backend: SttpBackend[Identity, Any] = HttpClientSyncBackend()
  val baseUrl: String = s"http://$host:$port/api/v1/dataset"

  override def multiGet(requests: scala.Seq[KVStore.GetRequest]): Future[scala.Seq[KVStore.GetResponse]] = ???

  override def multiPut(putRequests: scala.Seq[KVStore.PutRequest]): Future[scala.Seq[Boolean]] = {
    if (putRequests.isEmpty) {
      Future.successful(Seq.empty)
    } else {
      Future {
        basicRequest
          .post(uri"$baseUrl/data")
          .header("Content-Type", "application/json")
          .body(jsonList(putRequests))
          .send(backend)
      }.map { response =>
        response.code match {
          case StatusCode.Ok => Seq(true)
          case _ =>
            logger.error(s"HTTP multiPut failed with status ${response.code}: ${response.body}")
            Seq(false)
        }
      }
    }
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = ???

  override def create(dataset: String): Unit = {
    logger.warn(s"Skipping creation of $dataset in HTTP kv store implementation")
  }

  // wire up json conversion manually to side step serialization issues in spark executors
  def jsonString(request: PutRequest): String = {
    val keyBase64 = Base64.getEncoder.encodeToString(request.keyBytes)
    val valueBase64 = Base64.getEncoder.encodeToString(request.valueBytes)
    s"""{ "keyBytes": "${keyBase64}", "valueBytes": "${valueBase64}", "dataset": "${request.dataset}", "tsMillis": ${request.tsMillis.orNull}}""".stripMargin
  }

  def jsonList(requests: Seq[PutRequest]): String = {
    val requestsJson = requests.map(jsonString(_)).mkString(", ")

    s"[ $requestsJson ]"
  }
}
