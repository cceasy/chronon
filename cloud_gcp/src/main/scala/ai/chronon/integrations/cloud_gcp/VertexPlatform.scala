package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.ScalaJavaConversions.MapOps
import ai.chronon.online.{BatchPredictRequest, ModelPlatform, PredictRequest, PredictResponse}
import com.google.auth.oauth2.GoogleCredentials
import io.vertx.core.Vertx
import io.vertx.core.json.{JsonArray, JsonObject}
import io.vertx.ext.web.client.WebClient

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.jdk.CollectionConverters._

class VertexPlatform(project: String,
                     location: String,
                     webClient: Option[WebClient] = None,
                     credentials: Option[GoogleCredentials] = None)(implicit val ec: ExecutionContext)
    extends ModelPlatform {

  private lazy val client = webClient.getOrElse {
    val vertx = Vertx.vertx()
    WebClient.create(vertx)
  }

  // Init google creds - we need this to set the Auth header
  private lazy val googleCredentials: GoogleCredentials = credentials.getOrElse {
    GoogleCredentials
      .getApplicationDefault()
      .createScoped(List("https://www.googleapis.com/auth/aiplatform").asJava)
  }

  private def getAccessToken: String = {
    googleCredentials.refreshIfExpired()
    googleCredentials.getAccessToken.getTokenValue
  }

  val urlTemplates = Map(
    "publisher" -> s"https://$location-aiplatform.googleapis.com/v1/projects/$project/locations/$location/publishers/google/models/%s:predict",
    "custom" -> s"https://$location-aiplatform.googleapis.com/v1/projects/$project/locations/$location/endpoints/%s:predict"
  )

  override def predict(predictRequest: PredictRequest): Future[PredictResponse] = {
    val promise = Promise[PredictResponse]()

    try {
      val modelParams =
        Option(predictRequest.model.inferenceSpec.modelBackendParams)
          .map(_.asScala.toMap)
          .getOrElse(Map.empty)
      val modelType = modelParams.getOrElse("model_type", "publisher")

      // Handle missing model name by returning a Failure
      val modelName = modelParams.get("model_name") match {
        case Some(name) => name
        case None =>
          promise.success(
            PredictResponse(predictRequest,
                            Failure(new IllegalArgumentException("model_name is required in modelBackendParams"))))
          return promise.future
      }

      // Validate model type and get URL template
      val urlTemplate = urlTemplates.get(modelType) match {
        case Some(template) => template
        case None =>
          promise.success(
            PredictResponse(predictRequest,
                            Failure(new IllegalArgumentException(s"Unsupported model_type: $modelType"))))
          return promise.future
      }

      val url = urlTemplate.format(modelName)

      // Validate all inputs have 'instance' key
      val missingInstanceIndices = predictRequest.inputRequests.zipWithIndex.collect {
        case (inputRequest, index) if !inputRequest.contains("instance") => index
      }

      if (missingInstanceIndices.nonEmpty) {
        val errorMsg = s"Missing 'instance' key in input requests at indices: ${missingInstanceIndices.mkString(", ")}"
        promise.success(PredictResponse(predictRequest, Failure(new IllegalArgumentException(errorMsg))))
        return promise.future
      }

      val requestBody = createRequestBody(predictRequest.inputRequests, modelParams)

      client
        .postAbs(url)
        .putHeader("Content-Type", "application/json")
        .putHeader("Accept", "application/json")
        .putHeader("Authorization", s"Bearer $getAccessToken")
        .sendJsonObject(requestBody)
        .onComplete { asyncResult =>
          if (asyncResult.succeeded()) {
            val response = asyncResult.result()
            if (response.statusCode() == 200) {
              val responseBody = response.bodyAsJsonObject()
              val results = extractPredictionResults(responseBody)
              promise.success(PredictResponse(predictRequest, Success(results)))
            } else {
              val errorMsg = s"HTTP Request failed: ${response.statusCode()}: ${response.bodyAsString()}"
              promise.success(PredictResponse(predictRequest, Failure(new RuntimeException(errorMsg))))
            }
          } else {
            promise.success(
              PredictResponse(predictRequest,
                              Failure(new RuntimeException(s"HTTP Request failed", asyncResult.cause()))))
          }
        }
    } catch {
      case e: Exception =>
        promise.success(PredictResponse(predictRequest, Failure(e)))
    }

    promise.future
  }

  private def convertToVertxJson(obj: Any): Any = {
    obj match {
      case map: Map[_, _] =>
        val jsonObject = new JsonObject()
        map.foreach { case (key, value) =>
          jsonObject.put(key.toString, convertToVertxJson(value))
        }
        jsonObject
      case seq: Seq[_] =>
        val jsonArray = new JsonArray()
        seq.foreach { item =>
          jsonArray.add(convertToVertxJson(item))
        }
        jsonArray
      case other => other
    }
  }

  // Build the request body based on the Vertex API outlined (format is the same for both publisher and custom models)
  // We pass: { "instances": [ { ..req 1..}, { ... } ], "parameters": { ... } }
  // Publisher: https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings#googlegenaisdk_embeddings_docretrieval_with_txt-drest
  // Custom: https://docs.cloud.google.com/vertex-ai/docs/predictions/get-online-predictions#online_predict_custom_trained-drest
  private[cloud_gcp] def createRequestBody(inputRequests: Seq[Map[String, AnyRef]],
                                           modelParams: Map[String, String]): JsonObject = {
    val instancesArray = new JsonArray()

    inputRequests.foreach { inputRequest =>
      val instance = inputRequest("instance")
      val jsonInstance = convertToVertxJson(instance)
      instancesArray.add(jsonInstance)
    }

    val requestBody = new JsonObject()
    requestBody.put("instances", instancesArray)

    // Add parameters if present (exclude model_name and model_type)
    val additionalParams = modelParams.filterKeys(k => k != "model_name" && k != "model_type")
    if (additionalParams.nonEmpty) {
      val parametersObj = new JsonObject()
      additionalParams.foreach { case (key, value) =>
        parametersObj.put(key, value)
      }
      requestBody.put("parameters", parametersObj)
    }

    requestBody
  }

  // Response is a JsonObject with "predictions": [ {...}, {...} ]
  // We take each prediction object and convert it to a Map[String, AnyRef] and pass up
  private[cloud_gcp] def extractPredictionResults(responseBody: JsonObject): Seq[Map[String, AnyRef]] = {
    val predictions = responseBody.getJsonArray("predictions")

    if (predictions == null) {
      throw new RuntimeException("No 'predictions' array found in response")
    }

    (0 until predictions.size()).map { index =>
      val predictionJsonObject = predictions.getJsonObject(index)
      predictionJsonObject.getMap.asScala.toMap
    }
  }

  override def batchPredict(batchPredictRequest: BatchPredictRequest): Future[String] = ???
}

object VertexPlatform {
  import ai.chronon.api.{Builders => B, _}
  import java.util

  // Simple test app that hits the VertexPlatform's gemini-embedding-001 model
  def main(args: Array[String]): Unit = {
    import scala.concurrent.duration._

    val platform = new VertexPlatform("canary-443022", "us-central1")(ExecutionContext.global)
    val geminiModel = B.Model(
      metaData = B.MetaData(name = "gemini_model"),
      inferenceSpec = B.InferenceSpec(
        modelBackend = ModelBackend.VertexAI,
        modelBackendParams = Map("model_name" -> "gemini-embedding-001", "model_type" -> "publisher")
      )
    )

    val contentMap1 = util.Map.of("content", "Hello, world!")
    val contentMap2 = util.Map.of("content", "Another example for Vertex AI.")
    val inputRequests = Seq(
      Map("instance" -> contentMap1),
      Map("instance" -> contentMap2)
    )

    val predictRequest = PredictRequest(geminiModel, inputRequests)
    println("Making prediction request to Vertex AI...")
    val predictionFuture = platform.predict(predictRequest)

    val response = Await.result(predictionFuture, 10.seconds)
    response.outputs match {
      case Success(results) =>
        println("✅ Predictions successful:")
        results.zipWithIndex.foreach { case (result, index) =>
          println(s"  Input $index: $result")
        }
      case Failure(exception) =>
        println(s"❌ Prediction failed: ${exception.getMessage}")
        exception.printStackTrace()
    }

    println("VertexPlatform example completed.")
  }
}
