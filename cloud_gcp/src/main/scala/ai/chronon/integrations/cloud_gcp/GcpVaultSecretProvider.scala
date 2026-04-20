package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.secrets.VaultSecretProvider
import com.google.cloud.secretmanager.v1.{AccessSecretVersionRequest, SecretManagerServiceClient}

/** Fetches secrets from GCP Secret Manager.
  * URI format: `projects/{project}/secrets/{secret}` or
  *             `projects/{project}/secrets/{secret}/versions/{version}`
  * When no version is specified, defaults to "latest".
  */
class GcpVaultSecretProvider extends VaultSecretProvider {
  def getSecret(uri: String): String = {
    val resourceName = if (uri.contains("/versions/")) uri else s"$uri/versions/latest"
    val client = SecretManagerServiceClient.create()
    try {
      val request = AccessSecretVersionRequest.newBuilder().setName(resourceName).build()
      client.accessSecretVersion(request).getPayload.getData.toStringUtf8
    } finally {
      client.close()
    }
  }
}
