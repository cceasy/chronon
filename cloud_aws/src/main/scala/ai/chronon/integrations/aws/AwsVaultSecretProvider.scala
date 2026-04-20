package ai.chronon.integrations.aws

import ai.chronon.api.secrets.VaultSecretProvider
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest

/** Fetches secrets from AWS Secrets Manager.
  * URI is the secret ARN: `arn:aws:secretsmanager:{region}:{account}:secret:{name}`
  */
class AwsVaultSecretProvider extends VaultSecretProvider {
  def getSecret(uri: String): String = {
    val client = SecretsManagerClient.create()
    try {
      val request = GetSecretValueRequest.builder().secretId(uri).build()
      val response = client.getSecretValue(request)
      Option(response.secretString())
        .orElse(Option(response.secretBinary()).map(_.asUtf8String()))
        .getOrElse(throw new IllegalStateException(s"No secret payload found for ARN: $uri"))
    } finally {
      client.close()
    }
  }
}
