package ai.chronon.api.secrets

/** Cloud-agnostic interface for fetching a secret value from a vault URI.
  * Implementations live in the cloud-specific modules (cloud_azure, cloud_gcp, cloud_aws)
  * and are loaded dynamically at runtime by SecretResolver based on the URI domain.
  */
trait VaultSecretProvider {
  def getSecret(uri: String): String
}

object VaultSecretProvider {
  // Fully-qualified class names loaded dynamically — the running JVM's classpath
  // must include the matching cloud integration jar.
  private val AzureClass = "ai.chronon.integrations.cloud_azure.AzureVaultSecretProvider"
  private val GcpClass = "ai.chronon.integrations.cloud_gcp.GcpVaultSecretProvider"
  private val AwsClass = "ai.chronon.integrations.aws.AwsVaultSecretProvider"

  /** Selects and instantiates the correct provider based on the URI scheme/domain. */
  def forUri(uri: String): VaultSecretProvider =
    Class
      .forName(classNameForUri(uri))
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[VaultSecretProvider]

  /** Returns the provider class name for a given URI — exposed for testing. */
  def classNameForUri(uri: String): String =
    if (uri.contains("vault.azure.net"))
      AzureClass
    else if (uri.startsWith("projects/") || uri.contains("secretmanager.googleapis.com"))
      GcpClass
    else if (uri.startsWith("arn:aws:secretsmanager"))
      AwsClass
    else
      throw new IllegalArgumentException(
        s"Unrecognized vault URI format: $uri. " +
          "Expected Azure (*.vault.azure.net), GCP (projects/*/secrets/*), or AWS (arn:aws:secretsmanager:*) URI.")
}
