package ai.chronon.integrations.cloud_azure

import ai.chronon.api.secrets.VaultSecretProvider

/** Fetches secrets from Azure Key Vault using the full secret URI. */
class AzureVaultSecretProvider extends VaultSecretProvider {
  def getSecret(uri: String): String = {
    val (vaultUrl, secretName) = AzureKeyVaultHelper.parseSecretUri(uri)
    AzureKeyVaultHelper.getSecret(vaultUrl, secretName)
  }
}
