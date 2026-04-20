package ai.chronon.api.secrets

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VaultSecretProviderSpec extends AnyFlatSpec with Matchers {

  "VaultSecretProvider.classNameForUri" should "route Azure Key Vault URIs to AzureVaultSecretProvider" in {
    val uri = "https://my-vault.vault.azure.net/secrets/my-secret"
    VaultSecretProvider.classNameForUri(uri) shouldBe "ai.chronon.integrations.cloud_azure.AzureVaultSecretProvider"
  }

  it should "route GCP Secret Manager resource names to GcpVaultSecretProvider" in {
    VaultSecretProvider.classNameForUri("projects/my-project/secrets/my-secret") shouldBe
      "ai.chronon.integrations.cloud_gcp.GcpVaultSecretProvider"
    VaultSecretProvider.classNameForUri("projects/my-project/secrets/my-secret/versions/1") shouldBe
      "ai.chronon.integrations.cloud_gcp.GcpVaultSecretProvider"
    VaultSecretProvider.classNameForUri("https://secretmanager.googleapis.com/v1/projects/p/secrets/s/versions/latest") shouldBe
      "ai.chronon.integrations.cloud_gcp.GcpVaultSecretProvider"
  }

  it should "route AWS Secrets Manager ARNs to AwsVaultSecretProvider" in {
    val arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret-AbCdEf"
    VaultSecretProvider.classNameForUri(arn) shouldBe "ai.chronon.integrations.aws.AwsVaultSecretProvider"
  }

  it should "throw IllegalArgumentException for unrecognized URI formats" in {
    val ex = intercept[IllegalArgumentException] {
      VaultSecretProvider.classNameForUri("https://unknown-vault.example.com/secret")
    }
    ex.getMessage should include("Unrecognized vault URI format")
  }
}
