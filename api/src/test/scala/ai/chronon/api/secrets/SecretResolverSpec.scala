package ai.chronon.api.secrets

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SecretResolverSpec extends AnyFlatSpec with Matchers {

  private def stubProvider(value: String): VaultSecretProvider = (_: String) => value

  private def failingProvider(msg: String): VaultSecretProvider = (_: String) =>
    throw new RuntimeException(msg)

  "SecretResolver.resolveVaultUris" should "return env unchanged when no _VAULT_URI keys are present" in {
    val env = Map("FOO" -> "bar", "ARTIFACT_PREFIX" -> "s3://bucket")
    SecretResolver.resolveVaultUris(env, _ => stubProvider("unused")) shouldBe env
  }

  it should "resolve a single _VAULT_URI key to its stripped name" in {
    val env = Map(
      "TESTING_VAULT_URI" -> "https://my-vault.vault.azure.net/secrets/my-secret",
      "OTHER" -> "value"
    )
    val result = SecretResolver.resolveVaultUris(env, _ => stubProvider("resolved-secret"))
    result should contain("TESTING" -> "resolved-secret")
    result should contain("OTHER" -> "value")
    result should not contain key("TESTING_VAULT_URI")
  }

  it should "resolve multiple _VAULT_URI keys" in {
    val secrets = Map(
      "https://vault.vault.azure.net/secrets/a" -> "secret-a",
      "https://vault.vault.azure.net/secrets/b" -> "secret-b"
    )
    val env = Map(
      "FOO_VAULT_URI"  -> "https://vault.vault.azure.net/secrets/a",
      "BAR_VAULT_URI"  -> "https://vault.vault.azure.net/secrets/b",
      "PASS_THROUGH"   -> "unchanged"
    )
    val result = SecretResolver.resolveVaultUris(env, uri => stubProvider(secrets(uri)))
    result should contain("FOO" -> "secret-a")
    result should contain("BAR" -> "secret-b")
    result should contain("PASS_THROUGH" -> "unchanged")
    result should not contain key("FOO_VAULT_URI")
    result should not contain key("BAR_VAULT_URI")
  }

  it should "skip resolution when the target key is already defined in env" in {
    val env = Map(
      "TESTING_VAULT_URI" -> "https://my-vault.vault.azure.net/secrets/my-secret",
      "TESTING" -> "already-set"
    )
    val result = SecretResolver.resolveVaultUris(env, _ => failingProvider("should not be called"))
    result("TESTING") shouldBe "already-set"
    result should not contain key("TESTING_VAULT_URI")
  }

  it should "warn and skip rather than crash when a secret cannot be fetched" in {
    val env = Map(
      "GOOD_VAULT_URI" -> "https://vault.vault.azure.net/secrets/good",
      "BAD_VAULT_URI"  -> "https://vault.vault.azure.net/secrets/bad"
    )
    val provider: String => VaultSecretProvider = uri =>
      if (uri.contains("good")) stubProvider("good-secret") else failingProvider("vault unavailable")

    val result = SecretResolver.resolveVaultUris(env, provider)
    result should contain("GOOD" -> "good-secret")
    result should not contain key("BAD")
    result should not contain key("BAD_VAULT_URI")
  }

  it should "pass the full URI to the provider" in {
    var capturedUri = ""
    val capturingProvider: String => VaultSecretProvider = uri => {
      capturedUri = uri
      stubProvider("value")
    }
    val uri = "https://my-vault.vault.azure.net/secrets/my-secret"
    SecretResolver.resolveVaultUris(Map("KEY_VAULT_URI" -> uri), capturingProvider)
    capturedUri shouldBe uri
  }
}
