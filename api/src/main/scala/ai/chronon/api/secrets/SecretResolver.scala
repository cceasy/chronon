package ai.chronon.api.secrets

import org.slf4j.LoggerFactory

object SecretResolver {
  private val logger = LoggerFactory.getLogger(getClass)
  private val Suffix = "_VAULT_URI"

  /** Scans env for keys ending in `_VAULT_URI`, fetches each secret via the
    * appropriate cloud provider (routed by URI domain), and returns a new map
    * where each `${NAME}_VAULT_URI` key is replaced by `${NAME}` → secret value.
    *
    * Two safety rules applied per entry:
    *  - Skip resolution if the target key already exists in env (lets a downstream
    *    level override by simply setting the resolved name directly).
    *  - On any fetch failure, log a warning and leave the entry unresolved rather
    *    than crashing — the next access layer (e.g. driver-side resolution) can retry.
    */
  def resolveVaultUris(env: Map[String, String]): Map[String, String] =
    resolveVaultUris(env, VaultSecretProvider.forUri)

  /** Overload that accepts a provider factory — useful for injecting test stubs. */
  def resolveVaultUris(
      env: Map[String, String],
      providerFor: String => VaultSecretProvider
  ): Map[String, String] = {
    val (vaultEntries, rest) = env.partition { case (k, _) => k.endsWith(Suffix) }
    if (vaultEntries.isEmpty) return env

    logger.info(s"Found ${vaultEntries.size} vault URI(s) to resolve: ${vaultEntries.keys.mkString(", ")}")
    val resolved = vaultEntries.flatMap { case (key, uri) =>
      val targetKey = key.stripSuffix(Suffix)

      if (rest.contains(targetKey)) {
        logger.info(s"Skipping vault resolution for '$key': '$targetKey' is already set in env")
        None
      } else {
        try {
          logger.info(s"Resolving secret for env var '$targetKey' from URI: $uri")
          val secret = providerFor(uri).getSecret(uri)
          logger.info(s"Successfully resolved secret for '$targetKey'")
          Some(targetKey -> secret)
        } catch {
          case e: Exception =>
            logger.warn(s"Failed to resolve secret for '$targetKey' from URI '$uri': ${e.getMessage}. Skipping.")
            None
        }
      }
    }
    if (resolved.nonEmpty)
      logger.info(s"Secret resolution complete. Added env vars: ${resolved.keys.mkString(", ")}")
    rest ++ resolved
  }
}
