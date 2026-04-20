# Custom Flink Image

The stock `flink:1.20.3` image lacks cloud filesystem plugins. This image adds:

- **S3**: `flink-s3-fs-hadoop` (official, from Maven Central)
- **GCS**: `flink-gs-fs-hadoop` (official, from Maven Central)
- **Azure**: Custom build of `flink-azure-fs-hadoop` with Hadoop 3.4.1 (built in a multi-stage Docker build)

## Why custom Azure plugin?

The official `flink-azure-fs-hadoop` (Flink 1.20.3) bundles Hadoop 3.3.4, which
lacks `WorkloadIdentityTokenProvider` ([HADOOP-18610](https://issues.apache.org/jira/browse/HADOOP-18610),
added in 3.4.1). This class is required for AKS workload identity with ABFS checkpointing.

[FLINK-36694](https://issues.apache.org/jira/browse/FLINK-36694) (upgrade Hadoop shaded to 3.4.x)
is still open as of Flink 1.20.3. The Dockerfile uses a multi-stage build to compile the plugin
from the Flink 1.20.3 source tree with `-Dfs.hadoopshaded.version=3.4.1`, so no pre-built JAR
needs to be committed to the repo.
