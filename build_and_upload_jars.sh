#!/usr/bin/env bash

VERSION=${1:-0.2.0}
BUCKET=${2:-awx-metric-platform-nonprod-jars}
BAZEL_BIN_DIR=bazel-bin
echo "Building jars for version ${VERSION} and uploading to ${BUCKET}"

rm -f ${BAZEL_BIN_DIR}/cloud_gcp/cloud_gcp_lib_deploy.jar || true
rm -f ${BAZEL_BIN_DIR}/spark/spark_assembly_deploy.jar || true
rm -f ${BAZEL_BIN_DIR}/flink/flink_assembly_deploy.jar || true
rm -f ${BAZEL_BIN_DIR}/service/service_assembly_deploy.jar || true

bazel build //cloud_gcp:cloud_gcp_lib_deploy.jar
bazel build //spark:spark_assembly_deploy.jar
bazel build //flink:flink_assembly_deploy.jar
bazel build //service:service_assembly_deploy.jar

gsutil cp ${BAZEL_BIN_DIR}/cloud_gcp/cloud_gcp_lib_deploy.jar gs://${BUCKET}/release/${VERSION}/jars/
gsutil cp ${BAZEL_BIN_DIR}/spark/spark_assembly_deploy.jar gs://${BUCKET}/release/${VERSION}/jars/
gsutil cp ${BAZEL_BIN_DIR}/flink/flink_assembly_deploy.jar gs://${BUCKET}/release/${VERSION}/jars/
gsutil cp ${BAZEL_BIN_DIR}/service/service_assembly_deploy.jar gs://${BUCKET}/release/${VERSION}/jars/

gsutil ls -lh gs://${BUCKET}/release/${VERSION}/jars