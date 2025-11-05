#!/usr/bin/env bash

set -ex

JDK_MAJOR_VERSION="$(java -version 2>&1 | sed -nE '1s/.*\"([0-9]+)\..*/\1/p')"
if [ "$JDK_MAJOR_VERSION" != "11" ]; then
  echo "❌ Java 11 is required. Current version: $JDK_MAJOR_VERSION"
  exit 1
else
  echo "✅ Java 11 detected."
fi

ZIPLINE_VERSION="0.3.0"
BUCKET="awx-metric-platform-nonprod-jars"
OUT_DIR="out"

while [[ $# -gt 0 ]]; do
  case $1 in
    --version)
      ZIPLINE_VERSION="$2"
      shift 2
      ;;
    --bucket)
      BUCKET="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

echo "Building jars for ZIPLINE_VERSION ${ZIPLINE_VERSION} and uploading to ${BUCKET}"

./mill __.assembly

gsutil cp ${OUT_DIR}/cloud_gcp/assembly.dest/out.jar gs://${BUCKET}/release/${ZIPLINE_VERSION}/jars/cloud_gcp_lib_deploy.jar
gsutil cp ${OUT_DIR}/spark/assembly.dest/out.jar gs://${BUCKET}/release/${ZIPLINE_VERSION}/jars/spark_assembly_deploy.jar
gsutil cp ${OUT_DIR}/flink/assembly.dest/out.jar gs://${BUCKET}/release/${ZIPLINE_VERSION}/jars/flink_assembly_deploy.jar
gsutil cp ${OUT_DIR}/service/assembly.dest/out.jar gs://${BUCKET}/release/${ZIPLINE_VERSION}/jars/service_assembly_deploy.jar

gsutil cp "gs://${BUCKET}/release/${ZIPLINE_VERSION}/jars/*.jar" gs://awx-ml-platform-nonprod/chronon-release/${ZIPLINE_VERSION}/jars/

gsutil ls -lh gs://${BUCKET}/release/${ZIPLINE_VERSION}/jars