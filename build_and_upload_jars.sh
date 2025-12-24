#!/usr/bin/env bash

set -ex

JDK_MAJOR_VERSION="$(java -version 2>&1 | sed -nE '1s/.*\"([0-9]+)\..*/\1/p')"
if [ "$JDK_MAJOR_VERSION" != "11" ]; then
  echo "❌ Java 11 is required. Current version: $JDK_MAJOR_VERSION"
  exit 1
else
  echo "✅ Java 11 detected."
fi

OUT_DIR="out"
# shellcheck disable=SC2269
CHRONON_VERSION="${CHRONON_VERSION}"
BUCKET="mlp-chronon-nonprod-sg"
COPY_BUCKETS=("mlp-chronon-preprod-sg")
mill_assembly=true

while [[ $# -gt 0 ]]; do
  case $1 in
    --version)
      CHRONON_VERSION="$2"
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
    --assembly)
      mill_assembly="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

echo "Building jars for CHRONON_VERSION ${CHRONON_VERSION} and uploading to ${BUCKET}"

if [ -z "$CHRONON_VERSION" ]; then
  echo "❌ CHRONON_VERSION env is not set."
  exit 1
fi

if [ "$mill_assembly" == "true" ]; then
  echo "Building assembly using Mill..."
  ./mill __.assembly
else
  echo "Skipping assembly build as per configuration."
fi

gcloud storage cp ${OUT_DIR}/cloud_gcp/assembly.dest/out.jar gs://${BUCKET}/release/${CHRONON_VERSION}/jars/cloud_gcp_lib_deploy.jar
gcloud storage cp ${OUT_DIR}/spark/assembly.dest/out.jar gs://${BUCKET}/release/${CHRONON_VERSION}/jars/spark_assembly_deploy.jar
gcloud storage cp ${OUT_DIR}/flink/assembly.dest/out.jar gs://${BUCKET}/release/${CHRONON_VERSION}/jars/flink_assembly_deploy.jar
gcloud storage cp ${OUT_DIR}/service/assembly.dest/out.jar gs://${BUCKET}/release/${CHRONON_VERSION}/jars/service_assembly_deploy.jar

# Copy to other buckets
for TARGET_BUCKET in "${COPY_BUCKETS[@]}"; do
  gcloud storage cp "gs://${BUCKET}/release/${CHRONON_VERSION}/jars/*.jar" gs://${TARGET_BUCKET}/release/${CHRONON_VERSION}/jars/
done

gcloud storage ls -l --readable-sizes gs://${BUCKET}/release/${CHRONON_VERSION}/jars
