#!/bin/bash

export ENV=staging
export GCP_PROJECT_ID=platform-preprod-eeadfea1
export GCP_BIGTABLE_INSTANCE_ID=chronon-preprod-sg

export BIGTABLE_INITIAL_RPC_TIMEOUT_DURATION=PT0.5S
export BIGTABLE_MAX_RPC_TIMEOUT_DURATION=PT10S
export BIGTABLE_TOTAL_TIMEOUT_DURATION=PT60S
export BIGTABLE_MAX_ATTEMPTS=3
export BIGTABLE_RPC_TIMEOUT_MULTIPLIER=2

FETCHER_JAR="out/service/assembly.dest/out.jar"
GCP_JAR="out/cloud_gcp/assembly.dest/out.jar"
JVM_OPTS="-XX:+UseZGC -XX:ZAllocationSpikeTolerance=2.0 -XX:+DisableExplicitGC -XX:+ParallelRefProcEnabled -XX:+AlwaysPreTouch -XX:+ExitOnOutOfMemoryError --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED"
SYS_OPTS="-Dserver.port=8080 -Donline.jar=$GCP_JAR -Dai.chronon.fetcher.batch_ir_cache_size_elements=500 -Donline.class=ai.chronon.integrations.cloud_gcp.GcpApiImpl -Dai.chronon.metrics.prefix=risk.mlp.chronon -Dai.chronon.metrics.enabled=false"

java $JVM_OPTS $SYS_OPTS -jar $FETCHER_JAR run ai.chronon.service.FetcherVerticle