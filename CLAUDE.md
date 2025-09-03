# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

zipline-chronon is a fork of Airbnb's Chronon, a feature engineering platform that helps ML teams compute and serve features consistently across training and production environments. The platform solves the critical challenge of training-serving skew by ensuring features computed during model training exactly match what's served in production.

Key enhancements in this fork:
- Additional cloud connectors (BigQuery, Hudi, Glue, Iceberg, BigTable, DynamoDB, Pub/Sub)
- Performance optimizations including tiling architecture for <10ms feature serving
- Enhanced compiler support for GCP and AWS
- Support for temporally accurate label attribution

## Development Commands

### Building JARs
```bash
# Build all JARs
bazel build //spark:spark_assembly_deploy.jar
bazel build //flink:flink_assembly_deploy.jar
bazel build //service:service_assembly_deploy.jar
bazel build //cloud_gcp:cloud_gcp_lib_deploy.jar

# Build and upload JARs to GCS
./build_and_upload_jars.sh <version> <bucket>
```

### Running Tests
```bash
# Run all tests in a module
bazel test //aggregator/...
bazel test //api/...
bazel test //spark/...

# Run a specific test
bazel test //aggregator/src/test/scala/ai/zipline/aggregator:specific_test

# Python tests
cd api/python
pytest test/
pytest test/test_specific.py -v
```

### Python Development
```bash
cd api/python
pip install -r requirements/dev.txt  # Install development dependencies
ruff check .                        # Run linter
ruff format .                       # Format code
```

### Compiling Thrift
```bash
./compile_thrift.sh
```

## Architecture

### Core Concepts
- **Source**: Universal data adapter connecting to various data sources (batch tables, streaming topics). Provides unified interface for different data formats
- **GroupBy**: Aggregation engine that transforms raw events into ML features. Runs on both Spark (batch) and Flink (streaming) with operations like SUM, COUNT, AVG over time windows
- **Join**: Temporal feature orchestrator that assembles point-in-time correct feature vectors. Unlike traditional joins, specifically designed for ML feature assembly with historical accuracy
- **StagingQuery**: Arbitrary Spark SQL computations for data preprocessing

### Module Structure
- `api/`: Core API definitions (Python client, Thrift IDL, Scala API)
- `aggregator/`: Core aggregation logic and windowing
- `spark/`: Batch processing jobs for historical feature computation
- `flink/`: Streaming jobs for real-time feature updates
- `online/`: Online feature serving components (KV stores)
- `service/`: HTTP service for feature serving
- `cloud_aws/`: AWS integrations (DynamoDB, EMR, S3)
- `cloud_gcp/`: GCP integrations (BigQuery, BigTable, Dataproc)

### Data Flow
1. **Offline Path**: Spark jobs process historical data for model training
2. **Online Path**: Flink jobs process real-time events for serving
3. **Serving Layer**: Low-latency KV stores (BigTable, DynamoDB) for feature retrieval
4. **Consistency**: Ensures online/offline feature parity through shared computation logic

### Key Features
- **Lambda Architecture**: Dual-mode execution running features in both batch (Spark) and real-time (Flink), ensuring consistency between training and serving
- **Tiling Architecture**: Pre-aggregation system storing intermediate results at multiple granularities, reducing serving latency to <10ms
- **Derived Features**: On-the-fly calculations using SQL expressions, computed at serving time
- **Chained Features**: Multi-stage pipelines where outputs cascade through transformations
- **Schema Evolution**: Safe handling of changing schemas through versioning and compatibility checks

### System Components
- **Fetcher Service**: High-performance feature serving built on Vert.x, retrieves features in <10ms using caching and parallel I/O
- **Compiler & Validation**: Converts Python definitions to optimized Spark/Flink jobs, ensures online/offline consistency
- **Serialization**: Uses Thrift for configuration/KV store data, Avro for streaming

### Important Limitations
- Derived features NOT computed during Flink streaming (only at serving)
- Chained features require Spark for streaming (Flink lacks dynamic lookups)
- Tiling requires careful configuration based on data characteristics

### Build System
The project uses Bazel as the primary build system with Scala 2.12.18. Key configuration files:
- `WORKSPACE`: Bazel workspace configuration
- `BUILD.bazel`: Build targets in each module
- `scala_config.bzl`: Scala version configuration