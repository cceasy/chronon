# Building
We use the mill build system for this project. A couple of examples of command incantations:
- Clean the build artifacts: `./mill clean`
- Build the whole repo: `./mill __.compile`
- Build a module: `./mill cloud_gcp.compile`
- Run tests in a module: `./mill cloud_gcp.test`
- Run a particular test case inside a test class: `./mill spark.test.testOnly "ai.chronon.spark.kv_store.KVUploadNodeRunnerTest" -- -z "should handle GROUP_BY_UPLOAD_TO_KV successfully"`
- Run specific tests that match a pattern: `./mill spark.test.testOnly "ai.chronon.spark.analyzer.*"`
- List which modules / tasks are available: `./mill resolve _`
 
# Workflow
- Make sure to sanity check compilation works when you’re done making a series of code changes
- When done with compilation checks, make sure to run the related unit tests as well (either for the class or module)
- When applicable, suggest test additions / extensions to go with your code changes

## Project Overview

zipline-chronon is a fork of Airbnb's Chronon, a feature engineering platform that helps ML teams compute and serve features consistently across training and production environments. The platform solves the critical challenge of training-serving skew by ensuring features computed during model training exactly match what's served in production.

Key enhancements in this fork:
- Additional cloud connectors (BigQuery, Hudi, Glue, Iceberg, BigTable, DynamoDB, Pub/Sub)
- Performance optimizations including tiling architecture for <10ms feature serving
- Enhanced compiler support for GCP and AWS
- Support for temporally accurate label attribution

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
