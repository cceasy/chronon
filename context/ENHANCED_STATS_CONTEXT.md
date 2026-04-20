# Enhanced Statistics System - Development Context

This document provides comprehensive context for continuing development of the Enhanced Statistics system in Chronon. Use this as a prompt/reference when picking up this work.

## Quick Start Context

I'm working on the Enhanced Statistics feature in the Chronon codebase. This system computes cardinality-aware statistics on join outputs, stores them as mergeable intermediate representations (IRs) in a KV Store (BigTable on GCP), and provides a REST API to query aggregated statistics across arbitrary time ranges.

## What Was Built

### Core Components (3 Modules)

1. **Spark Module** - Statistics computation and storage
   - `EnhancedStatsCompute`: Detects cardinality and generates appropriate metrics
   - `EnhancedStatsStore`: Uploads stats to KV Store
   - Location: `spark/src/main/scala/ai/chronon/spark/stats/`

2. **Service Module** - REST API for querying statistics
   - `JavaStatsService`: Java-friendly wrapper for fetching and merging stats tiles
   - `StatsHandler`: Vert.x HTTP handler for `/v1/stats/:tableName` endpoint
   - Locations:
     - `online/src/main/scala/ai/chronon/online/JavaStatsService.scala`
     - `service/src/main/java/ai/chronon/service/handlers/StatsHandler.java`

3. **API Module** - Workflow planner integration
   - `JoinStatsComputeNode`: Thrift node for stats computation and KV upload (combined operation)
   - `MonolithJoinPlanner`: Integrated stats node into join workflow
   - Locations:
     - `thrift/planner.thrift`
     - `api/src/main/scala/ai/chronon/api/planner/MonolithJoinPlanner.scala`

## Visual Overview

### Orchestration Flow

The complete orchestration with stats nodes integrated into the workflow:

![Orchestration Snapshot](images/OrchestrationSnapshot.png)

### KV Store Data Sample

Example of statistics data stored in the KV Store:

![KV Store Sample](images/EnhancedStatsKVSample.png)

## Key Architecture Concepts

### IR Normalization (CRITICAL CONCEPT)

In Chronon, "normalize" does NOT mean "convert to final statistics". Instead:

```
normalize: IR objects → serializable form (CpcSketch → bytes)
denormalize: serializable form → IR objects (bytes → CpcSketch)
finalize: IR → final statistics (CpcSketch → Long count)
```

**Flow:**
```
Compute IR → normalize → encode to Avro → store in KV
Fetch from KV → decode Avro → denormalize → merge → finalize
```

### Cardinality-Aware Metrics

The system automatically chooses appropriate statistics based on column cardinality:

- **Numeric columns**: Always get full treatment (min, max, avg, variance, percentiles, sketches)
- **Low cardinality (≤100 unique)**: Also gets `__str` (RawToString) metric → enables exact counts, histograms, top-k
- **High cardinality (>100 unique)**: Approximate sketches only, no `__str` variant

See `StatsGenerator.buildEnhancedMetrics()` in `aggregator/src/main/scala/ai/chronon/aggregator/row/StatsGenerator.scala`

### TimedKvRdd Schema and Metadata Storage

When uploading stats, the following types of data are stored in the KV Store:

1. **Schema rows** (no date suffix — all versions accumulate in one BigTable row):
   - `{tableName}__keySchema` → JSON schema for Avro-encoded tile keys
   - `{tableName}__valueSchema` → JSON schema for Avro-encoded tile IRs

2. **Metadata rows** (no date suffix):
   - `{tableName}/selectedSchema` → Avro schema of the IR fields actually stored in tiles
   - `{tableName}/noKeysSchema` → Schema of non-key columns (input to buildEnhancedMetrics)
   - `{tableName}/cardinalityMap` → JSON map of column → cardinality used during compute

3. **Data tile rows** (one row per join × date, key = Avro bytes + `#YYYY-MM-DD`):
   - Key: Avro-encoded table name
   - Value: Avro-encoded normalized IRs
   - Timestamp: partition midnight (deterministic — two jobs on same day overwrite)

### BigTable Cell Versioning (CRITICAL)

- GC rule: `union(maxAge(90 days), maxVersions(2000))` — up to 2000 versions per cell.
- **BigTable returns cells in descending timestamp order (newest first).**
- **NEVER use `.last` on cell lists — it returns the OLDEST version.** Always use `.maxBy(_.millis)`.
- Metadata rows accumulate many versions as schemas evolve over time.
- Data tile rows: deterministic timestamp (partition midnight) so two jobs for the same day overwrite.

### Schema Mismatch and Graceful Degradation

When `buildEnhancedMetrics` produces metrics whose `inputColumn` doesn't exist in the stored `selectedSchema` (e.g., because code was updated to add `__str` metrics after the last stats job ran):

- **Do NOT hard-fail.** Filter to only compatible metrics, warn about skipped ones.
- `selectedSchema` is the source of truth — it reflects what was actually computed and stored.
- Re-run the stats job to get full metrics with the new schema.

See `JavaStatsService.fetchStats` for the partition/warn pattern.

### Semantic Hash Sharding

Stats tiles are sharded by `node.semanticHash` to prevent experimental jobs on feature branches from overwriting production data.

**Write path:** `EnhancedStatsStore(semanticHash: Option[String] = None)` — when set, prefixes all key bytes with `s"$hash/"` before writing to BigTable. `BatchNodeRunner` passes `Option(node.semanticHash).filter(_.nonEmpty)`.

**Read path:** `JavaStatsService.fetchStats(tableName, start, end, semanticHash: String)` — `null` = no prefix (backward compat with data written before sharding). Overload `fetchStats(tableName, start, end)` calls with `null`.

**Backward compat:** If `node.semanticHash` is null/empty, no prefix is applied on either side — fully transparent.

**Rollout:** NOT backward compatible when `node.semanticHash` is set. Requires re-running the stats job after deploying to populate the new sharded keys.

**Suffix-based routing in `EnhancedDatasetKVStoreImpl` still works** — `endsWith("__keySchema")` and `contains("/selectedSchema")` are unaffected by a leading `<hash>/` prefix.

## File Locations

### Spark Module
```
spark/src/main/scala/ai/chronon/spark/stats/
├── StatsCompute.scala              # Base class, has EnhancedStatsCompute
└── EnhancedStatsStore.scala        # Upload logic with semanticHash sharding

spark/src/test/scala/ai/chronon/spark/other/
└── StatsComputeTest.scala          # Tests including stale-cardinality-map regression

spark/README__STATS.md              # Complete Spark documentation
```

### Service Module
```
online/src/main/scala/ai/chronon/online/
└── JavaStatsService.scala          # Fetch/merge/decode with schema resilience

service/src/main/java/ai/chronon/service/
├── handlers/StatsHandler.java      # REST endpoint handler
└── FetcherVerticle.java            # Endpoint registration (line 64)

service/README__STATS.md            # Complete Service documentation
```

### API Module
```
thrift/planner.thrift               # Lines 84-90, 107-108 (node definitions)

api/src/main/scala/ai/chronon/api/planner/
└── MonolithJoinPlanner.scala       # Lines 87-132 (stats nodes)

api/README__STATS.md                # Complete API documentation
```

### Aggregator Module
```
aggregator/src/main/scala/ai/chronon/aggregator/row/
├── StatsGenerator.scala            # buildEnhancedMetrics, buildAggregator
└── RowAggregator.scala             # Throws IllegalArgumentException (not None.get) on missing inputColumn
```

## Key Methods and Their Purpose

### EnhancedStatsCompute (Spark)

```scala
// Compute cardinality for all columns
def computeCardinalityMap(): Map[String, Long]

// Lazy-evaluated metrics based on cardinality
lazy val enhancedMetrics: Seq[MetricTransform]

// Generate time-bucketed statistics, returns (flatDf, metadata map)
def enhancedDailySummary(sample: Double, timeBucketMinutes: Long): (DataFrame, Map[String, String])
```

### EnhancedStatsStore (Spark)

```scala
// semanticHash shards data by config version to avoid cross-branch contamination
class EnhancedStatsStore(api, tableBaseName, datasetName, semanticHash: Option[String] = None)

// Upload Avro DataFrame to KV Store (prefixes keys with hash/ when set)
def upload(avroDf: DataFrame, putsPerRequest: Int): Unit
```

### JavaStatsService (Online)

```scala
// Main entry point — semanticHash=null reads un-sharded legacy data
def fetchStats(tableName: String,
               startTimeMillis: Long,
               endTimeMillis: Long,
               semanticHash: String): CompletableFuture[JavaStatsResponse]

// Backward-compat overload (no sharding)
def fetchStats(tableName: String,
               startTimeMillis: Long,
               endTimeMillis: Long): CompletableFuture[JavaStatsResponse]
```

**Key behaviors in fetchStats:**
- Uses `values.maxBy(_.millis)` for ALL schema/metadata fetches (newest BigTable cell, not `.last`)
- Logs `ts=<millis>, versions=<count>` for every schema/metadata fetch for observability
- Filters metrics to only those whose `inputColumn` exists in `selectedSchema` (graceful degradation)
- Per-tile decode failures are caught and warned (up to 3), never crash the request
- Logs tile decode summary: `"Tile decode summary: X/N decoded, Y skipped"`

### Querying via REST API

```bash
curl "http://localhost:9000/v1/stats/gcp.demo.v1__1?startTime=1764115200000&endTime=1765310957443" | jq
```

Example response:
```json
{
  "success": true,
  "tableName": "gcp.demo.v1__1",
  "tilesCount": 7,
  "skippedTilesCount": 0,
  "statistics": {
    "listing_id_price_cents_min": 1283,
    "listing_id_price_cents_max": 756306,
    "listing_id_price_cents_average": 80666.27,
    "listing_id_price_cents_variance": 22822501518.05,
    "listing_id_price_cents_approx_unique_count": 38,
    "listing_id_price_cents_approx_percentile": [1283.0, 2954.0, 756306.0],
    "listing_id_price_cents__null_sum": 94,
    "total_count": 1262
  }
}
```

## Common Issues and Solutions

### tilesCount: 0 / all null statistics

**Cause:** `getSchemasFromKVStore` was using `.last` on BigTable cell lists → oldest IR schema (Dec 2025). All tiles in recent date ranges were encoded with the current (March 2026) schema → 100% decode failure.

**Why zero and not "at least one":** The entire query window falls within the "new schema era". No tiles from the old-schema period exist in a recent date range.

**Fix:** Use `.maxBy(_.millis)` everywhere — already applied in `getSchemasFromKVStore` and `getMetadataValue`.

### NoSuchElementException: None.get in RowAggregator

**Cause:** Stale/empty `cardinalityMap` caused numeric columns to default to low-cardinality, generating `__str` metrics whose `inputColumn` didn't exist in the stored `selectedSchema`.

**Fix:** `RowAggregator` now throws `IllegalArgumentException` naming the missing column. `JavaStatsService` filters out incompatible metrics with a warning rather than failing.

### Stats Compute Issues

**Problem: ClassCastException during merge**

**Cause:** IRs not denormalized before merging, or schema mismatch.

**Fix:** Ensure `aggregator.denormalize(ir)` is called before `aggregator.merge()`. Re-upload stats if schema changed.

### Stats Upload Issues

**Problem: "Failed to retrieve schemas"**

**Cause:** Stats not uploaded with `storeSchemasPrefix`.

**Fix:** Ensure `AvroKvEncoder.encodeTimed` is called with `storeSchemasPrefix = Some(joinConf.metaData.name)` and `metadata = Some(statsMetadata)`.

**Problem: Percentiles show as `[F@hexcode`**

**Cause:** `Array[Float]` printed with default toString.

**Solution:**
```scala
val percentiles = stats("col_approx_percentile").asInstanceOf[Array[Float]]
println(percentiles.mkString(", "))
```

## Build and Test Commands

```bash
# Compile relevant modules
./mill online.compile
./mill spark.compile

# Run stats tests
./mill spark.test.testOnly "ai.chronon.spark.other.StatsComputeTest"

# Run specific test
./mill spark.test.testOnly "ai.chronon.spark.other.StatsComputeTest" -- -z "fail with a clear error when cardinality map is stale"
```
