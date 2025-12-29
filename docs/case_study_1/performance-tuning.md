# Performance Tuning

## Overview

## Spark Session Configuration

```python
spark = SparkSession.builder \
    .appName("Billigence_Case_Study_1") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
    .getOrCreate()
```

| Configuration | Purpose |
|--------------|----------|
| `spark.sql.adaptive.enabled` | Dynamically optimizes query plans at runtime |
| `spark.sql.adaptive.coalescePartitions.enabled` | Combines small partitions after shuffle |
| `spark.sql.shuffle.partitions` | Optimized partition count for dataset size |
| `spark.sql.autoBroadcastJoinThreshold` | Auto-broadcast tables under threshold |

## Optimization Techniques

### Spark Session Configuration

- spark.sql.adaptive.enabled: Dynamically optimizes query plans at runtime

- spark.sql.adaptive.coalescePartitions.enabled: Combines small partitions

- spark.sql.shuffle.partitions: Optimized for dataset size (~500k rows)

- spark.sql.autoBroadcastJoinThreshold: Auto-broadcast tables under 10MB

### Explicit Schema Definition

**Benefit: Avoids full data scan for schema inference**

**Impact: 2-3x faster data loading on large files**

### Column Pruning at Load Time

**Benefit: Only loads required columns into memory**

**Impact: Reduces memory footprint and I/O**

### Cache frequently accessed DataFrame

**Benefit: Avoids recomputation when DataFrame is used multiple times**

**Impact: Significant speedup for iterative operations**

### Coalesce for small output files

**Benefit: Reduces number of output files for small datasets**

**Impact: Faster downstream reads, cleaner file structure**

## Inline Optimizations

The following optimizations are applied inline in the code:

- Predicate pushdown - filter before join reduces data shuffled
- Broadcast join - small dimension table sent to all executors
- Single-pass aggregation - all metrics in one groupBy
- Broadcast join - stores table is small (~200 rows)
- Compute derived columns before aggregation
- Single-pass aggregation - all metrics computed together
- Coalesce to single file for small result sets

## Performance Summary

| Optimization | Technique | Expected Improvement |
|-------------|-----------|---------------------|
| Schema Definition | Explicit schemas | 2-3x faster loading |
| Column Pruning | Select only needed columns | 20-40% memory reduction |
| Broadcast Joins | Small table broadcast | 10-100x faster joins |
| Predicate Pushdown | Filter before join | Reduces shuffle data |
| Caching | Cache reused DataFrames | Eliminates redundant I/O |
| Single-Pass Agg | Combined aggregations | 2-5x faster aggregations |
| Coalesce | Single output file | Faster downstream reads |
| AQE | Runtime optimization | 10-30% overall improvement |

## Implementation

All optimizations are implemented in `scripts/case_study_1_analysis.py` with inline comments documenting each technique.
