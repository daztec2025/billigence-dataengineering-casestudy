# Performance Tuning

## Overview

Performance optimizations applied to the University API Analysis pipeline.

## Spark Session Configuration

```python
spark = SparkSession.builder \
    .appName("Billigence-Case-Study-2") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()
```

| Configuration | Purpose |
|--------------|----------|
| `spark.sql.adaptive.enabled` | Runtime query optimization |
| `spark.sql.adaptive.coalescePartitions.enabled` | Combines small partitions after shuffle |
| `spark.sql.shuffle.partitions` | Reduced to 10 for small dataset (~10K rows) |

## Optimization Techniques

### API Connection Pooling

**Benefit:** Reuses HTTP connections for multiple requests

**Impact:** Reduced connection overhead, faster retries on failure

```python
session = requests.Session()
session.mount('http://', requests.adapters.HTTPAdapter(
    max_retries=3,
    pool_connections=10,
    pool_maxsize=10
))
```

### Explicit Schema Definition

**Benefit:** Avoids schema inference scan on data load

**Impact:** Faster DataFrame creation, consistent data types

### DataFrame Caching

**Benefit:** Avoids recomputation when DataFrame is used multiple times

**Impact:** Significant speedup for aggregation operations

### Single-Pass Aggregations

**Benefit:** All metrics computed in one groupBy operation

**Impact:** Reduces data shuffling and scan operations

### Coalesce for Output

**Benefit:** Reduces number of output files for small datasets

**Impact:** Faster downstream reads, cleaner file structure

## Performance Summary

| Optimization | Technique | Expected Improvement |
|-------------|-----------|---------------------|
| Connection Pooling | HTTP session reuse | Reduced network overhead |
| Schema Definition | Explicit schema | Faster data loading |
| Caching | Cache reused DataFrames | Eliminates redundant computation |
| Single-Pass Agg | Combined aggregations | 2-5x faster aggregations |
| Coalesce | Single output file | Faster downstream reads |
| Parquet Output | Columnar storage | 75% smaller files |
| AQE | Runtime optimization | 10-30% overall improvement |

## Future Recommendations

### Local API Caching

For repeated analysis, cache API responses locally:

```python
cache_file = Path("cache/universities.json")
if cache_file.exists() and cache_file.stat().st_mtime > time.time() - 3600:
    data = json.load(open(cache_file))
else:
    data = requests.get(api_url).json()
    json.dump(data, open(cache_file, 'w'))
```

### Incremental Updates

For production systems, implement incremental data loading rather than full refresh.

## Implementation

All optimizations are implemented in `scripts/case_study_2_analysis.py` with inline comments documenting each technique.
