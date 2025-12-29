# Performance Tuning

## Overview

Performance optimizations applied to the Customer Data Restructuring pipeline.

## Spark Session Configuration

```python
spark = SparkSession.builder \
    .appName("Billigence-Case-Study-3") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```

| Configuration | Purpose |
|--------------|----------|
| `spark.sql.adaptive.enabled` | Runtime query optimization |
| `spark.sql.adaptive.coalescePartitions.enabled` | Combines small partitions after shuffle |
| `spark.sql.shuffle.partitions` | Reduced to 4 for small dataset (~100 rows) |

## Optimization Techniques

### Native PySpark Functions

**Benefit:** Built-in functions run in JVM, not Python interpreter

**Impact:** 10-100x faster than equivalent Python UDFs

```python
# Native functions used instead of UDFs
df.withColumn("email_sha256", F.sha2(F.col("email"), 256))
df.withColumn("email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1))
df.withColumn("email_md5", F.md5(F.col("email")))
```

| Function | Purpose | Performance |
|----------|---------|-------------|
| `F.sha2()` | SHA-256 hashing | Native JVM execution |
| `F.md5()` | MD5 hashing | Native JVM execution |
| `F.regexp_extract()` | Domain extraction | Native regex engine |

### Chained Transformations

**Benefit:** Single pass through data for multiple column additions

**Impact:** Reduces scan operations and memory pressure

```python
df_masked = df.withColumn(
    "email_domain", F.regexp_extract(...)
).withColumn(
    "email_masked", F.concat(...)
).withColumn(
    "email_sha256", F.sha2(...)
)
```

### Single-Pass Validation

**Benefit:** All quality checks computed in one aggregation

**Impact:** Reduces multiple scans to single pass

### Coalesce for Output

**Benefit:** Reduces number of output files for small datasets

**Impact:** Faster downstream reads, cleaner file structure

### Parquet Format

**Benefit:** Columnar storage with built-in compression

**Impact:** 75% smaller files, 5-10x faster reads

## Performance Summary

| Optimization | Technique | Expected Improvement |
|-------------|-----------|---------------------|
| Native Functions | JVM-based hashing | 10-100x faster than UDFs |
| Chained Transforms | Single-pass column additions | Reduced memory usage |
| Single-Pass Validation | Combined aggregations | Fewer data scans |
| Coalesce | Single output file | Faster downstream reads |
| Parquet Output | Columnar storage | 75% smaller files |
| AQE | Runtime optimization | 10-30% overall improvement |

## Security Considerations

### Hash Function Selection

| Algorithm | Output Size | Security Level | Use Case |
|-----------|-------------|----------------|----------|
| SHA-256 | 64 chars | High | Production storage |
| MD5 | 32 chars | Low | Non-security lookups |

SHA-256 is recommended for production as MD5 has known collision vulnerabilities.

### Data Separation

Sensitive reference tables (original emails) should be stored separately with restricted access controls.

## Implementation

All optimizations are implemented in `scripts/case_study_3_analysis.py` with inline comments documenting each technique.
