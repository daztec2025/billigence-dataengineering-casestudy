# Technology Stack

## Platform

This solution was built using a Docker image designed to closely mimic **Databricks Runtime 14.3 LTS**, but is portable to any Spark engine such as Snowflake, AWS EMR, or Azure Synapse.

## Core Technologies

### PySpark 3.5.0
Distributed data processing engine for scalable analytics.

**Example usage:**
```python
# DataFrame API for transformations
top_products = (
    transactions_df
    .filter(F.col("store_key") != 443)
    .join(products_df, "sku_number")
    .groupBy("sku_number", "product_name")
    .agg(F.sum(F.col("quantity") * F.col("price")).alias("revenue"))
    .filter(F.col("revenue") > 500000)
    .orderBy(F.desc("revenue"))
    .limit(10)
)
```

**Key features:**
- In-memory processing for performance
- Catalyst optimizer for query planning
- Scales from gigabytes to petabytes

### Delta Lake 3.1.0
ACID-compliant storage layer for reliable data management.

**Example usage:**
```python
# Write with ACID guarantees
top_products.write.format("delta").mode("overwrite").saveAsTable("top_products")

# Read with schema enforcement
df = spark.read.format("delta").load("/path/to/table")
```

**Key features:**
- ACID transactions ensure consistency
- Time travel for historical queries
- Schema evolution without breaking changes

### Python 3.10+
Scripting language for automation and data manipulation.

**Libraries used:**
- PySpark for distributed processing
- Pandas for small dataset operations
- Standard library for utilities

## Development Environment

- **Runtime:** Docker image, closely mimics Databricks 14.3 LTS
- **Spark:** 3.5.0
- **Python:** 3.10+
- **Delta Lake:** 3.1.0

## Documentation

**MkDocs** with Material theme for professional documentation presentation.
