#!/usr/bin/env python3
"""
Billigence Case Study 1: Retail Electronics Analysis

This script analyzes retail electronics transaction data using PySpark.
Designed to run in Docker containers or any Spark environment.

Performance Optimizations Applied:
- Explicit schema definition (avoids schema inference scan)
- Broadcast joins for small dimension tables
- Predicate pushdown (filter before join)
- Column pruning (select only needed columns)
- Caching of reused DataFrames
- Adaptive Query Execution enabled
- Optimized shuffle partitions
- Coalesce for small output files

Author: Deren Ridley
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime
import os

# =============================================================================
# PERFORMANCE OPTIMIZATION: Spark Session Configuration
# =============================================================================
# - spark.sql.adaptive.enabled: Dynamically optimizes query plans at runtime
# - spark.sql.adaptive.coalescePartitions.enabled: Combines small partitions
# - spark.sql.shuffle.partitions: Optimized for dataset size (~500k rows)
# - spark.sql.autoBroadcastJoinThreshold: Auto-broadcast tables under 10MB
# =============================================================================

print("Initializing Spark Session with performance optimizations...")
spark = SparkSession.builder \
    .appName("Billigence_Case_Study_1") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")
print(f"Analysis Start Time: {datetime.now()}\n")

# File paths
BASE_PATH = "/workspaces/LocalSpark/data/case_study_1"
TRANSACTION_FILE = f"{BASE_PATH}/Transaction_Details.csv"
STORE_FILE = f"{BASE_PATH}/Store_Details.csv"
PRODUCT_FILE = f"{BASE_PATH}/Product_Details.csv"

OUTPUT_PATH = "/workspaces/LocalSpark/output/case_study_1"
os.makedirs(OUTPUT_PATH, exist_ok=True)

# =============================================================================
# PERFORMANCE OPTIMIZATION: Explicit Schema Definition
# =============================================================================
# Benefit: Avoids full data scan for schema inference
# Impact: 2-3x faster data loading on large files
# =============================================================================

print("=" * 80)
print("1. DEFINING DATA SCHEMAS (Performance: Avoids schema inference scan)")
print("=" * 80)

transaction_schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Order Number", IntegerType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Sku Number", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Cost", DoubleType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Store Key", IntegerType(), True),
    StructField("Cust Key", IntegerType(), True),
    StructField("Customer Name", StringType(), True)
])

store_schema = StructType([
    StructField("Store Key", IntegerType(), True),
    StructField("Store Name", StringType(), True),
    StructField("Store Region", StringType(), True),
    StructField("Store State", StringType(), True),
    StructField("Store City", StringType(), True),
    StructField("Store Latitude", DoubleType(), True),
    StructField("Store Longitude", DoubleType(), True)
])

product_schema = StructType([
    StructField("Sku Number", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Product Type", StringType(), True),
    StructField("Product Family", StringType(), True)
])

print("Schemas defined successfully\n")

# =============================================================================
# PERFORMANCE OPTIMIZATION: Column Pruning at Load Time
# =============================================================================
# Benefit: Only loads required columns into memory
# Impact: Reduces memory footprint and I/O
# =============================================================================

print("=" * 80)
print("2. LOADING DATA (Performance: Column pruning, explicit schemas)")
print("=" * 80)

print("Loading Transaction Details...")
transactions_raw = (
    spark.read
    .option("header", "true")
    .schema(transaction_schema)
    .csv(TRANSACTION_FILE)
)

# Select and rename only needed columns (column pruning)
transactions_df = (
    transactions_raw
    .select(
        F.to_timestamp(F.col("Date"), "yyyy-MM-dd HH:mm:ss.SSS").alias("transaction_date"),
        F.col("Order Number").alias("order_number"),
        F.col("Sku Number").alias("sku_number"),
        F.col("Quantity").alias("quantity"),
        F.round(F.col("Cost"), 2).alias("cost_per_unit"),
        F.round(F.col("Price"), 2).alias("price_per_unit"),
        F.col("Store Key").alias("store_key"),
        F.col("Cust Key").alias("customer_key")
    )
)

# =============================================================================
# PERFORMANCE OPTIMIZATION: Cache frequently accessed DataFrame
# =============================================================================
# Benefit: Avoids recomputation when DataFrame is used multiple times
# Impact: Significant speedup for iterative operations
# =============================================================================

transactions_df.cache()
transaction_count = transactions_df.count()
print(f"Loaded and cached {transaction_count:,} transaction records")

print("Loading Store Details...")
stores_df = (
    spark.read
    .option("header", "true")
    .schema(store_schema)
    .csv(STORE_FILE)
    .select(
        F.col("Store Key").alias("store_key"),
        F.col("Store Name").alias("store_name"),
        F.col("Store Region").alias("store_region")
    )
)
store_count = stores_df.count()
print(f"Loaded {store_count:,} store records")

print("Loading Product Details...")
products_df = (
    spark.read
    .option("header", "true")
    .schema(product_schema)
    .csv(PRODUCT_FILE)
    .select(
        F.col("Sku Number").alias("sku_number"),
        F.col("Product Name").alias("product_name"),
        F.col("Product Type").alias("product_type"),
        F.col("Product Family").alias("product_family")
    )
)
product_count = products_df.count()
print(f"Loaded {product_count:,} product records\n")

# Data Quality Validation
print("=" * 80)
print("3. DATA QUALITY VALIDATION")
print("=" * 80)

# Combine null checks into single pass (reduces number of actions)
null_counts = transactions_df.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in transactions_df.columns
]).collect()[0]

total_nulls = sum([null_counts[c] for c in transactions_df.columns])
print(f"Null value check: {total_nulls} nulls found")

# Combine business rule checks into single pass
business_checks = transactions_df.select(
    F.sum(F.when(F.col("quantity") < 0, 1).otherwise(0)).alias("negative_qty"),
    F.sum(F.when(F.col("price_per_unit") < 0, 1).otherwise(0)).alias("negative_price"),
    F.sum(F.when(F.col("price_per_unit") < F.col("cost_per_unit"), 1).otherwise(0)).alias("price_below_cost")
).collect()[0]

print(f"Business rules: {business_checks.negative_qty} negative quantities, "
      f"{business_checks.negative_price} negative prices, "
      f"{business_checks.price_below_cost} prices below cost\n")

# =============================================================================
# TASK 1: Top 10 Products by Revenue
# =============================================================================
# Performance Optimizations:
# - Predicate pushdown: Filter store_key != 443 BEFORE join
# - Broadcast join: Products table (~1K rows) broadcast to all executors
# - Single-pass aggregation: All metrics computed in one groupBy
# =============================================================================

print("=" * 80)
print("4. TASK 1: TOP 10 PRODUCTS BY REVENUE")
print("=" * 80)
print("Performance: Predicate pushdown, broadcast join, single-pass aggregation")

top_products_df = (
    transactions_df
    # PERFORMANCE: Predicate pushdown - filter before join reduces data shuffled
    .filter(F.col("store_key") != 443)
    # PERFORMANCE: Broadcast join - small dimension table sent to all executors
    .join(F.broadcast(products_df), "sku_number", "inner")
    .groupBy(
        "sku_number",
        "product_name",
        "product_type",
        "product_family"
    )
    # PERFORMANCE: Single-pass aggregation - all metrics in one groupBy
    .agg(
        F.round(F.sum(F.col("quantity") * F.col("price_per_unit")), 2).alias("total_sales_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_number").alias("total_orders"),
        F.countDistinct("store_key").alias("stores_sold_in"),
        F.round(
            F.sum(F.col("quantity") * F.col("price_per_unit")) / F.sum("quantity"), 2
        ).alias("avg_price_per_unit")
    )
    .filter(F.col("total_sales_revenue") > 500000)
    .orderBy(F.desc("total_sales_revenue"))
    .limit(10)
)

# Cache result for reuse
top_products_df.cache()
print(f"Found {top_products_df.count()} products meeting criteria")
print("\nTop 10 Products by Revenue:")
top_products_df.show(10, truncate=False)

# Save results
top_products_df.write.format("parquet").mode("overwrite").save(f"{OUTPUT_PATH}/top_10_products")
print(f"Saved to {OUTPUT_PATH}/top_10_products\n")

# =============================================================================
# TASK 2: Top 3 Regions by Profit
# =============================================================================
# Performance Optimizations:
# - Broadcast join: Stores table (~200 rows) broadcast to all executors
# - Computed columns in single withColumns call (Spark 3.3+)
# - Single-pass aggregation for all regional metrics
# =============================================================================

print("=" * 80)
print("5. TASK 2: TOP 3 REGIONS BY PROFIT")
print("=" * 80)
print("Performance: Broadcast join, computed columns, single-pass aggregation")

top_regions_df = (
    transactions_df
    # PERFORMANCE: Broadcast join - stores table is small (~200 rows)
    .join(F.broadcast(stores_df), "store_key", "inner")
    # PERFORMANCE: Compute derived columns before aggregation
    .withColumn("profit", F.col("quantity") * (F.col("price_per_unit") - F.col("cost_per_unit")))
    .withColumn("revenue", F.col("quantity") * F.col("price_per_unit"))
    .groupBy("store_region")
    # PERFORMANCE: Single-pass aggregation - all metrics computed together
    .agg(
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.round((F.sum("profit") / F.sum("revenue")) * 100, 2).alias("profit_margin_pct"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_number").alias("total_orders"),
        F.countDistinct("store_key").alias("store_count"),
        F.countDistinct("customer_key").alias("unique_customers")
    )
    .orderBy(F.desc("total_profit"))
    .limit(3)
)

# Cache result for reuse
top_regions_df.cache()
print(f"Found {top_regions_df.count()} regions")
print("\nTop 3 Regions by Profit:")
top_regions_df.show(10, truncate=False)

# Save results
top_regions_df.write.format("parquet").mode("overwrite").save(f"{OUTPUT_PATH}/top_3_regions")
print(f"Saved to {OUTPUT_PATH}/top_3_regions\n")

# =============================================================================
# PERFORMANCE OPTIMIZATION: Coalesce for small output files
# =============================================================================
# Benefit: Reduces number of output files for small datasets
# Impact: Faster downstream reads, cleaner file structure
# =============================================================================

print("=" * 80)
print("6. EXPORTING RESULTS TO CSV")
print("=" * 80)
print("Performance: Coalesce(1) for single-file output")

csv_output = f"{OUTPUT_PATH}/csv"

# Clean up and recreate CSV output directory to avoid permission issues
import shutil
if os.path.exists(csv_output):
    try:
        shutil.rmtree(csv_output)
    except Exception as e:
        print(f"Warning: Could not remove {csv_output}: {e}")
        # Use timestamped directory as fallback
        from datetime import datetime as dt
        csv_output = f"{OUTPUT_PATH}/csv_{dt.now().strftime('%Y%m%d_%H%M%S')}"

os.makedirs(csv_output, exist_ok=True)

# PERFORMANCE: Coalesce to single file for small result sets
top_products_df.coalesce(1).write.option("header", "true").csv(f"{csv_output}/top_products")
top_regions_df.coalesce(1).write.option("header", "true").csv(f"{csv_output}/top_regions")

print(f"CSV files saved to {csv_output}\n")

# =============================================================================
# Cleanup: Unpersist cached DataFrames
# =============================================================================

transactions_df.unpersist()
top_products_df.unpersist()
top_regions_df.unpersist()

# Summary
print("=" * 80)
print("7. ANALYSIS COMPLETE")
print("=" * 80)
print(f"""
Analysis Summary:
- Transactions analyzed: {transaction_count:,}
- Stores: {store_count:,}
- Products: {product_count:,}
- Top products identified: {top_products_df.count()}
- Top regions identified: {top_regions_df.count()}

Performance Optimizations Applied:
- Explicit schemas (avoided inference scan)
- Broadcast joins for dimension tables
- Predicate pushdown (filter before join)
- Column pruning (loaded only needed columns)
- DataFrame caching for reused data
- Single-pass aggregations
- Coalesce for output files
- Adaptive Query Execution enabled

Output Location: {OUTPUT_PATH}
- Parquet files: top_10_products/, top_3_regions/
- CSV exports: csv/

End Time: {datetime.now()}
""")

spark.stop()
