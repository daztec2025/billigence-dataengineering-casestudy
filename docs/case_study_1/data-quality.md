# Data Quality Validation

## Overview

Comprehensive data quality validation was performed on all datasets before analysis to ensure integrity and reliability. All checks passed with 100% compliance.

## Validation Summary

| Quality Dimension | Result | Status |
|------------------|--------|--------|
| Completeness | 0 null values | PASS |
| Referential Integrity | 0 orphaned records | PASS |
| Business Rules | 0 violations | PASS |
| Temporal Validity | All dates valid | PASS |

## Validation Results

### 1. Completeness Analysis

**Objective:** Ensure no critical data is missing

**Results:**
- 521,613 transaction records analyzed
- Zero null values across all 10 columns
- 100% data completeness

```python
null_counts = transactions_df.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in transactions_df.columns
])
```

### 2. Referential Integrity

**Objective:** Validate all foreign key relationships

**Results:**
- Zero orphaned store keys
- Zero orphaned SKU numbers
- All transactions reference valid dimension records

```python
# Validate store references
orphaned_stores = (
    transactions_df
    .join(stores_df, "store_key", "left")
    .where(F.col("store_name").isNull())
    .count()
)

# Validate product references
orphaned_skus = (
    transactions_df
    .join(products_df, "sku_number", "left")
    .where(F.col("product_type").isNull())
    .count()
)
```

### 3. Business Logic Validation

**Objective:** Ensure data conforms to business rules

**Rules Validated:**
- No negative quantities
- No negative prices
- No prices below cost
- No future transaction dates

**Results:** 100% compliance (0 violations)

```python
business_checks = transactions_df.select(
    F.sum(F.when(F.col("quantity") < 0, 1).otherwise(0)).alias("negative_qty"),
    F.sum(F.when(F.col("price_per_unit") < 0, 1).otherwise(0)).alias("negative_price"),
    F.sum(F.when(F.col("price_per_unit") < F.col("cost_per_unit"), 1).otherwise(0)).alias("price_below_cost"),
    F.sum(F.when(F.col("transaction_date") > F.current_date(), 1).otherwise(0)).alias("future_dates")
)
```

### 4. Temporal Validation

**Objective:** Verify transaction dates fall within expected range

**Results:**
- Date range: August 2021 to December 2022 (~547 days)
- All dates are historical
- Continuous data coverage

```python
date_range = transactions_df.select(
    F.min("transaction_date").alias("earliest"),
    F.max("transaction_date").alias("latest"),
    F.datediff(F.max("transaction_date"), F.min("transaction_date")).alias("days_span")
)
```

## Validation Framework

### Methodology

1. **Schema Validation** - Explicit schemas defined at load time
2. **Completeness Checks** - Null analysis per column
3. **Referential Integrity** - Foreign key validation via left joins
4. **Business Rules** - Domain-specific constraint verification
5. **Consistency Checks** - Data distribution and special case validation

### Implementation

All validation code is in [scripts/case_study_1_analysis.py](../../scripts/case_study_1_analysis.py):

- Lines 133-163: Data loading with explicit schemas
- Lines 166-182: Data quality validation
- Uses PySpark DataFrame API exclusively

## Dataset Profiling

| Metric | Value |
|--------|-------|
| Total Transactions | 521,613 |
| Total Stores | 197 |
| Total Products | 1,096 |
| Date Range | 2021-08-01 to 2022-12-31 |
| Analysis Period | 1.5 years |
