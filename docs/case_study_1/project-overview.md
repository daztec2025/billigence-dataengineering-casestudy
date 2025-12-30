# Project Overview

## Objectives

Analysis of retail electronics transaction data to identify top-performing products and profitable regions.

**Goals:**
1. **Identify Top Products** - Top 10 products by revenue (>$500K, excluding Store 443)
2. **Determine Profitable Regions** - Top 3 regions by profit margin
3. **Ensure Data Quality** - Validate data integrity throughout

## Data Scope

- 521,612 transactions analyzed
- 196 stores across multiple regions
- 1,095 unique products
- Time period: August 2021 - December 2022

## Technology Stack

This solution was built using a Docker image designed to closely mimic Databricks Runtime 14.3 LTS, but is portable to any Spark engine (Snowflake, EMR, Synapse).

**Core Technologies:**
- PySpark 3.5.0 for distributed processing
- Parquet for data storage
- Python 3.10+ for scripting

## Key Results

### Top 10 Products by Revenue
- Identified products exceeding $500K revenue threshold
- Excluded Store 443 as required
- Revenue range: $697K - $5.4M per product

### Top 3 Regions by Profit
- West: $26.6M profit (44.08% margin)
- South: $18.6M profit (44.13% margin)
- East: $15.1M profit (44.39% margin)

### Data Quality
- Zero referential integrity violations
- Zero null values in critical columns
- All business rules validated

## Deliverables

1. PySpark analysis script with complete transformations
2. Parquet files with persistent results
3. Unit test suite (25 tests, 100% pass rate)
4. Comprehensive documentation
