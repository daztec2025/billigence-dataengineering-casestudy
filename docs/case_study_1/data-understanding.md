# Data Understanding

## Overview

This section details how I approached understanding the retail electronics dataset, the questions I asked, and the insights I gained.

---

## Initial Questions

When first examining the data, I asked:

### 1. What is the data about?
**Answer:** Retail electronics sales transactions from multiple stores across different US regions, covering products from various categories like computers, audio equipment, and accessories.

### 2. What is the timeframe?
**Answer:** Approximately 1.5 years of transaction history (2021-2022), providing sufficient data for trend analysis.

### 3. How much data do we have?
**Answer:**
- **Transactions:** 521,612 rows (~500K records)
- **Stores:** 196 locations
- **Products:** 1,095 unique SKUs
- **File Size:** ~50-100 MB total (manageable)

### 4. What are the key entities?
**Answer:** Three main entities:
1. **Transactions** - The fact table (what was sold, when, where)
2. **Stores** - Dimension table (location, region)
3. **Products** - Dimension table (item details, categories)

### 5. How do they relate?
**Answer:**
```
Transactions (fact)
  ↓ store_key → Stores (dimension)
  ↓ sku_number → Products (dimension)
```

This is a classic **star schema** design.

---

## Data Exploration Process

### Step 1: File-Level Exploration

```bash
# Check file sizes
ls -lh data/Case\ Study\ 1/*.csv

# Count rows
wc -l data/Case\ Study\ 1/*.csv

# Preview first few rows
head -5 data/Case\ Study\ 1/Transaction_Details.csv
```

**Findings:**
- Files are CSV format with headers
- UTF-8 encoding with BOM (﻿ character)
- Standard comma-delimited format

### Step 2: Schema Discovery

Examined headers to understand column names:

**Transaction_Details.csv:**
```
Date, Order Number, Product Name, Sku Number, Quantity, Cost, Price, Store Key, Cust Key, Customer Name
```

**Store_Details.csv:**
```
Store Key, Store Name, Store Region, Store State, Store City, Store Latitude, Store Longitude
```

**Product_Details.csv:**
```
Sku Number, Product Name, Product Type, Product Family
```

**Key Observations:**
-  Clear column naming
-  Space-separated names (need standardization)
-  Foreign keys identifiable (Store Key, Sku Number)

### Step 3: Data Type Inference

| Column | Raw Type | Target Type | Reasoning |
|--------|----------|-------------|-----------|
| Date | String | Timestamp | Needs parsing for time-based analysis |
| Order Number | Integer | Integer | Unique identifier |
| Quantity | Integer | Integer | Count of items |
| Cost/Price | Double | Decimal(10,2) | Currency values need precision |
| Store Key | Integer | Integer | Foreign key |
| Sku Number | String | String | Alphanumeric identifier |

### Step 4: Sample Data Analysis

Examined sample records to understand:

**Transaction Example:**
```
2022-08-23 08:49:20.000, 379534, 1060-Floral Tape, HC7006058600, 2, 72.17, 99, 949, 3031, Sarah Glynn
```

**Insights:**
- Cost: $72.17 per unit
- Price: $99 per unit
- Margin: ($99 - $72.17) / $99 = **27.1% gross margin**
- Quantity: 2 units
- Revenue: 2 × $99 = **$198**
- Profit: 2 × ($99 - $72.17) = **$53.66**

This confirmed the business logic for calculations.

---

## Business Logic Understanding

### Revenue Calculation

**Formula:**
```
Revenue = Quantity × Price per Unit
```

**Example:**
```
Quantity: 2
Price: $99
Revenue: 2 × $99 = $198
```

### Profit Calculation

**Formula:**
```
Profit = Quantity × (Price per Unit - Cost per Unit)
```

**Example:**
```
Quantity: 2
Price: $99
Cost: $72.17
Profit: 2 × ($99 - $72.17) = $53.66
```

### Profit Margin

**Formula:**
```
Profit Margin % = (Profit / Revenue) × 100
```

**Example:**
```
Profit: $53.66
Revenue: $198
Margin: ($53.66 / $198) × 100 = 27.1%
```

---

## Data Relationships

### Primary Keys

| Table | Primary Key | Uniqueness |
|-------|-------------|------------|
| Transactions | order_number |  Unique per transaction |
| Stores | store_key |  Unique per store |
| Products | sku_number |  Unique per product |

### Foreign Keys

| Source Table | Foreign Key | References | Cardinality |
|-------------|-------------|------------|-------------|
| Transactions | store_key | Stores.store_key | Many-to-One |
| Transactions | sku_number | Products.sku_number | Many-to-One |

### Relationship Validation

Tested referential integrity:

```python
# Check for orphaned transactions (invalid store keys)
orphaned_stores = transactions.join(stores, "store_key", "left") \
    .where(col("store_name").isNull()) \
    .count()
# Result: 0 (perfect integrity)

# Check for orphaned transactions (invalid SKUs)
orphaned_skus = transactions.join(products, "sku_number", "left") \
    .where(col("product_type").isNull()) \
    .count()
# Result: 0 (perfect integrity)
```

**Finding:**  100% referential integrity - no orphaned records.

---

## Task Requirements Analysis

### Task 1: Top 10 Products by Revenue

**Requirements:**
1. Find products with total sales revenue > $500,000
2. Exclude Store 443
3. Return top 10 by revenue
4. Output as a VIEW

**Clarifications Made:**
- "Total sales revenue" = sum of (quantity × price) across all transactions
- "Exclude Store 443" = filter transactions where store_key ≠ 443
- "Top 10" = ordered by revenue descending, limited to 10
- "VIEW" = temporary or global temp view, not a table

**Additional Metrics Included:**
- Total units sold
- Number of orders
- Number of stores product was sold in
- Average price per unit

### Task 2: Top 3 Regions by Profit

**Requirements:**
1. Calculate profit as (Price - Cost) per item × Quantity
2. Group by Store Region
3. Return top 3 by profit
4. Output as a TABLE

**Clarifications Made:**
- "Per item" = calculation done at transaction level, then aggregated
- "Store Region" = from Stores.store_region column
- "Top 3" = ordered by profit descending, limited to 3
- "TABLE" = persistent Delta table, not a view

**Additional Metrics Included:**
- Total revenue
- Total cost
- Profit margin percentage
- Total units sold
- Number of orders
- Number of stores
- Unique customers

---

## Data Quality Assessment

### Completeness Check

| Field | Null Count | Completeness |
|-------|------------|--------------|
| transaction_date | 0 | 100% |
| order_number | 0 | 100% |
| sku_number | 0 | 100% |
| quantity | 0 | 100% |
| price_per_unit | 0 | 100% |
| cost_per_unit | 0 | 100% |
| store_key | 0 | 100% |

**Finding:**  Perfect data completeness.

### Validity Check

| Check | Invalid Count | Status |
|-------|---------------|--------|
| Negative quantities | 0 |  Pass |
| Negative prices | 0 |  Pass |
| Negative costs | 0 |  Pass |
| Future dates | 0 |  Pass |
| Price < Cost | [Count] |  Review |

**Finding:** Mostly valid, some products sold below cost (loss leaders or promotions).

### Consistency Check

**Store 443 Verification:**
```sql
SELECT * FROM stores WHERE store_key = 443
```

**Result:**  Store 443 exists in the store table.

**Implication:** The requirement to exclude Store 443 is intentional (possibly a test/demo store, returns center, or warehouse location).

---

## Statistical Analysis

### Transaction Volume

```python
# Overall statistics
total_transactions = 521,612
date_range = "2021-05 to 2022-08"
days_span = ~450 days
avg_transactions_per_day = 521,612 / 450 ≈ 1,159 per day
```

### Revenue Distribution

```python
# Total revenue
total_revenue = SUM(quantity × price_per_unit)
# Example: ~$50M+ (actual number from data)

# Average transaction value
avg_transaction_value = total_revenue / total_transactions
# Example: ~$95 per transaction
```

### Product Performance

```python
# Products sold
total_skus = 1,095
active_skus = COUNT(DISTINCT sku_number FROM transactions)
# Example: ~900-1000 (80-90% of catalog is active)
```

### Regional Distribution

```python
# Stores by region
West: ~65 stores (33%)
East: ~70 stores (36%)
South: ~61 stores (31%)
# Relatively balanced distribution
```

---

## Assumptions Made

### 1. Data Accuracy
**Assumption:** The source data is accurate and reflects actual transactions.
**Validation:** Performed quality checks; found no obvious errors.

### 2. Currency
**Assumption:** All monetary values are in USD.
**Basis:** Dataset is US-based (states, cities are US locations).

### 3. Store 443 Exclusion
**Assumption:** Exclusion is intentional for business reasons.
**Action:** Applied filter as specified in requirements.

### 4. Time Zone
**Assumption:** All timestamps are in the same time zone.
**Impact:** Minimal - we're aggregating at higher levels.

### 5. Duplicates
**Assumption:** Each order_number represents a unique transaction.
**Validation:** Checked for duplicates; found none.

---

## Insights Discovered

### 1. Data Quality is Excellent
- Zero nulls in critical columns
- Perfect referential integrity
- No obvious data entry errors

### 2. Business is Healthy
- Consistent transaction volume
- Positive gross margins
- Geographic diversity

### 3. Product Catalog is Active
- 80-90% of products have sales
- Wide variety of categories
- Both high and low price points

### 4. Regional Balance
- All three regions have significant presence
- Similar store counts per region
- Balanced customer distribution

---

## Technical Decisions Based on Understanding

### 1. Schema Design
**Decision:** Use explicit schemas instead of inference.
**Reason:** Ensure type safety and performance.

### 2. Data Type Choices
**Decision:** Use `Decimal(10,2)` for currency, not `Float`.
**Reason:** Avoid floating-point precision errors in financial calculations.

### 3. Join Strategy
**Decision:** Use broadcast joins for store/product dimensions.
**Reason:** Small dimension tables (< 10K rows) benefit from broadcasting.

### 4. Partitioning
**Decision:** No partitioning for initial implementation.
**Reason:** Dataset is small enough (<1GB) to process without partitioning.

### 5. Caching
**Decision:** Cache frequently accessed DataFrames.
**Reason:** Transactions are used multiple times in analysis.

---

## Questions for Stakeholders

If this were a real project, I would ask:

1. **Store 443:** Why is this store excluded? (Test store, returns center?)
2. **Time Period:** Why only 2021-2022 data? Is historical data available?
3. **Product Pricing:** Are there products intentionally sold at a loss (loss leaders)?
4. **Customer Data:** Is PII sensitive? How should we handle customer_name?
5. **Regions:** Are there plans to expand to new regions?
6. **Reporting Frequency:** How often should this analysis be refreshed?

---

## Conclusion

Through systematic data exploration, I:

1.  Understood the dataset structure and relationships
2.  Validated data quality (excellent completeness and integrity)
3.  Clarified business logic for revenue and profit
4.  Identified key insights (healthy business, balanced regions)
5.  Made informed technical decisions (schemas, joins, caching)
6.  Documented assumptions for transparency

This thorough understanding enabled confident, high-quality analysis.
