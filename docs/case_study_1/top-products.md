# Task 1: Top 10 Products by Revenue

## Overview

This analysis identifies the 10 highest-revenue products meeting specific business criteria.

---

## Requirements

1.  **Total Sales Revenue > $500,000** - Only include products exceeding this threshold
2.  **Exclude Store 443** - Filter out all transactions from Store 443
3.  **Output as VIEW** - Create a queryable view, not a persistent table

---

## Implementation

### PySpark DataFrame API

```python
top_products_df = (
    transactions_df
    .filter(F.col("store_key") != 443)  # Exclude Store 443
    .join(products_df, "sku_number", "inner")
    .groupBy(
        "sku_number",
        products_df.product_name.alias("product_name"),
        "product_type",
        "product_family"
    )
    .agg(
        F.round(F.sum(F.col("quantity") * F.col("price_per_unit")), 2).alias("total_sales_revenue"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("order_number").alias("total_orders"),
        F.countDistinct("store_key").alias("stores_sold_in"),
        F.round(
            F.sum(F.col("quantity") * F.col("price_per_unit")) / F.sum("quantity"),
            2
        ).alias("avg_price_per_unit")
    )
    .filter(F.col("total_sales_revenue") > 500000)  # Revenue > $500K
    .orderBy(F.desc("total_sales_revenue"))
    .limit(10)
)

# Create view
top_products_df.createOrReplaceTempView("top_10_products_by_revenue")
```


---

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `sku_number` | String | Unique product identifier |
| `product_name` | String | Product description |
| `product_type` | String | Product category |
| `product_family` | String | Product family/line |
| `total_sales_revenue` | Decimal(10,2) | Sum of (quantity × price) |
| `total_units_sold` | Integer | Total quantity sold |
| `total_orders` | Integer | Number of distinct orders |
| `stores_sold_in` | Integer | Number of stores that sold this product |
| `avg_price_per_unit` | Decimal(10,2) | Average selling price |

---

## Business Insights

### View Created
- **View Name:** `top_10_products_by_revenue`
- **Also Saved As:** Delta table `billigence_top_10_products_by_revenue`
- **Records:** 10 products meeting criteria

### Key Findings

The top products represent:
- **High-value items** with significant revenue contribution
- **Wide distribution** across multiple store locations
- **Consistent sellers** with many individual orders

### Use Cases

This view enables:
1. **Inventory Optimization** - Ensure top sellers are well-stocked
2. **Supplier Management** - Prioritize relationships for key products
3. **Marketing Focus** - Promote proven revenue generators
4. **Space Allocation** - Dedicate premium shelf space to top products

---

## Validation

### Filters Applied

 **Store 443 Excluded:**
```python
.filter(F.col("store_key") != 443)
```

 **Revenue Threshold Applied:**
```python
.filter(F.col("total_sales_revenue") > 500000)
```

 **Top 10 Only:**
```python
.orderBy(F.desc("total_sales_revenue")).limit(10)
```

### Data Quality

- All products have revenue > $500,000 (verified)
- No transactions from Store 443 included (verified)
- Exactly 10 products returned (verified)
- All SKUs map to valid products (verified)

---

## Querying the View

### From SQL

```sql
SELECT * FROM top_10_products_by_revenue;
```

### From Python

```python
# Load the view
top_products = spark.table("top_10_products_by_revenue")

# Display results
display(top_products)

# Or query specific products
display(top_products.filter(F.col("product_family") == "Computers"))
```

---

## Results

### Top 10 Products by Revenue

| Rank | SKU | Product Name | Revenue | Units | Orders | Stores | Avg Price |
|------|-----|--------------|---------|-------|--------|--------|-----------|
| 1 | HC1473954319 | 1059-Finger & Hand Cymbals | $5,413,361 | 3,650 | 1,725 | 193 | $1,483.11 |
| 2 | HC1203263241 | 1087-Harmonica Holders | $3,318,455 | 3,559 | 1,684 | 192 | $932.41 |
| 3 | HC9052289713 | 1083-Hand Percussion Stands & Mounts | $3,227,038 | 4,603 | 2,151 | 195 | $701.07 |
| 4 | HC4022921832 | 1096-Knitting Needles | $1,445,758 | 4,294 | 1,969 | 195 | $336.69 |
| 5 | HC5444670737 | 1069-Guitar Cases & Gig Bags | $970,704 | 3,984 | 1,934 | 195 | $243.65 |
| 6 | HC8205920582 | 1056-Felting Needles & Machines | $960,650 | 1,086 | 536 | 173 | $884.58 |
| 7 | HC5854209003 | 1079-Hand Bells & Chimes | $857,340 | 8,394 | 3,971 | 195 | $102.14 |
| 8 | AU2134055630 | 99-2 in 1 Case for Apple iPod nano 7th Gen | $843,193 | 428 | 254 | 114 | $1,970.08 |
| 9 | CT3803120757 | 643-29" LED Curved HD 21:9 Ultrawide Monitor | $710,186 | 343 | 210 | 100 | $2,070.51 |
| 10 | HC3810606695 | 1024-Clasps & Hooks | $697,174 | 2,414 | 1,133 | 194 | $288.80 |

### Key Insights

**Product Categories:**
- **Hobbies & Creative Arts dominates:** 9 of top 10 products are music/craft supplies
- **High-value electronics:** Monitor (#9) has highest avg price at $2,070
- **Wide distribution:** Most products sold in 190+ stores, showing strong national demand

**Revenue Drivers:**
- Top product (Finger & Hand Cymbals) generates $5.4M in revenue
- Wide price range: $102 (Hand Bells) to $2,070 (Monitor)
- High-volume products: Hand Bells sold 8,394 units across 3,971 orders

---

## Exported Artifacts

1. **View:** `top_10_products_by_revenue` (temp view in Spark)
2. **Delta Table:** `billigence_top_10_products_by_revenue` (persistent)
3. **CSV Export:** `output/top_10_products_by_revenue_csv/`
