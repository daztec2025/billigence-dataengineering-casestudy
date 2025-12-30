# Task 2: Top 3 Store Regions by Profit

## Objective

Identify the 3 most profitable store regions by calculating total profit (revenue - cost) for all transactions in each region.

## Methodology

```python
# Calculate profit for each transaction
profit_by_region = (
    transactions_df
    .join(stores_df, "store_key")
    .withColumn("profit", (F.col("price_per_unit") - F.col("cost_per_unit")) * F.col("quantity"))
    .groupBy("store_region")
    .agg(
        F.sum("profit").alias("total_profit"),
        F.sum(F.col("quantity") * F.col("price_per_unit")).alias("total_revenue"),
        F.sum(F.col("quantity") * F.col("cost_per_unit")).alias("total_cost"),
        F.count("*").alias("total_orders"),
        F.countDistinct("store_key").alias("store_count")
    )
    .withColumn("profit_margin_pct", F.round((F.col("total_profit") / F.col("total_revenue")) * 100, 2))
    .orderBy(F.desc("total_profit"))
    .limit(3)
)
```

## Results

### Top 3 Regions by Profit

| Rank | Region | Total Profit | Total Revenue | Profit Margin | Store Count | Orders | Unique Customers |
|------|--------|--------------|---------------|---------------|-------------|--------|------------------|
| 1 | **West** | $26,618,940 | $60,393,010 | 44.08% | 57 | 31,480 | 1,993 |
| 2 | **South** | $18,558,976 | $42,058,637 | 44.13% | 33 | 22,581 | 1,350 |
| 3 | **East** | $15,057,478 | $33,920,407 | 44.39% | 30 | 20,347 | 1,200 |

### Key Insights

**West Region:**
- Highest profitability with $26.6M in profit
- Excellent profit margin (44.08%) indicates efficient operations
- 57 stores generating strong returns
- Highest sales volume (31,480 orders)
- Largest customer base (1,993 unique customers)

**South Region:**
- Second highest profit at $18.6M
- Best profit margin (44.13%) - slightly higher than West
- Efficient operations with 33 stores
- 22,581 orders from 1,350 unique customers

**East Region:**
- Third highest profit at $15.1M
- Highest profit margin (44.39%) despite lower total profit
- Most efficient region with 30 stores
- 20,347 orders from 1,200 unique customers

## Query to View Results

```sql
SELECT * FROM billigence_top_3_regions_by_profit
ORDER BY total_profit DESC;
```

## Delta Table

**Table Name:** `billigence_top_3_regions_by_profit`

**Columns:**
- `store_region` - Region name
- `total_profit` - Total profit ($)
- `total_revenue` - Total revenue ($)
- `total_cost` - Total cost ($)
- `profit_margin_pct` - Profit margin (%)
- `total_units_sold` - Total units sold
- `total_orders` - Number of orders
- `store_count` - Number of stores
- `unique_customers` - Number of unique customers

**Data Summary:**
- Total transactions analyzed: 521,612
- Total stores: 196
- Analysis period: August 2021 - December 2022
