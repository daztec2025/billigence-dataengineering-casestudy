# Dataset Description

## Overview

Retail electronics sales data consisting of 521,612 transactions across 196 stores and 1,095 products over 1.5 years (2021-2022).

## Data Files

### Transaction_Details.csv (521,612 rows)

Sales transactions with pricing and customer information.

**Key columns:**
- Date, Order Number
- Product Name, SKU Number
- Quantity, Cost, Price
- Store Key, Customer Key

### Store_Details.csv (196 rows)

Store locations and regional assignments.

**Key columns:**
- Store Key (primary key)
- Store Region (East, West, Central)
- Store Name

### Product_Details.csv (1,095 rows)

Product catalog with hierarchical categories.

**Key columns:**
- SKU Number (primary key)
- Product Name
- Product Type, Product Family

## Data Structure

**Star Schema:**
- Fact table: Transactions
- Dimension tables: Stores, Products

**Relationships:**
- Transactions.store_key → Stores.store_key
- Transactions.sku_number → Products.sku_number
