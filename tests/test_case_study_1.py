"""
Unit Tests for Case Study 1: Retail Electronics Analysis

Tests cover:
- Data loading and schema validation
- Data quality checks
- Business logic transformations
- Aggregation calculations
- Filter conditions
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


class TestCaseStudy1(unittest.TestCase):
    """Comprehensive unit tests for Case Study 1"""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session and load data once for all tests"""
        cls.spark = SparkSession.builder \
            .appName("Test_Case_Study_1") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()

        # Define schemas
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

        # Load data
        cls.transactions_df = cls.spark.read \
            .option("header", "true") \
            .schema(transaction_schema) \
            .csv("/workspaces/LocalSpark/data/case_study_1/Transaction_Details.csv")

        cls.stores_df = cls.spark.read \
            .option("header", "true") \
            .schema(store_schema) \
            .csv("/workspaces/LocalSpark/data/case_study_1/Store_Details.csv")

        cls.products_df = cls.spark.read \
            .option("header", "true") \
            .schema(product_schema) \
            .csv("/workspaces/LocalSpark/data/case_study_1/Product_Details.csv")

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session after all tests"""
        cls.spark.stop()

    # Data Loading Tests
    def test_transaction_count(self):
        """Verify expected number of transactions loaded"""
        count = self.transactions_df.count()
        self.assertGreater(count, 500000, f"Expected >500k transactions, got {count}")

    def test_store_count(self):
        """Verify expected number of stores loaded"""
        count = self.stores_df.count()
        self.assertGreater(count, 150, f"Expected >150 stores, got {count}")

    def test_product_count(self):
        """Verify expected number of products loaded"""
        count = self.products_df.count()
        self.assertGreater(count, 1000, f"Expected >1000 products, got {count}")

    def test_transaction_schema(self):
        """Verify transaction schema has all required columns"""
        columns = self.transactions_df.columns
        required = ["Date", "Order Number", "Sku Number", "Quantity",
                   "Cost", "Price", "Store Key"]
        for col in required:
            self.assertIn(col, columns, f"Missing required column: {col}")

    def test_store_schema(self):
        """Verify store schema has all required columns"""
        columns = self.stores_df.columns
        required = ["Store Key", "Store Name", "Store Region"]
        for col in required:
            self.assertIn(col, columns, f"Missing required column: {col}")

    def test_product_schema(self):
        """Verify product schema has all required columns"""
        columns = self.products_df.columns
        required = ["Sku Number", "Product Name", "Product Type"]
        for col in required:
            self.assertIn(col, columns, f"Missing required column: {col}")

    # Data Quality Tests
    def test_no_null_transaction_keys(self):
        """Verify no null values in critical transaction columns"""
        null_counts = self.transactions_df.select(
            F.sum(F.when(F.col("Order Number").isNull(), 1).otherwise(0)).alias("order_nulls"),
            F.sum(F.when(F.col("Sku Number").isNull(), 1).otherwise(0)).alias("sku_nulls"),
            F.sum(F.when(F.col("Store Key").isNull(), 1).otherwise(0)).alias("store_nulls")
        ).collect()[0]

        self.assertEqual(null_counts.order_nulls, 0, "Found null order numbers")
        self.assertEqual(null_counts.sku_nulls, 0, "Found null SKU numbers")
        self.assertEqual(null_counts.store_nulls, 0, "Found null store keys")

    def test_no_negative_quantities(self):
        """Verify no negative quantities"""
        negative_count = self.transactions_df.filter(F.col("Quantity") < 0).count()
        self.assertEqual(negative_count, 0, f"Found {negative_count} negative quantities")

    def test_no_negative_prices(self):
        """Verify no negative prices"""
        negative_count = self.transactions_df.filter(F.col("Price") < 0).count()
        self.assertEqual(negative_count, 0, f"Found {negative_count} negative prices")

    def test_no_negative_costs(self):
        """Verify no negative costs"""
        negative_count = self.transactions_df.filter(F.col("Cost") < 0).count()
        self.assertEqual(negative_count, 0, f"Found {negative_count} negative costs")

    def test_price_greater_than_cost(self):
        """Verify prices are not below costs"""
        below_cost = self.transactions_df.filter(F.col("Price") < F.col("Cost")).count()
        self.assertEqual(below_cost, 0, f"Found {below_cost} transactions with price below cost")

    def test_referential_integrity_stores(self):
        """Verify all store keys reference valid stores"""
        orphaned = self.transactions_df \
            .join(self.stores_df, self.transactions_df["Store Key"] == self.stores_df["Store Key"], "left") \
            .where(F.col("Store Name").isNull()) \
            .count()
        self.assertEqual(orphaned, 0, f"Found {orphaned} transactions with invalid store keys")

    def test_referential_integrity_products(self):
        """Verify all SKU numbers reference valid products"""
        orphaned = self.transactions_df \
            .join(self.products_df, self.transactions_df["Sku Number"] == self.products_df["Sku Number"], "left") \
            .where(F.col("Product Type").isNull()) \
            .count()
        self.assertEqual(orphaned, 0, f"Found {orphaned} transactions with invalid SKU numbers")

    def test_unique_store_keys(self):
        """Verify store keys are unique"""
        total_count = self.stores_df.count()
        distinct_count = self.stores_df.select("Store Key").distinct().count()
        self.assertEqual(total_count, distinct_count, "Store keys are not unique")

    def test_unique_sku_numbers(self):
        """Verify SKU numbers are unique"""
        total_count = self.products_df.count()
        distinct_count = self.products_df.select("Sku Number").distinct().count()
        self.assertEqual(total_count, distinct_count, "SKU numbers are not unique")

    # Business Logic Tests
    def test_store_443_exists(self):
        """Verify Store 443 exists before applying exclusion filter"""
        store_443_count = self.stores_df.filter(F.col("Store Key") == 443).count()
        self.assertEqual(store_443_count, 1, "Store 443 not found in dataset")

    def test_revenue_calculation(self):
        """Verify revenue calculation: quantity * price"""
        test_data = [
            (1, 10, 5.0, 2.0),
            (2, 5, 10.0, 3.0)
        ]
        df = self.spark.createDataFrame(test_data, ["id", "quantity", "price", "cost"])
        result = df.withColumn("revenue", F.col("quantity") * F.col("price"))
        revenues = result.select("revenue").collect()

        self.assertEqual(revenues[0].revenue, 50.0, "Revenue calculation incorrect for row 1")
        self.assertEqual(revenues[1].revenue, 50.0, "Revenue calculation incorrect for row 2")

    def test_profit_calculation(self):
        """Verify profit calculation: quantity * (price - cost)"""
        test_data = [
            (1, 10, 5.0, 2.0),
            (2, 5, 10.0, 4.0)
        ]
        df = self.spark.createDataFrame(test_data, ["id", "quantity", "price", "cost"])
        result = df.withColumn("profit", F.col("quantity") * (F.col("price") - F.col("cost")))
        profits = result.select("profit").collect()

        self.assertEqual(profits[0].profit, 30.0, "Profit calculation incorrect for row 1")
        self.assertEqual(profits[1].profit, 30.0, "Profit calculation incorrect for row 2")

    def test_profit_margin_calculation(self):
        """Verify profit margin calculation: (profit / revenue) * 100"""
        test_data = [
            (50.0, 100.0),
            (25.0, 100.0)
        ]
        df = self.spark.createDataFrame(test_data, ["profit", "revenue"])
        result = df.withColumn("margin_pct", (F.col("profit") / F.col("revenue")) * 100)
        margins = result.select("margin_pct").collect()

        self.assertEqual(margins[0].margin_pct, 50.0, "Margin calculation incorrect for row 1")
        self.assertEqual(margins[1].margin_pct, 25.0, "Margin calculation incorrect for row 2")

    # Aggregation Tests
    def test_top_products_aggregation(self):
        """Test top products by revenue aggregation logic"""
        result = self.transactions_df \
            .filter(F.col("Store Key") != 443) \
            .join(self.products_df, "Sku Number") \
            .groupBy("Sku Number") \
            .agg(
                F.round(F.sum(F.col("Quantity") * F.col("Price")), 2).alias("total_revenue"),
                F.sum("Quantity").alias("total_units")
            ) \
            .filter(F.col("total_revenue") > 500000) \
            .orderBy(F.desc("total_revenue"))

        count = result.count()
        self.assertGreater(count, 0, "No products found with revenue > $500k")

        null_revenue = result.filter(F.col("total_revenue").isNull()).count()
        self.assertEqual(null_revenue, 0, "Found null revenue values")

        revenues = [row.total_revenue for row in result.collect()]
        self.assertEqual(revenues, sorted(revenues, reverse=True), "Results not ordered by revenue desc")

    def test_regional_profit_aggregation(self):
        """Test regional profit aggregation logic"""
        result = self.transactions_df \
            .join(self.stores_df, "Store Key") \
            .withColumn("profit", F.col("Quantity") * (F.col("Price") - F.col("Cost"))) \
            .groupBy("Store Region") \
            .agg(
                F.round(F.sum("profit"), 2).alias("total_profit"),
                F.countDistinct("Store Key").alias("store_count")
            ) \
            .orderBy(F.desc("total_profit"))

        count = result.count()
        self.assertGreaterEqual(count, 3, f"Expected at least 3 regions, got {count}")

        null_profit = result.filter(F.col("total_profit").isNull()).count()
        self.assertEqual(null_profit, 0, "Found null profit values")

        profits = [row.total_profit for row in result.collect()]
        self.assertEqual(profits, sorted(profits, reverse=True), "Results not ordered by profit desc")

    def test_store_exclusion_filter(self):
        """Verify Store 443 exclusion filter works correctly"""
        all_count = self.transactions_df.count()
        filtered_count = self.transactions_df.filter(F.col("Store Key") != 443).count()
        store_443_count = self.transactions_df.filter(F.col("Store Key") == 443).count()

        self.assertEqual(filtered_count, all_count - store_443_count,
                        "Store 443 exclusion filter not working correctly")
        self.assertLess(filtered_count, all_count, "Filter should exclude some records")

    def test_revenue_threshold_filter(self):
        """Verify revenue threshold filter works correctly"""
        aggregated = self.transactions_df \
            .join(self.products_df, "Sku Number") \
            .groupBy("Sku Number") \
            .agg(F.round(F.sum(F.col("Quantity") * F.col("Price")), 2).alias("revenue"))

        above_threshold = aggregated.filter(F.col("revenue") > 500000).count()
        below_threshold = aggregated.filter(F.col("revenue") <= 500000).count()

        self.assertGreater(above_threshold, 0, "No products above $500k threshold")
        self.assertGreater(below_threshold, 0, "No products below $500k threshold")
        self.assertEqual(above_threshold + below_threshold, aggregated.count(),
                        "Filter logic inconsistent")

    # Output Validation Tests
    def test_top_products_limit_10(self):
        """Verify top products returns exactly 10 results"""
        result = self.transactions_df \
            .filter(F.col("Store Key") != 443) \
            .join(self.products_df, "Sku Number") \
            .groupBy("Sku Number") \
            .agg(F.round(F.sum(F.col("Quantity") * F.col("Price")), 2).alias("revenue")) \
            .filter(F.col("revenue") > 500000) \
            .orderBy(F.desc("revenue")) \
            .limit(10)

        count = result.count()
        self.assertEqual(count, 10, f"Expected exactly 10 products, got {count}")

    def test_top_regions_limit_3(self):
        """Verify top regions returns exactly 3 results"""
        result = self.transactions_df \
            .join(self.stores_df, "Store Key") \
            .withColumn("profit", F.col("Quantity") * (F.col("Price") - F.col("Cost"))) \
            .groupBy("Store Region") \
            .agg(F.sum("profit").alias("total_profit")) \
            .orderBy(F.desc("total_profit")) \
            .limit(3)

        count = result.count()
        self.assertEqual(count, 3, f"Expected exactly 3 regions, got {count}")


if __name__ == '__main__':
    unittest.main()
