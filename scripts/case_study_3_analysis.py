"""
Billigence Case Study 3: Customer Data Restructuring & Email Protection
Author: Deren Ridley
Date: 2025-12-19

This script demonstrates:
1. Converting nested JSON to tabular format
2. Masking/encrypting email addresses while preserving domain names
3. Data quality validation and security best practices

Performance Optimizations Applied:
- Adaptive Query Execution enabled
- Optimized shuffle partitions for small dataset
- Native PySpark functions for hashing (no UDFs)
- Single-pass transformations with chained withColumn
- Coalesce for small output files
- Parquet format for persistent storage
"""

import hashlib
import json
import logging
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output/case_study_3_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CustomerDataProcessor:
    """
    Process customer data from nested JSON to secure tabular format.

    Demonstrates:
    - JSON flattening
    - Email masking/encryption
    - Data quality validation
    - Security best practices
    """

    def __init__(self, json_file: str, output_dir: str):
        """
        Initialize the processor.

        Args:
            json_file: Path to input JSON file
            output_dir: Directory for output files
        """
        self.json_file = Path(json_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # =============================================================================
        # PERFORMANCE OPTIMIZATION: Spark Session Configuration
        # =============================================================================
        # - spark.sql.adaptive.enabled: Runtime query optimization
        # - spark.sql.adaptive.coalescePartitions.enabled: Combines small partitions
        # - spark.sql.shuffle.partitions: Reduced for small dataset (~100 rows)
        # =============================================================================
        self.spark = (SparkSession.builder
                      .appName("Billigence-Case-Study-3")
                      .config("spark.sql.adaptive.enabled", "true")
                      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                      .config("spark.sql.shuffle.partitions", "4")
                      .getOrCreate())

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession initialized")

    def load_json_data(self):
        """
        Load nested JSON data.

        Returns:
            DataFrame: Raw JSON data as Spark DataFrame
        """
        logger.info(f"Loading JSON from {self.json_file}")

        try:
            # Read multi-line JSON
            df = self.spark.read.option("multiLine", "true").json(str(self.json_file))

            logger.info(f"Loaded JSON successfully")
            logger.info("Schema:")
            df.printSchema()

            return df

        except Exception as e:
            logger.error(f"Error loading JSON: {str(e)}")
            raise

    def flatten_to_tabular(self, raw_df):
        """
        Task 1: Convert nested JSON to tabular format.

        Args:
            raw_df: Raw DataFrame with nested structure

        Returns:
            DataFrame: Flattened tabular DataFrame
        """
        logger.info("Task 1: Converting nested JSON to tabular format")

        # Step 1: Explode CompanyInfo array
        customers_exploded = raw_df.select(
            F.col("CompanyID"),
            F.explode(F.col("CompanyInfo")).alias("company_info")
        )

        # Step 2: Extract company name and explode Customers array
        customers_flat = customers_exploded.select(
            F.col("CompanyID").alias("company_id"),
            F.col("company_info.Name").alias("company_name"),
            F.explode(F.col("company_info.Customers")).alias("customer")
        )

        # Step 3: Flatten all customer fields
        customers_tabular = customers_flat.select(
            F.col("company_id"),
            F.col("company_name"),
            F.col("customer.Name").alias("customer_name"),
            F.col("customer.Email").alias("email"),
            F.col("customer.Birth").alias("date_of_birth"),
            F.col("customer.Place of Birth").alias("place_of_birth"),
            F.col("customer.Phone").alias("phone_number"),
            F.col("customer.Role").alias("role")
        )

        # Step 4: Add data type conversions and enrichments
        customers_enhanced = customers_tabular.withColumn(
            "date_of_birth", F.to_date(F.col("date_of_birth"))
        ).withColumn(
            "age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
        ).withColumn(
            "load_timestamp", F.current_timestamp()
        ).withColumn(
            "customer_id", F.monotonically_increasing_id()
        )

        logger.info(f"Converted to tabular format: {customers_enhanced.count()} records")
        logger.info("Final schema:")
        customers_enhanced.printSchema()

        return customers_enhanced

    def mask_emails(self, df):
        """
        Task 2: Mask/encrypt email addresses.

        Implements three approaches:
        1. Display masking (*******@domain.com)
        2. SHA-256 cryptographic hashing
        3. Domain extraction for analytics

        Args:
            df: DataFrame with email column

        Returns:
            DataFrame: DataFrame with masked emails
        """
        logger.info("Task 2: Masking/encrypting email addresses")

        # =============================================================================
        # PERFORMANCE OPTIMIZATION: Native PySpark Functions
        # =============================================================================
        # Using built-in sha2(), md5(), regexp_extract() instead of UDFs
        # Benefit: 10-100x faster than Python UDFs (runs in JVM, not Python)
        # =============================================================================
        df_masked = df.withColumn(
            # Extract domain (everything after @)
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        ).withColumn(
            # Create masked email with asterisks
            "email_masked", F.concat(
                F.lit("*******@"),
                F.col("email_domain")
            )
        ).withColumn(
            # SHA-256 hash for secure storage
            "email_sha256", F.sha2(F.col("email"), 256)
        ).withColumn(
            # MD5 hash (alternative, shorter)
            "email_md5", F.md5(F.col("email"))
        )

        logger.info("Email masking complete:")
        logger.info("  - Display masking: *******@domain.com")
        logger.info("  - SHA-256 hashing applied")
        logger.info("  - Domain extraction complete")

        # Show sample
        sample = df_masked.select("customer_name", "email", "email_masked", "email_sha256").limit(5)
        logger.info("\nSample masked emails:")
        sample.show(truncate=False)

        return df_masked

    def create_secure_dataset(self, df):
        """
        Create production-ready secure dataset.

        Removes original email, keeps only masked version.

        Args:
            df: DataFrame with all email variations

        Returns:
            DataFrame: Secure dataset without plain text emails
        """
        logger.info("Creating secure production dataset")

        secure_df = df.select(
            F.col("customer_id"),
            F.col("company_id"),
            F.col("company_name"),
            F.col("customer_name"),
            F.col("email_masked").alias("email"),  # Only masked version
            F.col("email_sha256"),  # Hash for verification
            F.col("date_of_birth"),
            F.col("age"),
            F.col("place_of_birth"),
            F.col("phone_number"),
            F.col("role"),
            F.col("load_timestamp")
        )

        logger.info(f"Secure dataset created: {secure_df.count()} records")
        return secure_df

    def create_reference_table(self, df):
        """
        Create email reference table for authorized access.

        Maps hashes to original emails (restricted access).

        Args:
            df: DataFrame with all email variations

        Returns:
            DataFrame: Reference mapping table
        """
        logger.info("Creating email reference table (restricted access)")

        reference_df = df.select(
            F.col("email_sha256").alias("email_hash"),
            F.col("email").alias("original_email"),
            F.col("email_masked"),
            F.col("email_domain"),
            F.current_timestamp().alias("created_at")
        ).distinct()

        logger.info(f"Reference table created: {reference_df.count()} unique emails")
        logger.info("WARNING: This table contains PII and should have restricted access!")

        return reference_df

    def validate_data_quality(self, original_df, final_df):
        """
        Validate data transformation quality.

        Args:
            original_df: Original raw data
            final_df: Final transformed data

        Returns:
            dict: Validation results
        """
        logger.info("Validating data quality...")

        results = {}

        # PERFORMANCE: Single-pass validation using aggregations
        # Check 1: Row count
        original_count = original_df.count()
        final_count = final_df.count()
        results['row_count'] = {
            'original': original_count,
            'final': final_count,
            'status': 'PASS' if final_count > 0 else 'FAIL'
        }
        logger.info(f"Row count: {original_count} -> {final_count} ({results['row_count']['status']})")

        # Check 2: Null values
        null_checks = final_df.select(
            F.sum(F.when(F.col("customer_name").isNull(), 1).otherwise(0)).alias("null_names"),
            F.sum(F.when(F.col("email").isNull(), 1).otherwise(0)).alias("null_emails"),
            F.sum(F.when(F.col("email_sha256").isNull(), 1).otherwise(0)).alias("null_hashes")
        ).collect()[0]

        null_total = sum([null_checks['null_names'], null_checks['null_emails'], null_checks['null_hashes']])
        results['null_check'] = {
            'null_names': null_checks['null_names'],
            'null_emails': null_checks['null_emails'],
            'null_hashes': null_checks['null_hashes'],
            'status': 'PASS' if null_total == 0 else 'FAIL'
        }
        logger.info(f"Null checks: {results['null_check']['status']}")

        # Check 3: Email masking
        masked_check = final_df.filter(
            F.col("email").rlike(r"^\*+@.+\..+$")
        ).count()
        results['email_masking'] = {
            'properly_masked': masked_check,
            'total': final_count,
            'status': 'PASS' if masked_check == final_count else 'FAIL'
        }
        logger.info(f"Email masking: {masked_check}/{final_count} ({results['email_masking']['status']})")

        # Check 4: Hash uniqueness
        unique_hashes = final_df.select("email_sha256").distinct().count()
        results['hash_uniqueness'] = {
            'unique': unique_hashes,
            'total': final_count,
            'status': 'PASS' if unique_hashes == final_count else 'WARNING'
        }
        logger.info(f"Hash uniqueness: {unique_hashes}/{final_count} ({results['hash_uniqueness']['status']})")

        return results

    def save_to_database(self, df, table_name):
        """
        Save data to multiple formats.

        Args:
            df: DataFrame to save
            table_name: Name for the output table
        """
        logger.info(f"Saving data: {table_name}")

        # =============================================================================
        # PERFORMANCE OPTIMIZATION: Parquet Format
        # =============================================================================
        # Benefits: Columnar storage, built-in compression, embedded schema
        # 75% smaller files, 5-10x faster reads than CSV/JSON
        # =============================================================================
        parquet_path = self.output_dir / "parquet" / table_name
        (df.coalesce(1)
         .write
         .mode("overwrite")
         .parquet(str(parquet_path)))
        logger.info(f"  - Parquet: {parquet_path}")

        # Save as CSV
        csv_path = self.output_dir / f"{table_name}_csv"
        (df.coalesce(1)
         .write
         .mode("overwrite")
         .option("header", "true")
         .csv(str(csv_path)))
        logger.info(f"  - CSV: {csv_path}")

    def generate_summary(self, secure_df, reference_df):
        """
        Generate summary statistics.

        Args:
            secure_df: Secure customer dataset
            reference_df: Email reference table
        """
        logger.info("Generating summary statistics...")

        print("\n" + "=" * 80)
        print("SUMMARY STATISTICS")
        print("=" * 80)

        # Total customers
        total = secure_df.count()
        print(f"\nTotal Customers: {total}")

        # Customers by role
        print("\nCustomers by Role:")
        secure_df.groupBy("role").count().orderBy(F.desc("count")).show(truncate=False)

        # Email domains
        print("\nEmail Domains:")
        (secure_df.withColumn("domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1))
         .groupBy("domain")
         .count()
         .orderBy(F.desc("count"))
         .show(truncate=False))

        # Age distribution
        print("\nAge Statistics:")
        secure_df.select(
            F.min("age").alias("min_age"),
            F.max("age").alias("max_age"),
            F.avg("age").alias("avg_age")
        ).show()

    def stop(self):
        """Stop Spark session."""
        self.spark.stop()
        logger.info("SparkSession stopped")


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("BILLIGENCE CASE STUDY 3 - CUSTOMER DATA RESTRUCTURING")
    logger.info("=" * 80)

    # Configuration
    JSON_FILE = "data/case_study_3/Customers.json"
    OUTPUT_DIR = "output/case_study_3"

    # Initialize processor
    processor = CustomerDataProcessor(JSON_FILE, OUTPUT_DIR)

    try:
        # Load JSON data
        raw_df = processor.load_json_data()

        # Task 1: Convert to tabular format
        tabular_df = processor.flatten_to_tabular(raw_df)

        # Task 2: Mask emails
        masked_df = processor.mask_emails(tabular_df)

        # Create secure dataset (no plain text emails)
        secure_df = processor.create_secure_dataset(masked_df)

        # Create reference table (restricted access)
        reference_df = processor.create_reference_table(masked_df)

        # Validate data quality
        validation_results = processor.validate_data_quality(raw_df, secure_df)

        # Save to database
        processor.save_to_database(secure_df, "abc_customers_secure")
        processor.save_to_database(reference_df, "abc_email_reference")

        # Generate summary
        processor.generate_summary(secure_df, reference_df)

        logger.info("=" * 80)
        logger.info("ANALYSIS COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Results saved to {OUTPUT_DIR}/")

        # Print validation summary
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)
        for check_name, check_results in validation_results.items():
            print(f"\n{check_name}: {check_results['status']}")
            for key, value in check_results.items():
                if key != 'status':
                    print(f"  {key}: {value}")

    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}", exc_info=True)
        raise

    finally:
        processor.stop()


if __name__ == "__main__":
    main()
