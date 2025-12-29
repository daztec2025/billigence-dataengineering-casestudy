"""
Billigence Case Study 2: University API Analysis
Author: Deren Ridley
Date: 2025-12-19

This script analyzes university data from the Hipolabs API:
1. Remove the "domains" column
2. Find the top 3 countries by number of universities
3. Count universities in UK, France, and China
4. Write results to a database
5. (Optional) Analyze enrollment data for 2010

Performance Optimizations Applied:
- Connection pooling with timeout configuration
- Adaptive Query Execution enabled
- Coalesce for small output files
- Parquet format for database persistence
- Single-pass aggregations
- Caching for reused DataFrames
"""

import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('output/case_study_2_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class UniversityAnalyzer:
    """
    Analyzes university data from the Hipolabs API.

    Demonstrates:
    - API data fetching
    - JSON data processing
    - PySpark transformations
    - Database integration
    """

    def __init__(self, api_url: str, output_dir: str):
        """
        Initialize the analyzer.

        Args:
            api_url: URL of the universities API
            output_dir: Directory for output files
        """
        self.api_url = api_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # =============================================================================
        # PERFORMANCE OPTIMIZATION: Spark Session Configuration
        # =============================================================================
        # - spark.sql.adaptive.enabled: Runtime query optimization
        # - spark.sql.adaptive.coalescePartitions.enabled: Combines small partitions
        # - spark.sql.shuffle.partitions: Reduced for small dataset (~10K rows)
        # =============================================================================
        self.spark = (SparkSession.builder
                      .appName("Billigence-Case-Study-2")
                      .config("spark.sql.adaptive.enabled", "true")
                      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                      .config("spark.sql.shuffle.partitions", "10")
                      .getOrCreate())

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession initialized")

    def fetch_data(self):
        """
        Fetch university data from the API.

        Returns:
            DataFrame: Spark DataFrame with university data
        """
        logger.info(f"Fetching data from {self.api_url}")

        try:
            # PERFORMANCE: Connection pooling with timeout and retry configuration
            session = requests.Session()
            session.mount('http://', requests.adapters.HTTPAdapter(
                max_retries=3,
                pool_connections=10,
                pool_maxsize=10
            ))
            response = session.get(self.api_url, timeout=30)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Fetched {len(data)} university records")

            # Save raw data
            raw_file = self.output_dir / "universities_raw.json"
            with open(raw_file, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Raw data saved to {raw_file}")

            # PERFORMANCE: Define explicit schema to avoid inference scan
            schema = StructType([
                StructField("alpha_two_code", StringType(), True),
                StructField("country", StringType(), True),
                StructField("domains", ArrayType(StringType()), True),
                StructField("name", StringType(), True),
                StructField("state-province", StringType(), True),
                StructField("web_pages", ArrayType(StringType()), True)
            ])

            # Convert to Spark DataFrame - use spark.read.json for proper field matching
            df = self.spark.read.json(
                self.spark.sparkContext.parallelize([json.dumps(record) for record in data]),
                schema=schema
            )

            # PERFORMANCE: Cache DataFrame as it will be used multiple times
            df.cache()

            return df

        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            logger.info("Attempting to load from local fallback file...")

            # Try to load from local fallback file
            fallback_path = Path(__file__).parent.parent / "data" / "case_study_2" / "universities_raw.json"
            if fallback_path.exists():
                logger.info(f"Loading from fallback: {fallback_path}")
                with open(fallback_path, 'r') as f:
                    data = json.load(f)

                schema = StructType([
                    StructField("alpha_two_code", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("domains", ArrayType(StringType()), True),
                    StructField("name", StringType(), True),
                    StructField("state-province", StringType(), True),
                    StructField("web_pages", ArrayType(StringType()), True)
                ])

                # Use spark.read.json for proper field matching by name
                df = self.spark.read.json(
                    self.spark.sparkContext.parallelize([json.dumps(record) for record in data]),
                    schema=schema
                )
                df.cache()
                logger.info(f"Loaded {df.count()} records from fallback")
                return df
            else:
                logger.error(f"Fallback file not found: {fallback_path}")
                raise

    def remove_domains_column(self, df):
        """
        Task 1: Remove the "domains" column.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame: DataFrame without domains column
        """
        logger.info("Task 1: Removing 'domains' column")

        # Check if domains column exists
        if "domains" in df.columns:
            df_clean = df.drop("domains")
            logger.info(f"Removed 'domains' column. Remaining columns: {df_clean.columns}")
        else:
            logger.warning("'domains' column not found in DataFrame")
            df_clean = df

        return df_clean

    def find_top_countries(self, df, top_n=3):
        """
        Task 2: Find the top N countries by number of universities.

        Args:
            df: Input DataFrame
            top_n: Number of top countries to return

        Returns:
            DataFrame: Top N countries with university counts
        """
        logger.info(f"Task 2: Finding top {top_n} countries by university count")

        # PERFORMANCE: Single-pass aggregation with optimized groupBy
        top_countries = (
            df.groupBy("country")
            .agg(F.count("*").alias("university_count"))
            .orderBy(F.desc("university_count"))
            .limit(top_n)
        )

        logger.info(f"Top {top_n} countries:")
        top_countries.show(truncate=False)

        # PERFORMANCE: Coalesce to single file for small result set
        (top_countries
         .coalesce(1)
         .write
         .mode("overwrite")
         .option("header", "true")
         .csv(str(self.output_dir / "top_3_countries_csv")))

        return top_countries

    def count_specific_countries(self, df, countries):
        """
        Task 3: Count universities in specific countries.

        Args:
            df: Input DataFrame
            countries: List of country names to count

        Returns:
            DataFrame: University counts for specified countries
        """
        logger.info(f"Task 3: Counting universities in {countries}")

        # Note: UK is stored as "United Kingdom" in the API
        country_mapping = {
            "UK": "United Kingdom",
            "France": "France",
            "China": "China"
        }

        results = []
        for country_input in countries:
            # Get actual country name from mapping
            country_name = country_mapping.get(country_input, country_input)

            count = df.filter(F.col("country") == country_name).count()
            results.append({
                "country": country_input,
                "actual_country_name": country_name,
                "university_count": count
            })
            logger.info(f"{country_input} ({country_name}): {count} universities")

        # Create DataFrame
        results_df = self.spark.createDataFrame(results)

        results_df.show(truncate=False)

        # Save to CSV
        (results_df
         .coalesce(1)
         .write
         .mode("overwrite")
         .option("header", "true")
         .csv(str(self.output_dir / "uk_france_china_counts_csv")))

        return results_df

    def write_to_database(self, df, table_name):
        """
        Task 4: Write results to a database.

        For this demonstration, we'll use:
        - SQLite (lightweight, no setup required)
        - Delta Lake format (production-grade)

        Args:
            df: DataFrame to write
            table_name: Name of the table
        """
        logger.info(f"Task 4: Writing results to database: {table_name}")

        # PERFORMANCE: Parquet format for columnar storage and compression
        # Benefits: 75% smaller files, 5-10x faster reads, built-in schema
        parquet_path = self.output_dir / "parquet" / table_name
        (df
         .coalesce(1)
         .write
         .mode("overwrite")
         .parquet(str(parquet_path)))
        logger.info(f"Saved as Parquet: {parquet_path}")

        # Option 3: Convert to Pandas and save to SQLite
        try:
            import sqlite3

            db_path = self.output_dir / "universities.db"
            conn = sqlite3.connect(str(db_path))

            # Convert to Pandas for SQLite
            pandas_df = df.toPandas()
            pandas_df.to_sql(table_name, conn, if_exists='replace', index=False)

            conn.close()
            logger.info(f"Saved to SQLite database: {db_path}, table: {table_name}")

            # Verify
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            logger.info(f"Verified {count} records in SQLite table")
            conn.close()

        except Exception as e:
            logger.warning(f"Could not save to SQLite: {str(e)}")

    def generate_summary(self, df):
        """
        Generate summary statistics about the university data.

        Args:
            df: Input DataFrame
        """
        logger.info("Generating summary statistics...")

        # Total universities
        total = df.count()
        logger.info(f"Total universities: {total:,}")

        # Countries represented
        countries = df.select("country").distinct().count()
        logger.info(f"Countries represented: {countries}")

        # Top 10 countries
        logger.info("Top 10 countries by university count:")
        (df.groupBy("country")
         .agg(F.count("*").alias("count"))
         .orderBy(F.desc("count"))
         .limit(10)
         .show(truncate=False))

        # Universities by alpha code
        logger.info("Sample of countries with their codes:")
        (df.select("country", "alpha_two_code")
         .distinct()
         .orderBy("country")
         .limit(20)
         .show(truncate=False))

    def stop(self):
        """Stop Spark session."""
        self.spark.stop()
        logger.info("SparkSession stopped")


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("BILLIGENCE CASE STUDY 2 - UNIVERSITY API ANALYSIS")
    logger.info("=" * 80)

    # Configuration
    API_URL = "http://universities.hipolabs.com/search"
    OUTPUT_DIR = "output/case_study_2"

    # Initialize analyzer
    analyzer = UniversityAnalyzer(API_URL, OUTPUT_DIR)

    try:
        # Fetch data from API
        df = analyzer.fetch_data()

        # Task 1: Remove domains column
        df_clean = analyzer.remove_domains_column(df)

        # Generate summary statistics
        analyzer.generate_summary(df_clean)

        # Task 2: Find top 3 countries
        top_3_countries = analyzer.find_top_countries(df_clean, top_n=3)

        # Task 3: Count universities in UK, France, and China
        specific_counts = analyzer.count_specific_countries(
            df_clean,
            countries=["UK", "France", "China"]
        )

        # Task 4: Write to database
        analyzer.write_to_database(specific_counts, "uk_france_china_universities")
        analyzer.write_to_database(top_3_countries, "top_3_countries_universities")

        logger.info("=" * 80)
        logger.info("ANALYSIS COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Results saved to {OUTPUT_DIR}/")

    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}", exc_info=True)
        raise

    finally:
        analyzer.stop()


if __name__ == "__main__":
    main()
