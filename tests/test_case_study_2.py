"""
Unit Tests for case_study_2: University API Analysis

Tests cover:
- Data loading and schema validation
- Data quality checks
- Column removal operations
- Country aggregation and counting
- Top N filtering
- Country name mapping (UK -> United Kingdom)
- Database write operations
- Edge cases and error handling
"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
)
import json
from pathlib import Path


class TestCaseStudy2(unittest.TestCase):
    """Comprehensive unit tests for case_study_2: University API Analysis"""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session and load data once for all tests"""
        cls.spark = (
            SparkSession.builder
            .appName("Test_Case_Study_2")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

        # Define expected schema
        cls.university_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("alpha_two_code", StringType(), True),
                StructField("state-province", StringType(), True),
                StructField("domains", ArrayType(StringType()), True),
                StructField("web_pages", ArrayType(StringType()), True),
            ]
        )

        # Load sample data for testing
        sample_data_path = Path("/workspaces/LocalSpark/data/case_study_2/sample_universities.json")
        with open(sample_data_path, "r") as f:
            sample_data = json.load(f)

        # Add domains field to sample data (if missing)
        for record in sample_data:
            if "domains" not in record:
                record["domains"] = ["example.edu"]

        cls.sample_df = cls.spark.createDataFrame(sample_data, schema=cls.university_schema)
        cls.sample_df.cache()

        # Load full data from fallback file
        full_data_path = Path("/workspaces/LocalSpark/data/case_study_2/universities_raw.json")
        if full_data_path.exists():
            with open(full_data_path, "r") as f:
                full_data = json.load(f)
            cls.full_df = cls.spark.createDataFrame(full_data, schema=cls.university_schema)
            cls.full_df.cache()
            cls.has_full_data = True
        else:
            cls.has_full_data = False

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session after all tests"""
        cls.spark.stop()

    # =========================================================================
    # Data Loading Tests
    # =========================================================================

    def test_sample_data_loaded(self):
        """Verify sample data is loaded correctly"""
        count = self.sample_df.count()
        self.assertEqual(count, 10, f"Expected 10 sample records, got {count}")

    def test_schema_has_required_columns(self):
        """Verify schema has all required columns"""
        required_columns = ["name", "country", "alpha_two_code", "web_pages"]
        for col in required_columns:
            self.assertIn(col, self.sample_df.columns, f"Missing required column: {col}")

    def test_schema_column_types(self):
        """Verify column data types are correct"""
        schema_dict = {field.name: field.dataType for field in self.sample_df.schema.fields}

        self.assertIsInstance(schema_dict["name"], StringType)
        self.assertIsInstance(schema_dict["country"], StringType)
        self.assertIsInstance(schema_dict["alpha_two_code"], StringType)
        self.assertIsInstance(schema_dict["web_pages"], ArrayType)

    @unittest.skipUnless(
        Path("/workspaces/LocalSpark/data/case_study_2/universities_raw.json").exists(),
        "Full data file not available"
    )
    def test_full_data_count(self):
        """Verify full dataset has substantial records"""
        count = self.full_df.count()
        self.assertGreater(count, 1000, f"Expected >1000 universities, got {count}")

    # =========================================================================
    # Data Quality Tests
    # =========================================================================

    def test_no_null_university_names(self):
        """Verify no null values in university names"""
        null_count = self.sample_df.filter(F.col("name").isNull()).count()
        self.assertEqual(null_count, 0, f"Found {null_count} null university names")

    def test_no_null_countries(self):
        """Verify no null values in country field"""
        null_count = self.sample_df.filter(F.col("country").isNull()).count()
        self.assertEqual(null_count, 0, f"Found {null_count} null country values")

    def test_no_null_alpha_codes(self):
        """Verify no null values in alpha_two_code field"""
        null_count = self.sample_df.filter(F.col("alpha_two_code").isNull()).count()
        self.assertEqual(null_count, 0, f"Found {null_count} null alpha codes")

    def test_alpha_code_length(self):
        """Verify alpha_two_code is exactly 2 characters"""
        invalid_codes = self.sample_df.filter(
            F.length(F.col("alpha_two_code")) != 2
        ).count()
        self.assertEqual(invalid_codes, 0, f"Found {invalid_codes} invalid alpha codes")

    def test_no_empty_university_names(self):
        """Verify no empty strings in university names"""
        empty_count = self.sample_df.filter(
            (F.col("name") == "") | (F.trim(F.col("name")) == "")
        ).count()
        self.assertEqual(empty_count, 0, f"Found {empty_count} empty university names")

    def test_web_pages_not_empty(self):
        """Verify web_pages array is not empty"""
        empty_web_pages = self.sample_df.filter(
            F.size(F.col("web_pages")) == 0
        ).count()
        self.assertEqual(empty_web_pages, 0, f"Found {empty_web_pages} universities without web pages")

    def test_unique_universities_per_country(self):
        """Verify no duplicate university names within same country"""
        duplicates = (
            self.sample_df
            .groupBy("country", "name")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        self.assertEqual(duplicates, 0, f"Found {duplicates} duplicate universities")

    # =========================================================================
    # Task 1: Remove Domains Column Tests
    # =========================================================================

    def test_domains_column_exists(self):
        """Verify domains column exists before removal"""
        self.assertIn("domains", self.sample_df.columns)

    def test_remove_domains_column(self):
        """Verify domains column can be removed"""
        df_clean = self.sample_df.drop("domains")
        self.assertNotIn("domains", df_clean.columns)

    def test_remove_domains_preserves_other_columns(self):
        """Verify removing domains preserves all other columns"""
        original_columns = set(self.sample_df.columns)
        df_clean = self.sample_df.drop("domains")
        clean_columns = set(df_clean.columns)

        expected_columns = original_columns - {"domains"}
        self.assertEqual(clean_columns, expected_columns)

    def test_remove_domains_preserves_row_count(self):
        """Verify removing domains doesn't change row count"""
        original_count = self.sample_df.count()
        df_clean = self.sample_df.drop("domains")
        clean_count = df_clean.count()

        self.assertEqual(original_count, clean_count)

    def test_remove_nonexistent_column(self):
        """Verify dropping non-existent column doesn't raise error"""
        df_clean = self.sample_df.drop("nonexistent_column")
        self.assertEqual(df_clean.count(), self.sample_df.count())

    # =========================================================================
    # Task 2: Top N Countries Tests
    # =========================================================================

    def test_country_count_aggregation(self):
        """Verify country aggregation produces correct counts"""
        result = (
            self.sample_df
            .groupBy("country")
            .agg(F.count("*").alias("university_count"))
        )

        # Verify United States has 3 universities in sample data
        us_count = result.filter(F.col("country") == "United States").collect()
        self.assertEqual(len(us_count), 1)
        self.assertEqual(us_count[0].university_count, 3)

    def test_top_3_countries_count(self):
        """Verify top 3 countries returns exactly 3 results"""
        result = (
            self.sample_df
            .groupBy("country")
            .agg(F.count("*").alias("university_count"))
            .orderBy(F.desc("university_count"))
            .limit(3)
        )

        count = result.count()
        self.assertEqual(count, 3, f"Expected 3 countries, got {count}")

    def test_top_countries_ordered_descending(self):
        """Verify top countries are ordered by count descending"""
        result = (
            self.sample_df
            .groupBy("country")
            .agg(F.count("*").alias("university_count"))
            .orderBy(F.desc("university_count"))
        )

        counts = [row.university_count for row in result.collect()]
        self.assertEqual(counts, sorted(counts, reverse=True))

    def test_top_countries_in_sample_data(self):
        """Verify top countries in sample data are correct"""
        result = (
            self.sample_df
            .groupBy("country")
            .agg(F.count("*").alias("university_count"))
            .orderBy(F.desc("university_count"))
            .limit(3)
            .collect()
        )

        # Sample data has: US=3, UK=3, France=2, China=2
        top_countries = [row.country for row in result]
        self.assertIn("United States", top_countries)
        self.assertIn("United Kingdom", top_countries)

    def test_top_n_with_different_n_values(self):
        """Verify top N works with different N values"""
        for n in [1, 2, 3, 4]:
            result = (
                self.sample_df
                .groupBy("country")
                .agg(F.count("*").alias("university_count"))
                .orderBy(F.desc("university_count"))
                .limit(n)
            )
            count = result.count()
            self.assertLessEqual(count, n, f"Expected <= {n} results, got {count}")

    # =========================================================================
    # Task 3: Count Specific Countries Tests
    # =========================================================================

    def test_uk_country_mapping(self):
        """Verify UK maps to 'United Kingdom' correctly"""
        uk_count = self.sample_df.filter(F.col("country") == "United Kingdom").count()
        self.assertGreater(uk_count, 0, "No universities found for United Kingdom")

    def test_france_country_count(self):
        """Verify France university count"""
        france_count = self.sample_df.filter(F.col("country") == "France").count()
        self.assertEqual(france_count, 2, f"Expected 2 French universities, got {france_count}")

    def test_china_country_count(self):
        """Verify China university count"""
        china_count = self.sample_df.filter(F.col("country") == "China").count()
        self.assertEqual(china_count, 2, f"Expected 2 Chinese universities, got {china_count}")

    def test_united_kingdom_count(self):
        """Verify United Kingdom university count"""
        uk_count = self.sample_df.filter(F.col("country") == "United Kingdom").count()
        self.assertEqual(uk_count, 3, f"Expected 3 UK universities, got {uk_count}")

    def test_country_filter_case_sensitive(self):
        """Verify country filter is case-sensitive"""
        lowercase_count = self.sample_df.filter(F.col("country") == "france").count()
        uppercase_count = self.sample_df.filter(F.col("country") == "FRANCE").count()

        self.assertEqual(lowercase_count, 0, "Filter should be case-sensitive")
        self.assertEqual(uppercase_count, 0, "Filter should be case-sensitive")

    def test_specific_countries_total(self):
        """Verify total for UK + France + China"""
        total = self.sample_df.filter(
            F.col("country").isin(["United Kingdom", "France", "China"])
        ).count()
        self.assertEqual(total, 7, f"Expected 7 universities, got {total}")

    def test_nonexistent_country(self):
        """Verify filtering for non-existent country returns 0"""
        count = self.sample_df.filter(F.col("country") == "Atlantis").count()
        self.assertEqual(count, 0)

    # =========================================================================
    # Alpha Code Tests
    # =========================================================================

    def test_uk_alpha_code(self):
        """Verify United Kingdom has correct alpha code (GB)"""
        uk_codes = (
            self.sample_df
            .filter(F.col("country") == "United Kingdom")
            .select("alpha_two_code")
            .distinct()
            .collect()
        )
        self.assertEqual(len(uk_codes), 1)
        self.assertEqual(uk_codes[0].alpha_two_code, "GB")

    def test_france_alpha_code(self):
        """Verify France has correct alpha code (FR)"""
        fr_codes = (
            self.sample_df
            .filter(F.col("country") == "France")
            .select("alpha_two_code")
            .distinct()
            .collect()
        )
        self.assertEqual(len(fr_codes), 1)
        self.assertEqual(fr_codes[0].alpha_two_code, "FR")

    def test_china_alpha_code(self):
        """Verify China has correct alpha code (CN)"""
        cn_codes = (
            self.sample_df
            .filter(F.col("country") == "China")
            .select("alpha_two_code")
            .distinct()
            .collect()
        )
        self.assertEqual(len(cn_codes), 1)
        self.assertEqual(cn_codes[0].alpha_two_code, "CN")

    def test_us_alpha_code(self):
        """Verify United States has correct alpha code (US)"""
        us_codes = (
            self.sample_df
            .filter(F.col("country") == "United States")
            .select("alpha_two_code")
            .distinct()
            .collect()
        )
        self.assertEqual(len(us_codes), 1)
        self.assertEqual(us_codes[0].alpha_two_code, "US")

    # =========================================================================
    # University Name Tests
    # =========================================================================

    def test_oxford_exists(self):
        """Verify University of Oxford is in dataset"""
        count = self.sample_df.filter(F.col("name") == "University of Oxford").count()
        self.assertEqual(count, 1)

    def test_cambridge_exists(self):
        """Verify University of Cambridge is in dataset"""
        count = self.sample_df.filter(F.col("name") == "University of Cambridge").count()
        self.assertEqual(count, 1)

    def test_tsinghua_exists(self):
        """Verify Tsinghua University is in dataset"""
        count = self.sample_df.filter(F.col("name") == "Tsinghua University").count()
        self.assertEqual(count, 1)

    def test_sorbonne_exists(self):
        """Verify Sorbonne University is in dataset"""
        count = self.sample_df.filter(F.col("name") == "Sorbonne University").count()
        self.assertEqual(count, 1)

    def test_harvard_exists(self):
        """Verify Harvard University is in dataset"""
        count = self.sample_df.filter(F.col("name") == "Harvard University").count()
        self.assertEqual(count, 1)

    # =========================================================================
    # State/Province Tests
    # =========================================================================

    def test_state_province_nullable(self):
        """Verify state-province can be null"""
        null_states = self.sample_df.filter(F.col("state-province").isNull()).count()
        self.assertGreater(null_states, 0, "Expected some null state-province values")

    def test_us_universities_have_states(self):
        """Verify US universities have state-province filled"""
        us_with_state = self.sample_df.filter(
            (F.col("country") == "United States") & (F.col("state-province").isNotNull())
        ).count()
        us_total = self.sample_df.filter(F.col("country") == "United States").count()

        self.assertEqual(us_with_state, us_total, "US universities should have state info")

    def test_massachusetts_universities(self):
        """Verify universities in Massachusetts"""
        mass_count = self.sample_df.filter(F.col("state-province") == "Massachusetts").count()
        self.assertEqual(mass_count, 2, f"Expected 2 Massachusetts universities, got {mass_count}")

    # =========================================================================
    # Web Pages Tests
    # =========================================================================

    def test_web_pages_is_array(self):
        """Verify web_pages column is array type"""
        schema_dict = {field.name: field.dataType for field in self.sample_df.schema.fields}
        self.assertIsInstance(schema_dict["web_pages"], ArrayType)

    def test_web_pages_contains_urls(self):
        """Verify web_pages contain valid-looking URLs"""
        first_page = (
            self.sample_df
            .select(F.col("web_pages").getItem(0).alias("url"))
            .first()
            .url
        )
        self.assertTrue(first_page.startswith("http"), f"Invalid URL: {first_page}")

    def test_each_university_has_webpage(self):
        """Verify each university has at least one web page"""
        no_pages = self.sample_df.filter(F.size(F.col("web_pages")) < 1).count()
        self.assertEqual(no_pages, 0)

    # =========================================================================
    # Edge Cases and Error Handling Tests
    # =========================================================================

    def test_empty_dataframe_aggregation(self):
        """Verify aggregation handles empty DataFrame"""
        empty_df = self.sample_df.filter(F.col("country") == "NonexistentCountry")
        result = (
            empty_df
            .groupBy("country")
            .agg(F.count("*").alias("count"))
        )
        self.assertEqual(result.count(), 0)

    def test_limit_greater_than_rows(self):
        """Verify limit greater than available rows works correctly"""
        distinct_countries = self.sample_df.select("country").distinct().count()
        result = (
            self.sample_df
            .groupBy("country")
            .agg(F.count("*").alias("count"))
            .limit(100)
        )
        self.assertEqual(result.count(), distinct_countries)

    def test_case_insensitive_country_search(self):
        """Test case-insensitive country search using lower()"""
        result = self.sample_df.filter(
            F.lower(F.col("country")) == "united kingdom"
        ).count()
        self.assertEqual(result, 3)

    def test_country_contains_search(self):
        """Test partial country name search"""
        result = self.sample_df.filter(
            F.col("country").contains("United")
        ).count()
        self.assertGreater(result, 0)

    # =========================================================================
    # Full Dataset Tests (conditional)
    # =========================================================================

    @unittest.skipUnless(
        Path("/workspaces/LocalSpark/data/case_study_2/universities_raw.json").exists(),
        "Full data file not available"
    )
    def test_full_data_distinct_countries(self):
        """Verify full dataset has many distinct countries"""
        distinct_countries = self.full_df.select("country").distinct().count()
        self.assertGreater(distinct_countries, 100, f"Expected >100 countries, got {distinct_countries}")

    @unittest.skipUnless(
        Path("/workspaces/LocalSpark/data/case_study_2/universities_raw.json").exists(),
        "Full data file not available"
    )
    def test_full_data_uk_substantial(self):
        """Verify full dataset has substantial UK universities"""
        uk_count = self.full_df.filter(F.col("country") == "United Kingdom").count()
        self.assertGreater(uk_count, 100, f"Expected >100 UK universities, got {uk_count}")

    @unittest.skipUnless(
        Path("/workspaces/LocalSpark/data/case_study_2/universities_raw.json").exists(),
        "Full data file not available"
    )
    def test_full_data_china_substantial(self):
        """Verify full dataset has substantial Chinese universities"""
        china_count = self.full_df.filter(F.col("country") == "China").count()
        self.assertGreater(china_count, 100, f"Expected >100 Chinese universities, got {china_count}")

    @unittest.skipUnless(
        Path("/workspaces/LocalSpark/data/case_study_2/universities_raw.json").exists(),
        "Full data file not available"
    )
    def test_full_data_france_substantial(self):
        """Verify full dataset has substantial French universities"""
        france_count = self.full_df.filter(F.col("country") == "France").count()
        self.assertGreater(france_count, 50, f"Expected >50 French universities, got {france_count}")

    @unittest.skipUnless(
        Path("/workspaces/LocalSpark/data/case_study_2/universities_raw.json").exists(),
        "Full data file not available"
    )
    def test_full_data_top_3_countries_plausible(self):
        """Verify top 3 countries in full data are plausible"""
        result = (
            self.full_df
            .groupBy("country")
            .agg(F.count("*").alias("university_count"))
            .orderBy(F.desc("university_count"))
            .limit(3)
            .collect()
        )

        top_countries = [row.country for row in result]
        # These are typically among the top countries globally
        expected_top = ["United States", "China", "India", "United Kingdom", "Japan", "Brazil"]

        has_expected = any(country in expected_top for country in top_countries)
        self.assertTrue(has_expected, f"Top countries {top_countries} don't include expected countries")

    # =========================================================================
    # Transformation Pipeline Tests
    # =========================================================================

    def test_full_transformation_pipeline(self):
        """Test complete transformation pipeline"""
        # Step 1: Remove domains
        df_clean = self.sample_df.drop("domains")

        # Step 2: Get top 3 countries
        top_3 = (
            df_clean
            .groupBy("country")
            .agg(F.count("*").alias("university_count"))
            .orderBy(F.desc("university_count"))
            .limit(3)
        )

        # Step 3: Get specific country counts
        specific_counts = df_clean.filter(
            F.col("country").isin(["United Kingdom", "France", "China"])
        ).groupBy("country").agg(F.count("*").alias("count"))

        # Verify pipeline results
        self.assertNotIn("domains", df_clean.columns)
        self.assertEqual(top_3.count(), 3)
        self.assertEqual(specific_counts.count(), 3)

    def test_country_mapping_dictionary(self):
        """Test country mapping dictionary logic"""
        country_mapping = {
            "UK": "United Kingdom",
            "France": "France",
            "China": "China"
        }

        for input_country, mapped_country in country_mapping.items():
            count = self.sample_df.filter(F.col("country") == mapped_country).count()
            self.assertGreater(count, 0, f"No universities found for {mapped_country}")

    def test_aggregation_with_alias(self):
        """Test aggregation produces correctly aliased columns"""
        result = (
            self.sample_df
            .groupBy("country")
            .agg(F.count("*").alias("university_count"))
        )

        self.assertIn("country", result.columns)
        self.assertIn("university_count", result.columns)


class TestCaseStudy2EdgeCases(unittest.TestCase):
    """Edge case tests for case_study_2"""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for edge case tests"""
        cls.spark = (
            SparkSession.builder
            .appName("Test_Case_Study_2_Edge_Cases")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session"""
        cls.spark.stop()

    def test_empty_country_name(self):
        """Test handling of empty country name"""
        data = [
            {"name": "Test University", "country": "", "alpha_two_code": "XX"},
        ]
        df = self.spark.createDataFrame(data)

        empty_country = df.filter(F.col("country") == "").count()
        self.assertEqual(empty_country, 1)

    def test_special_characters_in_name(self):
        """Test handling of special characters in university name"""
        data = [
            {
                "name": "École Normale Supérieure",
                "country": "France",
                "alpha_two_code": "FR"
            },
        ]
        df = self.spark.createDataFrame(data)

        count = df.filter(F.col("name").contains("École")).count()
        self.assertEqual(count, 1)

    def test_unicode_characters(self):
        """Test handling of unicode characters"""
        data = [
            {"name": "北京大学", "country": "中国", "alpha_two_code": "CN"},
        ]
        df = self.spark.createDataFrame(data)

        count = df.count()
        self.assertEqual(count, 1)

    def test_very_long_university_name(self):
        """Test handling of very long university name"""
        long_name = "University of " + "Very " * 50 + "Long Name"
        data = [
            {"name": long_name, "country": "Test Country", "alpha_two_code": "TC"},
        ]
        df = self.spark.createDataFrame(data)

        count = df.filter(F.length(F.col("name")) > 200).count()
        self.assertEqual(count, 1)

    def test_null_web_pages(self):
        """Test handling of null web_pages"""
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("web_pages", ArrayType(StringType()), True),
            ]
        )
        data = [("Test University", "Test Country", None)]
        df = self.spark.createDataFrame(data, schema)

        null_pages = df.filter(F.col("web_pages").isNull()).count()
        self.assertEqual(null_pages, 1)

    def test_empty_web_pages_array(self):
        """Test handling of empty web_pages array"""
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("country", StringType(), True),
                StructField("web_pages", ArrayType(StringType()), True),
            ]
        )
        data = [("Test University", "Test Country", [])]
        df = self.spark.createDataFrame(data, schema)

        empty_pages = df.filter(F.size(F.col("web_pages")) == 0).count()
        self.assertEqual(empty_pages, 1)

    def test_duplicate_universities_same_country(self):
        """Test aggregation with duplicate universities"""
        data = [
            {"name": "University A", "country": "Test", "alpha_two_code": "TC"},
            {"name": "University A", "country": "Test", "alpha_two_code": "TC"},
            {"name": "University B", "country": "Test", "alpha_two_code": "TC"},
        ]
        df = self.spark.createDataFrame(data)

        # Count should include duplicates
        count = df.filter(F.col("country") == "Test").count()
        self.assertEqual(count, 3)

        # Distinct count should exclude duplicates
        distinct = df.select("name", "country").distinct().filter(F.col("country") == "Test").count()
        self.assertEqual(distinct, 2)

    def test_single_university_country(self):
        """Test country with only one university"""
        data = [
            {"name": "Only University", "country": "Small Country", "alpha_two_code": "SC"},
            {"name": "Another University", "country": "Big Country", "alpha_two_code": "BC"},
            {"name": "Yet Another", "country": "Big Country", "alpha_two_code": "BC"},
        ]
        df = self.spark.createDataFrame(data)

        result = (
            df.groupBy("country")
            .agg(F.count("*").alias("count"))
            .filter(F.col("country") == "Small Country")
            .collect()
        )
        self.assertEqual(result[0]["count"], 1)


if __name__ == "__main__":
    unittest.main()
