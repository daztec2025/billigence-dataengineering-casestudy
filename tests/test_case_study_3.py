"""
Unit Tests for Case Study 3: Customer Data Restructuring & Email Protection

Tests cover:
- JSON data loading and schema validation
- Nested JSON flattening to tabular format
- Email masking and encryption
- Domain extraction
- SHA-256 and MD5 hashing
- Data quality validation
- Secure dataset creation
- Reference table generation
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
    DateType,
    IntegerType,
    LongType,
    TimestampType,
)
import json
from pathlib import Path


class TestCaseStudy3(unittest.TestCase):
    """Comprehensive unit tests for Case Study 3: Customer Data Restructuring"""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session and load data once for all tests"""
        cls.spark = (
            SparkSession.builder
            .appName("Test_Case_Study_3")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

        # Load actual JSON data
        cls.json_path = Path("/workspaces/LocalSpark/data/case_study_3/Customers.json")
        cls.raw_df = cls.spark.read.option("multiLine", "true").json(str(cls.json_path))
        cls.raw_df.cache()

        # Create sample test data for unit tests
        cls.sample_nested_data = {
            "CompanyID": "123",
            "CompanyInfo": [
                {
                    "Name": "ABC Corporation",
                    "Customers": [
                        {
                            "Birth": "1991-01-05",
                            "Email": "carlos91@gmail.com",
                            "Name": "Carlos Saint",
                            "Phone": "555-5555-1234",
                            "Role": "Data Engineer",
                            "Place of Birth": "Madrid"
                        },
                        {
                            "Birth": "1968-01-30",
                            "Email": "Felipe_g@yahoo.com",
                            "Name": "Felipe Guerrero",
                            "Phone": "555-4444-1234",
                            "Role": "Business Analyst",
                            "Place of Birth": "Porto"
                        }
                    ]
                }
            ]
        }

        # Create flattened DataFrame for email tests
        cls.flattened_df = cls._create_flattened_df()

    @classmethod
    def _create_flattened_df(cls):
        """Create flattened DataFrame from nested JSON"""
        customers_exploded = cls.raw_df.select(
            F.col("CompanyID"),
            F.explode(F.col("CompanyInfo")).alias("company_info")
        )

        customers_flat = customers_exploded.select(
            F.col("CompanyID").alias("company_id"),
            F.col("company_info.Name").alias("company_name"),
            F.explode(F.col("company_info.Customers")).alias("customer")
        )

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

        return customers_tabular

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session after all tests"""
        cls.spark.stop()

    # =========================================================================
    # Data Loading Tests
    # =========================================================================

    def test_json_file_exists(self):
        """Verify JSON file exists"""
        self.assertTrue(self.json_path.exists(), f"JSON file not found: {self.json_path}")

    def test_json_loads_successfully(self):
        """Verify JSON loads without errors"""
        count = self.raw_df.count()
        self.assertGreater(count, 0, "No records loaded from JSON")

    def test_raw_schema_has_company_id(self):
        """Verify raw schema has CompanyID field"""
        self.assertIn("CompanyID", self.raw_df.columns)

    def test_raw_schema_has_company_info(self):
        """Verify raw schema has CompanyInfo field"""
        self.assertIn("CompanyInfo", self.raw_df.columns)

    def test_company_info_is_array(self):
        """Verify CompanyInfo is an array type"""
        schema_dict = {field.name: field.dataType for field in self.raw_df.schema.fields}
        self.assertIsInstance(schema_dict["CompanyInfo"], ArrayType)

    # =========================================================================
    # JSON Flattening Tests (Task 1)
    # =========================================================================

    def test_flatten_produces_correct_columns(self):
        """Verify flattening produces expected columns"""
        expected_columns = [
            "company_id",
            "company_name",
            "customer_name",
            "email",
            "date_of_birth",
            "place_of_birth",
            "phone_number",
            "role"
        ]
        for col in expected_columns:
            self.assertIn(col, self.flattened_df.columns, f"Missing column: {col}")

    def test_flatten_expands_customers(self):
        """Verify flattening expands nested customers"""
        count = self.flattened_df.count()
        self.assertEqual(count, 2, f"Expected 2 customer records, got {count}")

    def test_flatten_company_id_preserved(self):
        """Verify company_id is preserved after flattening"""
        company_ids = self.flattened_df.select("company_id").distinct().collect()
        self.assertEqual(len(company_ids), 1)
        self.assertEqual(company_ids[0].company_id, "123")

    def test_flatten_company_name_extracted(self):
        """Verify company_name is extracted correctly"""
        company_names = self.flattened_df.select("company_name").distinct().collect()
        self.assertEqual(len(company_names), 1)
        self.assertEqual(company_names[0].company_name, "ABC Corporation")

    def test_flatten_customer_names(self):
        """Verify customer names are extracted correctly"""
        names = [row.customer_name for row in self.flattened_df.select("customer_name").collect()]
        self.assertIn("Carlos Saint", names)
        self.assertIn("Felipe Guerrero", names)

    def test_flatten_emails(self):
        """Verify emails are extracted correctly"""
        emails = [row.email for row in self.flattened_df.select("email").collect()]
        self.assertIn("carlos91@gmail.com", emails)
        self.assertIn("Felipe_g@yahoo.com", emails)

    def test_flatten_birth_dates(self):
        """Verify birth dates are extracted correctly"""
        dates = [row.date_of_birth for row in self.flattened_df.select("date_of_birth").collect()]
        self.assertIn("1991-01-05", dates)
        self.assertIn("1968-01-30", dates)

    def test_flatten_roles(self):
        """Verify roles are extracted correctly"""
        roles = [row.role for row in self.flattened_df.select("role").collect()]
        self.assertIn("Data Engineer", roles)
        self.assertIn("Business Analyst", roles)

    def test_flatten_places_of_birth(self):
        """Verify places of birth are extracted correctly"""
        places = [row.place_of_birth for row in self.flattened_df.select("place_of_birth").collect()]
        self.assertIn("Madrid", places)
        self.assertIn("Porto", places)

    def test_flatten_phone_numbers(self):
        """Verify phone numbers are extracted correctly"""
        phones = [row.phone_number for row in self.flattened_df.select("phone_number").collect()]
        self.assertIn("555-5555-1234", phones)
        self.assertIn("555-4444-1234", phones)

    # =========================================================================
    # Email Masking Tests (Task 2)
    # =========================================================================

    def test_email_domain_extraction(self):
        """Verify email domain is extracted correctly"""
        df_with_domain = self.flattened_df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        )
        domains = [row.email_domain for row in df_with_domain.select("email_domain").collect()]
        self.assertIn("gmail.com", domains)
        self.assertIn("yahoo.com", domains)

    def test_email_masked_format(self):
        """Verify email masking produces correct format"""
        df_masked = self.flattened_df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        ).withColumn(
            "email_masked", F.concat(F.lit("*******@"), F.col("email_domain"))
        )

        masked_emails = [row.email_masked for row in df_masked.select("email_masked").collect()]
        self.assertIn("*******@gmail.com", masked_emails)
        self.assertIn("*******@yahoo.com", masked_emails)

    def test_email_masked_hides_username(self):
        """Verify email masking hides the username portion"""
        df_masked = self.flattened_df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        ).withColumn(
            "email_masked", F.concat(F.lit("*******@"), F.col("email_domain"))
        )

        for row in df_masked.collect():
            # Verify original username is not in masked email
            original_username = row.email.split("@")[0]
            self.assertNotIn(original_username, row.email_masked)

    def test_email_sha256_hash(self):
        """Verify SHA-256 hash is generated correctly"""
        df_hashed = self.flattened_df.withColumn(
            "email_sha256", F.sha2(F.col("email"), 256)
        )

        for row in df_hashed.collect():
            # SHA-256 produces 64 character hex string
            self.assertEqual(len(row.email_sha256), 64)
            # Verify it's valid hex
            self.assertTrue(
                all(c in "0123456789abcdef" for c in row.email_sha256),
                f"Invalid SHA-256 hash: {row.email_sha256}"
            )

    def test_email_md5_hash(self):
        """Verify MD5 hash is generated correctly"""
        df_hashed = self.flattened_df.withColumn(
            "email_md5", F.md5(F.col("email"))
        )

        for row in df_hashed.collect():
            # MD5 produces 32 character hex string
            self.assertEqual(len(row.email_md5), 32)
            # Verify it's valid hex
            self.assertTrue(
                all(c in "0123456789abcdef" for c in row.email_md5),
                f"Invalid MD5 hash: {row.email_md5}"
            )

    def test_hash_uniqueness(self):
        """Verify different emails produce different hashes"""
        df_hashed = self.flattened_df.withColumn(
            "email_sha256", F.sha2(F.col("email"), 256)
        )

        hashes = [row.email_sha256 for row in df_hashed.select("email_sha256").collect()]
        unique_hashes = set(hashes)
        self.assertEqual(len(hashes), len(unique_hashes), "Duplicate hashes found")

    def test_hash_deterministic(self):
        """Verify same email always produces same hash"""
        # Hash the same email twice
        df_hashed = self.flattened_df.withColumn(
            "email_sha256_1", F.sha2(F.col("email"), 256)
        ).withColumn(
            "email_sha256_2", F.sha2(F.col("email"), 256)
        )

        for row in df_hashed.collect():
            self.assertEqual(row.email_sha256_1, row.email_sha256_2)

    def test_known_sha256_hash(self):
        """Verify SHA-256 hash matches expected value for known input"""
        # Known hash for "carlos91@gmail.com"
        df_test = self.spark.createDataFrame(
            [("carlos91@gmail.com",)],
            ["email"]
        )
        df_hashed = df_test.withColumn("email_sha256", F.sha2(F.col("email"), 256))
        result = df_hashed.collect()[0].email_sha256

        # SHA-256 of "carlos91@gmail.com"
        import hashlib
        expected = hashlib.sha256("carlos91@gmail.com".encode()).hexdigest()
        self.assertEqual(result, expected)

    # =========================================================================
    # Date Transformation Tests
    # =========================================================================

    def test_date_of_birth_conversion(self):
        """Verify date of birth is converted to DateType"""
        df_with_date = self.flattened_df.withColumn(
            "date_of_birth", F.to_date(F.col("date_of_birth"))
        )

        schema_dict = {field.name: field.dataType for field in df_with_date.schema.fields}
        self.assertIsInstance(schema_dict["date_of_birth"], DateType)

    def test_age_calculation(self):
        """Verify age calculation is reasonable"""
        df_with_age = self.flattened_df.withColumn(
            "date_of_birth", F.to_date(F.col("date_of_birth"))
        ).withColumn(
            "age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
        )

        ages = [row.age for row in df_with_age.select("age").collect()]

        # Carlos born 1991 should be around 33-34 years old
        # Felipe born 1968 should be around 56-57 years old
        for age in ages:
            self.assertGreater(age, 20, f"Age {age} is too low")
            self.assertLess(age, 100, f"Age {age} is too high")

    def test_age_carlos(self):
        """Verify Carlos's age is calculated correctly (born 1991)"""
        df_with_age = self.flattened_df.withColumn(
            "date_of_birth", F.to_date(F.col("date_of_birth"))
        ).withColumn(
            "age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
        )

        carlos = df_with_age.filter(F.col("customer_name") == "Carlos Saint").collect()[0]
        # Carlos born 1991, should be around 33-34 in 2024-2025
        self.assertGreaterEqual(carlos.age, 33)
        self.assertLessEqual(carlos.age, 35)

    def test_age_felipe(self):
        """Verify Felipe's age is calculated correctly (born 1968)"""
        df_with_age = self.flattened_df.withColumn(
            "date_of_birth", F.to_date(F.col("date_of_birth"))
        ).withColumn(
            "age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
        )

        felipe = df_with_age.filter(F.col("customer_name") == "Felipe Guerrero").collect()[0]
        # Felipe born 1968, should be around 56-57 in 2024-2025
        self.assertGreaterEqual(felipe.age, 56)
        self.assertLessEqual(felipe.age, 58)

    # =========================================================================
    # Secure Dataset Tests
    # =========================================================================

    def test_secure_dataset_no_plain_email(self):
        """Verify secure dataset does not contain plain text emails"""
        # Create masked DataFrame
        df_masked = self.flattened_df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        ).withColumn(
            "email_masked", F.concat(F.lit("*******@"), F.col("email_domain"))
        ).withColumn(
            "email_sha256", F.sha2(F.col("email"), 256)
        )

        # Create secure dataset (drop original email)
        secure_df = df_masked.drop("email").withColumnRenamed("email_masked", "email")

        # Verify original emails are not present
        for row in secure_df.collect():
            self.assertNotIn("carlos91", row.email)
            self.assertNotIn("Felipe_g", row.email)

    def test_secure_dataset_has_hash(self):
        """Verify secure dataset retains SHA-256 hash for verification"""
        df_masked = self.flattened_df.withColumn(
            "email_sha256", F.sha2(F.col("email"), 256)
        )

        self.assertIn("email_sha256", df_masked.columns)

        for row in df_masked.select("email_sha256").collect():
            self.assertIsNotNone(row.email_sha256)
            self.assertEqual(len(row.email_sha256), 64)

    # =========================================================================
    # Reference Table Tests
    # =========================================================================

    def test_reference_table_structure(self):
        """Verify reference table has correct structure"""
        df_masked = self.flattened_df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        ).withColumn(
            "email_masked", F.concat(F.lit("*******@"), F.col("email_domain"))
        ).withColumn(
            "email_sha256", F.sha2(F.col("email"), 256)
        )

        reference_df = df_masked.select(
            F.col("email_sha256").alias("email_hash"),
            F.col("email").alias("original_email"),
            F.col("email_masked"),
            F.col("email_domain")
        ).distinct()

        expected_columns = ["email_hash", "original_email", "email_masked", "email_domain"]
        for col in expected_columns:
            self.assertIn(col, reference_df.columns)

    def test_reference_table_distinct(self):
        """Verify reference table has distinct entries"""
        df_masked = self.flattened_df.withColumn(
            "email_sha256", F.sha2(F.col("email"), 256)
        )

        reference_df = df_masked.select(
            F.col("email_sha256").alias("email_hash"),
            F.col("email").alias("original_email")
        ).distinct()

        total_count = reference_df.count()
        distinct_count = reference_df.select("email_hash").distinct().count()

        self.assertEqual(total_count, distinct_count)

    def test_reference_table_lookup(self):
        """Verify reference table can be used for hash lookup"""
        df_masked = self.flattened_df.withColumn(
            "email_sha256", F.sha2(F.col("email"), 256)
        )

        reference_df = df_masked.select(
            F.col("email_sha256").alias("email_hash"),
            F.col("email").alias("original_email")
        )

        # Lookup Carlos's email by hash
        import hashlib
        carlos_hash = hashlib.sha256("carlos91@gmail.com".encode()).hexdigest()

        result = reference_df.filter(F.col("email_hash") == carlos_hash).collect()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].original_email, "carlos91@gmail.com")

    # =========================================================================
    # Data Quality Validation Tests
    # =========================================================================

    def test_no_null_customer_names(self):
        """Verify no null customer names after flattening"""
        null_count = self.flattened_df.filter(F.col("customer_name").isNull()).count()
        self.assertEqual(null_count, 0)

    def test_no_null_emails(self):
        """Verify no null emails after flattening"""
        null_count = self.flattened_df.filter(F.col("email").isNull()).count()
        self.assertEqual(null_count, 0)

    def test_no_null_company_ids(self):
        """Verify no null company IDs after flattening"""
        null_count = self.flattened_df.filter(F.col("company_id").isNull()).count()
        self.assertEqual(null_count, 0)

    def test_email_format_valid(self):
        """Verify all emails have valid format"""
        invalid_emails = self.flattened_df.filter(
            ~F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$")
        ).count()
        self.assertEqual(invalid_emails, 0, f"Found {invalid_emails} invalid emails")

    def test_all_masked_emails_have_asterisks(self):
        """Verify all masked emails start with asterisks"""
        df_masked = self.flattened_df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        ).withColumn(
            "email_masked", F.concat(F.lit("*******@"), F.col("email_domain"))
        )

        invalid_masked = df_masked.filter(
            ~F.col("email_masked").startswith("*******@")
        ).count()
        self.assertEqual(invalid_masked, 0)

    def test_row_count_preserved(self):
        """Verify row count is preserved through transformations"""
        original_customer_count = 2  # From JSON structure

        flattened_count = self.flattened_df.count()
        self.assertEqual(flattened_count, original_customer_count)

    # =========================================================================
    # Monotonically Increasing ID Tests
    # =========================================================================

    def test_customer_id_generation(self):
        """Verify monotonically_increasing_id generates IDs"""
        df_with_id = self.flattened_df.withColumn(
            "customer_id", F.monotonically_increasing_id()
        )

        self.assertIn("customer_id", df_with_id.columns)

        ids = [row.customer_id for row in df_with_id.select("customer_id").collect()]
        self.assertEqual(len(ids), 2)
        self.assertIsNotNone(ids[0])
        self.assertIsNotNone(ids[1])

    def test_customer_id_unique(self):
        """Verify generated customer IDs are unique"""
        df_with_id = self.flattened_df.withColumn(
            "customer_id", F.monotonically_increasing_id()
        )

        ids = [row.customer_id for row in df_with_id.select("customer_id").collect()]
        unique_ids = set(ids)
        self.assertEqual(len(ids), len(unique_ids))

    # =========================================================================
    # Timestamp Tests
    # =========================================================================

    def test_load_timestamp_added(self):
        """Verify load timestamp is added"""
        df_with_timestamp = self.flattened_df.withColumn(
            "load_timestamp", F.current_timestamp()
        )

        self.assertIn("load_timestamp", df_with_timestamp.columns)

        for row in df_with_timestamp.select("load_timestamp").collect():
            self.assertIsNotNone(row.load_timestamp)


class TestCaseStudy3EdgeCases(unittest.TestCase):
    """Edge case tests for Case Study 3"""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for edge case tests"""
        cls.spark = (
            SparkSession.builder
            .appName("Test_Case_Study_3_Edge_Cases")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session"""
        cls.spark.stop()

    def test_email_with_subdomain(self):
        """Test email domain extraction with subdomain"""
        data = [("test@mail.example.com",)]
        df = self.spark.createDataFrame(data, ["email"])

        df_with_domain = df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        )

        result = df_with_domain.collect()[0].email_domain
        self.assertEqual(result, "mail.example.com")

    def test_email_with_plus_sign(self):
        """Test email handling with plus sign in username"""
        data = [("test+filter@gmail.com",)]
        df = self.spark.createDataFrame(data, ["email"])

        df_masked = df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        ).withColumn(
            "email_masked", F.concat(F.lit("*******@"), F.col("email_domain"))
        )

        result = df_masked.collect()[0]
        self.assertEqual(result.email_domain, "gmail.com")
        self.assertEqual(result.email_masked, "*******@gmail.com")

    def test_email_with_numbers(self):
        """Test email handling with numbers in domain"""
        data = [("user@mail2.example123.com",)]
        df = self.spark.createDataFrame(data, ["email"])

        df_with_domain = df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        )

        result = df_with_domain.collect()[0].email_domain
        self.assertEqual(result, "mail2.example123.com")

    def test_special_characters_in_name(self):
        """Test handling of special characters in customer name"""
        data = [
            {
                "customer_name": "José García-López",
                "email": "jose@example.com"
            }
        ]
        df = self.spark.createDataFrame(data)

        result = df.collect()[0].customer_name
        self.assertEqual(result, "José García-López")

    def test_unicode_in_place_of_birth(self):
        """Test handling of unicode in place of birth"""
        data = [
            {
                "customer_name": "Test User",
                "place_of_birth": "São Paulo"
            }
        ]
        df = self.spark.createDataFrame(data)

        result = df.collect()[0].place_of_birth
        self.assertEqual(result, "São Paulo")

    def test_empty_email(self):
        """Test handling of empty email string"""
        data = [("",)]
        df = self.spark.createDataFrame(data, ["email"])

        df_with_domain = df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        )

        result = df_with_domain.collect()[0].email_domain
        self.assertEqual(result, "")  # No match returns empty string

    def test_null_email_hash(self):
        """Test SHA-256 hash of null email"""
        schema = StructType([StructField("email", StringType(), True)])
        data = [(None,)]
        df = self.spark.createDataFrame(data, schema)

        df_hashed = df.withColumn("email_sha256", F.sha2(F.col("email"), 256))

        result = df_hashed.collect()[0].email_sha256
        self.assertIsNone(result)

    def test_very_long_email(self):
        """Test handling of very long email address"""
        long_local = "a" * 64  # Max local part length
        long_email = f"{long_local}@example.com"
        data = [(long_email,)]
        df = self.spark.createDataFrame(data, ["email"])

        df_hashed = df.withColumn("email_sha256", F.sha2(F.col("email"), 256))

        result = df_hashed.collect()[0].email_sha256
        self.assertEqual(len(result), 64)  # Valid SHA-256 hash

    def test_multiple_at_signs(self):
        """Test email with multiple @ signs (invalid but should handle)"""
        data = [("user@@domain.com",)]
        df = self.spark.createDataFrame(data, ["email"])

        df_with_domain = df.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        )

        result = df_with_domain.collect()[0].email_domain
        # Should capture everything after first @
        self.assertEqual(result, "@domain.com")

    def test_date_parsing_invalid_format(self):
        """Test date parsing with invalid format"""
        data = [("not-a-date",)]
        df = self.spark.createDataFrame(data, ["date_str"])

        df_with_date = df.withColumn("date", F.to_date(F.col("date_str")))

        result = df_with_date.collect()[0].date
        self.assertIsNone(result)

    def test_date_parsing_different_format(self):
        """Test date parsing with different format"""
        data = [("05/01/1991",)]
        df = self.spark.createDataFrame(data, ["date_str"])

        # Parse with specific format
        df_with_date = df.withColumn(
            "date", F.to_date(F.col("date_str"), "dd/MM/yyyy")
        )

        result = df_with_date.collect()[0].date
        self.assertIsNotNone(result)

    def test_empty_customers_array(self):
        """Test handling of empty customers array"""
        schema = StructType([
            StructField("CompanyID", StringType(), True),
            StructField("CompanyInfo", ArrayType(StructType([
                StructField("Name", StringType(), True),
                StructField("Customers", ArrayType(StructType([
                    StructField("Name", StringType(), True),
                    StructField("Email", StringType(), True)
                ])), True)
            ])), True)
        ])

        data = [("999", [{"Name": "Empty Corp", "Customers": []}])]
        df = self.spark.createDataFrame(data, schema)

        # Explode CompanyInfo
        exploded = df.select(
            F.col("CompanyID"),
            F.explode(F.col("CompanyInfo")).alias("company_info")
        )

        # Explode Customers (will result in 0 rows)
        customers = exploded.select(
            F.col("CompanyID"),
            F.explode(F.col("company_info.Customers")).alias("customer")
        )

        self.assertEqual(customers.count(), 0)

    def test_null_place_of_birth(self):
        """Test handling of null place of birth"""
        schema = StructType(
            [
                StructField("customer_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("place_of_birth", StringType(), True),
            ]
        )
        data = [("Test User", "test@example.com", None)]
        df = self.spark.createDataFrame(data, schema)

        null_count = df.filter(F.col("place_of_birth").isNull()).count()
        self.assertEqual(null_count, 1)

    def test_case_sensitive_email_hash(self):
        """Test that email hashing is case-sensitive"""
        data = [
            ("Test@Example.com",),
            ("test@example.com",)
        ]
        df = self.spark.createDataFrame(data, ["email"])

        df_hashed = df.withColumn("email_sha256", F.sha2(F.col("email"), 256))

        hashes = [row.email_sha256 for row in df_hashed.collect()]
        self.assertNotEqual(hashes[0], hashes[1], "Hashes should differ for different case")


class TestCaseStudy3FullPipeline(unittest.TestCase):
    """Full pipeline integration tests for Case Study 3"""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for integration tests"""
        cls.spark = (
            SparkSession.builder
            .appName("Test_Case_Study_3_Integration")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )

        cls.json_path = Path("/workspaces/LocalSpark/data/case_study_3/Customers.json")
        cls.raw_df = cls.spark.read.option("multiLine", "true").json(str(cls.json_path))

    @classmethod
    def tearDownClass(cls):
        """Stop Spark session"""
        cls.spark.stop()

    def test_full_pipeline(self):
        """Test complete transformation pipeline"""
        # Step 1: Flatten
        customers_exploded = self.raw_df.select(
            F.col("CompanyID"),
            F.explode(F.col("CompanyInfo")).alias("company_info")
        )

        customers_flat = customers_exploded.select(
            F.col("CompanyID").alias("company_id"),
            F.col("company_info.Name").alias("company_name"),
            F.explode(F.col("company_info.Customers")).alias("customer")
        )

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

        # Step 2: Add computed columns
        customers_enhanced = customers_tabular.withColumn(
            "date_of_birth", F.to_date(F.col("date_of_birth"))
        ).withColumn(
            "age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
        ).withColumn(
            "customer_id", F.monotonically_increasing_id()
        )

        # Step 3: Mask emails
        df_masked = customers_enhanced.withColumn(
            "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
        ).withColumn(
            "email_masked", F.concat(F.lit("*******@"), F.col("email_domain"))
        ).withColumn(
            "email_sha256", F.sha2(F.col("email"), 256)
        ).withColumn(
            "email_md5", F.md5(F.col("email"))
        )

        # Step 4: Create secure dataset
        secure_df = df_masked.select(
            F.col("customer_id"),
            F.col("company_id"),
            F.col("company_name"),
            F.col("customer_name"),
            F.col("email_masked").alias("email"),
            F.col("email_sha256"),
            F.col("date_of_birth"),
            F.col("age"),
            F.col("place_of_birth"),
            F.col("phone_number"),
            F.col("role")
        )

        # Verify final output
        self.assertEqual(secure_df.count(), 2)

        # Verify no plain text emails
        for row in secure_df.collect():
            self.assertTrue(row.email.startswith("*******@"))
            self.assertNotIn("carlos91", row.email)
            self.assertNotIn("Felipe_g", row.email)

        # Verify hashes are present
        for row in secure_df.collect():
            self.assertEqual(len(row.email_sha256), 64)

    def test_pipeline_idempotent(self):
        """Test that running pipeline twice produces same results"""
        def run_pipeline(df):
            exploded = df.select(
                F.col("CompanyID"),
                F.explode(F.col("CompanyInfo")).alias("company_info")
            )

            flat = exploded.select(
                F.col("CompanyID").alias("company_id"),
                F.col("company_info.Name").alias("company_name"),
                F.explode(F.col("company_info.Customers")).alias("customer")
            )

            return flat.select(
                F.col("company_id"),
                F.col("company_name"),
                F.col("customer.Name").alias("customer_name"),
                F.col("customer.Email").alias("email")
            ).withColumn(
                "email_sha256", F.sha2(F.col("email"), 256)
            )

        result1 = run_pipeline(self.raw_df)
        result2 = run_pipeline(self.raw_df)

        # Compare hashes (deterministic)
        hashes1 = sorted([row.email_sha256 for row in result1.collect()])
        hashes2 = sorted([row.email_sha256 for row in result2.collect()])

        self.assertEqual(hashes1, hashes2)


if __name__ == "__main__":
    unittest.main()
