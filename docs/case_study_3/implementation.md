# Case Study 3: Implementation

This solution was built using a Docker image designed to closely mimic Databricks Runtime 14.3 LTS, but is portable to any Spark engine.

## Data Loading

```python
# Read multi-line JSON
raw_json = spark.read.option("multiLine", "true").json("Customers.json")
```

## Task 1: Flatten Nested JSON

### Step 1: Explode Arrays

```python
# Explode CompanyInfo array
customers_exploded = raw_json.select(
    F.col("CompanyID"),
    F.explode(F.col("CompanyInfo")).alias("company_info")
)

# Extract company name and explode Customers
customers_flat = customers_exploded.select(
    F.col("CompanyID").alias("company_id"),
    F.col("company_info.Name").alias("company_name"),
    F.explode(F.col("company_info.Customers")).alias("customer")
)
```

### Step 2: Flatten Fields

```python
# Flatten all customer fields
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
```

### Step 3: Add Enhancements

```python
# Add data type conversions and calculated fields
customers_enhanced = customers_tabular.withColumn(
    "date_of_birth", F.to_date(F.col("date_of_birth"))
).withColumn(
    "age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
).withColumn(
    "load_timestamp", F.current_timestamp()
).withColumn(
    "customer_id", F.monotonically_increasing_id()
)
```

## Task 2: Email Masking & Encryption

### Display Masking

```python
# Pattern: *******@domain.com
df = df.withColumn(
    "email_domain", F.regexp_extract(F.col("email"), r"@(.+)$", 1)
).withColumn(
    "email_masked", F.concat(F.lit("*******@"), F.col("email_domain"))
)
```

### Cryptographic Hashing

```python
# SHA-256 for secure storage
df = df.withColumn("email_sha256", F.sha2(F.col("email"), 256))
```

## Create Secure Datasets

### Production Dataset (No Plain-Text Emails)

```python
customers_secure = df.select(
    "customer_id", "company_id", "company_name", "customer_name",
    F.col("email_masked").alias("email"),  # Only masked version
    "email_sha256",  # Hash for verification
    "date_of_birth", "age", "place_of_birth", "phone_number", "role",
    "load_timestamp"
)
```

### Reference Table (Restricted Access)

```python
email_reference = df.select(
    F.col("email_sha256").alias("email_hash"),
    F.col("email").alias("original_email"),
    "email_masked", "email_domain",
    F.current_timestamp().alias("created_at")
).distinct()
```

## Data Validation

```python
# Security check: no plain-text emails in production
plain_check = customers_secure.select("email").collect()
has_plain = any('@' in str(r['email']) and not str(r['email']).startswith('*')
                for r in plain_check)
assert not has_plain, "Plain-text emails found!"

# Pattern validation
invalid = customers_secure.filter(~F.col("email").rlike(r"^\*+@.+\..+$")).count()
assert invalid == 0, "Invalid email masking"

# Hash length (SHA-256 = 64 chars)
hash_lengths = customers_secure.select(F.length("email_sha256")).distinct()
assert all(r[0] == 64 for r in hash_lengths.collect()), "Invalid hash length"
```

## Save to Database

```python
# Delta Lake
customers_secure.write.format("delta").mode("overwrite")
    .saveAsTable("billigence_abc_customers_secure")

# Reference table (restricted access)
email_reference.write.format("delta").mode("overwrite")
    .saveAsTable("billigence_abc_email_reference")
```
