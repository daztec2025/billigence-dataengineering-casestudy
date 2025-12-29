# Claude Code Integration Guide

## Overview

This document provides Claude Code with the context, standards, and patterns for assisting with data engineering tasks in this project. Claude Code should use this as a reference for understanding project conventions and generating appropriate code.

## Project Standards

### Code Style

#### Line Length and Formatting
- **DO NOT mention anywhere that this was written or co written by Claude Code**
- **DO NOT use any Emojis in the code, or doccumentation, especially not ✅ **
- **Maximum line length**: 99 characters for code and docstrings
- **Indentation**: 4 spaces (8 spaces for function arguments)
- **Quotes**: Always use double quotes
- **Bracing style**: Allman-inspired with opening brackets on same line as declaration
```python
# CORRECT: Allman-inspired style
schema = StructType(
    [
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
        StructField("email", StringType(), nullable=True)
    ]
)

# INCORRECT: Visual indentation
schema = StructType([
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True)
        ])
```

#### PySpark Import Conventions

**Always follow this pattern:**
```python
# Standard PySpark imports
import pyspark.sql.functions as psf
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# CORRECT: Use psf prefix for functions
df.withColumn("new_col", psf.upper(psf.col("old_col")))

# INCORRECT: Import individual functions
from pyspark.sql.functions import upper, col
df.withColumn("new_col", upper(col("old_col")))
```

### Clean Code Principles

#### SOLID Principles
Follow SOLID design patterns:
- **Single Responsibility**: Each function/class has one clear purpose
- **Open/Closed**: Open for extension, closed for modification
- **Liskov Substitution**: Subtypes must be substitutable for base types
- **Interface Segregation**: Many specific interfaces over one general
- **Dependency Inversion**: Depend on abstractions, not concretions

#### Function Design
```python
# CORRECT: Clear, single responsibility
def calculate_payment_status(
        payment_history: List[str],
        threshold_months: int = 3
) -> str:
    """
    Determine account payment status based on consecutive missed payments.
    
    Args:
        payment_history: List of payment status codes (C, D, 0-9)
        threshold_months: Number of consecutive 'D' statuses to flag
        
    Returns:
        Status string: "CURRENT", "DELINQUENT", or "DEFAULTED"
    """
    consecutive_missed = count_consecutive_status(payment_history, "D")
    
    if consecutive_missed >= threshold_months:
        return "DEFAULTED"
    elif consecutive_missed > 0:
        return "DELINQUENT"
    else:
        return "CURRENT"

# INCORRECT: Multiple responsibilities, unclear purpose
def process_account(account_data):
    # Validates, transforms, calculates status, and logs - too many responsibilities
    pass
```

### Test-Driven Development

#### Red-Green-Refactor Cycle

**Always write tests before implementation:**
```python
# Step 1: RED - Write failing test
def test_payment_status_defaults_after_three_missed():
    """Account should be marked as DEFAULTED after 3+ consecutive missed payments."""
    payment_history = ["C", "C", "D", "D", "D", "C"]
    
    result = calculate_payment_status(payment_history, threshold_months=3)
    
    assert result == "DEFAULTED"

# Step 2: GREEN - Implement to pass test
def calculate_payment_status(payment_history: List[str], threshold_months: int = 3) -> str:
    consecutive_missed = 0
    max_consecutive = 0
    
    for status in payment_history:
        if status == "D":
            consecutive_missed += 1
            max_consecutive = max(max_consecutive, consecutive_missed)
        else:
            consecutive_missed = 0
    
    if max_consecutive >= threshold_months:
        return "DEFAULTED"
    elif max_consecutive > 0:
        return "DELINQUENT"
    else:
        return "CURRENT"

# Step 3: REFACTOR - Optimize while keeping tests green
def calculate_payment_status(
        payment_history: List[str],
        threshold_months: int = 3
) -> str:
    """
    Determine account payment status based on consecutive missed payments.
    
    Args:
        payment_history: List of payment status codes (C, D, 0-9)
        threshold_months: Number of consecutive 'D' statuses to flag
        
    Returns:
        Status string: "CURRENT", "DELINQUENT", or "DEFAULTED"
    """
    consecutive_missed = count_consecutive_status(payment_history, "D")
    
    if consecutive_missed >= threshold_months:
        return "DEFAULTED"
    elif consecutive_missed > 0:
        return "DELINQUENT"
    else:
        return "CURRENT"
```

#### Test Coverage Requirements

Every function must have:
- **Unit tests**: Test individual functions in isolation
- **Edge case tests**: At least 3 edge cases per function
- **Integration tests**: Test component interactions
- **Performance tests**: For operations on large datasets (>100K rows)
```python
# Comprehensive test suite example
class TestPaymentStatusCalculation:
    """Test suite for payment status calculation logic."""
    
    def test_current_status_no_missed_payments(self):
        """Account with no missed payments should be CURRENT."""
        assert calculate_payment_status(["C", "C", "C"]) == "CURRENT"
    
    def test_delinquent_status_one_missed_payment(self):
        """Account with 1-2 missed payments should be DELINQUENT."""
        assert calculate_payment_status(["C", "D", "C"]) == "DELINQUENT"
    
    def test_defaulted_status_three_consecutive_missed(self):
        """Account with 3+ consecutive missed payments should be DEFAULTED."""
        assert calculate_payment_status(["D", "D", "D"]) == "DEFAULTED"
    
    # Edge cases
    def test_empty_payment_history(self):
        """Empty history should return CURRENT."""
        assert calculate_payment_status([]) == "CURRENT"
    
    def test_non_consecutive_missed_payments(self):
        """Non-consecutive missed payments should not trigger DEFAULT."""
        assert calculate_payment_status(["D", "C", "D", "C", "D"]) == "DELINQUENT"
    
    def test_custom_threshold(self):
        """Custom threshold should be respected."""
        assert calculate_payment_status(["D", "D"], threshold_months=2) == "DEFAULTED"
```

### Documentation Standards

#### Docstring Format

Use Google-style docstrings for all public functions:
```python
def transform_customer_data(
        df: DataFrame,
        reference_date: str,
        include_inactive: bool = False
) -> DataFrame:
    """
    Transform raw customer data into standardized format for reporting.
    
    This function performs the following transformations:
    - Filters customers based on activity status
    - Calculates account age from reference date
    - Standardizes address formatting
    - Applies business rule validations
    
    Args:
        df: Input DataFrame containing raw customer records with columns:
            customer_id, name, address, status, created_date
        reference_date: ISO format date (YYYY-MM-DD) to calculate account age
        include_inactive: If True, include customers with status='INACTIVE'
        
    Returns:
        DataFrame with transformed customer data including:
            customer_id, standardized_name, formatted_address, 
            account_age_days, status
            
    Raises:
        ValueError: If reference_date is not in ISO format
        DataFrameSchemaError: If required columns are missing from input
        
    Examples:
        >>> from datetime import date
        >>> df = spark.createDataFrame([
        ...     (1, "John Doe", "123 Main St", "ACTIVE", "2020-01-01"),
        ...     (2, "Jane Smith", "456 Oak Ave", "INACTIVE", "2019-06-15")
        ... ], ["customer_id", "name", "address", "status", "created_date"])
        >>> 
        >>> result = transform_customer_data(df, "2024-01-01", include_inactive=True)
        >>> result.count()
        2
    """
    # Implementation here
    pass
```

#### Code Comments
```python
# GOOD: Explain WHY, not WHAT
# Use Z-ordering on customer_id because queries frequently filter by this field
# and our data shows high cardinality (50M+ unique values)
df.write.format("delta").mode("overwrite").option("zOrderBy", "customer_id").save(path)

# BAD: States the obvious
# Write the dataframe to delta format
df.write.format("delta").mode("overwrite").save(path)

# GOOD: Document business rules and edge cases
# Payment status 'X' indicates account in legal proceedings - exclude from
# standard reporting but include in regulatory submissions per FCA guidance
if report_type == "regulatory":
    df = df.filter(psf.col("status").isin(["C", "D", "X"]))
else:
    df = df.filter(psf.col("status").isin(["C", "D"]))

# BAD: No context for business logic
if report_type == "regulatory":
    df = df.filter(psf.col("status").isin(["C", "D", "X"]))
```

## PySpark Best Practices

### Performance Optimization

#### Databricks-Specific Optimizations
```python
# Enable Adaptive Query Execution (should be default, but ensure it's on)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Use Delta Lake optimizations
# Z-ordering for columns frequently used in filters
spark.sql("""
    OPTIMIZE delta.`/mnt/data/customers`
    ZORDER BY (customer_id, registration_date)
""")

# Vacuum old files (run periodically, not in pipelines)
spark.sql("VACUUM delta.`/mnt/data/customers` RETAIN 168 HOURS")
```

#### Efficient DataFrame Operations
```python
# CORRECT: Use broadcast join for small dimension tables (<100MB)
from pyspark.sql.functions import broadcast

large_df = spark.read.table("transactions")  # 500M rows
small_df = spark.read.table("product_codes")  # 10K rows

result = large_df.join(
    broadcast(small_df),
    on="product_code",
    how="left"
)

# CORRECT: Filter early to reduce data volume
result = (
    spark.read.table("transactions")
    .filter(psf.col("transaction_date") >= "2024-01-01")  # Filter first
    .join(other_df, on="id")                              # Then join
    .groupBy("category")                                  # Then aggregate
    .agg(psf.sum("amount").alias("total"))
)

# INCORRECT: Collect large DataFrame to driver
# This will cause OOM errors with large datasets
transactions = large_df.collect()  # DON'T DO THIS
for row in transactions:
    process(row)

# CORRECT: Process in distributed manner
result = large_df.withColumn(
    "processed",
    psf.udf(process_function, StringType())("column_name")
)

# INCORRECT: Unnecessary repartitioning
df = df.repartition(200)  # Without justification

# CORRECT: Strategic partitioning with reasoning
# Repartition to 200 because:
# - Current partition size is 2GB (target: 128MB-1GB)
# - Cluster has 100 cores, so 200 partitions = 2 partitions/core
# - Downstream operations benefit from more parallelism
df = df.repartition(200, "customer_id")  # Also partition by key for even distribution
```

#### Anti-Patterns to Avoid
```python
# ANTI-PATTERN 1: Multiple operations without caching when reused
df = spark.read.table("large_table")
count1 = df.filter(psf.col("status") == "A").count()
count2 = df.filter(psf.col("status") == "B").count()  # Re-reads entire table!

# CORRECT: Cache when DataFrame is reused
df = spark.read.table("large_table").cache()
count1 = df.filter(psf.col("status") == "A").count()
count2 = df.filter(psf.col("status") == "B").count()
df.unpersist()  # Clean up when done

# ANTI-PATTERN 2: UDF when built-in function exists
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def convert_to_upper(text):
    return text.upper() if text else None

df = df.withColumn("upper_name", convert_to_upper(psf.col("name")))

# CORRECT: Use built-in function
df = df.withColumn("upper_name", psf.upper(psf.col("name")))

# ANTI-PATTERN 3: Cross joins without size limits
df1.crossJoin(df2)  # Cartesian product - very dangerous!

# CORRECT: Avoid cross joins or limit scope
df1.join(df2, on=join_condition, how="inner")
```

### Schema Management
```python
# Define schemas explicitly for better performance and reliability
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

customer_schema = StructType(
    [
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("email", StringType(), nullable=True),
        StructField("registration_date", DateType(), nullable=False),
        StructField("status", StringType(), nullable=False)
    ]
)

# Use schema when reading data
df = spark.read.schema(customer_schema).parquet("/mnt/data/customers")

# Schema evolution with Delta Lake
# Add new column while maintaining backward compatibility
spark.sql("""
    ALTER TABLE customers
    ADD COLUMNS (loyalty_tier STRING COMMENT 'Customer loyalty tier: BRONZE, SILVER, GOLD')
""")
```

## Error Handling and Validation

### Defensive Programming
```python
def process_payment_data(
        df: DataFrame,
        expected_columns: List[str]
) -> DataFrame:
    """
    Process payment data with comprehensive validation.
    
    Args:
        df: Input DataFrame
        expected_columns: List of required column names
        
    Returns:
        Processed DataFrame
        
    Raises:
        ValueError: If required columns are missing
        DataQualityError: If data quality checks fail
    """
    # Validate schema
    missing_cols = set(expected_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Validate data quality
    null_counts = df.select(
        [psf.sum(psf.col(c).isNull().cast("int")).alias(c) for c in expected_columns]
    ).collect()[0].asDict()
    
    critical_nulls = {k: v for k, v in null_counts.items() if v > 0 and k in ["customer_id", "amount"]}
    if critical_nulls:
        raise DataQualityError(f"Null values in critical columns: {critical_nulls}")
    
    # Process with error handling for each stage
    try:
        df = df.withColumn("processed_date", psf.current_timestamp())
        df = df.withColumn("amount_usd", psf.col("amount") * psf.col("exchange_rate"))
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise
    
    return df
```

### Data Quality Checks
```python
def validate_data_quality(df: DataFrame, rules: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform comprehensive data quality checks.
    
    Args:
        df: DataFrame to validate
        rules: Dictionary of validation rules
        
    Returns:
        Dictionary containing validation results and metrics
    """
    results = {
        "total_records": df.count(),
        "validations": {},
        "passed": True
    }
    
    # Check for duplicates
    if "unique_key" in rules:
        duplicate_count = (
            df.groupBy(rules["unique_key"])
            .count()
            .filter(psf.col("count") > 1)
            .count()
        )
        results["validations"]["duplicates"] = {
            "passed": duplicate_count == 0,
            "count": duplicate_count
        }
        if duplicate_count > 0:
            results["passed"] = False
    
    # Check for required fields
    if "required_fields" in rules:
        for field in rules["required_fields"]:
            null_count = df.filter(psf.col(field).isNull()).count()
            results["validations"][f"{field}_nulls"] = {
                "passed": null_count == 0,
                "count": null_count
            }
            if null_count > 0:
                results["passed"] = False
    
    # Check data ranges
    if "range_checks" in rules:
        for field, (min_val, max_val) in rules["range_checks"].items():
            out_of_range = df.filter(
                (psf.col(field) < min_val) | (psf.col(field) > max_val)
            ).count()
            results["validations"][f"{field}_range"] = {
                "passed": out_of_range == 0,
                "count": out_of_range
            }
            if out_of_range > 0:
                results["passed"] = False
    
    return results
```

## Project Structure

### Typical Project Layout
```
project-root/
├── src/
│   ├── transformations/
│   │   ├── __init__.py
│   │   ├── customer_data.py
│   │   └── payment_data.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── spark_helpers.py
│   │   └── validation.py
│   └── config/
│       ├── __init__.py
│       └── settings.py
├── tests/
│   ├── unit/
│   │   ├── test_customer_data.py
│   │   └── test_payment_data.py
│   ├── integration/
│   │   └── test_pipeline.py
│   └── fixtures/
│       └── sample_data.py
├── notebooks/
│   ├── exploratory/
│   └── production/
├── docs/
│   ├── architecture.md
│   ├── coding-standards.md
│   └── claude.md  # This file
├── .ruff.toml
├── pyproject.toml
└── README.md
```

### Module Organization
```python
# src/transformations/customer_data.py
"""
Customer data transformation module.

This module contains functions for transforming and validating customer data
from various source systems into standardized format for downstream consumption.
"""

import pyspark.sql.functions as psf
from pyspark.sql import DataFrame
from typing import List, Dict

from src.utils.validation import validate_data_quality
from src.config.settings import CUSTOMER_REQUIRED_FIELDS


def standardize_customer_names(df: DataFrame) -> DataFrame:
    """
    Standardize customer name formatting.
    
    Applies the following transformations:
    - Converts to title case
    - Removes extra whitespace
    - Handles null values
    
    Args:
        df: DataFrame with 'name' column
        
    Returns:
        DataFrame with standardized 'name' column
    """
    return df.withColumn(
        "name",
        psf.trim(psf.initcap(psf.col("name")))
    )


def transform_customer_pipeline(df: DataFrame) -> DataFrame:
    """
    Main customer data transformation pipeline.
    
    Args:
        df: Raw customer data
        
    Returns:
        Transformed customer data
    """
    # Validate input
    validation_results = validate_data_quality(
        df,
        {"required_fields": CUSTOMER_REQUIRED_FIELDS}
    )
    if not validation_results["passed"]:
        raise ValueError(f"Input validation failed: {validation_results}")
    
    # Apply transformations
    df = standardize_customer_names(df)
    df = enrich_with_segments(df)
    df = calculate_customer_metrics(df)
    
    return df
```

## Linting and Code Quality

### Ruff Configuration
```toml
# .ruff.toml
line-length = 99
indent-width = 4

[lint]
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # pyflakes
    "I",   # isort
    "N",   # pep8-naming
    "UP",  # pyupgrade
    "B",   # flake8-bugbear
    "C4",  # flake8-comprehensions
    "DTZ", # flake8-datetimez
    "T10", # flake8-debugger
    "PT",  # flake8-pytest-style
]

ignore = [
    "E501",  # Line length handled by formatter
]

[format]
quote-style = "double"
indent-style = "space"
```

### Pre-commit Checks
```python
# Run before committing
# 1. Ruff linting
!ruff check .

# 2. Ruff formatting
!ruff format .

# 3. Run tests
!pytest tests/

# 4. Type checking (if using mypy)
!mypy src/
```

## Common Patterns and Examples

### Pattern: ETL Pipeline Template
```python
from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class ETLPipeline:
    """Base ETL pipeline with standard extract, transform, load pattern."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize ETL pipeline.
        
        Args:
            spark: Active SparkSession
            config: Pipeline configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract(self) -> DataFrame:
        """
        Extract data from source.
        
        Returns:
            Raw DataFrame from source
        """
        raise NotImplementedError("Subclasses must implement extract()")
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform extracted data.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            Transformed DataFrame
        """
        raise NotImplementedError("Subclasses must implement transform()")
    
    def load(self, df: DataFrame) -> None:
        """
        Load transformed data to destination.
        
        Args:
            df: Transformed DataFrame to load
        """
        raise NotImplementedError("Subclasses must implement load()")
    
    def run(self) -> None:
        """Execute full ETL pipeline with error handling and logging."""
        try:
            self.logger.info("Starting ETL pipeline")
            
            # Extract
            self.logger.info("Extracting data")
            raw_df = self.extract()
            self.logger.info(f"Extracted {raw_df.count()} records")
            
            # Transform
            self.logger.info("Transforming data")
            transformed_df = self.transform(raw_df)
            self.logger.info(f"Transformed to {transformed_df.count()} records")
            
            # Load
            self.logger.info("Loading data")
            self.load(transformed_df)
            self.logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {str(e)}")
            raise


# Concrete implementation
class CustomerETL(ETLPipeline):
    """ETL pipeline for customer data processing."""
    
    def extract(self) -> DataFrame:
        """Extract customer data from source table."""
        return self.spark.read.table(self.config["source_table"])
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply customer-specific transformations."""
        return (
            df
            .filter(psf.col("status").isNotNull())
            .withColumn("full_name", psf.concat_ws(" ", "first_name", "last_name"))
            .withColumn("processed_at", psf.current_timestamp())
        )
    
    def load(self, df: DataFrame) -> None:
        """Load to Delta Lake table with overwrite."""
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.config["target_table"])
        )
```

### Pattern: Window Functions for Analytics
```python
from pyspark.sql.window import Window

def calculate_customer_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate customer-level metrics using window functions.
    
    Args:
        df: DataFrame with transaction history
        
    Returns:
        DataFrame with calculated metrics
    """
    # Define window specifications
    customer_window = Window.partitionBy("customer_id")
    
    customer_time_window = (
        Window
        .partitionBy("customer_id")
        .orderBy("transaction_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    # Calculate metrics
    result = df.withColumn(
        # Total transactions per customer
        "total_transactions",
        psf.count("*").over(customer_window)
    ).withColumn(
        # Lifetime value
        "customer_lifetime_value",
        psf.sum("transaction_amount").over(customer_window)
    ).withColumn(
        # Running total
        "running_total",
        psf.sum("transaction_amount").over(customer_time_window)
    ).withColumn(
        # Rank transactions by amount within customer
        "amount_rank",
        psf.rank().over(
            Window
            .partitionBy("customer_id")
            .orderBy(psf.desc("transaction_amount"))
        )
    ).withColumn(
        # Days since last transaction
        "days_since_last_transaction",
        psf.datediff(
            psf.current_date(),
            psf.max("transaction_date").over(customer_window)
        )
    )
    
    return result
```

### Pattern: Configuration Management
```python
# src/config/settings.py
"""
Project configuration settings.

This module centralizes all configuration parameters for the project.
"""

from dataclasses import dataclass
from typing import List, Dict
import os


@dataclass
class SparkConfig:
    """Spark configuration parameters."""
    app_name: str = "DataPipeline"
    shuffle_partitions: int = 200
    adaptive_enabled: bool = True
    

@dataclass
class DataConfig:
    """Data-related configuration."""
    source_path: str = "/mnt/data/raw"
    target_path: str = "/mnt/data/processed"
    checkpoint_location: str = "/mnt/data/checkpoints"
    

@dataclass
class ValidationConfig:
    """Data validation rules."""
    required_columns: List[str] = None
    max_null_percentage: float = 0.05  # 5%
    date_format: str = "yyyy-MM-dd"
    
    def __post_init__(self):
        if self.required_columns is None:
            self.required_columns = ["id", "timestamp", "amount"]


class Config:
    """Main configuration class."""
    
    def __init__(self, environment: str = "dev"):
        """
        Initialize configuration.
        
        Args:
            environment: Environment name (dev, test, prod)
        """
        self.environment = environment
        self.spark = SparkConfig()
        self.data = DataConfig()
        self.validation = ValidationConfig()
        
        # Load environment-specific overrides
        self._load_environment_config()
    
    def _load_environment_config(self):
        """Load environment-specific configuration overrides."""
        if self.environment == "prod":
            self.spark.shuffle_partitions = 500
            self.validation.max_null_percentage = 0.01  # Stricter in prod
        elif self.environment == "test":
            self.data.source_path = "/tmp/test/raw"
            self.data.target_path = "/tmp/test/processed"


# Usage
config = Config(environment=os.getenv("ENV", "dev"))
```

## CI/CD Integration

### Example Pipeline Configuration
```yaml
# .github/workflows/ci.yml or azure-pipelines.yml
name: Data Engineering CI/CD

on:
  pull_request:
    branches: [ main ]
  push:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.10'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install ruff pytest pytest-cov
    
    - name: Run Ruff linting
      run: ruff check .
    
    - name: Run Ruff formatting check
      run: ruff format --check .
    
    - name: Run tests with coverage
      run: pytest tests/ --cov=src --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v2
```

## Guidelines for Claude Code

When assisting with code in this project:

1. **Always prioritize test-first development**: Write tests before implementation
2. **Follow the 99-character line limit strictly**
3. **Use PySpark idiomatically**: Prefer built-in functions over UDFs
4. **Include comprehensive docstrings**: Every public function needs documentation
5. **Consider performance**: Think about data volume and distributed computing
6. **Validate inputs**: Add defensive checks for data quality
7. **Use type hints**: Make function signatures clear with typing
8. **Log appropriately**: Include logging for debugging and monitoring
9. **Handle errors gracefully**: Anticipate failures and handle them explicitly
10. **Document business logic**: Explain WHY, not just WHAT

### When Generating Code

- Start with the test cases
- Provide complete, runnable examples
- Include error handling
- Add performance considerations as comments
- Suggest optimizations where applicable
- Reference relevant documentation

### When Reviewing Code

- Check against SOLID principles
- Verify test coverage
- Look for performance anti-patterns
- Ensure documentation is complete
- Validate error handling
- Confirm style guide adherence

---

**Version**: 1.0  
**Last Updated**: December 2024  
**Purpose**: Guide Claude Code in assisting with data engineering tasks