# Case Study 2: Implementation

This solution was built using a Docker image designed to closely mimic Databricks Runtime 14.3 LTS, but is portable to any Spark engine.

## Data Loading

### API Integration

```python
import requests
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Fetch from API with fallback
API_URL = "http://universities.hipolabs.com/search"

try:
    response = requests.get(API_URL, timeout=60)
    data = response.json()
    df = spark.createDataFrame(data, schema=university_schema)
except:
    df = spark.read.json("data/case_study_2/universities_raw.json")
```

### Schema Definition

```python
university_schema = StructType([
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("alpha_two_code", StringType(), True),
    StructField("web_pages", ArrayType(StringType()), True),
    StructField("domains", ArrayType(StringType()), True),
    StructField("state-province", StringType(), True)
])
```

## Task 1: Remove Domains Column

```python
universities_clean = universities_df.drop("domains")
```

## Task 2: Top 3 Countries

```python
top_3_countries = (
    universities_clean
    .groupBy("country")
    .agg(F.count("*").alias("university_count"))
    .orderBy(F.desc("university_count"))
    .limit(3)
)

# Save to Delta
top_3_countries.write.format("delta").mode("overwrite")
    .saveAsTable("billigence_top_3_countries_universities")
```

## Task 3: Specific Country Counts

```python
# Handle country name mapping (UK -> United Kingdom)
target_countries = {
    "UK": "United Kingdom",
    "France": "France",
    "China": "China"
}

results = []
for display_name, api_name in target_countries.items():
    count = universities_clean.filter(F.col("country") == api_name).count()
    results.append({
        "country_display": display_name,
        "country_actual": api_name,
        "university_count": count
    })

uk_france_china = spark.createDataFrame(results)
```

## Task 4: Database Output

Multiple format support for different systems:

```python
# Delta Lake (primary)
df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# SQLite (for portability)
pandas_df = df.toPandas()
pandas_df.to_sql(table_name, conn, if_exists='replace', index=False)

# CSV (for SQL Server import)
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
```

## Task 5: Enrollment Analysis

Sample enrollment data structure for demonstration:

```python
enrollment_data = [
    {"country": "United States", "year": 2010, "enrollment_rate": 94.0,
     "total_enrolled": 20275000, "population": 309321666},
    # ...
]

# Analysis
top_3_list = [row.country for row in top_3_countries.collect()]
top_3_enrollment = enrollment_df.filter(F.col("country").isin(top_3_list))
```

## Data Validation

```python
# Verify row counts
assert df.count() > 0, "No data loaded"

# Check for nulls in critical fields
null_count = df.filter(F.col("country").isNull()).count()
assert null_count == 0, "Null countries found"
```
