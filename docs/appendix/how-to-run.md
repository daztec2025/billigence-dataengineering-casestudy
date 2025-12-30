# How to Run This Analysis

This guide provides instructions for running the Billigence Case Studies using the Docker-based PySpark environment.

---

## Platform Portability

This Docker image can be run on any local Docker environment or cloud platform. The PySpark solution is designed to be easily ported to:

- **Databricks** - Native Spark support with Delta Lake
- **Snowflake** - Via Snowpark for Python
- **AWS EMR** - Managed Spark clusters
- **Azure Synapse** - Azure-native Spark
- **Google Dataproc** - GCP managed Spark
- **Any Spark engine** - Standard PySpark API compatibility

---

## Prerequisites

- Docker installed and running
- Python 3.10+
- Git (to clone the repository)

---

## Step 1: Clone Repository

```bash
git clone https://github.com/daztec2025/billigence-dataengineering-casestudy.git
cd billigence-dataengineering-casestudy
```

---

## Step 2: Start Docker Environment

The project includes a Docker configuration that mimics Databricks Runtime 14.3 LTS.

```bash
# Build and start the container
docker-compose up -d

# Or use the devcontainer in VS Code
# Open folder in VS Code → "Reopen in Container"
```

---

## Step 3: Run the Analysis Scripts

### Case Study 1: Retail Electronics

```bash
python3 scripts/case_study_1_analysis.py
```

**Output:**
- `output/case_study_1/top_10_products/` (Parquet)
- `output/case_study_1/top_3_regions/` (Parquet)
- `output/case_study_1/csv/` (CSV exports)

### Case Study 2: University API

```bash
python3 scripts/case_study_2_analysis.py
```

**Output:**
- `output/case_study_2/parquet/` (Parquet files)
- `output/case_study_2/universities.db` (SQLite database)
- `output/case_study_2/*.csv` (CSV exports)

### Case Study 3: Customer Data Security

```bash
python3 scripts/case_study_3_analysis.py
```

**Output:**
- `output/case_study_3/parquet/` (Parquet files)
- `output/case_study_3/*.csv` (CSV exports)

---

## Step 4: Run Unit Tests

```bash
# Run all tests for Case Study 1
python3 -m unittest tests.test_case_study_1 -v
```

---

## Step 5: View Documentation

```bash
# Start MkDocs server
mkdocs serve -a 0.0.0.0:8001

# Open http://localhost:8001 in your browser
```

---

## Data File Locations

| File | Location |
|------|----------|
| Transaction_Details.csv | `data/case_study_1/` |
| Store_Details.csv | `data/case_study_1/` |
| Product_Details.csv | `data/case_study_1/` |
| universities_raw.json | `data/case_study_2/` |
| Customers.json | `data/case_study_3/` |

---

## Troubleshooting

### Issue: "Module not found"

```bash
pip install -r requirements.txt
```

### Issue: "Spark not starting"

Ensure Java is installed:
```bash
java -version
```

### Issue: "Out of memory"

Increase Docker memory allocation in Docker Desktop settings.

