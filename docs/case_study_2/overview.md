# Case Study 2: University API Analysis - Overview

**Objective:** Analyze global university data from the Hipolabs API

---

## Business Requirements

This case study demonstrates API integration and data processing capabilities by analyzing university data worldwide.

### Tasks

1. **Data Transformation**
   - Remove the "domains" column from the API response
   - Clean and prepare data for analysis

2. **Country Rankings**
   - Identify the top 3 countries by number of universities
   - Provide comprehensive statistics

3. **Specific Country Analysis**
   - Count universities in the United Kingdom, France, and China
   - Handle country name variations (e.g., "UK" vs "United Kingdom")

4. **Database Integration**
   - Write results to a SQL database (SQL Server or equivalent)
   - Support multiple export formats

5. **Optional: Enrollment Analysis**
   - Analyze 2010 higher education enrollment data
   - Calculate enrollment percentages for top countries
   - Identify country with highest enrollment

---

## Data Source

**API Endpoint:** `http://universities.hipolabs.com/search`

The Hipolabs University API provides comprehensive data on universities worldwide, including:

- University name
- Country
- Alpha-2 country code
- Web pages
- Domains
- State/province information

### API Response Structure

```json
[
  {
    "name": "University of Oxford",
    "country": "United Kingdom",
    "alpha_two_code": "GB",
    "web_pages": ["http://www.ox.ac.uk/"],
    "domains": ["ox.ac.uk"],
    "state-province": null
  },
  ...
]
```

---

## Technical Approach

### Data Fetching

- Primary: Fetch from REST API using Python `requests` library
- Fallback: Load from local JSON file if API unavailable
- Error handling for network issues and timeouts

### Data Processing

- **PySpark DataFrame API** for distributed processing
- Explicit schema definition for type safety
- Column transformations and filtering
- Aggregations and grouping operations

### Database Output

Multiple output formats to ensure compatibility:

1. **Delta Lake Tables** - Production-grade ACID compliant storage
2. **SQLite Database** - Lightweight SQL database
3. **CSV Exports** - Universal format for SQL Server import
4. **Parquet Files** - Columnar format for analytics

---

## Expected Outcomes

### Task 1: Data Transformation
- DataFrame without the "domains" column
- Verification of column removal

### Task 2: Top 3 Countries
- Delta table: `billigence_top_3_countries_universities`
- Columns: country, university_count
- Ordered by university count (descending)

### Task 3: UK, France, China Counts
- Delta table: `billigence_uk_france_china_universities`
- Columns: country_display, country_actual, university_count
- Handles country name mapping

### Task 4: Database Output
- SQLite database with results
- CSV files ready for SQL Server import
- SQL INSERT statements generated

### Task 5: Enrollment Analysis (Optional)
- 2010 enrollment percentages for top countries
- Country with highest enrollment numbers
- Enrollment rate comparisons

---

## Skills Demonstrated

- REST API integration
- JSON data processing
- PySpark transformations
- Error handling and fallback mechanisms
- Multiple database export formats
- Data validation and quality checks
- Professional documentation

---

## Files

- **Script:** [case_study_2_analysis.py](../../scripts/case_study_2_analysis.py)
- **Tests:** [test_case_study_2.py](../../tests/test_case_study_2.py)
- **Documentation:** This section

---

## Testing

Comprehensive unit tests validate all transformations and business logic.

**Test File:** `tests/test_case_study_2.py`

```bash
# Run tests
python3 -m pytest tests/test_case_study_2.py -v
```

**Test Coverage:** 63 tests covering:

| Category | Tests |
|----------|-------|
| Data Loading | 4 |
| Data Quality | 7 |
| Task 1 (Domains) | 5 |
| Task 2 (Top N) | 5 |
| Task 3 (Countries) | 7 |
| Alpha Codes | 4 |
| Edge Cases | 8 |
| Full Dataset | 5 |
| **Total** | **63** |

See [Unit Tests](../case_study_1/unit-tests.md#case-study-2-university-api-analysis) for detailed test documentation.

---
