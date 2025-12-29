# Billigence Data Engineering Case Study - Deren Ridley

**Author:** Deren Ridley
**Date:** December 19, 2025
**Platform:** PySpark 3.5.0 on Docker (mimics Databricks 14.3 LTS)

---

## Case Study 1: Retail Electronics Analysis

Analysis of 500K+ retail transactions to identify top products and profitable regions.

### Key Deliverables

1. **Top 10 Products by Revenue** - Highest-grossing products (revenue > $500K)
2. **Top 3 Regions by Profit** - Most profitable geographic regions
3. **Data Quality Framework** - Validation pipeline ensuring data integrity
4. **25 Unit Tests** - Comprehensive test coverage

---

## Case Study 2: University API Analysis

Integration with Hipolabs University API to analyze global higher education data.

### Key Deliverables

1. **Data Transformation** - Removed domains column from API response
2. **Top 3 Countries Analysis** - Countries with most universities
3. **Specific Country Counts** - University counts for UK, France, and China
4. **Database Integration** - Results written to Delta Lake, SQLite, and CSV formats
5. **63 Unit Tests** - Comprehensive test coverage

---

## Case Study 3: Customer Data Restructuring & Email Protection

Transformation of nested JSON customer data with PII protection for ABC Corporation.

### Key Deliverables

1. **Tabular Format Conversion** - Flattened 3-level nested JSON to SQL-queryable table
2. **Email Masking** - Display protection showing only `*******@domain.com`
3. **Cryptographic Hashing** - SHA-256 encryption for secure storage
4. **Access Control Design** - Segregated datasets (secure vs. reference)
5. **GDPR Compliance** - UK/EU regulatory alignment
6. **57 Unit Tests** - Comprehensive test coverage

---

## Technologies Used

| Technology | Purpose | Used In |
|-----------|---------|---------|
| **PySpark** | Distributed data processing | All case studies |
| **Delta Lake** | Reliable data storage | All case studies |
| **Docker** | Containerized runtime environment | All case studies |
| **REST API** | External data integration | Case Study 2 |
| **SQLite** | Lightweight database | Case Study 2 |
| **SHA-256** | Cryptographic hashing | Case Study 3 |
| **pytest** | Unit testing framework | All case studies |
| **MkDocs** | Documentation | This site |

---

## Quick Start

```bash
# Run Case Study 1: Retail Electronics
python3 scripts/case_study_1_analysis.py

# Run Case Study 2: University API
python3 scripts/case_study_2_analysis.py

# Run Case Study 3: Customer Data Security
python3 scripts/case_study_3_analysis.py

# Run all tests
python3 -m pytest tests/ -v
```

See [How to Run](appendix/how-to-run.md) for detailed instructions.

---

## Documentation Structure

### Case Study 1 - Retail Electronics
- **Overview** - Project overview and dataset description
- **Methodology** - Technology stack, data understanding, performance tuning
- **Implementation** - Data loading and quality checks
- **Results** - Top products and top regions analysis
- **Testing** - 25 unit tests with 100% coverage

### Case Study 2 - University API
- **Overview** - API integration and requirements
- **Implementation** - Data fetching and transformations
- **Performance Tuning** - Optimization strategies
- **Results** - Country rankings and statistics
- **Database Output** - Multi-format database exports
- **Testing** - 63 unit tests with 100% coverage

### Case Study 3 - Customer Data Security
- **Overview** - Business challenge and data structure
- **Implementation** - JSON flattening and email protection
- **Performance Tuning** - Optimization strategies
- **Results** - Transformation outcomes
- **Security** - GDPR and ISO 27001 compliance
- **Testing** - 57 unit tests with 100% coverage

### Appendix
- **Running the Project** - Setup and execution instructions
- **Data Files Review** - Input file inventory
- **SQL Scripts** - Database queries and scripts

---

## Test Summary

| Case Study | Tests | Status |
|------------|-------|--------|
| Case Study 1 | 25 | All Passing |
| Case Study 2 | 63 | All Passing |
| Case Study 3 | 57 | All Passing |
| **Total** | **145** | **All Passing** |

---
