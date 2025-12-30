# Data Files Review

**Reviewer:** Deren Ridley
**Date:** December 2025

---

## Overview

This document provides a comprehensive review of all data files used across the three case studies in this portfolio. Each case study uses different data sources and formats, demonstrating proficiency with various data engineering scenarios.

---

## Complete File Inventory

```
data/
├── case_study_1/
│   ├── Transaction_Details.csv      66 MB   *** PRIMARY ***
│   ├── Store_Details.csv            13 KB   *** PRIMARY ***
│   ├── Product_Details.csv          95 KB   *** PRIMARY ***
│   ├── Transaction_Details.xlsx     27 MB   (Excel duplicate)
│   ├── Transaction_Details(1).xlsx  27 MB   (Excel duplicate)
│   ├── Store_Details.xlsx           16 KB   (Excel duplicate)
│   ├── Product_Details.xlsx         44 KB   (Excel duplicate)
│   ├── Product_Details(1).xlsx      44 KB   (Excel duplicate)
│   └── Customers.json               639 B   (Not part of requirements)
│
├── case_study_2/
│   ├── universities_raw.json        2.1 MB  *** PRIMARY (API cache) ***
│   └── sample_universities.json     1.5 KB  (Test/sample data)
│
└── case_study_3/
    └── Customers.json               639 B   *** PRIMARY ***
```

---

# Case Study 1: Retail Electronics Analysis

## Data Files Used

| File | Size | Rows | Status |
|------|------|------|--------|
| Transaction_Details.csv | 66 MB | 521,612 | PRIMARY |
| Store_Details.csv | 13 KB | 196 | PRIMARY |
| Product_Details.csv | 95 KB | 1,095 | PRIMARY |

### Transaction_Details.csv (PRIMARY)

- **Purpose:** Main fact table containing all sales transactions
- **Columns:**
  - Date - Transaction date
  - Order Number - Unique order identifier
  - Product Name - Product description
  - Sku Number - Product SKU (links to Product_Details)
  - Quantity - Units sold
  - Cost - Unit cost
  - Price - Unit price
  - Store Key - Store identifier (links to Store_Details)
  - Cust Key - Customer identifier
  - Customer Name - Customer name
- **Notes:** Core dataset for revenue and profit analysis

### Store_Details.csv (PRIMARY)

- **Purpose:** Dimension table with store locations
- **Columns:**
  - Store Key - Unique store identifier
  - Store Name - Store name
  - Store Region - Geographic region (North, South, East, West)
  - Store State - US state
  - Store City - City name
  - Store Latitude - Geographic coordinates
  - Store Longitude - Geographic coordinates
- **Notes:** Critical for regional profit analysis (Task 2)

### Product_Details.csv (PRIMARY)

- **Purpose:** Dimension table with product catalog
- **Columns:**
  - Sku Number - Unique product identifier
  - Product Name - Product description
  - Product Type - Product category
  - Product Family - Product grouping
- **Notes:** Essential for top products by revenue (Task 1)

## Files Not Used (Case Study 1)

### Excel Files (.xlsx)

All Excel files are duplicates of their CSV counterparts:

| Excel File | CSV Equivalent | Decision |
|------------|----------------|----------|
| Transaction_Details.xlsx | Transaction_Details.csv | CSV preferred |
| Transaction_Details(1).xlsx | Same as above | Duplicate file |
| Store_Details.xlsx | Store_Details.csv | CSV preferred |
| Product_Details.xlsx | Product_Details.csv | CSV preferred |
| Product_Details(1).xlsx | Same as above | Duplicate file |

**Rationale for using CSV over Excel:**

1. Native PySpark CSV reader - no format conversion
2. Faster loading performance
3. Direct column access without worksheet handling
4. Industry standard for data engineering pipelines
5. Consistent format across all three tables

### Customers.json

- **Size:** 639 bytes (2 sample records)
- **Decision:** Not used in Case Study 1
- **Rationale:**
  1. Case study requirements specify "Transaction Table, Store Table and Product Detail Table" - Customer table is not mentioned
  2. Contains only 2 sample records vs 4,500+ unique customers in transactions
  3. Customer names in this file do not match any customers in Transaction_Details
  4. Customer information already exists in Transaction_Details (Cust Key, Customer Name)

---

# Case Study 2: University API Analysis

## Data Source

**Primary Source:** Hipolabs University API (`http://universities.hipolabs.com/search`)

The analysis fetches data directly from the REST API. Local files serve as fallback and test data.

## Data Files

| File | Size | Records | Status |
|------|------|---------|--------|
| universities_raw.json | 2.1 MB | ~10,000+ | PRIMARY (API Dump) |
| sample_universities.json | 1.5 KB | 10 | Test Data |

### universities_raw.json (PRIMARY)

- **Purpose:** Complete university dataset dumped from the Hipolabs API
- **Format:** JSON array of university objects
- **Schema:**
  ```json
  {
    "name": "University of Oxford",
    "country": "United Kingdom",
    "alpha_two_code": "GB",
    "web_pages": ["http://www.ox.ac.uk/"],
    "domains": ["ox.ac.uk"],
    "state-province": null
  }
  ```
- **Why This File Exists:**
  - The Hipolabs API (`http://universities.hipolabs.com/search`) was not working reliably during development
  - To ensure consistent and reproducible results, the API response was dumped to this JSON file
  - The analysis script uses this local file as the primary data source, with the API as an alternative
  - Contains the full global university dataset (~10,000+ records from 200+ countries)

### sample_universities.json (TEST DATA)

- **Purpose:** Small sample for unit testing
- **Records:** 10 universities from UK, France, China, USA
- **Decision:** Not used in production analysis
- **Rationale:**
  1. Subset for rapid unit test execution
  2. Contains known values for test assertions
  3. Ensures tests run without API dependency

---

# Case Study 3: Customer Data Security

## Data Files Used

| File | Size | Records | Status |
|------|------|---------|--------|
| Customers.json | 639 B | 2 | PRIMARY |

### Customers.json (PRIMARY)

- **Purpose:** Nested JSON structure for data transformation exercise
- **Format:** Multi-level nested JSON with arrays
- **Schema:**
  ```json
  {
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
          }
        ]
      }
    ]
  }
  ```
- **Records:**
  1. Carlos Saint (Data Engineer, born 1991, Madrid)
  2. Felipe Guerrero (Business Analyst, born 1968, Porto)
- **Notes:**
  - This is the correct and complete input for Case Study 3
  - The case study demonstrates JSON flattening and email masking on this dataset
  - Small dataset size is intentional - the focus is on transformation logic, not volume
  - Same file structure as `data/case_study_1/Customers.json` (they serve different purposes)

---

## Cross-Case Study Analysis

### File Naming Observation

The file `Customers.json` appears in two locations:

| Location | Purpose | Used |
|----------|---------|------|
| `data/case_study_1/Customers.json` | Not part of Case Study 1 requirements | No |
| `data/case_study_3/Customers.json` | Primary input for Case Study 3 | Yes |

**Clarification:** Although these files have identical content, they serve different purposes:

- In Case Study 1: This file is extraneous (not mentioned in requirements, insufficient data for the retail analysis)
- In Case Study 3: This file IS the primary input - the entire case study is about transforming this nested JSON structure

This is not an error - each case study has distinct requirements and data sources.

### Format Selection Rationale

| Case Study | Format | Reason |
|------------|--------|--------|
| Case Study 1 | CSV | Large dataset (500K+ rows), standard tabular data |
| Case Study 2 | JSON (API) | REST API returns JSON, natural format for web data |
| Case Study 3 | JSON (Nested) | Demonstrates flattening complex structures |

### Data Quality Verification

All primary data files have been validated:

**Case Study 1:**
- Transaction_Details.csv: 521,612 rows, no header issues
- Store_Details.csv: 196 rows, all regions present
- Product_Details.csv: 1,095 rows, complete SKU coverage

**Case Study 2:**
- API Response: ~10,000+ universities from 200+ countries
- Fallback JSON: Complete snapshot of API data

**Case Study 3:**
- Customers.json: Valid JSON structure, 2 customer records
- All required fields present (Name, Email, Birth, Phone, Role, Place of Birth)

---

## Validation: Requirements Coverage

### Case Study 1 Requirements

> "The company has a database containing Transaction Table, Store Table and Product Detail Table."

| Requirement | File Used |
|-------------|-----------|
| Transaction Table | Transaction_Details.csv |
| Store Table | Store_Details.csv |
| Product Detail Table | Product_Details.csv |

**Status:** All requirements covered.

### Case Study 2 Requirements

> "Fetch data from the Hipolabs University API"

| Requirement | Source |
|-------------|--------|
| University Data | Local: `universities_raw.json` (dumped from API) |
| Alternative | API: `http://universities.hipolabs.com/search` |

**Status:** The Hipolabs API was not working reliably during development, so the API response was dumped to a local JSON file. The analysis uses this local file as the primary data source to ensure consistent, reproducible results.

### Case Study 3 Requirements

> "Convert nested JSON to tabular format and mask email addresses"

| Requirement | File Used |
|-------------|-----------|
| Nested JSON Input | Customers.json |

**Status:** Correct input file identified and processed.

---

## Conclusion

### Files Analyzed
- **Total files:** 12
- **Primary files used:** 5 (3 for CS1, 1 API + 1 fallback for CS2, 1 for CS3)
- **Duplicate/test files excluded:** 7

### Key Decisions Documented

1. **CSV over Excel:** Performance and compatibility
2. **API with fallback:** Reliability and offline capability
3. **Small dataset for CS3:** Focus on transformation logic

### Thoroughness Verification

- All data directories reviewed
- All file formats assessed
- Duplicate files identified
- Test data distinguished from production data
- Requirements cross-referenced with file usage

---

**Status:** Complete Review
**All Files Accounted For:** Yes
