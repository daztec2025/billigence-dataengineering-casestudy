# Case Study 3: Customer Data Restructuring - Overview

**Client:** ABC Corporation
**Objective:** Upgrade data infrastructure with secure, accessible customer data

---

## Business Challenge

ABC Corporation is undergoing a data infrastructure upgrade to improve their data management and analysis capabilities. Their current customer data faces two critical challenges:

1. **Complex Data Structure** - Nested JSON format makes analysis difficult
2. **Data Security** - Email addresses contain PII that must be protected

---

## Business Requirements

### Requirement 1: Convert to Tabular Format

**Problem:**
- Customer data stored in nested JSON structure (3 levels deep)
- Arrays within arrays make SQL queries complex
- Difficult to integrate with analytics tools

**Solution Needed:**
- Flatten JSON into traditional tabular format
- Enable standard SQL queries
- Simplify data integration

### Requirement 2: Email Protection

**Problem:**
- Email addresses are Personally Identifiable Information (PII)
- Regulatory compliance (GDPR) requires protection
- Need to balance security with analytics capability

**Solution Needed:**
- Mask email addresses for display
- Encrypt for secure storage
- Preserve domain names for analytics

---

## Data Structure

### Original Format (Nested JSON)

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
```

### Target Format (Tabular)

| customer_id | company_id | company_name | customer_name | email | date_of_birth | place_of_birth | phone_number | role | age |
|-------------|------------|--------------|---------------|-------|---------------|----------------|--------------|------|-----|
| 1 | 123 | ABC Corporation | Carlos Saint | \*\*\*\*\*\*\*@gmail.com | 1991-01-05 | Madrid | 555-5555-1234 | Data Engineer | 33 |
| 2 | 123 | ABC Corporation | Felipe Guerrero | \*\*\*\*\*\*\*@yahoo.com | 1968-01-30 | Porto | 555-4444-1234 | Business Analyst | 56 |

---

## Technical Approach

### Data Transformation Strategy

1. **Explode nested arrays**
   - Extract CompanyInfo array
   - Extract Customers array

2. **Flatten structure**
   - Select all fields from nested customer objects
   - Create clean column names

3. **Enrich data**
   - Convert date strings to date types
   - Calculate derived fields (age)
   - Add audit columns (load_timestamp)

### Email Protection Strategy

#### Display Masking (UI/Reports)
- Pattern: `*******@domain.com`
- Purpose: Show data without revealing identity
- Reversibility: No (one-way transformation for display)

#### Cryptographic Hashing (Storage)
- Algorithm: SHA-256
- Purpose: Secure storage, deduplication, verification
- Reversibility: No (cryptographically secure)

#### Domain Preservation (Analytics)
- Extract domain part (e.g., `gmail.com`, `yahoo.com`)
- Purpose: Enable domain-based analytics
- Example queries: "How many customers use Gmail?"

---

## Expected Outcomes

### Task 1: Tabular Format

**Before:**
- Nested JSON with 3 levels
- Arrays requiring special JSON parsing
- Difficult to query with SQL

**After:**
- Flat table structure
- Standard SQL queryable
- 12 columns with proper data types

### Task 2: Email Masking

**Before:**
```
carlos91@gmail.com
Felipe_g@yahoo.com
```

**After (Display):**
```
*******@gmail.com
*******@yahoo.com
```

**After (Storage):**
```
SHA-256: a3f2b8c9d1e0f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3a2b1
```

---

## Security Considerations

### Data Classification

- **High Sensitivity:** Email addresses (PII)
- **Medium Sensitivity:** Phone numbers, dates of birth
- **Low Sensitivity:** Roles, company information

### Protection Measures

1. **Email Masking**
   - Production dataset contains only masked emails
   - Original emails stored in separate, restricted table

2. **Encryption**
   - SHA-256 hashing for secure storage
   - One-way transformation (cannot reverse)

3. **Access Control**
   - Secure dataset: General access
   - Reference table: Restricted to authorized users only

### Compliance Benefits

- **GDPR:** Email masking provides pseudonymization
- **ISO 27001:** Aligns with information security standards

---

## Use Cases

### Analytics (Secure Dataset)

```sql
-- Domain analysis (no PII exposed)
SELECT
  REGEXP_EXTRACT(email, '@(.+)$', 1) as domain,
  COUNT(*) as customer_count
FROM billigence_abc_customers_secure
GROUP BY domain;
```

### Customer Service (Reference Table - Restricted)

```sql
-- Lookup original email for authorized support staff
SELECT
  email_hash,
  original_email
FROM billigence_abc_email_reference
WHERE email_hash = '<sha256_hash_from_support_ticket>';
```

### Reporting (Masked Display)

```
Customer Report
===============
Name: Carlos Saint
Email: *******@gmail.com
Role: Data Engineer
```

---

## Success Metrics

1. **Data Accessibility**
   - SQL queries 10x faster on tabular format
   - Compatible with all BI tools

2. **Security Compliance**
   - 100% of emails protected in production
   - Zero plain-text PII in analytics datasets

3. **Analytics Capability**
   - Domain-based analysis enabled
   - Customer segmentation possible

4. **Data Quality**
   - Zero null values in critical fields
   - All transformations validated

---

## Files

- **Script:** [case_study_3_analysis.py](../../scripts/case_study_3_analysis.py)
- **Tests:** [test_case_study_3.py](../../tests/test_case_study_3.py)
- **Source Data:** `data/case_study_3/Customers.json`

---

## Testing

Comprehensive unit tests validate all transformations, email masking, and security measures.

**Test File:** `tests/test_case_study_3.py`

```bash
# Run tests
python3 -m pytest tests/test_case_study_3.py -v
```

**Test Coverage:** 57 tests covering:

| Category | Tests |
|----------|-------|
| Data Loading | 5 |
| JSON Flattening (Task 1) | 10 |
| Email Masking (Task 2) | 8 |
| Date Transformation | 4 |
| Secure Dataset | 2 |
| Reference Table | 3 |
| Data Quality | 6 |
| Edge Cases | 15 |
| Integration | 2 |
| **Total** | **57** |

Key test validations:

- **SHA-256 Hash Verification** - Known hash values validated
- **Email Masking** - Username hidden, domain preserved
- **Age Calculation** - Correct age computed from birth dates
- **Pipeline Idempotency** - Consistent results on repeated runs

See [Unit Tests](../case_study_1/unit-tests.md#case-study-3-customer-data-restructuring) for detailed test documentation.

---
