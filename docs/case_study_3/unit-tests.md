# Unit Testing

**Test File:** `tests/test_case_study_3.py`

Tests cover:
- JSON data loading and schema validation
- Nested JSON flattening to tabular format (Task 1)
- Email masking and encryption (Task 2)
- Domain extraction
- SHA-256 and MD5 hashing
- Secure dataset creation
- Reference table generation
- Data quality validation
- Edge cases and error handling

## Running Tests

```bash
# Run Case Study 3 tests
python3 -m pytest tests/test_case_study_3.py -v
```

## Test Results

**Total Tests:** 57

**Status:** All tests passing

```
============================= 57 passed in 29.18s ==============================
```

## TestCaseStudy3

Comprehensive unit tests for Case Study 3: Customer Data Restructuring

### Data Loading Tests (5 tests)

| Test | Description |
|------|-------------|
| `test_json_file_exists` | Verify JSON file exists |
| `test_json_loads_successfully` | Verify JSON loads without errors |
| `test_raw_schema_has_company_id` | Verify schema has CompanyID |
| `test_raw_schema_has_company_info` | Verify schema has CompanyInfo |
| `test_company_info_is_array` | Verify CompanyInfo is array type |

### Task 1: JSON Flattening (10 tests)

| Test | Description |
|------|-------------|
| `test_flatten_produces_correct_columns` | Verify expected columns produced |
| `test_flatten_expands_customers` | Verify nested customers expanded |
| `test_flatten_company_id_preserved` | Verify company_id preserved |
| `test_flatten_company_name_extracted` | Verify company_name extracted |
| `test_flatten_customer_names` | Verify customer names extracted |
| `test_flatten_emails` | Verify emails extracted |
| `test_flatten_birth_dates` | Verify birth dates extracted |
| `test_flatten_roles` | Verify roles extracted |
| `test_flatten_places_of_birth` | Verify places of birth extracted |
| `test_flatten_phone_numbers` | Verify phone numbers extracted |

### Task 2: Email Masking (8 tests)

| Test | Description |
|------|-------------|
| `test_email_domain_extraction` | Verify domain extraction |
| `test_email_masked_format` | Verify masked format (*******@domain.com) |
| `test_email_masked_hides_username` | Verify username hidden |
| `test_email_sha256_hash` | Verify SHA-256 hash generation |
| `test_email_md5_hash` | Verify MD5 hash generation |
| `test_hash_uniqueness` | Verify different emails produce different hashes |
| `test_hash_deterministic` | Verify same email always produces same hash |
| `test_known_sha256_hash` | Verify hash matches expected value |

### Date Transformation Tests (4 tests)

| Test | Description |
|------|-------------|
| `test_date_of_birth_conversion` | Verify DateType conversion |
| `test_age_calculation` | Verify age calculation is reasonable |
| `test_age_carlos` | Verify Carlos's age (born 1991) |
| `test_age_felipe` | Verify Felipe's age (born 1968) |

### Secure Dataset Tests (2 tests)

| Test | Description |
|------|-------------|
| `test_secure_dataset_no_plain_email` | Verify no plain text emails |
| `test_secure_dataset_has_hash` | Verify SHA-256 hash retained |

### Reference Table Tests (3 tests)

| Test | Description |
|------|-------------|
| `test_reference_table_structure` | Verify table structure |
| `test_reference_table_distinct` | Verify distinct entries |
| `test_reference_table_lookup` | Verify hash lookup functionality |

### Data Quality Tests (6 tests)

| Test | Description |
|------|-------------|
| `test_no_null_customer_names` | Verify no null customer names |
| `test_no_null_emails` | Verify no null emails |
| `test_no_null_company_ids` | Verify no null company IDs |
| `test_email_format_valid` | Verify valid email format |
| `test_all_masked_emails_have_asterisks` | Verify masked format |
| `test_row_count_preserved` | Verify row count preserved |

### Edge Cases (15 tests)

| Test | Description |
|------|-------------|
| `test_email_with_subdomain` | Test subdomain extraction |
| `test_email_with_plus_sign` | Test plus sign handling |
| `test_email_with_numbers` | Test numbers in domain |
| `test_special_characters_in_name` | Test special characters |
| `test_unicode_in_place_of_birth` | Test unicode handling |
| `test_empty_email` | Test empty email string |
| `test_null_email_hash` | Test null email hash |
| `test_very_long_email` | Test very long email |
| `test_multiple_at_signs` | Test multiple @ signs |
| `test_date_parsing_invalid_format` | Test invalid date format |
| `test_date_parsing_different_format` | Test different date format |
| `test_empty_customers_array` | Test empty customers array |
| `test_null_place_of_birth` | Test null place of birth |
| `test_case_sensitive_email_hash` | Test case-sensitive hashing |

### Integration Tests (2 tests)

| Test | Description |
|------|-------------|
| `test_full_pipeline` | Test complete transformation pipeline |
| `test_pipeline_idempotent` | Test pipeline produces consistent results |

## Test Coverage

| Category | Tests | Coverage |
|----------|-------|----------|
| Data Loading | 5 | 100% |
| JSON Flattening | 10 | 100% |
| Email Masking | 8 | 100% |
| Date Transformation | 4 | 100% |
| Secure Dataset | 2 | 100% |
| Reference Table | 3 | 100% |
| Data Quality | 6 | 100% |
| ID Generation | 2 | 100% |
| Timestamp | 1 | 100% |
| Edge Cases | 15 | 100% |
| Integration | 2 | 100% |
| **Total** | **57** | **100%** |
