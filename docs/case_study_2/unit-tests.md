# Unit Testing

**Test File:** `tests/test_case_study_2.py`

Tests cover:
- Data loading and schema validation
- Data quality checks
- Column removal operations (Task 1)
- Country aggregation and top N filtering (Task 2)
- Specific country counts with name mapping (Task 3)
- Alpha code validation
- Edge cases and error handling

## Running Tests

```bash
# Run Case Study 2 tests
python3 -m pytest tests/test_case_study_2.py -v
```

## Test Results

**Total Tests:** 63

**Status:** All tests passing

```
============================= 63 passed in 36.88s ==============================
```

## TestCaseStudy2

Comprehensive unit tests for Case Study 2: University API Analysis

### Data Loading Tests (4 tests)

| Test | Description |
|------|-------------|
| `test_sample_data_loaded` | Verify sample data is loaded correctly |
| `test_schema_has_required_columns` | Verify schema has all required columns |
| `test_schema_column_types` | Verify column data types are correct |
| `test_full_data_count` | Verify full dataset has substantial records |

### Data Quality Tests (7 tests)

| Test | Description |
|------|-------------|
| `test_no_null_university_names` | Verify no null values in university names |
| `test_no_null_countries` | Verify no null values in country field |
| `test_no_null_alpha_codes` | Verify no null values in alpha_two_code |
| `test_alpha_code_length` | Verify alpha_two_code is exactly 2 characters |
| `test_no_empty_university_names` | Verify no empty strings in names |
| `test_web_pages_not_empty` | Verify web_pages array is not empty |
| `test_unique_universities_per_country` | Verify no duplicate universities |

### Task 1: Remove Domains Column (5 tests)

| Test | Description |
|------|-------------|
| `test_domains_column_exists` | Verify domains column exists before removal |
| `test_remove_domains_column` | Verify domains column can be removed |
| `test_remove_domains_preserves_other_columns` | Verify other columns preserved |
| `test_remove_domains_preserves_row_count` | Verify row count unchanged |
| `test_remove_nonexistent_column` | Verify handling non-existent column |

### Task 2: Top N Countries (5 tests)

| Test | Description |
|------|-------------|
| `test_country_count_aggregation` | Verify aggregation produces correct counts |
| `test_top_3_countries_count` | Verify top 3 returns exactly 3 results |
| `test_top_countries_ordered_descending` | Verify descending order |
| `test_top_countries_in_sample_data` | Verify expected countries in results |
| `test_top_n_with_different_n_values` | Verify top N works with various N |

### Task 3: Specific Country Counts (7 tests)

| Test | Description |
|------|-------------|
| `test_uk_country_mapping` | Verify UK maps to "United Kingdom" |
| `test_france_country_count` | Verify France university count |
| `test_china_country_count` | Verify China university count |
| `test_united_kingdom_count` | Verify United Kingdom count |
| `test_country_filter_case_sensitive` | Verify case sensitivity |
| `test_specific_countries_total` | Verify total for UK + France + China |
| `test_nonexistent_country` | Verify filtering for non-existent country |

### Alpha Code Tests (4 tests)

| Test | Description |
|------|-------------|
| `test_uk_alpha_code` | Verify GB code for United Kingdom |
| `test_france_alpha_code` | Verify FR code for France |
| `test_china_alpha_code` | Verify CN code for China |
| `test_us_alpha_code` | Verify US code for United States |

### Edge Cases (8 tests)

| Test | Description |
|------|-------------|
| `test_empty_dataframe_aggregation` | Verify aggregation handles empty DataFrame |
| `test_limit_greater_than_rows` | Verify limit greater than available rows |
| `test_case_insensitive_country_search` | Test case-insensitive search |
| `test_country_contains_search` | Test partial country name search |
| `test_empty_country_name` | Test handling of empty country name |
| `test_special_characters_in_name` | Test special characters in name |
| `test_unicode_characters` | Test unicode character handling |
| `test_very_long_university_name` | Test very long university name |

## Test Coverage

| Category | Tests | Coverage |
|----------|-------|----------|
| Data Loading | 4 | 100% |
| Data Quality | 7 | 100% |
| Task 1 (Domains) | 5 | 100% |
| Task 2 (Top N) | 5 | 100% |
| Task 3 (Countries) | 7 | 100% |
| Alpha Codes | 4 | 100% |
| University Names | 5 | 100% |
| State/Province | 3 | 100% |
| Web Pages | 3 | 100% |
| Edge Cases | 8 | 100% |
| Full Dataset | 5 | 100% |
| Transformation Pipeline | 2 | 100% |
| **Total** | **63** | **100%** |
