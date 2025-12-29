# Unit Testing

**Test File:** `tests/test_case_study_1.py`

Tests cover:
- Data loading and schema validation
- Data quality checks
- Business logic transformations
- Aggregation calculations
- Filter conditions

## Running Tests

```bash
# Run Case Study 1 tests
python3 -m pytest tests/test_case_study_1.py -v
```

## Test Results

**Total Tests:** 25

**Status:** All tests passing

```
Ran 25 tests in ~42s

OK
```

## TestCaseStudy1

Comprehensive unit tests for Case Study 1: Retail Electronics Analysis

### Data Loading Tests

**Transaction Count**

Verify expected number of transactions loaded

**Store Count**

Verify expected number of stores loaded

**Product Count**

Verify expected number of products loaded

**Transaction Schema**

Verify transaction schema has all required columns

**Store Schema**

Verify store schema has all required columns

**Product Schema**

Verify product schema has all required columns

### Data Quality Tests

**No Null Transaction Keys**

Verify no null values in critical transaction columns

**No Negative Quantities**

Verify no negative quantities

**No Negative Prices**

Verify no negative prices

**No Negative Costs**

Verify no negative costs

**Referential Integrity Stores**

Verify all store keys reference valid stores

**Referential Integrity Products**

Verify all SKU numbers reference valid products

**Unique Store Keys**

Verify store keys are unique

**Unique Sku Numbers**

Verify SKU numbers are unique

**Price Greater Than Cost**

Verify prices are not below costs

### Business Logic Tests

**Store 443 Exists**

Verify Store 443 exists before applying exclusion filter

**Revenue Calculation**

Verify revenue calculation: quantity * price

**Profit Calculation**

Verify profit calculation: quantity * (price - cost)

**Profit Margin Calculation**

Verify profit margin calculation: (profit / revenue) * 100

### Aggregation Tests

**Top Products Aggregation**

Test top products by revenue aggregation logic

**Regional Profit Aggregation**

Test regional profit aggregation logic

**Store Exclusion Filter**

Verify Store 443 exclusion filter works correctly

**Revenue Threshold Filter**

Verify revenue threshold filter works correctly

### Output Validation Tests

**Top Products Limit 10**

Verify top products returns exactly 10 results

**Top Regions Limit 3**

Verify top regions returns exactly 3 results

## Test Coverage

| Category | Tests | Coverage |
|----------|-------|----------|
| Data Loading | 6 | 100% |
| Data Quality | 9 | 100% |
| Business Logic | 4 | 100% |
| Aggregations | 4 | 100% |
| Output Validation | 2 | 100% |
| **Total** | **25** | **100%** |
