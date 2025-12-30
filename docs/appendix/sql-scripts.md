# SQL Scripts

This section documents SQL scripts used across the case studies.

## Case Study 2: SQLite Database

The University API analysis stores results in a SQLite database for persistence and easy querying.

### Table Schema

```sql
CREATE TABLE IF NOT EXISTS universities (
    name TEXT,
    country TEXT,
    alpha_two_code TEXT,
    state_province TEXT,
    web_pages TEXT
);
```

### Sample Queries

**Count universities by country:**
```sql
SELECT country, COUNT(*) as count
FROM universities
GROUP BY country
ORDER BY count DESC
LIMIT 10;
```

**Find universities in a specific country:**
```sql
SELECT name, state_province, web_pages
FROM universities
WHERE country = 'United Kingdom';
```

## Notes

- SQLite is used for demonstration purposes
- In production, consider using a more robust database like PostgreSQL or a cloud-native solution
- The database file is created at `output/case_study_2/universities.db`
