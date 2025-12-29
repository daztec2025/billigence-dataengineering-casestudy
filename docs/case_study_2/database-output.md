# Case Study 2: Database Output

## Output Formats

Multiple formats provided for different use cases:

### Delta Lake Tables

**Primary storage format with ACID compliance**

```python
df.write.format("delta").mode("overwrite").saveAsTable(table_name)
```

Tables created:
- `billigence_top_3_countries_universities`
- `billigence_uk_france_china_universities`

Query example:
```sql
SELECT * FROM billigence_top_3_countries_universities
ORDER BY university_count DESC;
```

### SQLite Database

**Portable SQL database**

File: `output/case_study_2/universities.db`

```python
import sqlite3
conn = sqlite3.connect('output/case_study_2/universities.db')
cursor = conn.cursor()
cursor.execute("SELECT * FROM uk_france_china_universities")
results = cursor.fetchall()
```

### CSV Files

**For SQL Server import or Excel**

```python
df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
```

SQL Server import:
```sql
BULK INSERT uk_france_china_universities
FROM 'C:\path\to\file.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');
```

### SQL INSERT Statements

```sql
CREATE TABLE uk_france_china_universities (
    country_display VARCHAR(50),
    country_actual VARCHAR(100),
    university_count INT
);

INSERT INTO uk_france_china_universities VALUES ('China', 'China', 800);
INSERT INTO uk_france_china_universities VALUES ('France', 'France', 400);
INSERT INTO uk_france_china_universities VALUES ('UK', 'United Kingdom', 160);
```

## Query Examples

```sql
-- Top 3 countries
SELECT * FROM billigence_top_3_countries_universities;

-- UK, France, China with percentages
SELECT
    country_display,
    university_count,
    ROUND(100.0 * university_count / SUM(university_count) OVER(), 2) as pct
FROM billigence_uk_france_china_universities
ORDER BY university_count DESC;
```

## Verification

```sql
-- Check record counts
SELECT COUNT(*) FROM billigence_uk_france_china_universities;  -- Expected: 3

-- Validate data quality
SELECT * FROM billigence_uk_france_china_universities
WHERE university_count IS NULL OR university_count <= 0;  -- Expected: 0 rows
```
