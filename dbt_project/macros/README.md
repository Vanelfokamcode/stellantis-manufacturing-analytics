# dbt Macros Documentation

## Custom Macros

### classify_oee(oee_column)
Classifies OEE percentage into performance categories.

**Returns:** `EXCELLENT` | `GOOD` | `ACCEPTABLE` | `NEEDS_ATTENTION`

**Usage:**
```sql
SELECT 
    {{ classify_oee('oee_percent') }} as oee_category
FROM source_table
```

**Thresholds:**
- EXCELLENT: >= 85%
- GOOD: >= 70%
- ACCEPTABLE: >= 60%
- NEEDS_ATTENTION: < 60%

---

### calculate_defect_rate(defects_column, units_column)
Calculates defect rate percentage with null safety.

**Returns:** `FLOAT` (0-100)

**Usage:**
```sql
SELECT 
    {{ calculate_defect_rate('defects', 'units_produced') }} as defect_rate
FROM source_table
```

---

### generate_key(columns)
Generates MD5 hash surrogate key from multiple columns.

**Returns:** `TEXT` (MD5 hash)

**Usage:**
```sql
SELECT 
    {{ generate_key(['production_line', 'date', 'shift']) }} as unique_key
FROM source_table
```

---

### validate_date(date_column)
Validates that date is not in the future.

**Returns:** `BOOLEAN`

**Usage:**
```sql
WHERE {{ validate_date('production_date') }}
```

---

### grant_select_on_schemas(schemas, role)
Grants SELECT permissions on schemas to a role.

**Usage:**
```sql
{{ grant_select_on_schemas(['dbt_dev_marts', 'dbt_dev_dimensions'], 'analytics_role') }}
```

---

### log_run_info()
Logs dbt run information (target, schema, database, user).

**Usage:**
```yaml
# In dbt_project.yml
on-run-start:
  - "{{ log_run_info() }}"
```

---

## Operations

### get_row_counts()
Generates row count report for all models.

**Usage:**
```bash
dbt run-operation get_row_counts
```

---

### truncate_staging_tables()
Truncates all staging tables.

**Usage:**
```bash
dbt run-operation truncate_staging_tables
```

---

### drop_staging_tables()
Drops all staging tables (use with caution!).

**Usage:**
```bash
dbt run-operation drop_staging_tables
```

---

### refresh_all_views()
Refreshes all materialized views.

**Usage:**
```bash
dbt run-operation refresh_all_views
```

