-- macros/operations.sql
-- dbt operations - macros that can be run as standalone commands

-- Operation: Drop all staging tables
{% macro drop_staging_tables() %}
    {% set tables = ['stg_production', 'stg_maintenance', 'stg_quality'] %}
    
    {{ log("Dropping staging tables...", info=True) }}
    
    {% for table in tables %}
        {% set drop_query %}
            DROP TABLE IF EXISTS {{ target.schema }}_staging.{{ table }} CASCADE
        {% endset %}
        
        {{ log("Dropping " ~ table, info=True) }}
        {% do run_query(drop_query) %}
    {% endfor %}
    
    {{ log("Staging tables dropped successfully!", info=True) }}
{% endmacro %}


-- Operation: Truncate all staging tables
{% macro truncate_staging_tables() %}
    {% set tables = ['stg_production', 'stg_maintenance', 'stg_quality'] %}
    
    {{ log("Truncating staging tables...", info=True) }}
    
    {% for table in tables %}
        {% set truncate_query %}
            TRUNCATE TABLE {{ target.schema }}_staging.{{ table }}
        {% endset %}
        
        {{ log("Truncating " ~ table, info=True) }}
        {% do run_query(truncate_query) %}
    {% endfor %}
    
    {{ log("Staging tables truncated successfully!", info=True) }}
{% endmacro %}


-- Operation: Get row counts for all models
{% macro get_row_counts() %}
    {{ log("=" * 80, info=True) }}
    {{ log("ROW COUNT REPORT", info=True) }}
    {{ log("=" * 80, info=True) }}
    
    {% set schemas = ['staging', 'intermediate', 'marts', 'dimensions'] %}
    
    {% for schema in schemas %}
        {% set full_schema = target.schema ~ '_' ~ schema %}
        
        {{ log("Schema: " ~ full_schema, info=True) }}
        {{ log("-" * 80, info=True) }}
        
        {% set query %}
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{{ full_schema }}'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        {% endset %}
        
        {% set results = run_query(query) %}
        {% if execute and results %}
            {% for row in results %}
                {% set count_query %}
                    SELECT COUNT(*) as cnt FROM {{ full_schema }}.{{ row.table_name }}
                {% endset %}
                {% set count_result = run_query(count_query) %}
                {% if count_result %}
                    {{ log(row.table_name ~ ": " ~ count_result.columns[0].values()[0] ~ " rows", info=True) }}
                {% endif %}
            {% endfor %}
        {% else %}
            {{ log("No tables found", info=True) }}
        {% endif %}
        {{ log("", info=True) }}
    {% endfor %}
    
    {{ log("=" * 80, info=True) }}
{% endmacro %}


-- Operation: Refresh all materialized views
{% macro refresh_all_views() %}
    {{ log("Refreshing all materialized views...", info=True) }}
    
    {% set query %}
        SELECT schemaname, matviewname 
        FROM pg_matviews
        WHERE schemaname LIKE '{{ target.schema }}%'
    {% endset %}
    
    {% set results = run_query(query) %}
    {% if execute %}
        {% for row in results %}
            {% set refresh_query %}
                REFRESH MATERIALIZED VIEW {{ row.schemaname }}.{{ row.matviewname }}
            {% endset %}
            {{ log("Refreshing " ~ row.schemaname ~ "." ~ row.matviewname, info=True) }}
            {% do run_query(refresh_query) %}
        {% endfor %}
    {% endif %}
    
    {{ log("All views refreshed!", info=True) }}
{% endmacro %}
