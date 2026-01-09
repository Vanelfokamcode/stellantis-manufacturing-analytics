-- macros/log_run_info.sql
-- Log information about the current dbt run
-- Usage: {{ log_run_info() }}

{% macro log_run_info() %}
    {% if execute %}
        {{ log("=" * 60, info=True) }}
        {{ log("dbt Run Information:", info=True) }}
        {{ log("Target: " ~ target.name, info=True) }}
        {{ log("Schema: " ~ target.schema, info=True) }}
        {{ log("Database: " ~ target.database, info=True) }}
        {{ log("User: " ~ target.user, info=True) }}
        {{ log("Threads: " ~ target.threads, info=True) }}
        {{ log("=" * 60, info=True) }}
    {% endif %}
{% endmacro %}
