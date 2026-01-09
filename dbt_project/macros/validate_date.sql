-- macros/validate_date.sql
-- Validate that date is not in the future
-- Usage: {{ validate_date('date_column') }}

{% macro validate_date(date_column) %}
    {{ date_column }} <= CURRENT_DATE
{% endmacro %}
