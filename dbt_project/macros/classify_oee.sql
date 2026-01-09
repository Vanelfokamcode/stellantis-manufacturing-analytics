-- macros/classify_oee.sql
-- Classify OEE percentage into performance categories
-- Usage: {{ classify_oee('oee_percent') }}

{% macro classify_oee(oee_column) %}
    CASE 
        WHEN {{ oee_column }} >= 85 THEN 'EXCELLENT'
        WHEN {{ oee_column }} >= 70 THEN 'GOOD'
        WHEN {{ oee_column }} >= 60 THEN 'ACCEPTABLE'
        ELSE 'NEEDS_ATTENTION'
    END
{% endmacro %}

