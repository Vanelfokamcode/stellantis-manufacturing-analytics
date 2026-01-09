-- macros/calculate_defect_rate.sql
-- Calculate defect rate percentage with null safety
-- Usage: {{ calculate_defect_rate('defects', 'units_produced') }}

{% macro calculate_defect_rate(defects_column, units_column) %}
    CASE 
        WHEN {{ units_column }} > 0 
        THEN ({{ defects_column }}::FLOAT / {{ units_column }} * 100)
        ELSE 0 
    END
{% endmacro %}
