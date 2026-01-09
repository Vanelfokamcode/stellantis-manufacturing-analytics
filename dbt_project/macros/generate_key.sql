-- macros/generate_key.sql
-- Generate surrogate key from multiple columns
-- Usage: {{ generate_key(['col1', 'col2']) }}

{% macro generate_key(columns) %}
    MD5(
        CONCAT(
            {% for col in columns %}
                COALESCE(CAST({{ col }} AS TEXT), '')
                {% if not loop.last %}, '-', {% endif %}
            {% endfor %}
        )
    )
{% endmacro %}
