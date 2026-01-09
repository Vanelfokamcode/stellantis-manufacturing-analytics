-- macros/grant_select_on_schemas.sql
-- Grant SELECT permissions on schemas to a role
-- Usage: {{ grant_select_on_schemas(['schema1', 'schema2'], 'role_name') }}

{% macro grant_select_on_schemas(schemas, role) %}
    {% for schema in schemas %}
        GRANT USAGE ON SCHEMA {{ schema }} TO {{ role }};
        GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} TO {{ role }};
        ALTER DEFAULT PRIVILEGES IN SCHEMA {{ schema }} 
        GRANT SELECT ON TABLES TO {{ role }};
    {% endfor %}
{% endmacro %}
