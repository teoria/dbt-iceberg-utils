{% macro optimize_table(table_name) %}
    {% set query = 'OPTIMIZE {{table_name}} REWRITE DATA USING BIN_PACK' %}
    {% do run_query(query) %}
{% endmacro %}

{% macro vacuum_table(table_name) %}
    {% set query = 'vacuum {{table_name}}' %}
    {% do run_query(query) %}
{% endmacro %}

