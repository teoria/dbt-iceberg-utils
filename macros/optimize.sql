{% macro optimize_table(table_name) %}  
    {{ return(adapter.dispatch('optimize_table', 'iceberg_utils')(table_name)) }}
{% endmacro %}

{% macro default__optimize_table(table_name) %} 
    {% do exceptions.raise_compiler_error("'optimize_table' not implemented on '{}'.".format(target.type)) %}
{% endmacro %}

{% macro athena__optimize_table(table_name) %}
    OPTIMIZE {{ table_name }} REWRITE DATA USING BIN_PACK
{% endmacro %}

{% macro trino__optimize_table(table_name) %}
    ALTER TABLE {{ table_name }} EXECUTE optimize
{% endmacro %}


{% macro vacuum_table(table_name) %}
    {{ return(adapter.dispatch('vacuum_table', 'iceberg_utils')(table_name)) }}
{% endmacro %}

{% macro default__vacuum_table(table_name) %}
    {% do exceptions.raise_compiler_error("'vacuum_table' not implemented on '{}'.".format(target.type)) %}
{% endmacro %}

{% macro athena__vacuum_table(table_name) %}
    vacuum {{ table_name }}
{% endmacro %}


{% macro run_optimize(table_name) %}
    {% set query = iceberg_utils.optimize_table(table_name) %}
    {{ log(query, true) }}
    {% set results = run_query(query) %}

    {% if execute %}
        {% if results is not none %}
            {{ log("optimize completed", info=True) }} 
        {% endif %}
    {% endif %}
{% endmacro %}

{% macro run_vacuum(table_name) %}
    {% set query = iceberg_utils.vacuum_table(table_name) %}
    {% set results = run_query(query) %}

    {% if execute %}
        {% if results is not none %}
            {{ log("vacuum completed", info=True) }} 
        {% endif %}
    {% endif %}
{% endmacro %}