{% macro get_table_metrics_sql(table_name) %}   

    {% if table_name is string %}
        {% set rel = ref(table_name).render().replace('"','').split(".") %}
        {{ log ( ref(table_name).render().schema), True }}
        {{ log ( ref(table_name).render()), True }}
        {{ log ( "_______STRING____________"), True }}
        {% set catalog = rel[0] %}
        {% set schema = rel[1] %}
        {% set table_name = rel[2] %}

        {{ return(adapter.dispatch('get_table_metrics_sql', 'iceberg_utils')(catalog,schema,table_name)) }}
    {% else %}
        {% set catalog = table_name.database %}
        {% set schema = table_name.schema %}
        {% set table_name = table_name.name %}
        {{ log ( "_______MODEL____________"), True }}
        {% if model.config.table_type == 'iceberg' %}  
            {{ return(adapter.dispatch('get_table_metrics_sql', 'iceberg_utils')(catalog,schema,table_name)) }}
        {% endif %}
    {% endif %}
{% endmacro %}

