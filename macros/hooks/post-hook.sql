{% macro get_table_metrics_sql(table_name) %}   

    {% if table_name is string %}
        {% if table_name.contains(".") %}
            {% set   database=table_name.split(".")[0], 
                schema=table_name.split(".")[1], 
                table_name=table_name.split(".")[2]   %}
        {% else %}

            {% set rel = ref(table_name).render().replace('"','').split(".") %}
            {{ log ( ref(table_name).render().schema), false }}
            {{ log ( ref(table_name).render()), false }}
            {{ log ( "_______STRING____________"), false }}
            {% set catalog = rel[0] %}
            {% set schema = rel[1] %}
            {% set table_name = rel[2] %}
        {% endif %}
        
        {{ return(adapter.dispatch('get_table_metrics_sql', 'iceberg_utils')(catalog,schema,table_name)) }}
    {% else %}
        {% set catalog = table_name.database %}
        {% set schema = table_name.schema %}
        {% set table_name = table_name.name %}
        {{ log ( "_______MODEL____________"), false }}
        {% if model.config.table_type == 'iceberg' %}  
            {{ return(adapter.dispatch('get_table_metrics_sql', 'iceberg_utils')(catalog,schema,table_name)) }}
        {% endif %}
    {% endif %}
{% endmacro %}

