{% macro get_table_metrics_sql(table_name) %}   

    {% if table_name is string %}
        {% if table_name.split(".")|length == 3 %}
             {{ log ( "_______STRING_3___________", false) }}
            {% set   catalog=table_name.split(".")[0] %} 
            {% set   schema=table_name.split(".")[1] %} 
            {% set   table_name=table_name.split(".")[2]   %}

        {% elif table_name.split(".")|length == 2 %}
            {{ log ( "_______STRING_2___________", false) }}
            {% set   catalog= database %} 
            {% set   schema=table_name.split(".")[0] %} 
            {% set   table_name=table_name.split(".")[1]   %}
        
        {% elif table_name.split(".")|length == 2 %}
            {% set rel = ref(table_name).render().replace('"','').split(".") %}
            {{ log ( ref(table_name).render().schema), false }}
            {{ log ( ref(table_name).render()), false }}
            {{ log ( "_______STRING_1___________", false) }}
            {% set catalog = rel[0] %}
            {% set schema = rel[1] %}
            {% set table_name = rel[2] %}
         
             
        {% endif %}
        
        {{ return(adapter.dispatch('get_table_metrics_sql', 'iceberg_utils')(catalog,schema,table_name)) }}
    {% else %}
        {% set catalog = table_name.database %}
        {% set schema = table_name.schema %}
        {% set table_name = table_name.name %}
        {{ log ( "_______MODEL____________", false) }}
        {% if model.config.table_type == 'iceberg' %}  
            {{ return(adapter.dispatch('get_table_metrics_sql', 'iceberg_utils')(catalog,schema,table_name)) }}
        {% endif %}
    {% endif %}
{% endmacro %}

