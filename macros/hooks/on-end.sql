{% macro on_run_end() %}
 
    {%- if execute %}
       
        {{ log("========== Iceberg Utils ==========", info=True) }}
        {{ log("Modelos iceberg degradados", true ) }} 
        {% set models_name = iceberg_utils.get_iceberg_models() %}
        {{ log(models_name, true) }}    
    {% endif %}
  
{% endmacro %}

