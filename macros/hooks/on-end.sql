{% macro on_run_end() %}
 

    {%- if execute %}
        {% set result = iceberg_utils.get_iceberg_tables_needing_maintenance(limit_size=50) %}
        {% if result|length > 0 %}
        
            {{ print("\033[1;36m" ) }}  
            {{ print("      ___         _                      _   _ _   _ _       " ) }}
            {{ print("     |_ _|___ ___| |__   ___ _ __ __ _  | | | | |_(_) |___   " ) }}
            {{ print("      | |/ __/ _ \ '_ \ / _ \ '__/ _` | | | | | __| | / __|   " ) }}
            {{ print("      | | (_|  __/ |_) |  __/ | | (_| | | |_| | |_| | \__ \   " ) }}
            {{ print("     |___\___\___|_.__/ \___|_|  \__, |  \___/ \__|_|_|___/    " ) }}
            {{ print("                                 |___/                          " ) }}
            {{ print("  \033[0m") }}
            {{ print("\033[1m" ~"Iceberg tables that need maintenance: (" ~ result|length ~ ")" ) }}  

            {{ print("-------------------------------------"  ~ "\033[0m" ) }}
            {% set max_table_name_length = [] %}
            {% set max_why_length = [] %} 
            {% for row in result %} 
                {% do max_table_name_length.append(row.table_name|length ) %} 
                {% do max_why_length.append(row.why|length) %}  
            {% endfor %}
            {% set max_table_name_length = max_table_name_length | max %}
            {% set max_why_length = max_why_length | max %}
 
            {% set dict_result = [] %}
            {{ print("╔"~"-"*(max_table_name_length + 3)~"-"~"-"*(max_why_length + 2)~"╗") }}
            {{ print("│ "~  iceberg_utils.centerjust("Table Name", max_table_name_length + 2) ~"│"~  iceberg_utils.centerjust("Why?", max_why_length + 2) ~"│") }}
            {{ print("│"~"-"*(max_table_name_length + 3)~"|"~"-"*(max_why_length + 2)~"|") }}
            {% for row in result %}
                {% do dict_result.append({"table_name": "\033[1;36m" ~ row.table_name  ~ "\033[0m", "why": row.why.split(',')}) %}
                {{ print("│ "~ "\033[1;36m" ~ iceberg_utils.centerjust( row.table_name , max_table_name_length + 2) ~ "\033[0m" ~"│"~  "\033[2;93m" ~ iceberg_utils.centerjust(row.why  , max_why_length + 2) ~ "\033[0m"   ~"│") }}
            {% endfor %}   
            {{ print("╚"~"-"*(max_table_name_length + 3)~"-"~"-"*(max_why_length + 2)~"╝") }}
            {{ print(" ") }}  
            
            {{ print(" ") }}
            {{ print("\033[1mIceberg Table Maintenance:\033[0m" ) }} 
             
            {{ print(" ") }}
            {{ print( "Run the 'iceberg_utils.run_optimize' macro and 'iceberg_utils.run_vacuum' to optimize and vacuum these tables." ) }}
            {{ print(" ") }}
            {{ print( "\033[1mUsage example:\033[0m" ) }}
            {{ print(" ") }}
            {{ print( " dbt run-operation \033[1;93mrun_optimize\033[0m --args 'table_name:\033[3;36m "~result[0].table_name~"\033[0m'") }}
            {{ print( " dbt run-operation \033[1;93mrun_vacuum\033[0m --args 'table_name:\033[3;36m "~result[0].table_name~"\033[0m'") }}
            
            {{ print(" ") }}
            {{ print("\033[36m" ~ "¸,ø¤º°`°º¤ø¤º°`°º¤ø,¸¸,ø¤º°`°º¤ø¤º°`°º¤ø,¸¸,ø¤º°`°º¤ø¤º°`°º¤ø,¸"  ~ "\033[0m") }}
            {{ print(" ") }}
        {% endif %} 
        
    {% endif %}
  
{% endmacro %}
