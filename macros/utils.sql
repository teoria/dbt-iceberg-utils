{% macro format_print_ice(message, cols=80, lmargin=5) %}
    {% set border = "-" * (cols - lmargin) %}
    {% set space = " " * (cols - lmargin) %}
    {% set lspace = " " * lmargin %}
    {% set formatted_message = message.split("\n") %}
    
    {{ print(lspace + "╔" + border + "╗") }}
    {{ print(lspace + "│" + space + "│") }}

    {% for line in formatted_message %}
        {set padded_line = line | iceberg_utils.ljust(cols - lmargin - 2) }
        {{ print(lspace + "│" + padded_line + "│") }}
    {% endfor %}

    {{ print(lspace + "│" + space + "│") }}
    {{ print(lspace + "╚" + border + "╝") }}

     
{% endmacro %}


{%- macro ljust(s, width, fillchar=' ', max_column_length=40) -%}
    {%- set text_length = s | length -%}
    
    {%- if text_length > max_column_length -%}
        {%- set s = s[:max_column_length - 2] + '..' -%}
    {%- endif -%}
    
    {%- set fill_length = width - s | length -%}
    {%- set fill_string = fillchar * fill_length -%}
    {{ fill_string + s }}
{%- endmacro -%}

{%- macro rjust(s, width, fillchar=' ', max_column_length=40) -%}
    {%- set text_length = s | length -%}
    
    {%- if text_length > max_column_length -%}
        {%- set s = s[:max_column_length - 2] + '..' -%}
    {%- endif -%}
    
    {%- set fill_length = width - s | length -%}
    {%- set fill_string = fillchar * fill_length -%}
    {{ s + fill_string }}
{%- endmacro -%}
{%- macro centerjust(s, width, fillchar=' ', max_column_length=60) -%}
    {%- set text_length = s | length -%}
    
    {%- if text_length > max_column_length -%}
        {%- set s = s[:max_column_length - 2] + '..' -%}
    {%- endif -%}
    
    {%- set total_fill = width - s | length -%}
    {%- set left_fill = total_fill // 2 -%}
    {%- set right_fill = total_fill - left_fill -%}
    {%- set left_fill_string = fillchar * left_fill -%}
    {%- set right_fill_string = fillchar * right_fill -%}
    {{ left_fill_string + s + right_fill_string }}
{%- endmacro -%}