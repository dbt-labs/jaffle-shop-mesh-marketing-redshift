{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ return(jaffle_utils.generate_schema_name(custom_schema_name, node)) }}
{%- endmacro %}