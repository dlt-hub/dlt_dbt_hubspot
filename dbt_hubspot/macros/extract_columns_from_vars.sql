{% macro extract_columns_from_vars(source_columns, selected_columns) -%}

{%- set source_column_names = source_columns|map(attribute='name')|map('lower')|list -%}

{%- for column in selected_columns %}
    {% if column.name in source_column_names -%}
        {{ column.name }} as
        {%- if 'alias' in column %} {{ column.alias }} {% else %} {{ column.name }} {%- endif -%}
    {%- if not loop.last -%} , {% endif -%}
    {%- endif -%}
{% endfor %}

{% endmacro %}
