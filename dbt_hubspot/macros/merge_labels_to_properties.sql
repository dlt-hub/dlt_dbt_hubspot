{% macro merge_labels_to_properties(col_list, cte_name) %}

select {{ cte_name }}.*

{% if col_list != [] %}
    {%- for column in col_list -%} -- Create label cols
        {% if column.add_property_label %}
            {%- set col_alias = (column.alias | default(column.name)) %}
          , {{ column.name }}_options.property_label as {{ col_alias }}_label
        {% endif -%}
    {%- endfor %}

    from {{ cte_name }}

    {% for column in col_list -%} -- Create joins
        {%- if column.add_property_label -%}
            {%- set col_alias = (column.alias | default(column.name)) %}
            left join -- create subset of property and property_options for property in question
                (select
                  properties_options.property_value,
                  properties_options.property_label
                from {{ ref('stg_hubspot__properties_options') }} as properties_options
                join {{ ref('stg_hubspot__properties') }} as properties
                  on properties_options._dlt_parent_id = properties._dlt_id
                where properties.name = '{{ column.name.replace('property_', '') }}'
                ) as {{ column.name }}_options
            on {{ cte_name }}.{{ col_alias }} = {{ column.name }}_options.property_value
        {% endif -%}
    {%- endfor %}

{%- else -%}
  from {{ cte_name }}

{%- endif -%}
{% endmacro %}