{% set object_type = 'deals' %}

with object_cte as (
    select
        {{
            extract_columns_from_vars(
                source_columns=adapter.get_columns_in_relation(source(var('schema'), object_type)),
                selected_columns=var(object_type)
            )
        }},
    _dlt_id
    from
        {{ source(var('schema'), object_type) }}
)

{{ merge_labels_to_properties(var(object_type), 'object_cte') }}
