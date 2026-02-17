{% set table_name = 'trace__steps__extract_info__job_metrics' %}
{% set relations = dbt_utils.get_relations_by_pattern(
    schema_pattern = '%_trace',
    table_pattern = table_name
) %}
{% if execute %}
    {% if relations | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "get_relations_by_pattern found no '" ~ table_name ~ "' tables in any schema."
        ) }}
    {% endif %}

    {{ dbt_utils.union_relations(
        relations = relations
    ) }}
{% endif %}
