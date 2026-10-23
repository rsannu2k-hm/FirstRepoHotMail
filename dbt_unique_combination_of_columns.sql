
{% test unique_combination_of_columns(model, combination_of_columns, quote_columns=false) %}

{% if not quote_columns %}
    {%- set column_list=combination_of_columns %}
{% elif quote_columns %}
    {%- set column_list=[] %}
    {% for column in combination_of_columns -%}
        {% do column_list.append( adapter.quote(column) ) %}
    {%- endfor %}
{% else %}
    {{ exceptions.raise_compiler_error(
        "`quote_columns` argument for unique_combination_of_columns test must be one of [True, False] Got: '" ~ quote_columns ~ "'.'"
    ) }}
{% endif %}

{%- set columns_csv=column_list | join(', ') %}

with validation_errors as (

    select
        {{ columns_csv }}
    from {{ model }}
    group by {{ columns_csv }}
    having count(*) > 1

)

select *
from validation_errors

{% endtest %}

-----** calling the above test script from xxx_schema.yml
  - name: sp_populate_cashallocations
    description: ""
    config:
      enabled: true
      materialized: table
      alias: t_ax_cashallocations
      contract:
        enforced: true
    tests:
      - unique_combination_of_columns:
          combination_of_columns:
            - PKStatsLedgerSeq
            - PKStatsPostingAmtSeq

