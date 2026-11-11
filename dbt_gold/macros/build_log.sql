{# on-run-end: append (invocation, time, model, max source LSN) to <target schema>.build_log.
   Future t_gold capture point for S4 (not measured in G4). Holds LSNs and times only. #}
{% macro gold_build_log(results) -%}
  {%- if execute -%}
    {%- set lsn_expr = {
        'dim_customer': 'max(greatest(valid_from_lsn, coalesce(valid_to_lsn, valid_from_lsn)))',
        'dim_merchant': 'max(greatest(valid_from_lsn, coalesce(valid_to_lsn, valid_from_lsn)))',
        'fct_transactions': 'max(src_lsn)',
        'fct_ledger_entries': 'max(source_lsn)'
    } -%}
    {%- set selects = [] -%}
    {%- for r in results if r.node.resource_type == 'model' and r.status == 'success' and r.node.name in lsn_expr -%}
      {%- do selects.append("select '" ~ r.node.name ~ "' as model_name, " ~ lsn_expr[r.node.name] ~ " as max_src_lsn from " ~ r.node.relation_name) -%}
    {%- endfor -%}
    {%- if selects | length > 0 -%}
      {%- set log_rel = api.Relation.create(database=target.database, schema=target.schema, identifier='build_log') -%}
      {%- do run_query("create table if not exists " ~ log_rel ~ " (invocation_id varchar, completed_at timestamp(6) with time zone, model_name varchar, max_src_lsn bigint)") -%}
      {%- do run_query("insert into " ~ log_rel ~ " select '" ~ invocation_id ~ "', cast(current_timestamp as timestamp(6) with time zone), model_name, max_src_lsn from (" ~ (selects | join(' union all ')) ~ ")") -%}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}
