{# One-day backfill (ADR-0027). Returns the validated 'YYYY-MM-DD' of var backfill_day, or '' when unset. #}
{% macro backfill_day() -%}
  {%- set day = var('backfill_day', '') | string | trim -%}
  {%- if day != '' and not modules.re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}$', day) -%}
    {{ exceptions.raise_compiler_error("backfill_day must be YYYY-MM-DD, got: " ~ day) }}
  {%- endif -%}
  {{- day -}}
{%- endmacro %}

{# A backfill only makes sense against an existing table: a first build or --full-refresh would keep one day only. #}
{% macro assert_backfill_is_incremental(day) -%}
  {%- if execute and day != '' and not is_incremental() -%}
    {{ exceptions.raise_compiler_error("backfill_day needs an existing table and no --full-refresh") }}
  {%- endif -%}
{%- endmacro %}

{# pre_hook of fct_ledger_entries: drop the day's entries so the anti-join re-emits them. Empty (skipped) otherwise.
   fct_transactions has no such hook: delete+insert already replaces by key and the carry-forward needs the old rows. #}
{% macro backfill_delete_day() -%}
  {%- set day = backfill_day() -%}
  {%- if day != '' and is_incremental() -%}
    delete from {{ this }} where business_date = date '{{ day }}'
  {%- endif -%}
{%- endmacro %}
