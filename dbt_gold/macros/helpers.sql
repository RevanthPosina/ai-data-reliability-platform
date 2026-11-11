{# UTC business date of a timestamp with time zone. #}
{% macro business_date(ts) -%}
cast(at_timezone({{ ts }}, 'UTC') as date)
{%- endmacro %}

{# yyyymmdd as integer, from a date expression. #}
{% macro date_key(d) -%}
cast(year({{ d }}) * 10000 + month({{ d }}) * 100 + day({{ d }}) as integer)
{%- endmacro %}

{# Deterministic surrogate key of a version: id and the change LSN that opened it. #}
{% macro version_sk(id_col, lsn_col) -%}
to_hex(xxhash64(to_utf8(cast({{ id_col }} as varchar) || ':' || cast({{ lsn_col }} as varchar))))
{%- endmacro %}

{# xxhash64 over '|'-joined text columns (null becomes empty). #}
{% macro attr_hash(cols) -%}
to_hex(xxhash64(to_utf8(
    {%- for c in cols -%}coalesce(cast({{ c }} as varchar), ''){% if not loop.last %} || '|' || {% endif %}{%- endfor -%}
)))
{%- endmacro %}

{# Largest LSN, for point-in-time joins on an open interval. #}
{% macro max_lsn() -%}9223372036854775807{%- endmacro %}
