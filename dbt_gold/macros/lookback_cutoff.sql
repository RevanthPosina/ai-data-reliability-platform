{# Incremental cursor cutoff: the newest cursor value already in this model's table minus the lookback.
   Rows (or bronze change events) newer than this are (re)processed. Later arrivals are missed and
   caught by the completeness tests (ADR-0027). Only call this inside is_incremental(). #}
{% macro lookback_cutoff(cursor_col) -%}
(
    select coalesce(max({{ cursor_col }}), cast(timestamp '1970-01-01 00:00:00 UTC' as timestamp(6) with time zone))
           - interval '{{ var("gold_lookback_minutes", 60) | int }}' minute
    from {{ this }}
)
{%- endmacro %}
