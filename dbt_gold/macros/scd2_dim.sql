{# SCD2 from the bronze change log (ADR-0025). `changes` is a staging relation with one row per (key, transaction):
   <key>, lsn, txid, source_ts, kafka_ts, is_delete and the attribute columns.
   - Versions are ordered by LSN; consecutive versions with an identical hash of `tracked` (delete flag included)
     are merged, so a no-op update (e.g. only updated_at) does not open a version, and a delete is never merged away.
   - A delete closes the open version (closed_by_delete) and starts none; a re-insert opens a new one.
   - valid_from/valid_to are commit times (ms); [valid_from_lsn, valid_to_lsn) is the authoritative interval.
   - Incremental: keys with a change whose kafka_ts is newer than (max src_kafka_ts in this - lookback) are rebuilt
     from their whole history (delete+insert by key), so late and out-of-order events inside the lookback are handled. #}
{% macro scd2_dim(changes, key, tracked, carried) %}
with changes as (
    select * from {{ changes }}
),

{% if is_incremental() %}
affected as (
    select distinct {{ key }}
    from changes
    where kafka_ts > {{ lookback_cutoff('src_kafka_ts') }}
),

scoped as (
    select c.*
    from changes c
    inner join affected a on a.{{ key }} = c.{{ key }}
),
{% else %}
scoped as (
    select * from changes
),
{% endif %}

hashed as (
    select
        *,
        {{ attr_hash(tracked + ['is_delete']) }} as attr_hash
    from scoped
),

flagged as (
    select
        *,
        lag(attr_hash) over (partition by {{ key }} order by lsn) as prev_hash
    from hashed
),

islands as (
    select
        *,
        sum(case when prev_hash is null or prev_hash <> attr_hash then 1 else 0 end)
            over (partition by {{ key }} order by lsn rows between unbounded preceding and current row) as island
    from flagged
),

firsts as (
    select
        *,
        row_number() over (partition by {{ key }}, island order by lsn) as rn,
        max(kafka_ts) over (partition by {{ key }}, island) as island_kafka_ts
    from islands
),

versions as (
    select * from firsts where rn = 1
),

bounded as (
    select
        *,
        lead(source_ts) over (partition by {{ key }} order by lsn) as next_source_ts,
        lead(lsn) over (partition by {{ key }} order by lsn) as next_lsn,
        lead(is_delete) over (partition by {{ key }} order by lsn) as next_is_delete
    from versions
)

select
    {{ version_sk(key, 'lsn') }} as {{ key | replace('_id', '_sk') }},
    cast({{ key }} as bigint) as {{ key }},
    {% for c in carried -%}
    {{ c }},
    {% endfor -%}
    cast(source_ts as timestamp(6) with time zone) as valid_from,
    cast(next_source_ts as timestamp(6) with time zone) as valid_to,
    lsn as valid_from_lsn,
    next_lsn as valid_to_lsn,
    txid,
    next_lsn is null as is_current,
    coalesce(next_is_delete, false) as closed_by_delete,
    cast(island_kafka_ts as timestamp(6) with time zone) as src_kafka_ts
from bounded
where not is_delete
{% endmacro %}
