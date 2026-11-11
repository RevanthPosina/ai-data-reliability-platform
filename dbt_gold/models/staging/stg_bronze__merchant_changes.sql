{# Same rules as stg_bronze__customer_changes, for merchants. #}
with events as (
    select
        cast(json_extract_scalar(pk, '$.id') as bigint) as merchant_id,
        lsn,
        txid,
        op,
        source_ts,
        kafka_ts,
        kafka_partition,
        kafka_offset,
        after
    from {{ source('bronze', 'cdc_events') }}
    where source_table = 'public.merchants'
      and op is not null
),

by_lsn as (
    select
        *,
        row_number() over (partition by merchant_id, lsn order by kafka_ts, kafka_partition, kafka_offset) as rn_lsn
    from events
),

by_txn as (
    select
        *,
        row_number() over (
            partition by merchant_id, coalesce(cast(txid as varchar), 'lsn' || cast(lsn as varchar))
            order by lsn desc
        ) as rn_txn
    from by_lsn
    where rn_lsn = 1
)

select
    merchant_id,
    lsn,
    txid,
    op,
    source_ts,
    kafka_ts,
    op = 'd' as is_delete,
    json_extract_scalar(after, '$.name') as name,
    json_extract_scalar(after, '$.category') as category,
    json_extract_scalar(after, '$.country') as country,
    cast(from_iso8601_timestamp(json_extract_scalar(after, '$.created_at')) as timestamp(6) with time zone) as created_at
from by_txn
where rn_txn = 1
