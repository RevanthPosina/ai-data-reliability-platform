{# One row per customer *transaction*: the final change of each (customer, txid), deduplicated by (customer, lsn).
   - (pk, lsn) dedupe absorbs F1 replay and F2 re-produced records (earliest kafka_ts wins, then lowest partition/offset).
   - same-transaction collapse keeps the max LSN per (pk, txid), so a transaction yields one version with its
     final values and no zero-length interval.
   `kafka_ts` is the incremental cursor (broker arrival, so a late commit is still picked up). #}
with events as (
    select
        cast(json_extract_scalar(pk, '$.id') as bigint) as customer_id,
        lsn,
        txid,
        op,
        source_ts,
        kafka_ts,
        kafka_partition,
        kafka_offset,
        after
    from {{ source('bronze', 'cdc_events') }}
    where source_table = 'public.customers'
      and op is not null
),

by_lsn as (
    select
        *,
        row_number() over (partition by customer_id, lsn order by kafka_ts, kafka_partition, kafka_offset) as rn_lsn
    from events
),

by_txn as (
    select
        *,
        row_number() over (
            partition by customer_id, coalesce(cast(txid as varchar), 'lsn' || cast(lsn as varchar))
            order by lsn desc
        ) as rn_txn
    from by_lsn
    where rn_lsn = 1
)

select
    customer_id,
    lsn,
    txid,
    op,
    source_ts,
    kafka_ts,
    op = 'd' as is_delete,
    json_extract_scalar(after, '$.full_name') as full_name,
    json_extract_scalar(after, '$.email') as email,
    json_extract_scalar(after, '$.phone') as phone,
    json_extract_scalar(after, '$.address') as address,
    cast(json_extract_scalar(after, '$.date_of_birth') as date) as date_of_birth,
    cast(from_iso8601_timestamp(json_extract_scalar(after, '$.created_at')) as timestamp(6) with time zone) as created_at
from by_txn
where rn_txn = 1
