{# First change event per account (min LSN). op = 'c': the opening balance is after.balance.
   op = 'r' (account opened before the CDC snapshot) is NOT supported by the ledger; the singular test
   assert_no_snapshot_opened_accounts fails if one appears. #}
with events as (
    select
        cast(json_extract_scalar(pk, '$.id') as bigint) as account_id,
        op,
        lsn,
        source_ts,
        kafka_ts,
        after,
        row_number() over (partition by pk order by lsn, kafka_partition, kafka_offset) as rn
    from {{ source('bronze', 'cdc_events') }}
    where source_table = 'public.accounts'
      and op is not null
)

select
    account_id,
    op,
    lsn,
    source_ts,
    kafka_ts,
    cast(json_extract_scalar(after, '$.balance') as decimal(14, 2)) as opening_balance,
    json_extract_scalar(after, '$.currency') as currency,
    cast(from_iso8601_timestamp(json_extract_scalar(after, '$.created_at')) as timestamp(6) with time zone) as created_at
from events
where rn = 1
