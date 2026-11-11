select
    cast(id as bigint) as refund_id,
    cast(transaction_id as bigint) as transaction_id,
    cast(amount as decimal(12, 2)) as amount,
    cast(created_at as timestamp(6) with time zone) as created_at,
    lsn,
    source_ts
from {{ source('silver', 'refunds') }}
where not is_deleted
