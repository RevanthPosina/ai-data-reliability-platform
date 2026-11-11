{# Live (non-tombstone) silver transactions. `description` is deliberately not carried (data minimization). #}
select
    cast(id as bigint) as transaction_id,
    cast(txn_type as varchar) as txn_type,
    cast(customer_id as bigint) as customer_id,
    cast(merchant_id as bigint) as merchant_id,
    cast(payer_account_id as bigint) as payer_account_id,
    cast(payee_account_id as bigint) as payee_account_id,
    cast(amount as decimal(12, 2)) as amount,
    cast(currency as varchar) as currency,
    cast(status as varchar) as status,
    cast(created_at as timestamp(6) with time zone) as created_at,
    lsn,
    source_ts
from {{ source('silver', 'transactions') }}
where not is_deleted
