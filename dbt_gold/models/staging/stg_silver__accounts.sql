select
    cast(id as bigint) as account_id,
    cast(owner_type as varchar) as owner_type,
    cast(customer_id as bigint) as customer_id,
    cast(merchant_id as bigint) as merchant_id,
    cast(currency as varchar) as currency,
    cast(balance as decimal(14, 2)) as balance,
    lsn,
    source_ts
from {{ source('silver', 'accounts') }}
where not is_deleted
