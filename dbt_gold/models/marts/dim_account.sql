{# Type-1 account dimension from silver. Re-keyed automatically when forget re-keys silver (ADR-0017). #}
select
    account_id,
    owner_type,
    customer_id,
    merchant_id,
    currency
from {{ ref('stg_silver__accounts') }}
