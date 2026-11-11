-- LSN intervals [valid_from_lsn, valid_to_lsn) of one key never overlap.
select 'customer' as dim, a.customer_id as id, a.valid_from_lsn
from {{ ref('dim_customer') }} a
inner join {{ ref('dim_customer') }} b
    on a.customer_id = b.customer_id
   and a.valid_from_lsn < b.valid_from_lsn
   and coalesce(a.valid_to_lsn, {{ max_lsn() }}) > b.valid_from_lsn
union all
select 'merchant', a.merchant_id, a.valid_from_lsn
from {{ ref('dim_merchant') }} a
inner join {{ ref('dim_merchant') }} b
    on a.merchant_id = b.merchant_id
   and a.valid_from_lsn < b.valid_from_lsn
   and coalesce(a.valid_to_lsn, {{ max_lsn() }}) > b.valid_from_lsn
