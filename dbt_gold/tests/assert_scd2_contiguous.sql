-- A closed version that was not closed by a delete is followed by a version starting at its valid_to_lsn.
select 'customer' as dim, a.customer_id as id, a.valid_from_lsn
from {{ ref('dim_customer') }} a
where a.valid_to_lsn is not null
  and not a.closed_by_delete
  and not exists (
      select 1 from {{ ref('dim_customer') }} b
      where b.customer_id = a.customer_id and b.valid_from_lsn = a.valid_to_lsn
  )
union all
select 'merchant', a.merchant_id, a.valid_from_lsn
from {{ ref('dim_merchant') }} a
where a.valid_to_lsn is not null
  and not a.closed_by_delete
  and not exists (
      select 1 from {{ ref('dim_merchant') }} b
      where b.merchant_id = a.merchant_id and b.valid_from_lsn = a.valid_to_lsn
  )
