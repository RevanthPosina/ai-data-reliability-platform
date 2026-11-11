-- Completeness (F7 named check): every collapsed bronze change (one per key and transaction) is reflected by a
-- dim version in force at its LSN with the same tracked attributes (this also covers the merged no-ops), and
-- every delete closes a version. A late arrival beyond the lookback fails this test.
select 'customer' as dim, e.customer_id as id, e.lsn
from {{ ref('stg_bronze__customer_changes') }} e
where not e.is_delete
  and not exists (
      select 1 from {{ ref('dim_customer') }} d
      where d.customer_id = e.customer_id
        and d.valid_from_lsn <= e.lsn
        and e.lsn < coalesce(d.valid_to_lsn, {{ max_lsn() }})
        and d.full_name = e.full_name and d.email = e.email and d.phone = e.phone
        and d.address = e.address and d.date_of_birth = e.date_of_birth
  )
union all
select 'customer', e.customer_id, e.lsn
from {{ ref('stg_bronze__customer_changes') }} e
where e.is_delete
  and exists (select 1 from {{ ref('stg_bronze__customer_changes') }} p where p.customer_id = e.customer_id and p.lsn < e.lsn and not p.is_delete)
  and not exists (
      select 1 from {{ ref('dim_customer') }} d
      where d.customer_id = e.customer_id and d.valid_to_lsn = e.lsn and d.closed_by_delete
  )
union all
select 'merchant', e.merchant_id, e.lsn
from {{ ref('stg_bronze__merchant_changes') }} e
where not e.is_delete
  and not exists (
      select 1 from {{ ref('dim_merchant') }} d
      where d.merchant_id = e.merchant_id
        and d.valid_from_lsn <= e.lsn
        and e.lsn < coalesce(d.valid_to_lsn, {{ max_lsn() }})
        and d.name = e.name and d.category = e.category and d.country = e.country
  )
union all
select 'merchant', e.merchant_id, e.lsn
from {{ ref('stg_bronze__merchant_changes') }} e
where e.is_delete
  and exists (select 1 from {{ ref('stg_bronze__merchant_changes') }} p where p.merchant_id = e.merchant_id and p.lsn < e.lsn and not p.is_delete)
  and not exists (
      select 1 from {{ ref('dim_merchant') }} d
      where d.merchant_id = e.merchant_id and d.valid_to_lsn = e.lsn and d.closed_by_delete
  )
