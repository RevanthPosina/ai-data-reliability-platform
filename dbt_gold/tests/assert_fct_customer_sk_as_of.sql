-- fct_transactions.customer_sk names the dim_customer version in force at first_lsn (and is not null).
-- Merchant side only for rows that have a merchant. Exact for a single-threaded writer; see ADR-0025.
select f.transaction_id, 'customer_sk' as issue
from {{ ref('fct_transactions') }} f
left join {{ ref('dim_customer') }} d on d.customer_sk = f.customer_sk
where d.customer_sk is null
   or not (d.valid_from_lsn <= f.first_lsn and f.first_lsn < coalesce(d.valid_to_lsn, {{ max_lsn() }}))
union all
select f.transaction_id, 'merchant_sk'
from {{ ref('fct_transactions') }} f
left join {{ ref('dim_merchant') }} d on d.merchant_sk = f.merchant_sk
where f.merchant_id is not null
  and (d.merchant_sk is null
       or not (d.valid_from_lsn <= f.first_lsn and f.first_lsn < coalesce(d.valid_to_lsn, {{ max_lsn() }})))
