-- No version has an empty or inverted LSN interval.
select 'customer' as dim, customer_id as id, valid_from_lsn, valid_to_lsn
from {{ ref('dim_customer') }}
where valid_to_lsn is not null and valid_to_lsn <= valid_from_lsn
union all
select 'merchant', merchant_id, valid_from_lsn, valid_to_lsn
from {{ ref('dim_merchant') }}
where valid_to_lsn is not null and valid_to_lsn <= valid_from_lsn
