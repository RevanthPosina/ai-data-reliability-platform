-- At most one current row per key; exactly one unless the key's history ends in a delete.
select 'customer' as dim, customer_id as id, count_if(is_current) as n_current
from {{ ref('dim_customer') }}
group by customer_id
having count_if(is_current) > 1
    or (count_if(is_current) = 0 and not bool_or(closed_by_delete))
union all
select 'merchant', merchant_id, count_if(is_current)
from {{ ref('dim_merchant') }}
group by merchant_id
having count_if(is_current) > 1
    or (count_if(is_current) = 0 and not bool_or(closed_by_delete))
