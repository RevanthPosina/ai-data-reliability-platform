{# Input of the dbt snapshot: key and attribute hash only, so the snapshot holds no plain PII. #}
select
    cast(id as bigint) as customer_id,
    {{ attr_hash(['email', 'phone', 'address']) }} as attr_hash,
    cast(updated_at as timestamp(6) with time zone) as updated_at
from {{ source('silver', 'customers') }}
where not is_deleted
