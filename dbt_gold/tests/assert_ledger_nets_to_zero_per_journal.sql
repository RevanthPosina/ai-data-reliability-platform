-- Every journal's signed legs sum to zero.
select journal_id, sum(amount) as net
from {{ ref('fct_ledger_entries') }}
group by journal_id
having sum(amount) <> 0
