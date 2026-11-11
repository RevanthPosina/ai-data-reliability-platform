-- A payment plus its refund reversal nets to zero per transaction id (opening journals have no transaction).
select transaction_id, sum(amount) as net
from {{ ref('fct_ledger_entries') }}
where transaction_id is not null
group by transaction_id
having sum(amount) <> 0
