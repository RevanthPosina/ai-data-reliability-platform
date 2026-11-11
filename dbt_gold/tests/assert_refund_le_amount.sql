-- A refund never exceeds the payment it refunds.
select transaction_id, amount, refunded_amount
from {{ ref('fct_transactions') }}
where refunded_amount is not null
  and (refunded_amount > amount or refunded_amount <= 0)
