{{ config(severity='error' if var('strict_reconcile', false) else 'warn') }}
-- Ledger balance per real account equals silver accounts.balance (Postgres accounts.balance after merge).
-- Silver tables are merged one after another, so this only holds at a quiescent cut: warn by default, and
-- an error with --vars '{strict_reconcile: true}' (used by the checks at the cut).
with ledger as (
    select account_id, sum(amount) as ledger_balance
    from {{ ref('fct_ledger_entries') }}
    where account_id > 0
    group by account_id
)

select
    coalesce(a.account_id, l.account_id) as account_id,
    a.balance as silver_balance,
    coalesce(l.ledger_balance, cast(0 as decimal(14, 2))) as ledger_balance
from {{ ref('stg_silver__accounts') }} a
full outer join ledger l on l.account_id = a.account_id
where a.account_id is null
   or coalesce(l.ledger_balance, cast(0 as decimal(14, 2))) <> a.balance
