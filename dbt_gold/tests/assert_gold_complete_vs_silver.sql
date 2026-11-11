-- Completeness (F7 named check): everything silver holds is in gold. A late arrival beyond the lookback
-- fails this test; a backfill of its business_date recovers it.
select 'fct_transactions' as model, cast(t.transaction_id as varchar) as missing_id
from {{ ref('stg_silver__transactions') }} t
left join {{ ref('fct_transactions') }} f on f.transaction_id = t.transaction_id
where f.transaction_id is null

union all

select 'fct_ledger_entries', e.journal_id
from (
    select txn_type || ':' || cast(transaction_id as varchar) as journal_id
    from {{ ref('stg_silver__transactions') }}
    where txn_type in ('payment', 'topup')
    union all
    select 'refund:' || cast(r.refund_id as varchar)
    from {{ ref('stg_silver__refunds') }} r
    inner join {{ ref('stg_silver__transactions') }} t on t.transaction_id = r.transaction_id
    union all
    select 'opening:' || cast(account_id as varchar)
    from {{ ref('stg_bronze__account_openings') }}
    where op = 'c' and opening_balance <> 0
) e
where not exists (
    select 1 from {{ ref('fct_ledger_entries') }} l where l.journal_id = e.journal_id
)
