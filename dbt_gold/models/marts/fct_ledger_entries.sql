{# Grain: one row per (journal, leg). Double-entry, signed amounts (+ raises the account balance).
   Append-only by construction: incremental append, only entries whose entry_id is not already in this are
   selected, and full_refresh is false (a full refresh would rebuild from bronze, which a forget has trimmed;
   ADR-0026/0027). views_enabled false keeps the incremental temp relation a table (no views in the catalog).
   No customer_id: join through dim_account (the ledger is never touched by forget).
   Backfill (var backfill_day): pre_hook deletes that business_date, then the anti-join re-emits it. #}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    full_refresh=false,
    views_enabled=false,
    on_schema_change='fail',
    pre_hook="{{ backfill_delete_day() }}"
) }}

{% set day = backfill_day() %}
{{ assert_backfill_is_incremental(day) }}

with openings as (
    select
        'opening' as journal_type,
        cast(account_id as varchar) as source_id,
        cast(null as bigint) as transaction_id,
        cast(null as bigint) as refund_id,
        'account' as leg_a,
        account_id as account_a,
        cast(opening_balance as decimal(14, 2)) as amount_a,
        'equity' as leg_b,
        cast(-2 as bigint) as account_b,
        cast(-opening_balance as decimal(14, 2)) as amount_b,
        currency,
        {{ business_date('created_at') }} as business_date,
        lsn as source_lsn,
        source_ts as src_source_ts
    from {{ ref('stg_bronze__account_openings') }}
    where op = 'c'
      and opening_balance <> 0
),

txn_journals as (
    select
        txn_type as journal_type,
        cast(transaction_id as varchar) as source_id,
        transaction_id,
        cast(null as bigint) as refund_id,
        case when txn_type = 'topup' then 'payee' else 'payer' end as leg_a,
        case when txn_type = 'topup' then payee_account_id else payer_account_id end as account_a,
        cast(case when txn_type = 'topup' then amount else -amount end as decimal(14, 2)) as amount_a,
        case when txn_type = 'topup' then 'funding' else 'payee' end as leg_b,
        case when txn_type = 'topup' then cast(-1 as bigint) else payee_account_id end as account_b,
        cast(case when txn_type = 'topup' then -amount else amount end as decimal(14, 2)) as amount_b,
        currency,
        {{ business_date('created_at') }} as business_date,
        lsn as source_lsn,
        source_ts as src_source_ts
    from {{ ref('stg_silver__transactions') }}
    where txn_type in ('payment', 'topup')
),

refund_journals as (
    select
        'refund' as journal_type,
        cast(r.refund_id as varchar) as source_id,
        r.transaction_id,
        r.refund_id,
        'payee' as leg_a,
        t.payee_account_id as account_a,
        cast(-r.amount as decimal(14, 2)) as amount_a,
        'payer' as leg_b,
        coalesce(t.payer_account_id, cast(-1 as bigint)) as account_b,
        cast(r.amount as decimal(14, 2)) as amount_b,
        t.currency,
        {{ business_date('r.created_at') }} as business_date,
        r.lsn as source_lsn,
        r.source_ts as src_source_ts
    from {{ ref('stg_silver__refunds') }} r
    inner join {{ ref('stg_silver__transactions') }} t on t.transaction_id = r.transaction_id
),

journals as (
    select * from openings
    union all
    select * from txn_journals
    union all
    select * from refund_journals
),

windowed as (
    select * from journals
    {% if day != '' -%}
    where business_date = date '{{ day }}'
    {%- elif is_incremental() -%}
    where src_source_ts > {{ lookback_cutoff('src_source_ts') }}
    {%- endif %}
),

legs as (
    select journal_type, source_id, transaction_id, refund_id, leg_a as leg, account_a as account_id,
           amount_a as amount, currency, business_date, source_lsn, src_source_ts
    from windowed
    union all
    select journal_type, source_id, transaction_id, refund_id, leg_b, account_b,
           amount_b, currency, business_date, source_lsn, src_source_ts
    from windowed
),

entries as (
    select
        journal_type || ':' || source_id || ':' || leg as entry_id,
        journal_type || ':' || source_id as journal_id,
        journal_type,
        transaction_id,
        refund_id,
        account_id,
        amount,
        currency,
        business_date,
        source_lsn,
        src_source_ts
    from legs
)

select
    cast(e.entry_id as varchar) as entry_id,
    cast(e.journal_id as varchar) as journal_id,
    cast(e.journal_type as varchar) as journal_type,
    cast(e.transaction_id as bigint) as transaction_id,
    cast(e.refund_id as bigint) as refund_id,
    cast(e.account_id as bigint) as account_id,
    cast(e.amount as decimal(14, 2)) as amount,
    cast(e.currency as varchar) as currency,
    e.business_date,
    {{ date_key('e.business_date') }} as date_key,
    cast(e.source_lsn as bigint) as source_lsn,
    cast(e.src_source_ts as timestamp(6) with time zone) as src_source_ts
from entries e
{% if is_incremental() -%}
where not exists (select 1 from {{ this }} t where t.entry_id = e.entry_id)
{%- endif %}
