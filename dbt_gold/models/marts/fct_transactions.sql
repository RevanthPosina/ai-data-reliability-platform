{# Grain: one row per transaction id (a refund, at most one per transaction, is folded in).
   Source is silver (it survives the forget DELETE on bronze; a re-key flows in on the next run).
   Incremental delete+insert by transaction_id. Window: transactions or refunds changed after
   (max src_source_ts in this - lookback). With var backfill_day only that business_date is reselected.
   Carry-forward: first_lsn, customer_sk and merchant_sk are kept from the existing row, so a re-key changes only
   customer_id. customer_sk is a point-in-time join of the transaction's first change LSN onto dim_customer:
   exact for this single-threaded generator, approximate under concurrent writers (ADR-0025). #}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='transaction_id',
    on_schema_change='fail'
) }}

{% set day = backfill_day() %}
{{ assert_backfill_is_incremental(day) }}

with txn as (
    select * from {{ ref('stg_silver__transactions') }}
),

refunds as (
    select * from {{ ref('stg_silver__refunds') }}
),

joined as (
    select
        t.transaction_id,
        t.txn_type,
        t.customer_id,
        t.merchant_id,
        t.payer_account_id,
        t.payee_account_id,
        t.amount,
        t.currency,
        t.status,
        t.created_at,
        t.lsn as txn_lsn,
        r.refund_id,
        r.amount as refunded_amount,
        greatest(t.lsn, coalesce(r.lsn, t.lsn)) as src_lsn,
        greatest(t.source_ts, coalesce(r.source_ts, t.source_ts)) as src_source_ts
    from txn t
    left join refunds r on r.transaction_id = t.transaction_id
),

windowed as (
    select * from joined
    {% if day != '' -%}
    where {{ business_date('created_at') }} = date '{{ day }}'
    {%- elif is_incremental() -%}
    where src_source_ts > {{ lookback_cutoff('src_source_ts') }}
    {%- endif %}
),

{% if is_incremental() %}
existing as (
    select transaction_id, first_lsn, customer_sk, merchant_sk
    from {{ this }}
),
{% endif %}

keyed as (
    select
        w.*,
        {% if is_incremental() -%}
        coalesce(e.first_lsn, w.txn_lsn) as first_lsn,
        e.customer_sk as old_customer_sk,
        e.merchant_sk as old_merchant_sk
        {%- else -%}
        w.txn_lsn as first_lsn,
        cast(null as varchar) as old_customer_sk,
        cast(null as varchar) as old_merchant_sk
        {%- endif %}
    from windowed w
    {% if is_incremental() -%}
    left join existing e on e.transaction_id = w.transaction_id
    {%- endif %}
),

resolved as (
    select
        k.*,
        dc.customer_sk as new_customer_sk,
        dm.merchant_sk as new_merchant_sk
    from keyed k
    left join {{ ref('dim_customer') }} dc
        on dc.customer_id = k.customer_id
       and dc.valid_from_lsn <= k.first_lsn
       and k.first_lsn < coalesce(dc.valid_to_lsn, {{ max_lsn() }})
    left join {{ ref('dim_merchant') }} dm
        on dm.merchant_id = k.merchant_id
       and dm.valid_from_lsn <= k.first_lsn
       and k.first_lsn < coalesce(dm.valid_to_lsn, {{ max_lsn() }})
)

select
    cast(transaction_id as bigint) as transaction_id,
    txn_type,
    cast(customer_id as bigint) as customer_id,
    cast(coalesce(old_customer_sk, new_customer_sk) as varchar) as customer_sk,
    cast(merchant_id as bigint) as merchant_id,
    cast(coalesce(old_merchant_sk, new_merchant_sk) as varchar) as merchant_sk,
    cast(payer_account_id as bigint) as payer_account_id,
    cast(payee_account_id as bigint) as payee_account_id,
    cast(amount as decimal(12, 2)) as amount,
    currency,
    status,
    created_at,
    {{ business_date('created_at') }} as business_date,
    {{ date_key(business_date('created_at')) }} as date_key,
    cast(refund_id as bigint) as refund_id,
    cast(refunded_amount as decimal(12, 2)) as refunded_amount,
    cast(first_lsn as bigint) as first_lsn,
    cast(src_lsn as bigint) as src_lsn,
    cast(src_source_ts as timestamp(6) with time zone) as src_source_ts
from resolved
