{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    views_enabled=false,
    unique_key='merchant_id',
    on_schema_change='fail'
) }}

{{ scd2_dim(
    ref('stg_bronze__merchant_changes'),
    'merchant_id',
    ['name', 'category', 'country'],
    ['name', 'category', 'country', 'created_at']
) }}
