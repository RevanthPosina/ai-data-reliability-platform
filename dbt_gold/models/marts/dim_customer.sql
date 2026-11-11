{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='customer_id',
    on_schema_change='fail'
) }}

{{ scd2_dim(
    ref('stg_bronze__customer_changes'),
    'customer_id',
    ['full_name', 'email', 'phone', 'address', 'date_of_birth'],
    ['full_name', 'email', 'phone', 'address', 'date_of_birth', 'created_at']
) }}
