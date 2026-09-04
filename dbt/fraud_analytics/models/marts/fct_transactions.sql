{{ config(materialized='incremental', unique_key='event_id', on_schema_change='fail') }}

select *
from {{ ref('int_transaction_classification') }}
{% if is_incremental() %}
where loaded_at >= (select coalesce(max(loaded_at), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
