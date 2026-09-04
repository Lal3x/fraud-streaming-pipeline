{{ config(materialized='incremental', unique_key='event_id', on_schema_change='fail') }}

select
    a.*,
    t.is_fraud,
    t.is_flagged_fraud,
    t.rule_evaluation_class
from {{ ref('stg_fraud_alerts') }} a
join {{ ref('int_transaction_classification') }} t using (event_id)
{% if is_incremental() %}
where a.loaded_at >= (select coalesce(max(loaded_at), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
