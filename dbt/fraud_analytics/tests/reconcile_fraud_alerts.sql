select
    (select count(*) from {{ source('silver', 'fraud_alerts') }}) as silver_count,
    (select count(*) from {{ ref('fct_fraud_alerts') }}) as gold_count
where (select count(*) from {{ source('silver', 'fraud_alerts') }})
   <> (select count(*) from {{ ref('fct_fraud_alerts') }})
