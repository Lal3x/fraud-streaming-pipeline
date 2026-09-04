select event_date, transaction_type, count(*) as transaction_count,
    sum(amount) as total_amount, avg(amount) as average_amount,
    count(*) filter (where predicted_fraud) as predicted_fraud_count,
    count(*) filter (where is_fraud) as labeled_fraud_count,
    avg(risk_score) as average_risk_score
from {{ ref('stg_transactions') }} group by 1, 2
