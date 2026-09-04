select
    event_date,
    transaction_type,
    transaction_count,
    total_amount,
    average_amount,
    predicted_fraud_count,
    labeled_fraud_count,
    average_risk_score,
    predicted_fraud_count::numeric / nullif(transaction_count, 0) as alert_rate,
    labeled_fraud_count::numeric / nullif(transaction_count, 0) as labeled_fraud_rate
from {{ ref('int_daily_transaction_metrics') }}
