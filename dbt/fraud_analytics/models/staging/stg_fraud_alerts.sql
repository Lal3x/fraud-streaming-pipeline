select event_id, event_time, upper(trim(transaction_type)) as transaction_type,
    amount::numeric(20, 2) as amount, origin_account, destination_account,
    triggered_rules, risk_score, upper(trim(risk_level)) as risk_level,
    predicted_fraud, source_file, load_id, loaded_at
from {{ source('silver', 'fraud_alerts') }}
