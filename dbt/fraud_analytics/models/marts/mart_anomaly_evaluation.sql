with latest_model as (
    select model_version
    from monitoring.ml_model_runs
    where status = 'SUCCESS'
    order by finished_at desc
    limit 1
), evaluated as (
    select
        scores.model_version,
        scores.dataset_split,
        scores.is_anomaly,
        transactions.is_fraud
    from {{ source('anomaly_scoring', 'transaction_anomaly_scores') }} scores
    join latest_model using (model_version)
    join {{ ref('fct_transactions') }} transactions using (event_id)
    where scores.dataset_split = 'EVALUATION'
), confusion as (
    select
        model_version,
        count(*) filter (where is_anomaly and is_fraud) as true_positive,
        count(*) filter (where is_anomaly and not is_fraud) as false_positive,
        count(*) filter (where not is_anomaly and is_fraud) as false_negative,
        count(*) filter (where not is_anomaly and not is_fraud) as true_negative
    from evaluated
    group by model_version
)
select
    *,
    true_positive::numeric / nullif(true_positive + false_positive, 0) as precision,
    true_positive::numeric / nullif(true_positive + false_negative, 0) as recall,
    2.0 * true_positive
        / nullif(2 * true_positive + false_positive + false_negative, 0) as f1_score
from confusion
