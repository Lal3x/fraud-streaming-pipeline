select *,
    case
        when predicted_fraud and is_fraud then 'TRUE_POSITIVE'
        when predicted_fraud and not is_fraud then 'FALSE_POSITIVE'
        when not predicted_fraud and is_fraud then 'FALSE_NEGATIVE'
        else 'TRUE_NEGATIVE'
    end as rule_evaluation_class
from {{ ref('stg_transactions') }}
