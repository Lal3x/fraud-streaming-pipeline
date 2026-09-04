with account_events as (
    select origin_account as account_id, event_date,
        count(*) as outgoing_transaction_count, sum(amount) as outgoing_amount,
        0::bigint as incoming_transaction_count, 0::numeric as incoming_amount
    from {{ ref('stg_transactions') }} group by 1, 2
    union all
    select destination_account, event_date, 0::bigint, 0::numeric,
        count(*), sum(amount)
    from {{ ref('stg_transactions') }} group by 1, 2
)
select account_id, event_date,
    sum(outgoing_transaction_count) as outgoing_transaction_count,
    sum(outgoing_amount) as outgoing_amount,
    sum(incoming_transaction_count) as incoming_transaction_count,
    sum(incoming_amount) as incoming_amount
from account_events group by 1, 2
