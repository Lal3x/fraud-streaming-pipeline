with accounts as (
    select origin_account as account_id, false as is_merchant from {{ ref('stg_transactions') }}
    union
    select destination_account, is_merchant_destination from {{ ref('stg_transactions') }}
), activity as (
    select account_id, min(event_date) as first_activity_date,
        max(event_date) as last_activity_date,
        sum(outgoing_transaction_count) as outgoing_transaction_count,
        sum(outgoing_amount) as outgoing_amount,
        sum(incoming_transaction_count) as incoming_transaction_count,
        sum(incoming_amount) as incoming_amount
    from {{ ref('int_account_activity') }} group by 1
)
select a.account_id, bool_or(a.is_merchant) as is_merchant,
    max(t.first_activity_date) as first_activity_date,
    max(t.last_activity_date) as last_activity_date,
    max(t.outgoing_transaction_count) as outgoing_transaction_count,
    max(t.outgoing_amount) as outgoing_amount,
    max(t.incoming_transaction_count) as incoming_transaction_count,
    max(t.incoming_amount) as incoming_amount
from accounts a left join activity t using (account_id)
group by a.account_id
