with base as (
    select
        event_id, event_time, amount, transaction_type, event_hour,
        origin_account, destination_account, origin_balance_change,
        destination_balance_change, origin_balance_difference,
        has_origin_balance_anomaly, has_destination_balance_anomaly,
        ln(1 + amount) as log1p_amount,
        count(*) over (
            partition by origin_account order by extract(epoch from event_time)
            range between 86400 preceding and 1 preceding
        ) as recent_transaction_count,
        coalesce(sum(amount) over (
            partition by origin_account order by extract(epoch from event_time)
            range between 86400 preceding and 1 preceding
        ), 0) as recent_transaction_volume,
        avg(ln(1 + amount)) over (
            partition by origin_account order by event_time
            rows between unbounded preceding and 1 preceding
        ) as historical_log_amount_mean,
        stddev_samp(ln(1 + amount)) over (
            partition by origin_account order by event_time
            rows between unbounded preceding and 1 preceding
        ) as historical_log_amount_stddev
    from {{ ref('stg_transactions') }}
), robust as (
    select b.*,
        stats.historical_log_amount_median,
        stats.historical_log_amount_mad
    from base b
    left join lateral (
        with history as (
            select ln(1 + h.amount) as value
            from {{ ref('stg_transactions') }} h
            where h.origin_account = b.origin_account and h.event_time < b.event_time
        ), med as (
            select percentile_cont(0.5) within group (order by value) as median from history
        )
        select med.median as historical_log_amount_median,
            percentile_cont(0.5) within group (order by abs(history.value - med.median)) as historical_log_amount_mad
        from history cross join med group by med.median
    ) stats on true
)
select
    event_id, event_time, amount, log1p_amount, transaction_type, event_hour,
    origin_account, destination_account, origin_balance_change,
    destination_balance_change, origin_balance_difference,
    has_origin_balance_anomaly, has_destination_balance_anomaly,
    recent_transaction_count, recent_transaction_volume,
    historical_log_amount_mean, historical_log_amount_stddev,
    historical_log_amount_median, historical_log_amount_mad,
    (log1p_amount - historical_log_amount_mean)
        / nullif(historical_log_amount_stddev, 0) as log_amount_zscore,
    0.6745 * (log1p_amount - historical_log_amount_median)
        / nullif(historical_log_amount_mad, 0) as log_amount_robust_zscore
from robust
