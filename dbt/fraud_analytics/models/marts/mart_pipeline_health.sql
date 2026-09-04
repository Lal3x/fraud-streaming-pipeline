select
    date_trunc('day', started_at)::date as execution_date,
    count(*) as execution_count,
    count(*) filter (where status = 'SUCCESS') as successful_count,
    count(*) filter (where status = 'FAILED') as failed_count,
    sum(records_read) as records_read,
    sum(records_inserted) as records_inserted,
    sum(records_duplicated) as records_duplicated,
    sum(records_rejected) as records_rejected,
    avg(duration_seconds) filter (where status = 'SUCCESS') as average_duration_seconds,
    max(finished_at) as last_finished_at
from {{ ref('stg_pipeline_metrics') }}
group by 1
