select load_id, pipeline_name, upper(trim(status)) as status, started_at, finished_at,
    files_discovered, files_processed, records_read, records_valid,
    records_rejected, records_inserted, records_duplicated, alerts_read,
    alerts_inserted, error_message,
    extract(epoch from (finished_at - started_at))::numeric as duration_seconds
from {{ source('monitoring', 'pipeline_metrics') }}
