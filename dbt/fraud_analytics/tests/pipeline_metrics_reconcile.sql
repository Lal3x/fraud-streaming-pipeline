select *
from {{ ref('stg_pipeline_metrics') }}
where status = 'SUCCESS'
  and (
      records_read <> records_valid + records_rejected
      or records_read <> records_inserted + records_duplicated
      or alerts_inserted > alerts_read
  )
