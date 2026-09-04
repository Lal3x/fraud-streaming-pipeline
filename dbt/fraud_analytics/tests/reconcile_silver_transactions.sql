select
    (select count(*) from {{ source('silver', 'transactions') }}) as silver_count,
    (select count(*) from {{ ref('fct_transactions') }}) as gold_count
where (select count(*) from {{ source('silver', 'transactions') }})
   <> (select count(*) from {{ ref('fct_transactions') }})
