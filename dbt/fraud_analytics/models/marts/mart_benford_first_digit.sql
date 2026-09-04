with eligible as (
    select event_date, transaction_type,
        substring(trim(to_char(amount, 'FM99999999999999999990D99')) from '[1-9]')::smallint as first_digit
    from {{ ref('stg_transactions') }}
    where amount > 0
), observed as (
    select event_date, transaction_type, first_digit,
        count(*) as observed_count,
        sum(count(*)) over (partition by event_date, transaction_type) as sample_size
    from eligible group by 1, 2, 3
), digits as (
    select generate_series(1, 9)::smallint as first_digit
), populations as (
    select distinct event_date, transaction_type, sample_size from observed
    where sample_size >= {{ var('benford_min_sample_size', 100) }}
)
select p.event_date, p.transaction_type, d.first_digit,
    coalesce(o.observed_count, 0) as observed_count,
    coalesce(o.observed_count, 0)::numeric / p.sample_size as observed_proportion,
    log(10, 1 + 1.0 / d.first_digit) as expected_proportion,
    abs(coalesce(o.observed_count, 0)::numeric / p.sample_size
        - log(10, 1 + 1.0 / d.first_digit)) as absolute_difference,
    p.sample_size
from populations p cross join digits d
left join observed o using (event_date, transaction_type, first_digit)
