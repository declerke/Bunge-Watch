-- Mart: Sponsor accountability statistics.
-- One row per sponsor with their legislative track record.

with bills as (
    select * from {{ ref('int_bills_unified') }}
    where sponsor is not null and sponsor != 'Unknown'
)

select
    sponsor,
    chamber,
    count(*)                                            as bills_introduced,
    count(*) filter (where is_passed = true)            as bills_passed,
    count(*) filter (where is_passed = false)           as bills_pending,
    round(
        100.0 * count(*) filter (where is_passed = true)
        / nullif(count(*), 0),
        1
    )                                                   as pass_rate_pct,
    min(date_introduced)                                as first_bill_date,
    max(date_introduced)                                as latest_bill_date,
    count(distinct bill_year)                           as active_years
from bills
group by sponsor, chamber
order by bills_introduced desc
