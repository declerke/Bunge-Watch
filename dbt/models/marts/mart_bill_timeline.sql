-- Mart: Recent bill stage transitions — powers the "Recent Changes" feed.

select
    s.bill_id,
    s.title,
    s.chamber,
    s.sponsor,
    s.stage_name,
    s.stage_order,
    s.stage_date,
    s.observed_at,
    s.observed_at::date                          as change_date,
    now()::date - s.observed_at::date            as days_ago
from {{ ref('int_bill_stages') }} s
where s.observed_at >= now() - interval '30 days'
  and s.stage_name != 'Published'
order by s.observed_at desc
