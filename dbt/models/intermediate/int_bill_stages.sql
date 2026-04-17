-- Intermediate: Bill stage history enriched with ordering.

with stages as (
    select
        bs.id,
        bs.bill_id,
        bs.stage_name,
        bs.stage_date,
        bs.source,
        bs.observed_at,
        b.title,
        b.chamber,
        b.sponsor
    from {{ source('bungewatch', 'bill_stages') }} bs
    join {{ source('bungewatch', 'bills') }} b on b.bill_id = bs.bill_id
),

ordered as (
    select *,
        case stage_name
            when 'Published'       then 1
            when '1st Reading'     then 2
            when 'Committee Stage' then 3
            when '2nd Reading'     then 4
            when '3rd Reading'     then 5
            when 'Assented'        then 6
            else 0
        end as stage_order,
        row_number() over (
            partition by bill_id, stage_name
            order by observed_at desc
        ) as rn
    from stages
)

select
    id,
    bill_id,
    title,
    chamber,
    sponsor,
    stage_name,
    stage_order,
    stage_date,
    source,
    observed_at
from ordered
where rn = 1
