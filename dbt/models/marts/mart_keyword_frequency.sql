-- Mart: Trending keywords across all bills.
-- Powers the keyword cloud and search facets.

select
    k.keyword,
    count(distinct k.bill_id)                    as bill_count,
    avg(k.relevance_score)                       as avg_relevance,
    max(b.last_updated_at)                       as last_seen_at,
    count(distinct k.bill_id) filter (
        where b.is_passed = false
    )                                            as active_bill_count
from {{ source('bungewatch', 'bill_keywords') }} k
join {{ source('bungewatch', 'bills') }} b on b.bill_id = k.bill_id
group by k.keyword
having count(distinct k.bill_id) >= 1
order by bill_count desc, avg_relevance desc
