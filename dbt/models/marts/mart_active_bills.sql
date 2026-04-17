-- Mart: Currently active bills (not yet assented).
-- Enriched with keyword tags and summary short text.

with bills as (
    select * from {{ ref('int_bills_unified') }}
    where is_passed = false
),

summaries as (
    select bill_id, summary_short
    from {{ source('bungewatch', 'bill_summaries') }}
    where language = 'en'
),

keywords as (
    select
        bill_id,
        string_agg(keyword, ', ' order by relevance_score desc) as keyword_tags
    from {{ source('bungewatch', 'bill_keywords') }}
    group by bill_id
),

foreign_match_counts as (
    select bill_id, count(*) as foreign_match_count
    from {{ source('bungewatch', 'bill_foreign_matches') }}
    where similarity_score >= 30
    group by bill_id
)

select
    b.bill_id,
    b.title,
    b.bill_number,
    b.sponsor,
    b.chamber,
    b.date_introduced,
    b.gazette_no,
    b.current_stage,
    b.bill_year,
    b.source,
    b.source_url,
    b.pdf_url,
    b.last_updated_at,
    coalesce(s.summary_short, 'Summary pending…') as summary_short,
    coalesce(k.keyword_tags, '')                   as keyword_tags,
    coalesce(fmc.foreign_match_count, 0)           as foreign_match_count
from bills b
left join summaries s on s.bill_id = b.bill_id
left join keywords k on k.bill_id = b.bill_id
left join foreign_match_counts fmc on fmc.bill_id = b.bill_id
order by b.last_updated_at desc
