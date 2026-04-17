-- Staging: Kenya Law bills
-- Flattens raw bills from kenyalaw source into a clean, typed view.

with source as (
    select
        bill_id,
        bill_number,
        title,
        sponsor,
        sponsor_party,
        chamber,
        date_introduced,
        gazette_no,
        current_stage,
        is_passed,
        assent_date,
        source_url,
        pdf_url,
        text_sha256,
        first_seen_at,
        last_updated_at
    from {{ source('bungewatch', 'bills') }}
    where source = 'kenyalaw'
)

select
    bill_id,
    bill_number,
    trim(title)                                    as title,
    trim(coalesce(sponsor, 'Unknown'))             as sponsor,
    sponsor_party,
    upper(chamber)                                 as chamber,
    date_introduced,
    gazette_no,
    current_stage,
    is_passed,
    assent_date,
    source_url,
    pdf_url,
    text_sha256,
    first_seen_at,
    last_updated_at,
    extract(year from coalesce(date_introduced, first_seen_at::date))::int as bill_year
from source
