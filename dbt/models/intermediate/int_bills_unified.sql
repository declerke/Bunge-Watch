-- Intermediate: Unified bills from both sources, deduplicated.
-- When same bill appears in both sources, kenyalaw wins on structured fields
-- (it has better metadata), parliament wins on PDF URL (direct link).

with kenyalaw as (
    select *, 'kenyalaw' as source from {{ ref('stg_kenyalaw_bills') }}
),

parliament as (
    select *, 'parliament' as source from {{ ref('stg_parliament_bills') }}
),

-- Normalise title for matching: lowercase, strip punctuation, trim
kenyalaw_norm as (
    select *,
        regexp_replace(lower(trim(title)), '[^a-z0-9 ]', '', 'g') as title_norm
    from kenyalaw
),

parliament_norm as (
    select *,
        regexp_replace(lower(trim(title)), '[^a-z0-9 ]', '', 'g') as title_norm
    from parliament
),

-- Bills only in kenyalaw
only_kenyalaw as (
    select k.*
    from kenyalaw_norm k
    where not exists (
        select 1 from parliament_norm p
        where p.title_norm = k.title_norm
    )
),

-- Bills only in parliament
only_parliament as (
    select p.*
    from parliament_norm p
    where not exists (
        select 1 from kenyalaw_norm k
        where k.title_norm = p.title_norm
    )
),

-- Bills in both sources — merge, kenyalaw wins on metadata
merged as (
    select
        k.bill_id,
        k.title,
        k.bill_number,
        k.sponsor,
        k.sponsor_party,
        k.chamber,
        k.date_introduced,
        k.gazette_no,
        k.current_stage,
        k.is_passed,
        k.assent_date,
        k.source_url,
        coalesce(p.pdf_url, k.pdf_url)  as pdf_url,
        k.text_sha256,
        k.first_seen_at,
        k.last_updated_at,
        k.bill_year,
        'both'                          as source
    from kenyalaw_norm k
    join parliament_norm p on p.title_norm = k.title_norm
),

all_bills as (
    select bill_id, title, bill_number, sponsor, sponsor_party, chamber,
           date_introduced, gazette_no, current_stage, is_passed, assent_date,
           source_url, pdf_url, text_sha256, first_seen_at, last_updated_at,
           bill_year, source
    from only_kenyalaw
    union all
    select bill_id, title, bill_number, sponsor, sponsor_party, chamber,
           date_introduced, gazette_no, current_stage, is_passed, assent_date,
           source_url, pdf_url, text_sha256, first_seen_at, last_updated_at,
           bill_year, source
    from only_parliament
    union all
    select * from merged
)

select * from all_bills
