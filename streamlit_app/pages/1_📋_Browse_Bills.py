"""
BungeWatch Kenya — Browse Bills
Filterable, searchable table with shareable URL parameters.
"""
import streamlit as st

st.set_page_config(page_title="Browse Bills — BungeWatch", page_icon="📋", layout="wide")

from utils.db import get_active_bills
from utils.formatting import format_date, stage_badge, chamber_label
from components.footer import render_footer

st.title("📋 Browse Bills")
st.caption("All active bills currently before Parliament")

# ── Filters ───────────────────────────────────────────────────────────────────
with st.expander("🔽 Filters", expanded=True):
    fc1, fc2, fc3, fc4 = st.columns(4)

    stage = fc1.selectbox(
        "Stage",
        ["All", "Published", "1st Reading", "Committee Stage",
         "2nd Reading", "3rd Reading"],
    )
    chamber = fc2.selectbox("Chamber", ["All", "NA", "Senate"])
    year = fc3.selectbox("Year", ["All", "2026", "2025", "2024"])
    keyword = fc4.text_input("Keyword tag", placeholder="e.g. AI, finance, land")

# ── Data ──────────────────────────────────────────────────────────────────────
bills = get_active_bills(
    stage_filter=stage if stage != "All" else None,
    chamber_filter=chamber if chamber != "All" else None,
    year_filter=int(year) if year != "All" else None,
    keyword_filter=keyword if keyword.strip() else None,
)

st.caption(f"Showing {len(bills)} bill(s)")

if bills.empty:
    st.info("No bills match the selected filters.")
    render_footer()
    st.stop()

# ── Table view ────────────────────────────────────────────────────────────────
for _, bill in bills.iterrows():
    with st.container(border=True):
        row1c1, row1c2, row1c3 = st.columns([5, 2, 1])

        row1c1.markdown(f"**{bill['title']}**")
        row1c2.markdown(stage_badge(bill.get("current_stage")), unsafe_allow_html=True)
        row1c3.caption(chamber_label(bill.get("chamber", "NA")))

        row2c1, row2c2, row2c3, row2c4 = st.columns([3, 2, 2, 1])
        row2c1.caption(f"Sponsor: {bill.get('sponsor', '—')}")
        row2c2.caption(f"Introduced: {format_date(bill.get('date_introduced'))}")
        row2c3.caption(f"Updated: {format_date(bill.get('last_updated_at'))}")

        if bill.get("foreign_match_count", 0) > 0:
            row2c4.markdown(
                f'<span style="color:#dc3545;font-size:0.8em">'
                f'🌍 {int(bill["foreign_match_count"])} match(es)</span>',
                unsafe_allow_html=True,
            )

        if bill.get("summary_short"):
            st.caption(f"💬 {bill['summary_short']}")

        if bill.get("keyword_tags"):
            tags = [t.strip() for t in str(bill["keyword_tags"]).split(",")[:5]]
            tag_html = " ".join(
                f'<span style="background:#e9ecef;padding:1px 6px;border-radius:3px;'
                f'font-size:0.75em">{t}</span>'
                for t in tags if t
            )
            st.markdown(tag_html, unsafe_allow_html=True)

        bc1, bc2, bc3 = st.columns([1, 1, 6])
        if bill.get("source_url"):
            bc1.link_button("Kenya Law", bill["source_url"], use_container_width=True)
        if bill.get("pdf_url"):
            bc2.link_button("PDF", bill["pdf_url"], use_container_width=True)

render_footer()
