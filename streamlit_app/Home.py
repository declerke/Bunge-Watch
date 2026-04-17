"""
BungeWatch Kenya — Home Page
Hero + KPI stats + Recent Changes feed + Featured bills
"""
import streamlit as st

st.set_page_config(
    page_title="BungeWatch Kenya",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

from utils.db import get_dashboard_stats, get_recent_changes, get_active_bills
from utils.formatting import format_date, stage_badge, chamber_label
from components.footer import render_footer

st.markdown(
    """
    <div style='text-align:center;padding:2rem 0 1rem'>
        <h1 style='font-size:2.8em'>🏛️ BungeWatch Kenya</h1>
        <p style='font-size:1.2em;color:#444'>
            Track what Parliament is doing — in plain English, before it becomes law.
        </p>
        <p style='font-size:0.95em;color:#888'>
            Powered by real-time scraping from Kenya Law and Parliament of Kenya.
            NLP summaries and keyword extraction run fully offline.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

st.divider()

stats = get_dashboard_stats()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Active Bills", stats.get("active_bills", 0),
            help="Bills currently before Parliament")
col2.metric("Bills This Month", stats.get("bills_this_month", 0),
            help="Bills introduced this calendar month")
col3.metric("Near Passage", stats.get("near_passage", 0),
            help="Bills at 2nd or 3rd Reading — close to becoming law")
col4.metric("Possibly Foreign-Inspired", stats.get("foreign_inspired", 0),
            help="Bills with ≥40% similarity to a known foreign law")

st.divider()

st.subheader("📢 Recent Changes")
st.caption("Stage transitions in the last 7 days")

changes = get_recent_changes(days=7)

if changes.empty:
    st.info("No stage changes detected in the last 7 days. The pipeline updates daily at 6 AM.")
else:
    for _, row in changes.iterrows():
        with st.container(border=True):
            c1, c2, c3 = st.columns([4, 2, 2])
            c1.markdown(f"**{row['title']}**")
            c2.markdown(stage_badge(row["stage_name"]), unsafe_allow_html=True)
            c3.caption(f"{format_date(row.get('observed_at'))}")

st.divider()

st.subheader("📌 Latest Bills")
st.caption("Most recently updated active bills")

bills = get_active_bills()

if bills.empty:
    st.warning(
        "No bills in the database yet. "
        "Run the pipeline first: `docker-compose up airflow-scheduler` "
        "or `python scripts/run_standalone.py`"
    )
else:
    for _, bill in bills.head(6).iterrows():
        with st.expander(f"{'🔴' if bill.get('foreign_match_count', 0) > 0 else '📄'} {bill['title']}"):
            cols = st.columns([3, 1, 1])
            cols[0].markdown(f"**Sponsor:** {bill.get('sponsor', '—')}")
            cols[1].markdown(
                stage_badge(bill.get("current_stage")), unsafe_allow_html=True
            )
            cols[2].caption(chamber_label(bill.get("chamber", "NA")))

            if bill.get("summary_short"):
                st.info(f"💬 {bill['summary_short']}")

            if bill.get("foreign_match_count", 0) > 0:
                st.warning(
                    f"🌍 This bill has **{bill['foreign_match_count']} foreign law "
                    f"match(es)**. See full details in Browse Bills."
                )

            btn_col1, btn_col2 = st.columns(2)
            if bill.get("source_url"):
                btn_col1.link_button("View on Kenya Law", bill["source_url"])
            if bill.get("pdf_url"):
                btn_col2.link_button("Official PDF", bill["pdf_url"])

render_footer()
