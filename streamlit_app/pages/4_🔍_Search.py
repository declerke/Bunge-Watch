"""
BungeWatch Kenya — Search
Full-text search across bill titles, summaries, and extracted text.
"""
import streamlit as st

st.set_page_config(page_title="Search Bills — BungeWatch", page_icon="🔍", layout="wide")

from utils.db import search_bills
from utils.formatting import format_date, stage_badge, chamber_label
from components.footer import render_footer

st.title("🔍 Search Bills")
st.caption(
    "Search bill titles, plain-English summaries, and full bill text. "
    "Try: **AI**, **finance**, **data protection**, **land**, **employment**"
)

query = st.text_input("Search", placeholder="Enter keywords…", label_visibility="collapsed")

if not query.strip():
    st.info("Type a keyword above to search all parliamentary bills.")
    render_footer()
    st.stop()

results = search_bills(query.strip())

if results.empty:
    st.warning(
        f"No bills found matching **'{query}'**. "
        "Try a different term or check spelling."
    )
    render_footer()
    st.stop()

st.caption(f"{len(results)} result(s) for **'{query}'**")
st.divider()

for _, bill in results.iterrows():
    with st.container(border=True):
        c1, c2, c3 = st.columns([5, 2, 1])
        c1.markdown(f"**{bill['title']}**")
        c2.markdown(stage_badge(bill.get("current_stage")), unsafe_allow_html=True)
        c3.caption(chamber_label(bill.get("chamber", "NA")))

        meta1, meta2 = st.columns(2)
        meta1.caption(f"Sponsor: {bill.get('sponsor', '—')}")
        meta2.caption(f"Introduced: {format_date(bill.get('date_introduced'))}")

        if bill.get("summary_short"):
            st.caption(f"💬 {bill['summary_short']}")

render_footer()
