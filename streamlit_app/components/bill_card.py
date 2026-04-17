"""Reusable bill summary card component."""
import streamlit as st
from utils.formatting import stage_badge, format_date, chamber_label


def render_bill_card(bill: dict):
    """Render a compact bill summary card with stage badge and key metadata."""
    with st.container(border=True):
        col1, col2 = st.columns([4, 1])
        with col1:
            st.markdown(f"**{bill.get('title', 'Untitled')}**")
            st.markdown(
                stage_badge(bill.get("current_stage")),
                unsafe_allow_html=True,
            )
        with col2:
            st.caption(chamber_label(bill.get("chamber", "NA")))
            if bill.get("foreign_match_count", 0) > 0:
                st.caption(f"🌍 {bill['foreign_match_count']} foreign match(es)")

        if bill.get("summary_short"):
            st.caption(bill["summary_short"])

        meta_cols = st.columns(3)
        meta_cols[0].caption(f"Sponsor: {bill.get('sponsor', '—')}")
        meta_cols[1].caption(f"Introduced: {format_date(bill.get('date_introduced'))}")
        meta_cols[2].caption(f"Updated: {format_date(bill.get('last_updated_at'))}")
