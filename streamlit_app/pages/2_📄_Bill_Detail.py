"""
BungeWatch Kenya — Bill Detail
Full bill view: summary, stage progress, foreign law comparison.
This is the civic core of BungeWatch.
"""
import streamlit as st

st.set_page_config(page_title="Bill Detail — BungeWatch", page_icon="📄", layout="wide")

from utils.db import get_bill_detail, get_active_bills
from utils.formatting import (
    format_date, chamber_label, STAGE_LABELS, similarity_colour
)
from components.stage_progress import render_stage_progress
from components.footer import render_footer

st.title("📄 Bill Detail")

bills = get_active_bills()

if bills.empty:
    st.warning("No bills found. Run the pipeline to populate the database.")
    render_footer()
    st.stop()

bill_options = dict(zip(bills["title"], bills["bill_id"]))
id_to_title = {v: k for k, v in bill_options.items()}

url_bill_id = st.query_params.get("bill_id")
if url_bill_id and url_bill_id in id_to_title:
    default_title = id_to_title[url_bill_id]
else:
    default_title = list(bill_options.keys())[0]

selected_title = st.selectbox(
    "Select a bill",
    list(bill_options.keys()),
    index=list(bill_options.keys()).index(default_title),
)
bill_id = bill_options[selected_title]

data = get_bill_detail(bill_id)
bill = data.get("bill", {})
stages = data.get("stages", [])
keywords = data.get("keywords", [])
foreign_matches = data.get("foreign_matches", [])

if not bill:
    st.error("Bill not found.")
    st.stop()

st.divider()

st.subheader(bill.get("title", "—"))

meta_c1, meta_c2, meta_c3, meta_c4 = st.columns(4)
meta_c1.metric("Chamber", chamber_label(bill.get("chamber", "NA")))
meta_c2.metric("Sponsor", bill.get("sponsor") or "Unknown")
meta_c3.metric("Introduced", format_date(bill.get("date_introduced")))
meta_c4.metric("Gazette No.", bill.get("gazette_no") or "—")

st.divider()

st.subheader("Legislative Journey")
completed = [s["stage_name"] for s in stages]
render_stage_progress(bill.get("current_stage", "Published"), completed)

st.divider()

st.subheader("Plain-English Summary")
st.caption("🤖 Auto-generated summary using NLP (spaCy extractive). Always refer to the official PDF for authoritative text.")

if bill.get("summary_short"):
    st.info(f"**In brief:** {bill['summary_short']}")

if bill.get("summary_detailed"):
    st.markdown(bill["summary_detailed"])
else:
    st.caption("Summary not yet generated. The pipeline will process this bill on the next run.")

col_pdf, col_law = st.columns(2)
if bill.get("pdf_url"):
    col_pdf.link_button("📥 Official Bill PDF", bill["pdf_url"])
if bill.get("source_url"):
    col_law.link_button("🔗 View on Kenya Law", bill["source_url"])

st.divider()

if keywords:
    st.subheader("Key Topics")
    tag_html = " ".join(
        f'<span style="background:#dee2e6;padding:3px 10px;border-radius:12px;'
        f'font-size:0.9em;margin:2px">{kw["keyword"]}</span>'
        for kw in keywords
    )
    st.markdown(tag_html, unsafe_allow_html=True)
    st.divider()

st.subheader("🌍 Similar Foreign Laws")
st.caption(
    "Does this bill resemble legislation from other countries? "
    "This section helps Kenyans understand the international context "
    "and whether provisions have been adapted — or copied — from abroad."
)

if not foreign_matches:
    st.info(
        "No significant foreign law similarities detected for this bill, "
        "or the comparison has not yet been run."
    )
else:
    for match in foreign_matches:
        score = match.get("similarity_score", 0)
        colour = similarity_colour(score)

        with st.container(border=True):
            hc1, hc2 = st.columns([5, 1])
            hc1.markdown(
                f"**{match['jurisdiction']} — {match['law_name']}** "
                f"({match.get('law_year', '—')})"
            )
            hc2.markdown(
                f'<div style="text-align:right;color:{colour};font-size:1.2em;'
                f'font-weight:bold">{int(score)}% similar</div>',
                unsafe_allow_html=True,
            )

            st.markdown(match.get("explanation", ""))

            if match.get("full_text_url"):
                st.link_button(
                    f"Read {match['law_name']}",
                    match["full_text_url"],
                )

        st.caption(
            "**Similarity guide:** 70–100% = near-identical provisions; "
            "40–69% = substantial overlap; 20–39% = partial parallels."
        )

render_footer()
