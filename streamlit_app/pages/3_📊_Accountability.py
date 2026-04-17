"""
BungeWatch Kenya — Accountability
Sponsor leaderboards, pass rates, and MP legislative activity.
"""
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Accountability — BungeWatch", page_icon="📊", layout="wide")

from utils.db import get_sponsor_stats
from utils.formatting import chamber_label
from components.footer import render_footer

st.title("📊 Accountability")
st.caption("Who is introducing legislation — and how much of it passes?")

stats = get_sponsor_stats()

if stats.empty:
    st.info("No sponsor data yet. Run the pipeline to populate this page.")
    render_footer()
    st.stop()

# ── Summary stats ─────────────────────────────────────────────────────────────
s1, s2, s3 = st.columns(3)
s1.metric("Total Sponsors", stats["sponsor"].nunique())
s2.metric("Most Active Sponsor", stats.iloc[0]["sponsor"] if not stats.empty else "—")
s3.metric(
    "Highest Pass Rate",
    f"{stats['pass_rate_pct'].max():.0f}%"
    if stats["pass_rate_pct"].notna().any() else "—",
)

st.divider()

# ── Top sponsors by bills introduced ─────────────────────────────────────────
st.subheader("Most Active Sponsors")
top_n = stats.head(20)

fig_intro = px.bar(
    top_n,
    x="bills_introduced",
    y="sponsor",
    orientation="h",
    color="pass_rate_pct",
    color_continuous_scale=[[0, "#dc3545"], [0.5, "#ffc107"], [1, "#006600"]],
    labels={
        "bills_introduced": "Bills Introduced",
        "sponsor": "Sponsor",
        "pass_rate_pct": "Pass Rate %",
    },
    title="Bills Introduced (colour = pass rate)",
)
fig_intro.update_layout(yaxis={"categoryorder": "total ascending"}, height=500)
st.plotly_chart(fig_intro, use_container_width=True)

st.divider()

# ── Bills passed vs pending ───────────────────────────────────────────────────
st.subheader("Bills Passed vs Pending — Top 15 Sponsors")
top15 = stats.head(15).copy()

fig_status = px.bar(
    top15.melt(
        id_vars=["sponsor"],
        value_vars=["bills_passed", "bills_pending"],
        var_name="Status",
        value_name="Count",
    ),
    x="Count",
    y="sponsor",
    color="Status",
    orientation="h",
    color_discrete_map={"bills_passed": "#006600", "bills_pending": "#fd7e14"},
    labels={"sponsor": "Sponsor", "Count": "Bills"},
    title="Bills Passed (green) vs Pending (orange)",
)
fig_status.update_layout(yaxis={"categoryorder": "total ascending"}, height=480)
st.plotly_chart(fig_status, use_container_width=True)

st.divider()

# ── Detail table ──────────────────────────────────────────────────────────────
st.subheader("Full Sponsor Table")
display = stats[
    ["sponsor", "chamber", "bills_introduced", "bills_passed",
     "bills_pending", "pass_rate_pct", "first_bill_date", "latest_bill_date"]
].copy()
display.columns = [
    "Sponsor", "Chamber", "Introduced", "Passed",
    "Pending", "Pass Rate %", "First Bill", "Latest Bill",
]
display["Chamber"] = display["Chamber"].apply(
    lambda c: chamber_label(c) if c else "—"
)
st.dataframe(display, use_container_width=True, hide_index=True)

render_footer()
