"""
BungeWatch Kenya — Accountability
Sponsor leaderboards, pass rates, and full pipeline breakdown.
"""
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Accountability — BungeWatch", page_icon="📊", layout="wide")

from utils.db import get_sponsor_stats, get_pipeline_overview, get_foreign_inspired_bills
from utils.formatting import chamber_label, format_date
from components.footer import render_footer

st.title("📊 Accountability")
st.caption("Who is introducing legislation — and how much of it passes?")

st.subheader("📈 Bill Pipeline — All Bills")

overview = get_pipeline_overview()

if not overview.empty:
    ov1, ov2 = st.columns(2)

    with ov1:
        fig_stage = px.bar(
            overview.groupby("current_stage")["bill_count"].sum().reset_index()
                    .sort_values("bill_count", ascending=False),
            x="current_stage",
            y="bill_count",
            labels={"current_stage": "Stage", "bill_count": "Bills"},
            title="Bills by Stage (all sources)",
            color="bill_count",
            color_continuous_scale=[[0, "#adb5bd"], [1, "#006600"]],
        )
        fig_stage.update_layout(coloraxis_showscale=False, height=350)
        st.plotly_chart(fig_stage, use_container_width=True)

    with ov2:
        fig_source = px.bar(
            overview.groupby(["source", "chamber"])["bill_count"].sum().reset_index(),
            x="source",
            y="bill_count",
            color="chamber",
            barmode="stack",
            labels={"source": "Source", "bill_count": "Bills", "chamber": "Chamber"},
            title="Bills by Source & Chamber",
            color_discrete_map={"NA": "#0d6efd", "Senate": "#fd7e14"},
        )
        fig_source.update_layout(height=350)
        st.plotly_chart(fig_source, use_container_width=True)

st.divider()

st.subheader("🌍 Foreign-Inspired Bills")
st.caption("Bills with TF-IDF cosine similarity ≥ 20 against the 15-law foreign reference corpus")

foreign = get_foreign_inspired_bills()

if foreign.empty:
    st.info("No foreign law matches found above the threshold.")
else:
    for _, row in foreign.iterrows():
        with st.container(border=True):
            c1, c2 = st.columns([5, 1])
            c1.markdown(f"**{row['title']}**")
            c2.markdown(
                f'<span style="background:#dc3545;color:white;padding:2px 8px;'
                f'border-radius:4px;font-size:0.8em">Score {int(row["top_score"])}</span>',
                unsafe_allow_html=True,
            )
            st.caption(
                f"Chamber: {chamber_label(row.get('chamber', ''))}  ·  "
                f"Stage: {row.get('current_stage', '—')}  ·  "
                f"{row['match_count']} match(es)"
            )
            st.caption(f"🔗 Matched laws: {row['matched_laws']}")
            bc1, bc2 = st.columns([1, 9])
            bc1.link_button("Detail →", f"./Bill_Detail?bill_id={row['bill_id']}", use_container_width=True)

st.divider()

st.subheader("🏅 Sponsor Leaderboard")
st.info(
    "Sponsor data is only available for bills sourced from **KenyaLaw** (43 bills). "
    "Parliament.go.ke does not publish sponsor names on the bill listing page.",
    icon="ℹ️",
)

stats = get_sponsor_stats()

if stats.empty:
    st.info("No sponsor data yet. Run the pipeline to populate this section.")
    render_footer()
    st.stop()

s1, s2, s3 = st.columns(3)
s1.metric("Total Sponsors", stats["sponsor"].nunique())
s2.metric("Most Active Sponsor", stats.iloc[0]["sponsor"] if not stats.empty else "—")
s3.metric(
    "Highest Pass Rate",
    f"{stats['pass_rate_pct'].max():.0f}%"
    if stats["pass_rate_pct"].notna().any() else "—",
)

st.divider()

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
