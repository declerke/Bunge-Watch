"""Visual legislative stage progress bar component."""
import streamlit as st
from utils.formatting import STAGE_LABELS, STAGE_COLOURS


def render_stage_progress(current_stage: str, stages_completed: list[str]):
    """
    Render a horizontal stage progress bar.
    Green = completed, bold = current, grey = upcoming.
    """
    cols = st.columns(len(STAGE_LABELS))
    for i, (col, label) in enumerate(zip(cols, STAGE_LABELS)):
        if label in stages_completed:
            colour = "#006600"
            icon = "✅"
            weight = "normal"
        elif label == current_stage:
            colour = "#fd7e14"
            icon = "▶"
            weight = "bold"
        else:
            colour = "#dee2e6"
            icon = "○"
            weight = "normal"

        col.markdown(
            f'<div style="text-align:center;padding:4px">'
            f'<div style="font-size:1.2em">{icon}</div>'
            f'<div style="font-size:0.7em;font-weight:{weight};color:{colour}">'
            f'{label}</div></div>',
            unsafe_allow_html=True,
        )
