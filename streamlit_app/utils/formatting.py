"""Display formatters for Streamlit pages."""
from datetime import date, datetime
from typing import Optional

STAGE_LABELS = [
    "Published",
    "1st Reading",
    "Committee Stage",
    "2nd Reading",
    "3rd Reading",
    "Assented",
]

STAGE_COLOURS = {
    "Published": "#6c757d",
    "1st Reading": "#17a2b8",
    "Committee Stage": "#fd7e14",
    "2nd Reading": "#ffc107",
    "3rd Reading": "#007bff",
    "Assented": "#006600",
}


def format_date(d) -> str:
    if d is None:
        return "—"
    if isinstance(d, (date, datetime)):
        return d.strftime("%-d %b %Y")
    return str(d)


def stage_badge(stage_name: Optional[str]) -> str:
    if not stage_name:
        return ""
    colour = STAGE_COLOURS.get(stage_name, "#6c757d")
    return (
        f'<span style="background:{colour};color:white;padding:2px 8px;'
        f'border-radius:4px;font-size:0.8em">{stage_name}</span>'
    )


def chamber_label(chamber: str) -> str:
    return "National Assembly" if chamber == "NA" else "Senate"


def similarity_colour(score: float) -> str:
    if score >= 70:
        return "#dc3545"
    if score >= 40:
        return "#fd7e14"
    return "#ffc107"
