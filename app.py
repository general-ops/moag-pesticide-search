"""Streamlit UI for MoAG Pesticide Search.

RTL Hebrew interface with Google-style search layout.
"""

import io
import traceback

import streamlit as st
import pandas as pd

from agent import search_pesticides, COLUMNS, COL_PEST
from lists import CROPS, PESTS

# ── Pastel colors for pest color-coding ───────────────────────────
PASTEL_COLORS = [
    "#FFE0B2", "#B3E5FC", "#C8E6C9", "#F8BBD0", "#D1C4E9",
    "#FFCCBC", "#B2DFDB", "#FFF9C4", "#F0F4C3", "#DCEDC8",
    "#E1BEE7", "#BBDEFB", "#FFE0E0", "#C5CAE9", "#B2EBF2",
    "#D7CCC8", "#CFD8DC", "#FFECB3", "#E8EAF6", "#FBE9E7",
]

# ── Page config ────────────────────────────────────────────────────
st.set_page_config(
    page_title="חיפוש חומרי הדברה - משרד החקלאות",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── RTL CSS injection ──────────────────────────────────────────────
st.markdown("""
<style>
    /* Global RTL */
    .main, .block-container, .stMarkdown, .stDataFrame,
    .stSelectbox, .stMultiSelect, .stButton {
        direction: rtl;
        text-align: right;
    }

    /* Selectbox & multiselect RTL internals */
    .stSelectbox div[data-baseweb="select"],
    .stMultiSelect div[data-baseweb="select"] {
        direction: rtl;
        text-align: right;
    }
    .stSelectbox label, .stMultiSelect label {
        direction: rtl;
        text-align: right;
    }

    /* Input fields RTL */
    input, textarea {
        direction: rtl !important;
        text-align: right !important;
    }

    /* Table RTL */
    .stDataFrame table {
        direction: rtl;
    }
    .stDataFrame th, .stDataFrame td {
        text-align: right !important;
    }

    /* Header styling */
    h1, h2, h3 {
        direction: rtl;
        text-align: right;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Green header banner */
    .search-header {
        background: linear-gradient(135deg, #2e7d32 0%, #4caf50 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 1.5rem;
    }

    /* Search bar container */
    .search-bar {
        background: #f5f5f5;
        padding: 1.2rem 1.5rem;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        margin-bottom: 1.5rem;
    }

    /* Green primary button override */
    .stButton > button[kind="primary"] {
        background-color: #2e7d32;
        border-color: #2e7d32;
        color: white;
    }
    .stButton > button[kind="primary"]:hover {
        background-color: #1b5e20;
        border-color: #1b5e20;
    }

    /* Welcome state */
    .welcome-box {
        text-align: center;
        padding: 3rem 1rem;
        color: #666;
    }
    .welcome-box h2 {
        text-align: center;
        color: #2e7d32;
    }
    .welcome-box p {
        font-size: 1.1rem;
        max-width: 600px;
        margin: 0.5rem auto;
    }
</style>
""", unsafe_allow_html=True)

# ── Header ─────────────────────────────────────────────────────────
st.markdown("""
<div class="search-header">
    <h1 style="color: white; margin: 0;">🌱 חיפוש חומרי הדברה</h1>
    <p style="color: #e8f5e9; margin: 0.5rem 0 0 0;">מאגר חומרי ההדברה של משרד החקלאות</p>
</div>
""", unsafe_allow_html=True)

# ── Search bar (main body, Google-style) ───────────────────────────
col_crop, col_pests, col_btn = st.columns([2, 3, 1], vertical_alignment="bottom")

with col_crop:
    crop = st.selectbox(
        "גידול",
        options=[""] + CROPS,
        index=0,
        format_func=lambda x: "-- בחר גידול --" if x == "" else x,
        help="בחר את שם הגידול",
    )

with col_pests:
    pests = st.multiselect(
        "פגעים",
        options=PESTS,
        default=[],
        help="בחר פגע אחד או יותר (השאר ריק לחיפוש כל הפגעים)",
        placeholder="-- כל הפגעים --",
    )

with col_btn:
    search_clicked = st.button("🔍 חפש", type="primary", use_container_width=True)

st.divider()

# ── Results area ───────────────────────────────────────────────────
if search_clicked:
    if not crop and not pests:
        st.warning("⚠️ יש להזין לפחות גידול או פגע לחיפוש")
    else:
        parts = [crop] + pests if crop else pests
        search_desc = " + ".join(parts) if parts else ""

        progress_bar = st.progress(0)
        status_text = st.empty()

        def _on_progress(pct: int, msg: str):
            progress_bar.progress(min(pct, 100))
            status_text.markdown(f"**{msg}**")

        try:
            df = search_pesticides(crop, pests, progress_cb=_on_progress)
            progress_bar.empty()
            status_text.empty()

            if df.empty:
                st.info(f"לא נמצאו תוצאות עבור: {search_desc}")
            else:
                st.success(f"נמצאו {len(df)} תוצאות עבור: {search_desc}")

                # ── Category-based color coding ───────────────
                selected_pests = pests  # user's multiselect

                def _categorize(pest_cell: str) -> str:
                    """Map a row's pest string to a category based on selected pests."""
                    if not selected_pests:
                        first = pest_cell.split(",")[0].strip()
                        return first if first else ""
                    cell_pests = {p.strip() for p in pest_cell.split(",")}
                    matched = [sp for sp in selected_pests if sp in cell_pests]
                    if not matched:
                        return "אחר"
                    if len(matched) == 1:
                        return matched[0]
                    return " + ".join(matched)

                # Compute category per row
                categories = df[COL_PEST].fillna("").apply(_categorize)
                unique_cats = list(dict.fromkeys(categories))  # preserve order

                # Assign colors: selected pests first, then combos, then "other"
                cat_colors: dict[str, str] = {}
                ci = 0
                for sp in selected_pests:
                    if sp in unique_cats:
                        cat_colors[sp] = PASTEL_COLORS[ci % len(PASTEL_COLORS)]
                        ci += 1
                for cat in unique_cats:
                    if cat not in cat_colors:
                        cat_colors[cat] = PASTEL_COLORS[ci % len(PASTEL_COLORS)]
                        ci += 1

                # Legend
                if unique_cats:
                    legend_items = " ".join(
                        f"<span style='background-color: {cat_colors[cat]}; "
                        f"padding: 3px 10px; border-radius: 6px; "
                        f"margin: 2px 4px; display: inline-block; "
                        f"font-size: 0.9em;'>{cat}</span>"
                        for cat in unique_cats if cat
                    )
                    st.markdown(
                        f"<div style='direction: rtl; margin-bottom: 1rem;'>"
                        f"<strong>מקרא פגעים:</strong> {legend_items}</div>",
                        unsafe_allow_html=True,
                    )

                # Style function using precomputed categories
                cat_list = categories.tolist()

                def _row_color(row):
                    cat = cat_list[row.name]
                    color = cat_colors.get(cat, "")
                    if color:
                        return [f"background-color: {color}"] * len(row)
                    return [""] * len(row)

                styled = df.style.apply(_row_color, axis=1)

                # Display results table
                col_config = {}
                for col in COLUMNS:
                    col_config[col] = st.column_config.TextColumn(col, width="medium")

                st.dataframe(
                    styled,
                    width="stretch",
                    hide_index=True,
                    column_config=col_config,
                )

                # Download Excel with same color-coding
                buffer = io.BytesIO()
                styled.to_excel(buffer, index=False, engine="openpyxl")
                buffer.seek(0)
                st.download_button(
                    label="📊 הורד כ-Excel",
                    data=buffer,
                    file_name=f"pesticides_{crop}_{'_'.join(pests)}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

        except Exception as e:
            progress_bar.empty()
            status_text.empty()
            st.error(f"❌ שגיאה: {type(e).__name__}: {e}")
            st.code(traceback.format_exc(), language="python")
            st.info("💡 נסה שוב. אם הבעיה חוזרת, ייתכן שהאתר אינו זמין כרגע.")

else:
    # Welcome empty state
    st.markdown("""
    <div class="welcome-box">
        <h2>👋 ברוכים הבאים!</h2>
        <p>בחרו גידול ופגע מהרשימות למעלה ולחצו על <strong>חיפוש</strong> כדי לראות את חומרי ההדברה המאושרים.</p>
        <p style="font-size: 0.9rem; color: #999; margin-top: 1rem;">המידע מגיע ישירות מאתר משרד החקלאות</p>
    </div>
    """, unsafe_allow_html=True)
