# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A Streamlit app that searches the Israeli Ministry of Agriculture (MoAG) pesticide database via direct OData API calls. Users select a crop and/or pests (in Hebrew), and get a color-coded table of approved pesticides with dosage, PHI, and notes. Deployed on Streamlit Community Cloud.

## Commands

```bash
# Run the app
streamlit run app.py

# Test a search programmatically
python -c "from agent import search_pesticides; print(search_pesticides('אפרסק', ['קימחון']))"

# Install dependencies
pip install -r requirements.txt
```

## Architecture

```
app.py  →  agent.py  →  MoAG OData APIs (4 endpoints)
                     →  pdf_parser.py (fallback for missing dosage/PHI)
           lists.py  →  static CROPS (530) and PESTS (722) for dropdowns
```

**Data flow in `search_pesticides()`:**
1. Resolve crop name → `seqGidul` via `gidulim/$query`
2. Resolve each pest name → `rownumID` via `negaim/$query`
3. Search products via `SearchTachshirim/$query` (paginated, 100/page)
4. Fetch full details via `GetTachshirInfoById` (parallel, 5 workers)
5. Enrich missing dosage/PHI from PDF labels (parallel, 3 workers)
6. Aggregate by `(activeIngredient, activityGroup)` — merges commercial names
7. Filter to only selected pests, return DataFrame with 7 Hebrew columns

**Key design decisions:**
- Direct HTTP `requests` to OData APIs — no browser automation (Playwright was abandoned due to instability)
- Session warmup hits the main page first to pick up cookies/anti-bot tokens
- All API endpoints use POST with `Content-Type: text/plain` (except GetTachshirInfoById which uses `application/json` with a bare integer body)
- All dict accesses on API responses must use `.get()` — MoAG returns inconsistent keys

## MoAG API Endpoints

Base: `https://pesticides.moag.gov.il`

| Endpoint | Body Format | Returns |
|----------|-------------|---------|
| `POST /api/gidulim/$query` | OData `$filter=contains(teur,'...')` | `seqGidul` (crop ID) |
| `POST /api/negaim/$query` | `seqGidul=X&$select=...` | `rownumID` (pest ID) |
| `POST /api/SearchTachshirim/$query` | OData with `$apply`, `$skip`, `$top` | `prodNum` list |
| `POST /api/Tachshirim/GetTachshirInfoById` | bare integer (`1447`) | full product detail JSON |

## Output Columns (Hebrew, RTL)

`פגע`, `קבוצת פעילות`, `שם גנרי`, `חומרים`, `מינון`, `ימי המתנה`, `הערות`

## Color-Coding (app.py)

Rows are colored by **category** derived from user's selected pests (not raw pest text). Categories: individual pests, combinations ("A + B"), or "אחר". The same Styler is used for both `st.dataframe()` display and `.to_excel()` export.

## Constraints

- No database — all data fetched live per search
- `lists.py` is static; won't reflect new crops/pests added to MoAG
- PDF parsing fails on scanned/image-based PDFs (pdfplumber limitation)
- Large crop-only searches (300+ products) are slow due to PDF enrichment
