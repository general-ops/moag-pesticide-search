"""MoAG Pesticide Database agent — direct API version.

Calls the MoAG OData APIs directly via HTTP (no browser automation).

API endpoints (all POST, discovered via network sniffing):
  1. gidulim/$query   — look up crop seqGidul by Hebrew name
  2. negaim/$query    — look up pest rownumID (optionally filtered by crop)
  3. SearchTachshirim/$query — search products (returns prodNum list)
  4. Tachshirim/GetTachshirInfoById — full product detail (ingredients,
     dosage, PHI, notes, activity groups)
"""

import io
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

# Force stdout/stderr to UTF-8 on Windows
if sys.platform == "win32":
    for _stream_name in ("stdout", "stderr"):
        try:
            _stream = getattr(sys, _stream_name)
            if _stream and hasattr(_stream, "buffer"):
                _stream.buffer.fileno()  # raises if closed
                setattr(sys, _stream_name, io.TextIOWrapper(
                    _stream.buffer, encoding="utf-8", errors="replace",
                    line_buffering=True,
                ))
        except Exception:
            pass

import pandas as pd
import requests

from pdf_parser import extract_pdf_data

# ── PRD Column names (Hebrew) ─────────────────────────────────────
COL_PEST = "פגע"
COL_ACTIVITY_GROUP = "קבוצת פעילות"
COL_GENERIC_NAME = "שם גנרי"
COL_COMMERCIAL_NAMES = "חומרים"
COL_DOSAGE = "מינון"
COL_PHI = "ימי המתנה"
COL_NOTES = "הערות"

COLUMNS = [
    COL_PEST,
    COL_ACTIVITY_GROUP,
    COL_GENERIC_NAME,
    COL_COMMERCIAL_NAMES,
    COL_DOSAGE,
    COL_PHI,
    COL_NOTES,
]

MOAG_BASE = "https://pesticides.moag.gov.il"
DEBUG_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_PATH = os.path.join(DEBUG_DIR, "agent_debug.log")
_log_file = None


def _log(msg: str):
    global _log_file
    if _log_file is None or _log_file.closed:
        _log_file = open(_LOG_PATH, "w", encoding="utf-8")
    _log_file.write(f"[AGENT] {msg}\n")
    _log_file.flush()
    try:
        print(f"[AGENT] {msg}", flush=True)
    except Exception:
        pass


# ── HTTP session ──────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "he-IL",
        "Referer": f"{MOAG_BASE}/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    })
    return s


# ── Data classes ──────────────────────────────────────────────────

@dataclass
class GidulEntry:
    crop: str = ""
    pest: str = ""
    dosage: str = ""
    phi: str = ""
    notes: str = ""


@dataclass
class ProductDetail:
    prod_num: int = 0
    prod_name: str = ""
    active_ingredients: list[str] = field(default_factory=list)
    activity_groups: list[str] = field(default_factory=list)
    pdf_url: str = ""
    gidulim: list[GidulEntry] = field(default_factory=list)


# ── API calls ─────────────────────────────────────────────────────

def _lookup_crop(session: requests.Session, crop_name: str) -> int:
    """Look up seqGidul for a crop by Hebrew name. Returns 0 if not found."""
    body = (
        f"$select=teur,teure,seqgidul"
        f"&$filter=contains(teur, '{crop_name}') or contains(teurE, '{crop_name}')"
        f"&$orderby=teur"
    )
    r = session.post(
        f"{MOAG_BASE}/api/gidulim/$query",
        data=body.encode("utf-8"),
        headers={"Content-Type": "text/plain"},
        timeout=30,
    )
    r.raise_for_status()
    values = r.json().get("value", [])
    _log(f"Crop lookup '{crop_name}': {len(values)} matches")
    if not values:
        return 0
    # Prefer exact match
    for v in values:
        if v.get("teur", "").strip() == crop_name:
            _log(f"  Exact match: seqGidul={v['seqGidul']}")
            return v["seqGidul"]
    seq = values[0]["seqGidul"]
    _log(f"  Using first match: seqGidul={seq} ({values[0].get('teur')})")
    return seq


def _lookup_pest(session: requests.Session, pest_name: str,
                 seq_gidul: int = 0) -> int:
    """Look up rownumID for a pest by Hebrew name. Returns 0 if not found."""
    body = (
        f"seqGidul={seq_gidul}"
        f"&$select=esevpegaheb,esevpegakveng,esevpegaeng,esevpegakvkod,rownumID"
        f"&$orderby=esevPegaHeb"
    )
    r = session.post(
        f"{MOAG_BASE}/api/negaim/$query",
        data=body.encode("utf-8"),
        headers={"Content-Type": "text/plain"},
        timeout=30,
    )
    r.raise_for_status()
    values = r.json().get("value", [])
    _log(f"Pest lookup '{pest_name}' (seqGidul={seq_gidul}): {len(values)} items")
    for v in values:
        if v.get("esevPegaHeb", "").strip() == pest_name:
            rid = v["rownumID"]
            _log(f"  Exact match: rownumID={rid}")
            return rid
    # Partial match
    for v in values:
        heb = v.get("esevPegaHeb", "")
        if pest_name in heb or heb in pest_name:
            rid = v["rownumID"]
            _log(f"  Partial match: rownumID={rid} ({heb})")
            return rid
    _log(f"  No match for '{pest_name}'")
    return 0


def _search_products(session: requests.Session, seq_gidul: int,
                     rownum_id: int) -> list[dict]:
    """Search for products. Returns the full list from all pages."""
    # Build OData filter
    filters = []
    if seq_gidul:
        filters.append(f"seqgidul eq {seq_gidul}")

    apply_clause = "groupby((prodNameEng,prodNum,prodName,toaritheb," \
                   "toaritEng,rishayonHeb,rishayonEng,mspRishayon," \
                   "dargatReilut,DargatReilutEng,mspUn,ktovetTavit," \
                   "tikProdNum,sugTachshir,reentry))"
    if filters:
        filter_str = " and ".join(filters)
        apply_clause = f"filter( {filter_str})/{apply_clause}"

    all_items: list[dict] = []
    page_size = 100
    skip = 0
    server_total = None

    while True:
        body = (
            f"seqGidul={seq_gidul}"
            f"&RowNumID={rownum_id}"
            f"&$count=true"
            f"&$apply={apply_clause}"
            f"&$skip={skip}&$top={page_size}"
            f"&$orderby=prodName"
        )
        r = session.post(
            f"{MOAG_BASE}/api/SearchTachshirim/$query",
            data=body.encode("utf-8"),
            headers={"Content-Type": "text/plain"},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        items = data.get("value", [])
        count = data.get("count", 0)
        if server_total is None:
            server_total = count
            _log(f"Search: server reports {server_total} total products")

        all_items.extend(items)
        _log(f"  Page at skip={skip}: got {len(items)} "
             f"(total collected: {len(all_items)}/{server_total})")

        if len(items) < page_size or len(all_items) >= server_total:
            break
        skip += page_size

    _log(f"Search complete: {len(all_items)} products")
    return all_items


def _fetch_product_detail(session: requests.Session,
                          prod_num: int) -> dict | None:
    """Fetch full detail for a single product."""
    r = session.post(
        f"{MOAG_BASE}/api/Tachshirim/GetTachshirInfoById",
        data=str(prod_num),
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if r.status_code != 200:
        _log(f"  Detail {prod_num}: HTTP {r.status_code}")
        return None
    return r.json()


# ── Parsing ───────────────────────────────────────────────────────

def _parse_product_detail(data: dict, crop_filter: str = "") -> ProductDetail:
    detail = ProductDetail(
        prod_num=data.get("prodNum", 0),
        prod_name=data.get("prodName", "") or data.get("prodNameEng", ""),
        pdf_url=data.get("ktovetTavit", ""),
    )

    for hp in data.get("homerPailim", []):
        name = hp.get("shemGeneriE", "")
        if name and name not in detail.active_ingredients:
            detail.active_ingredients.append(name)

    for ms in data.get("matarotShimushim", []):
        group = ms.get("teur", "") or ms.get("mShimushHeb", "")
        if group and group not in detail.activity_groups:
            detail.activity_groups.append(group)

    for g in data.get("gidulim", []):
        entry = GidulEntry(
            crop=g.get("gidulHeb", "") or g.get("gidulEng", ""),
            pest=g.get("pegaHeb", "") or g.get("pegaEng", ""),
            dosage=str(g.get("minun", "") or "").strip(),
            phi=str(g.get("tkufatHamtana", "") or "").strip(),
            notes=str(g.get("gidulHeara", "") or "").strip(),
        )
        if crop_filter and entry.crop and crop_filter not in entry.crop:
            continue
        detail.gidulim.append(entry)

    return detail


def _enrich_from_pdf(detail: ProductDetail):
    """If gidulim didn't provide dosage/PHI/notes, try the PDF."""
    has_dosage = any(g.dosage for g in detail.gidulim)
    has_phi = any(g.phi for g in detail.gidulim)
    if has_dosage and has_phi:
        return

    pdf_url = detail.pdf_url
    if not pdf_url:
        return
    if not pdf_url.startswith("http"):
        pdf_url = f"{MOAG_BASE}/{pdf_url.lstrip('/')}"

    _log(f"  PDF for '{detail.prod_name}': {pdf_url}")
    pdf_data = extract_pdf_data(pdf_url)
    _log(f"  PDF result: dosage='{pdf_data.dosage}' "
         f"phi='{pdf_data.phi}' notes='{pdf_data.notes}'")

    for g in detail.gidulim:
        if not g.dosage and pdf_data.dosage:
            g.dosage = pdf_data.dosage
        if not g.phi and pdf_data.phi:
            g.phi = pdf_data.phi
        if not g.notes and pdf_data.notes:
            g.notes = pdf_data.notes

    if not detail.gidulim and (pdf_data.dosage or pdf_data.phi or pdf_data.notes):
        detail.gidulim.append(GidulEntry(
            dosage=pdf_data.dosage, phi=pdf_data.phi, notes=pdf_data.notes))


# ── Aggregation ───────────────────────────────────────────────────

def _aggregate(details: list[ProductDetail]) -> pd.DataFrame:
    if not details:
        return pd.DataFrame(columns=COLUMNS)

    groups: dict[tuple[str, str], dict] = {}

    for d in details:
        best_dosage = ""
        best_phi = ""
        best_notes = ""
        pest_names: list[str] = []
        for g in d.gidulim:
            if g.dosage and not best_dosage:
                best_dosage = g.dosage
            if g.phi and not best_phi:
                best_phi = g.phi
            if g.notes and not best_notes:
                best_notes = g.notes
            if g.pest and g.pest not in pest_names:
                pest_names.append(g.pest)

        ingredients = d.active_ingredients or [""]
        act_groups = d.activity_groups or [""]

        for ingredient in ingredients:
            for group in act_groups:
                key = (ingredient, group)
                if key not in groups:
                    groups[key] = {
                        "commercial_names": [],
                        "pests": [],
                        "dosage": "",
                        "phi": "",
                        "notes": "",
                    }
                g = groups[key]
                if d.prod_name and d.prod_name not in g["commercial_names"]:
                    g["commercial_names"].append(d.prod_name)
                for p in pest_names:
                    if p not in g["pests"]:
                        g["pests"].append(p)
                if best_dosage and not g["dosage"]:
                    g["dosage"] = best_dosage
                if best_phi and not g["phi"]:
                    g["phi"] = best_phi
                if best_notes and not g["notes"]:
                    g["notes"] = best_notes

    rows = []
    for (ingredient, group), g in groups.items():
        rows.append({
            COL_PEST: ", ".join(g["pests"]) if g["pests"] else "",
            COL_ACTIVITY_GROUP: group or "",
            COL_GENERIC_NAME: ingredient or "",
            COL_COMMERCIAL_NAMES: ", ".join(g["commercial_names"]),
            COL_DOSAGE: g["dosage"],
            COL_PHI: g["phi"],
            COL_NOTES: g["notes"],
        })

    return pd.DataFrame(rows, columns=COLUMNS)


# ── Main entry point ──────────────────────────────────────────────

def _fetch_and_parse(session: requests.Session, seq_gidul: int,
                     rownum_id: int, crop: str) -> list[ProductDetail]:
    """Search products for one (crop, pest) combo and return parsed details."""
    search_results = _search_products(session, seq_gidul, rownum_id)
    if not search_results:
        _log("No products found for this combo")
        return []

    prod_nums = [item["prodNum"] for item in search_results if "prodNum" in item]
    _log(f"Fetching details for {len(prod_nums)} products...")

    detail_data: dict[int, dict] = {}
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            pool.submit(_fetch_product_detail, session, pn): pn
            for pn in prod_nums
        }
        for future in as_completed(futures):
            pn = futures[future]
            try:
                body = future.result()
                if body and body.get("prodNum"):
                    detail_data[body["prodNum"]] = body
                else:
                    errors.append(f"prodNum={pn}: empty response")
            except Exception as e:
                errors.append(f"prodNum={pn}: {e}")

    _log(f"Got details for {len(detail_data)}/{len(prod_nums)} products")
    if errors:
        _log(f"  {len(errors)} errors: {errors[:5]}")

    if not detail_data:
        return []

    product_details = []
    for pn, data in detail_data.items():
        detail = _parse_product_detail(data, crop_filter=crop)
        product_details.append(detail)

    # PDF enrichment
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(
            lambda d: _enrich_from_pdf(d),
            [d for d in product_details
             if not any(g.dosage for g in d.gidulim)
             or not any(g.phi for g in d.gidulim)],
        ))

    return product_details


def search_pesticides(crop: str, pests: list[str] | None = None,
                      progress_cb=None) -> pd.DataFrame:
    """Search the MoAG pesticide database.

    Args:
        crop: Hebrew crop name (required unless pests is provided).
        pests: List of Hebrew pest names. Empty list or None = all pests.
        progress_cb: Optional callback(percent: int, message: str) for progress.
    """
    def _progress(pct: int, msg: str):
        if progress_cb:
            progress_cb(pct, msg)

    if pests is None:
        pests = []
    session = _make_session()

    # Step 1: Resolve crop name → seqGidul
    _progress(0, "🚜 מניע את הטרקטור...")
    seq_gidul = 0
    if crop:
        seq_gidul = _lookup_crop(session, crop)
        if not seq_gidul:
            raise RuntimeError(f"הגידול '{crop}' לא נמצא במאגר")

    # Step 2: Build list of (seq_gidul, rownum_id) combos to search
    _progress(10, "🌾 חורש את שדות הנתונים...")
    if not pests:
        _log("No pests specified, searching all pests for this crop")
        combos = [(seq_gidul, 0)]
    else:
        combos = []
        for pest_name in pests:
            rownum_id = _lookup_pest(session, pest_name, seq_gidul)
            if not rownum_id:
                _log(f"WARNING: pest '{pest_name}' not found, skipping")
                continue
            combos.append((seq_gidul, rownum_id))
        if not combos:
            raise RuntimeError(
                f"אף אחד מהפגעים שנבחרו לא נמצא במאגר: {', '.join(pests)}")

    # Step 3: Fetch & parse for each combo, deduplicate by prodNum
    all_details: dict[int, ProductDetail] = {}
    for i, (sg, rid) in enumerate(combos):
        # Scale progress 25–70% across combos
        combo_pct = 25 + int(45 * i / max(len(combos), 1))
        _progress(combo_pct, "🔍 סורק את מאגרי משרד החקלאות...")
        _log(f"Searching: seqGidul={sg}, rownumID={rid}")
        details = _fetch_and_parse(session, sg, rid, crop)
        for d in details:
            if d.prod_num not in all_details:
                all_details[d.prod_num] = d

    _log(f"Total unique products: {len(all_details)}")

    if not all_details:
        _progress(100, "✅ החיפוש הסתיים")
        return pd.DataFrame(columns=COLUMNS)

    # Step 4: Aggregate
    _progress(75, "🧪 מנתח ומסנן את החומרים הפעילים...")
    df = _aggregate(list(all_details.values()))
    _log(f"Aggregated: {len(df)} rows")

    # Step 5: Filter by selected pests (if user picked specific ones)
    if pests and not df.empty:
        pest_set = set(pests)

        def _matches_selected(pest_cell: str) -> bool:
            for p in pest_cell.split(", "):
                if p.strip() in pest_set:
                    return True
            return False

        mask = df[COL_PEST].apply(_matches_selected)
        df = df[mask].reset_index(drop=True)
        _log(f"After pest filter: {len(df)} rows (filtered to: {pests})")

    _progress(90, "📦 אורז את התוצאות לטבלה יפה...")
    _log(f"Final result: {len(df)} rows")
    _progress(100, "✅ החיפוש הסתיים!")
    return df
