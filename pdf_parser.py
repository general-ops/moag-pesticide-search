"""PDF label parser for MoAG pesticide labels.

Downloads PDF from URL and extracts dosage, PHI (pre-harvest interval),
and notes using pdfplumber. Includes verbose logging for debugging.
"""

import io
import os
import re
import sys
from dataclasses import dataclass

import pdfplumber
import requests

# Log file for PDF parsing debug output
_PDF_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pdf_debug.log")
_pdf_log = open(_PDF_LOG_PATH, "w", encoding="utf-8")


def _log(msg: str):
    _pdf_log.write(f"[PDF] {msg}\n")
    _pdf_log.flush()
    try:
        print(f"[PDF] {msg}", flush=True)
    except Exception:
        pass


@dataclass
class PdfExtractedData:
    dosage: str = ""       # מינון
    phi: str = ""          # ימי המתנה (Pre-Harvest Interval)
    notes: str = ""        # הערות
    raw_text: str = ""     # full extracted text for debugging


# Patterns to find dosage info
_DOSAGE_PATTERNS = [
    re.compile(r"מינון[:\s\-–]*(.+?)(?:\n|$)", re.MULTILINE),
    re.compile(r"ריכוז[:\s\-–]*(.+?)(?:\n|$)", re.MULTILINE),
    re.compile(r"כמות[:\s\-–]*(.+?)(?:\n|$)", re.MULTILINE),
    re.compile(r"(\d+[\.,]?\d*\s*(?:מ\"?ל|גרם|ק\"?ג|ליטר|סמ\"?ק|אחוז|%)\s*/?\s*(?:דונם|הקטר|ל(?:יטר)?|טון)?)", re.MULTILINE),
]

# Patterns to find PHI (pre-harvest interval)
_PHI_PATTERNS = [
    re.compile(r"(?:ימי|תקופת)\s*המתנה[:\s\-–]*(.+?)(?:\n|$)", re.MULTILINE),
    re.compile(r"המתנה[:\s\-–]*(\d+\s*(?:ימים|יום))", re.MULTILINE),
    re.compile(r"(\d+)\s*(?:ימים|יום)\s*(?:לפני|המתנה)", re.MULTILINE),
]

# Patterns to find notes/restrictions
_NOTES_PATTERNS = [
    re.compile(r"הערות[:\s\-–]*(.+?)(?:\n\n|$)", re.MULTILINE | re.DOTALL),
    re.compile(r"הגבלות[:\s\-–]*(.+?)(?:\n\n|$)", re.MULTILINE | re.DOTALL),
    re.compile(r"אזהרות[:\s\-–]*(.+?)(?:\n\n|$)", re.MULTILINE | re.DOTALL),
]


def _match_first(patterns: list[re.Pattern], text: str, field_name: str) -> str:
    """Return the first match from a list of regex patterns."""
    for pat in patterns:
        m = pat.search(text)
        if m:
            result = m.group(1).strip()
            if len(result) > 300:
                result = result[:300] + "..."
            _log(f"  {field_name}: MATCH with pattern '{pat.pattern[:50]}' -> '{result[:100]}'")
            return result
    _log(f"  {field_name}: NO MATCH (tried {len(patterns)} patterns)")
    return ""


def _extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract all text from a PDF using pdfplumber."""
    text_parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        _log(f"  PDF has {len(pdf.pages)} pages")
        for i, page in enumerate(pdf.pages):
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
                _log(f"  Page {i}: {len(page_text)} chars")
            else:
                _log(f"  Page {i}: no text extracted")
            # Also try extracting tables
            tables = page.extract_tables()
            for j, table in enumerate(tables):
                for row in table:
                    cells = [c for c in row if c]
                    if cells:
                        text_parts.append(" | ".join(cells))
                _log(f"  Page {i} table {j}: {len(table)} rows")
    full_text = "\n".join(text_parts)
    _log(f"  Total extracted text: {len(full_text)} chars")
    return full_text


def extract_pdf_data(pdf_url: str) -> PdfExtractedData:
    """Download a PDF from the given URL and extract pesticide label data.

    Never raises — returns empty fields on any error.
    """
    if not pdf_url:
        _log("extract_pdf_data called with empty URL")
        return PdfExtractedData()

    _log(f"Downloading PDF: {pdf_url}")

    try:
        resp = requests.get(pdf_url, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        resp.raise_for_status()
        _log(f"  Download OK: {len(resp.content)} bytes, content-type={resp.headers.get('content-type', '?')}")
    except Exception as e:
        _log(f"  Download FAILED: {type(e).__name__}: {e}")
        return PdfExtractedData()

    # Check if it's actually a PDF
    content_type = resp.headers.get("content-type", "")
    if "pdf" not in content_type.lower() and not resp.content[:5] == b"%PDF-":
        _log(f"  WARNING: Response is not PDF! content-type={content_type}, first bytes={resp.content[:20]}")
        return PdfExtractedData()

    try:
        raw_text = _extract_text_from_pdf(resp.content)
    except Exception as e:
        _log(f"  Text extraction FAILED: {type(e).__name__}: {e}")
        return PdfExtractedData()

    if not raw_text:
        _log("  WARNING: No text extracted from PDF (possibly scanned image)")
        return PdfExtractedData()

    # Log first 500 chars for debugging
    _log(f"  First 500 chars of text:\n{raw_text[:500]}")

    dosage = _match_first(_DOSAGE_PATTERNS, raw_text, "dosage")
    phi = _match_first(_PHI_PATTERNS, raw_text, "phi")
    notes = _match_first(_NOTES_PATTERNS, raw_text, "notes")

    _log(f"  Final: dosage='{dosage[:50]}' phi='{phi[:50]}' notes='{notes[:50]}'")

    return PdfExtractedData(
        dosage=dosage,
        phi=phi,
        notes=notes,
        raw_text=raw_text,
    )
