#!/usr/bin/env python3
"""Optional, opt-in extensions for the CN provincial framework.

These are additive helpers the W2-A scale-up needed that the pilot's common.py
did not have. Importing this module has NO side effects unless you call the
`enable_*` functions -- so it never perturbs the pilot provinces or the sibling
agent's configs. Kept in a separate file (not common.py) so parallel appends
merge cleanly.

Two extensions:

1. enable_legacy_doc_parsing()
   Registers a legacy binary `.doc` (OLE / Word 97-2003) table parser into
   common.PARSERS and teaches common.parse_attachment to route OLE-Word files
   to it. Several provinces (Hunan, Hebei) publish rosters as `.doc`, which
   common.py explicitly does not handle (its OLE branch only tries xlrd/BIFF).
   Implementation: Word COM automation on Windows (Word is installed on the
   harvest box). It converts each .doc to a temp .docx in-place and reuses
   common.parse_docx. No-op / clear error off Windows -- but harvesting is a
   local-Windows step anyway (the parquet, not the parser, goes to Databricks).

2. WaybackAttachmentMixin / resolve_via_wayback()
   Some portals (Guangdong gdstc) DELETE a notice's roster PDF from the live
   server once its publicity window (公示期) closes, leaving only the filename
   as plain text with no href. The file is still recoverable from the Internet
   Archive. `resolve_via_wayback()` reconstructs candidate attachment URLs from
   the article + queries the Wayback CDX API for an archived copy.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any, Optional

from . import common


# ---------------------------------------------------------------------------
# 1. Legacy .doc (Word 97-2003 / OLE) parsing via Word COM
# ---------------------------------------------------------------------------

def parse_doc_wordcom(path: Path) -> list[list[list[Any]]]:
    """Extract tables from a legacy binary .doc using Word COM (Windows).

    Returns the same shape as common.parse_docx: list[table] where each table
    is list[row] of list[cell]. Falls back to [] with a warning if Word COM
    is unavailable (non-Windows / no Word / pywin32 missing).
    """
    if sys.platform != "win32":
        print(f"    [WARN] legacy .doc needs Windows+Word COM; skipping {path.name}")
        return []
    try:
        import win32com.client as win32  # type: ignore
        import pythoncom  # type: ignore
    except ImportError:
        print(f"    [WARN] pywin32 not installed; cannot parse legacy .doc {path.name}")
        return []
    pythoncom.CoInitialize()
    word = None
    doc = None
    out_docx = path.with_suffix(".converted.docx")
    try:
        word = win32.DispatchEx("Word.Application")
        word.Visible = False
        word.DisplayAlerts = False
        doc = word.Documents.Open(str(path.resolve()), ReadOnly=True,
                                  ConfirmConversions=False, AddToRecentFiles=False)
        # wdFormatXMLDocument = 12
        doc.SaveAs2(str(out_docx.resolve()), FileFormat=12)
        doc.Close(False)
        doc = None
    except Exception as exc:
        print(f"    [WARN] Word COM failed for {path.name}: {exc}")
        try:
            if doc is not None:
                doc.Close(False)
        except Exception:
            pass
        out_docx = None
    finally:
        try:
            if word is not None:
                word.Quit()
        except Exception:
            pass
        pythoncom.CoUninitialize()
    if not out_docx or not out_docx.exists():
        return []
    try:
        return common.parse_docx(out_docx)
    finally:
        out_docx.unlink(missing_ok=True)


_LEGACY_DOC_ENABLED = False


def enable_legacy_doc_parsing() -> None:
    """Route OLE-Word .doc attachments to parse_doc_wordcom.

    Idempotent. Wraps common.parse_attachment so an OLE file that is a Word
    doc (not a BIFF .xls) is parsed via Word COM instead of returning [].
    """
    global _LEGACY_DOC_ENABLED
    if _LEGACY_DOC_ENABLED:
        return
    common.PARSERS["doc"] = parse_doc_wordcom
    _orig_parse_attachment = common.parse_attachment

    def _parse_attachment(path: Path) -> list[list[list[Any]]]:
        kind = common._sniff(path)
        if kind == "ole":
            # Try BIFF .xls first (cheap); if it yields nothing, try Word .doc.
            try:
                tables = common.parse_xls_biff(path)
                if tables:
                    return tables
            except Exception:
                pass
            return parse_doc_wordcom(path)
        return _orig_parse_attachment(path)

    common.parse_attachment = _parse_attachment
    _LEGACY_DOC_ENABLED = True
    print("[extras] legacy .doc parsing enabled (Word COM)")


# ---------------------------------------------------------------------------
# 2. Wayback recovery for publicity-expired roster attachments
# ---------------------------------------------------------------------------

WB_UA = {"User-Agent": common.UA}


def resolve_relative_to_article(article_html: str, article_url: str, base_url: str) -> list[tuple[str, str]]:
    """Attachment resolver that joins hrefs against the ARTICLE url, not base_url.

    common.process_article joins a non-http attachment href against cfg.base_url,
    which is wrong for portals (e.g. Hunan kjt.hunan.gov.cn) whose roster files
    live at an ARTICLE-relative path like `33585991/files/<hash>.doc`. Joining
    that against the site root yields a 502 stub. This resolver joins against the
    article url so `.../202502/t20250212_33585991.html` + `33585991/files/x.doc`
    -> `.../202502/33585991/files/x.doc`.
    """
    from urllib.parse import urljoin
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    body = re.sub(r"<script.*?</script>", "", article_html, flags=re.S)
    pat = re.compile(r"\.(xlsx?|pdf|docx?|et|wps)(\?|$)|/files/", re.I)
    for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', body, re.S):
        href = m.group(1).strip()
        text = re.sub(r"<[^>]+>", "", m.group(2)).strip()
        if not pat.search(href):
            continue
        full = href if href.startswith("http") else urljoin(article_url, href)
        if full in seen:
            continue
        seen.add(full)
        out.append((full, text))
    return out


def _wayback_lookup(original_url: str, timeout: int = 30) -> Optional[str]:
    """Return a playable Wayback capture URL for original_url, or None."""
    cdx = ("http://web.archive.org/cdx/search/cdx?url="
           + urllib.request.quote(original_url, safe="")
           + "&output=json&filter=statuscode:200&limit=1")
    try:
        req = urllib.request.Request(cdx, headers=WB_UA)
        data = json.load(urllib.request.urlopen(req, timeout=timeout))
    except Exception as exc:
        print(f"    [WARN] wayback CDX failed for {original_url}: {exc}")
        return None
    if len(data) < 2:
        return None
    ts = data[1][1]
    # id_ suffix asks Wayback for the raw archived bytes (no rewrite banner).
    return f"http://web.archive.org/web/{ts}id_/{original_url}"


def resolve_via_wayback(article_html: str, article_url: str, base_url: str) -> list[tuple[str, str]]:
    """Find roster attachment URLs, live or via Wayback.

    Guangdong pattern: an active notice carries
    `<a class="nfw-cms-attachment" href=".../attachment/0/AAA/AAAAAA/POST.pdf">`.
    An expired notice keeps the filename as plain text and drops the href, but
    the file is on Wayback under the same attachment path. We (a) take any live
    attachment hrefs, and (b) for the article's own post id, reconstruct the
    attachment URL from an archived copy of the article and resolve it.
    Returns [(file_url, display_name)] where file_url may be a Wayback capture.
    """
    from urllib.parse import urljoin
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add(href: str, name: str):
        full = href if href.startswith("http") else urljoin(base_url, href)
        if full not in seen:
            seen.add(full)
            out.append((full, name))

    # (a) live hrefs
    for m in re.finditer(r'<a[^>]+href="([^"]+\.(?:pdf|xlsx?|docx?))"[^>]*>(.*?)</a>',
                         article_html, re.S | re.I):
        add(m.group(1), re.sub(r"<[^>]+>", "", m.group(2)).strip())

    if out:
        return out

    # (b) expired: recover from an archived copy of the ARTICLE, which still
    #     carries the attachment href, then Wayback-resolve each PDF.
    arch_article = _wayback_lookup(article_url)
    if not arch_article:
        return out
    try:
        req = urllib.request.Request(arch_article, headers=WB_UA)
        arch_html = urllib.request.urlopen(req, timeout=40).read().decode("utf-8", "replace")
    except Exception as exc:
        print(f"    [WARN] wayback article fetch failed: {exc}")
        return out
    for m in re.finditer(r'href="([^"]*attachment/[^"]+\.(?:pdf|xlsx?|docx?))"',
                         arch_html, re.I):
        raw = m.group(1)
        # Strip any Wayback prefix to get the original gdstc URL.
        om = re.search(r"(https?://[^/]*gdstc[^\"']+)", raw)
        original = om.group(1) if om else urljoin(base_url, raw)
        capture = _wayback_lookup(original)
        if capture:
            add(capture, Path(original).name)
        time.sleep(0.4)
    return out
