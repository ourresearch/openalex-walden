"""oxjob #695 — supervised DOM-marker mining for classifier v4.

For a host, take rows whose live outcome is known (pdf = free, html = not) and
compare token frequency between the two groups. A token that appears in most
winners and few losers is a candidate free-marker (and vice versa). No model —
this just surfaces candidates for a human to turn into a rule.
"""
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.databricks_sql import run_query

HOST = sys.argv[1] if len(sys.argv) > 1 else "link.springer.com"
LIMIT = int(sys.argv[2]) if len(sys.argv) > 2 else 60

rows = run_query(f"""
SELECT r.content_type, h.html
FROM openalex.parseland.pdf_candidate_classification c
JOIN openalex.taxicab.taxicab_results r ON r.url = c.pdf_url
JOIN openalex.landing_page.pdf_candidate_html h ON h.file_key = c.file_key
WHERE c.class = 'needs_validation' AND c.url_host = '{HOST}'
  AND r.content_type IN ('pdf','html') AND h.status = 'ok'
LIMIT {LIMIT * 4}
""", size="xlarge")

# class/id/data-* attribute tokens are what past rules were built from
TOKEN = re.compile(r'(?:class|id|data-[a-z-]+)=["\']([^"\']{3,60})["\']', re.I)
win, lose = Counter(), Counter()
nw = nl = 0
for r in rows:
    d = r.asDict() if hasattr(r, "asDict") else dict(r)
    html = d["html"] or ""
    toks = {t.strip().lower() for m in TOKEN.finditer(html) for t in m.group(1).split()}
    if d["content_type"] == "pdf":
        nw += 1
        win.update(toks)
    else:
        nl += 1
        lose.update(toks)

print(f"{HOST}: {nw} winners / {nl} losers")
if not nw or not nl:
    print("  need both classes to diff"); raise SystemExit

cands = []
for tok in set(win) | set(lose):
    wf, lf = win[tok] / nw, lose[tok] / nl
    if max(wf, lf) < 0.30:
        continue
    cands.append((wf - lf, tok, wf, lf))
cands.sort(key=lambda x: -abs(x[0]))
print(f"\n  {'token':<48} {'win%':>6} {'lose%':>6}  signal")
for sep, tok, wf, lf in cands[:18]:
    tag = "FREE-marker" if sep > 0 else "PAYWALL-marker"
    if abs(sep) < 0.35:
        continue
    print(f"  {tok[:46]:<48} {100*wf:>5.0f}% {100*lf:>5.0f}%  {tag}")
