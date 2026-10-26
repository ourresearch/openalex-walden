"""Publication-year resolver (oxjob #1644): reads every harvested record of a work with no year and asks Jev which year
is the item's own, among the year-like numbers the records contain, or "unknown". The prompt text below is the one
measured on the 499-item audit gold and a fresh 300-item audit draw (oxjobs #1644 EXPLORE § 12-14); changing a word of
it means re-measuring and a new PROMPT_VERSION. Accepted = year with confidence >= ACCEPT_CONF and its (source, year)
cell not on the denylist. Charter lines it encodes: plans/xpac-integration.md, What "right" means: publication year.
Ported from oxjobs #1644 scratch/records.py + scratch/resolver_jev.py (the evaluated copies).
"""
import html, json, re

# DataCite / JSON keys that never carry the item's date or identity
DROP = {"rightsList", "rights", "sizes", "formats", "schemaVersion", "version", "fundingReferences", "geoLocations",
        "contentUrl", "url", "state", "isActive", "viewCount", "downloadCount", "referenceCount", "citationCount",
        "partCount", "partOfCount", "versionCount", "versionOfCount", "viewsOverTime", "downloadsOverTime",
        "citationsOverTime", "metadataVersion", "source", "prefix", "suffix", "xml", "agency", "reason", "landingPage",
        "container", "affiliation", "nameIdentifiers", "affiliationIdentifier", "schemeUri", "nameIdentifierScheme",
        "affiliationIdentifierScheme", "rightsUri", "rightsIdentifier", "rightsIdentifierScheme", "lang", "nameType",
        "clientId", "providerId"}
YEAR = re.compile(r"(?<![\d])(1[0-9]{3}|20[0-2][0-9])(?![\d])")


def _flat(o, prefix="", out=None):
    if out is None:
        out = []
    if isinstance(o, dict):
        for k, v in o.items():
            kk = k.split(":")[-1]
            if k in DROP or kk in DROP:
                continue
            _flat(v, (prefix + "." if prefix else "") + kk, out)
    elif isinstance(o, list):
        for v in o:
            _flat(v, prefix, out)
    elif o not in (None, "", []):
        s = str(o).strip()
        if s:
            out.append((prefix, s))
    return out


def compact(rec, max_value=600):
    rec = (rec or "").strip()
    if rec.startswith('"'):
        try:
            rec = json.loads(rec)
        except Exception:
            pass
    if isinstance(rec, str) and rec.lstrip().startswith("<"):
        pairs = []
        ds = re.search(r"<datestamp>(.*?)</datestamp>", rec)
        if ds:
            pairs.append(("oai_datestamp", ds.group(1)))
        for tag, val in re.findall(r"<(?:dc|dcterms):(\w+)[^>]*>(.*?)</(?:dc|dcterms):\1>", rec, flags=re.S):
            v = html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", val))).strip()
            if v and tag != "rights":
                pairs.append((tag, v))
    else:
        try:
            o = json.loads(rec) if isinstance(rec, str) else rec
        except Exception:
            return rec[:4000]
        pairs = _flat(o)
    lines, seen = [], set()
    for k, v in pairs:
        k = re.sub(r"^(dc|metadata|dc\.dc)\.", "", k)
        v = re.sub(r"\s+", " ", v)
        if (k, v) in seen:
            continue
        seen.add((k, v))
        lines.append(f"{k}: {v[:max_value]}")
    return "\n".join(lines)


def work_text(w, recs, max_records=8, max_chars=12000):
    """The text a reader sees for one work: OpenAlex title and type, then each record compacted."""
    rs = sorted(recs, key=lambda r: (r["prov"] != "repo", r["nid"]))[:max_records]
    parts = [f"OpenAlex title: {w.get('title')}\nOpenAlex type: {w.get('type')}\nRecords: {len(rs)}"]
    for i, r in enumerate(rs, 1):
        parts.append(f"--- record {i} ({r['prov']}, {r.get('sname') or 'unknown source'}) ---\n{compact(r['rec'])}")
    return "\n\n".join(parts)[:max_chars]


def candidates(text, max_n=12):
    """Distinct year-like numbers in the text with up to two short contexts each (field name + the value around it)."""
    out = {}
    for line in text.split("\n"):
        field = line.split(":", 1)[0][:40] if ":" in line else ""
        for m in YEAR.finditer(line):
            y = m.group(1)
            ctx = line[max(0, m.start() - 50):m.end() + 30].strip()
            out.setdefault(y, [])
            if len(out[y]) < 2:
                out[y].append(f"{field}: ...{ctx}..." if field and not ctx.startswith(field) else ctx)
    # keep the years seen in date-like fields first, then the rest, capped
    order = sorted(out, key=lambda y: (not any(re.search(r"date|year|issued|published|imprint|created", c, re.I)
                                                for c in out[y]), y))
    return {y: out[y] for y in order[:max_n]}


PROMPT_VERSION = "jev-v1"
YEAR_Q = ("In which year was the item ITSELF published, printed, issued or made? For a digitised item that is the "
          "original's year, never the year of the scan, the upload or the catalogue record (OAI datestamps, upload "
          "timestamps, 'created'/'registered' dates of a repository deposit). For a facsimile, reprint or later "
          "edition it is the reproduction's own year. A deposit or upload date on an article whose citation in the "
          "record gives an earlier year is not its year. Pick 'unknown' unless one year is clearly the item's own.")
UNKNOWN = ("No single year is clearly the item's own: the date is questioned or approximate ('[1872?]', 'ca. 1850'), "
           "only a span, multi-volume set or serial run ('1862-69', '1979-'), a reproduction whose own date is not "
           "given, only digitisation, upload or record dates, or the records disagree.")
HOW = {"stated": "The record states this year for this item and nothing in the record contradicts it.",
       "inferred": "A cataloguer's inference ('[1874]'), a copyright year, a dateline in the title, or a choice "
                   "between records that the record itself supports.",
       "uncertain": "A best guess: questioned or approximate, one end of a span, or conflicting records or fields.",
       "none": "The record gives no usable year for the item."}
HOW_Q = "How sure does the record itself make the item's year?"


ACCEPT_CONF = 0.95
JEV_MODEL = "jev-1.13.0"


def request(work: dict, recs: list) -> tuple | None:
    """(state, questions, n_candidates) for one work, or None when its records hold no year-like number."""
    text = work_text(work, recs)
    cands = candidates(text)
    if not cands:
        return None
    crit = {y: " | ".join(ctx)[:240] for y, ctx in cands.items()}
    crit["unknown"] = UNKNOWN
    return text, {"year": {"type": "choice", "instructions": YEAR_Q, "criteria": crit},
                  "how": {"type": "choice", "instructions": HOW_Q, "criteria": HOW}}, len(cands)


def parse(answers: dict) -> dict:
    ya, ha = answers.get("year") or {}, answers.get("how") or {}
    pick = ya.get("choice")
    return {"year": int(pick) if pick and str(pick).isdigit() else None, "conf": ya.get("confidence"),
            "certainty": ha.get("choice"), "how_conf": ha.get("confidence")}
