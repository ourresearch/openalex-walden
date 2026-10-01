#!/usr/bin/env python3
"""
Magnus Ehrnrooth Foundation (Magnus Ehrnroothin säätiö) to S3
==============================================================

Source: the foundation's own "Awarded grants" listings,
https://magnusehrnroothinsaatio.fi/en/grants/awarded-grants/, which are
WordPress posts of the custom type `myonnetyt_apurahat`: one post per
(grant year x field of science), published in Finnish, Swedish and English
with identical grant lists. They are read from the site's public REST API
(/wp-json/wp/v2/myonnetyt_apurahat), English versions only; the year comes
from the `vuosi` taxonomy and the field from `tieteenala`. Each grant is one
paragraph:

    <em>[degree] Name</em> <strong>AMOUNT</strong><br/>Project title

The field totals printed on the awarded-grants page (h3 per post) are used
to reconcile the parsed amounts.

Coverage: grant years 2024-2026 (the site's current listings). Older lists
(2010-2023) were blog posts and PDFs on the previous site and are no longer
served (the PDF links in the surviving 2019-2023 announcement posts 404).

Fields: Physics, Chemistry, Medicinal Chemistry, Mathematics, Astronomy, and
Education (school-teaching / science-education grants; kept under the batch
scope rule and flagged via `field`).

The listing prints the grantee's degree and name in one string; the name is
taken as the trailing run of name-like tokens (capitalised, no degree word,
no '.', '(' or ')'), then split with the canonical split_name.

funder_award_id: no grant number is published (citing works write internal
project numbers such as 'ma2014n1' / 'KE2012n8' that never appear in the
listings), so a synthetic 'MEF-<year>-<field>-<family>-<given>' slug is used.

Output: s3://openalex-ingest/awards/magnus_ehrnrooth/magnus_ehrnrooth_projects.parquet
"""

import argparse
import html
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (runbook 4.0 grep marker: sys.stdout.reconfigure)
import sys as _sys_utf8
try:
    _sys_utf8.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    _sys_utf8.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except (AttributeError, ValueError):
    pass

if _sys_utf8.platform == "win32":
    import builtins as _builtins_utf8
    import pathlib as _pathlib_utf8

    _orig_wt = _pathlib_utf8.Path.write_text
    def _wt(self, data, encoding=None, errors=None, newline=None):
        return _orig_wt(self, data, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.write_text = _wt

    _orig_rt = _pathlib_utf8.Path.read_text
    def _rt(self, encoding=None, errors=None, newline=None):
        return _orig_rt(self, encoding=encoding or "utf-8", errors=errors, newline=newline)
    _pathlib_utf8.Path.read_text = _rt

    _orig_open = _builtins_utf8.open
    def _open_utf8(file, mode="r", buffering=-1, encoding=None, errors=None, newline=None, closefd=True, opener=None):
        if "b" not in mode and encoding is None:
            encoding = "utf-8"
        return _orig_open(file, mode, buffering, encoding, errors, newline, closefd, opener)
    _builtins_utf8.open = _open_utf8
# --- end shim ---

SITE = "https://magnusehrnroothinsaatio.fi"
API = f"{SITE}/wp-json/wp/v2"
LISTING = f"{SITE}/en/grants/awarded-grants/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/magnus_ehrnrooth/magnus_ehrnrooth_projects.parquet"
HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 1.0

# Words that belong to a degree / position, never to a person's name.
DEGREE_WORDS = {
    "master", "masters", "bachelor", "bachelore", "bachelor's", "bachelore's", "doctor", "doctoral", "doctorate",
    "science", "sciences", "technology", "tech", "engineering", "engineer", "degree", "programme", "program",
    "medicine", "medical", "pharmacy", "geneticist", "clinical", "researcher", "student", "candidate",
    "licentiate", "licenciatura", "docent", "docentship", "professor", "assistant", "associate", "senior",
    "junior", "postdoctoral", "postdoc", "fellow", "lecturer", "university", "teacher", "physics", "chemistry",
    "mathematics", "in", "of", "and", "for", "the", "arts", "philosophy", "natural", "social", "life",
    "bioproduct", "phd", "msc", "bsc", "ma", "ba", "md", "dsc", "fd", "fm", "fl", "fk", "ft", "lkt", "lk", "ll",
    "lt", "di", "tkt", "tk", "tri", "dr", "prof", "doc", "dosentti", "professori", "maisteri", "tohtori",
    "kandidaatti", "lisensiaatti", "insinööri", "diplomi-insinööri", "filosofian", "tekniikan", "lääketieteen",
    "farmasian", "filosofie", "teknologie", "magister", "doktor", "diplomingenjör", "kandidat", "dosent",
    "tutkijatohtori", "väitöskirjatutkija", "yliopistonlehtori", "lehtori", "opettaja", "rehtori", "lukion",
    "fil", "mag", "dipl", "ing", "physicist", "chemist", "mathematician", "astronomer", "biochemist",
    "pharmacist", "physician", "specialist", "economics", "pharmaceutical", "biomedicine", "biology",
    "ytm", "kt", "mkd", "tkd", "lic", "dmsc", "ms", "bs", "meng", "dos", "maist", "sc", "tri", "yo",
    "emeritusprofessori", "projektledare", "proviisori", "prorektor", "väitellyt", "policies", "currently",
    "research", "cancer", "diploma", "graduate", "post", "biochemistry", "biotechnology",
}
DEGREE_STEM_RE = re.compile(
    r"(maist|tohtor|kandid|lisens|insin|profess|dosent|docent|doktor|doctor|master|bachelor|scien|siences|"
    r"technol|engineer|degree|program|student|diploma|graduate|candidate|emerit|proviisori|projekt|rektor|"
    r"researcher|fellow|lecturer|biochem|biotech|magister|policies)", re.I)
ORG_RE = re.compile(r"(\bry\b|\br\.y\.|\boy\b|\bab\b|seura\b|society|association|förening|förbund|liitto\b|säätiö|stiftelse|"
                    r"universit|yliopisto|koulu\b|lukio\b|school|institute|instituutti)", re.I)
PARTICLES = {"de", "da", "di", "del", "della", "van", "von", "der", "den", "af", "la", "le", "el", "du", "bin", "ben"}
# Family-first listings with no other row to settle the order (grantee string -> (given, family)).
NAME_ORDER_OVERRIDES = {
    "Fay Yvann": ("Yvann", "Le Fay"),               # 'M. Sc Le Fay Yvann'
    "Välimäki Jasmin": ("Jasmin", "Välimäki"),
    "Keitaanranta Atte": ("Atte", "Keitaanranta"),
    "Suominen Heikki": ("Heikki", "Suominen"),
    "Ding Changzeng": ("Changzeng", "Ding"),         # family name first in the listing
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str, params: dict | None = None) -> requests.Response:
    last = None
    for attempt in range(4):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=90)
            log(f"GET {r.url} -> {r.status_code} ({len(r.content)} bytes)")
            r.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return r
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed after retries: {last}")


def get_all(endpoint: str, fields: str | None = None) -> list[dict]:
    """WP REST collection; X-WP-TotalPages is the authoritative terminator."""
    out, page, pages = [], 1, None
    while pages is None or page <= pages:
        params = {"per_page": 100, "page": page}
        if fields:
            params["_fields"] = fields
        r = get(f"{API}/{endpoint}", params)
        pages = int(r.headers.get("X-WP-TotalPages", "1"))
        total = int(r.headers.get("X-WP-Total", "0"))
        out += r.json()
        page += 1
    if len(out) != total:
        raise SystemExit(f"{endpoint}: got {len(out)} of X-WP-Total {total}")
    return out


def split_name(name: str) -> tuple[str | None, str | None]:
    """Split 'James P. Eisenstein' -> ('James P.', 'Eisenstein').
    Verbatim port of wolf_to_s3.py (runbook 2.4.1).

    Strips trailing degree/suffix tokens (PhD, MD, Jr., Sr., II, III) before
    splitting. Last whitespace-separated token = family name; rest = given.
    """
    if not name:
        return None, None
    # Drop trailing degree/suffix tokens
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


def is_name_token(tok: str) -> bool:
    low = tok.lower().strip(",;:.")
    if low in PARTICLES:
        return True
    if re.fullmatch(r"[A-ZÅÄÖ]\.", tok):      # middle initial 'G.'
        return True
    if low in DEGREE_WORDS or DEGREE_STEM_RE.search(low):
        return False
    if any(part in DEGREE_WORDS for part in low.split("-")) and "-" in low:
        return False
    if re.search(r"[.()\d,/&’']", tok):        # 'Ph.D.', '(Tech)', 'Bachelore’s'
        return False
    return tok[:1].isupper()


def name_from_head(head: str) -> tuple[str, str]:
    """'Master of Science (Tech) Fereshteh Sohrabi' -> ('Master of Science (Tech)', 'Fereshteh Sohrabi')."""
    toks = head.split()
    k = len(toks)
    while k > 0 and is_name_token(toks[k - 1]) and len(toks) - k < 5:
        k -= 1
    while k < len(toks) and toks[k].lower() in PARTICLES:   # a name never starts with 'de'/'van' here
        k += 1
    return " ".join(toks[:k]).strip(" ,"), " ".join(toks[k:])


def clean(fragment: str) -> str:
    t = html.unescape(re.sub(r"<[^>]+>", "", fragment))
    return re.sub(r"\s+", " ", t.replace("\xa0", " ")).strip()


AMOUNT_RE = re.compile(r"^(?P<head>.*?)\s*(?P<amt>\d{1,3}(?:\.\d{3})+|\d{1,6})\s*€?\s*$")


def parse_post(post: dict) -> tuple[list[dict], list[str]]:
    grants, problems = [], []
    for para in re.findall(r"<p[^>]*>(.*?)</p>", post["content"]["rendered"], flags=re.S):
        # first line = degree + name + amount, the rest (after <br>) = project title
        parts = re.split(r"<br\s*/?>", para, maxsplit=1)
        first = clean(parts[0])
        title = clean(parts[1]) if len(parts) > 1 else ""
        if not first:
            continue
        m = AMOUNT_RE.match(first)
        if not m:
            problems.append(f"post {post['id']}: no amount in {first!r}")
            continue
        head = m.group("head")
        if ORG_RE.search(head):          # e.g. 'Suomen Tähtitieteilijäseura ry' (an association, no person)
            degree, name, gtype = None, None, "organisation"
        else:
            degree, name = name_from_head(head)
            gtype = "person"
            name_tokens = re.findall(r"\S+", name)
            if len(name_tokens) < 2:
                problems.append(f"post {post['id']}: could not isolate a name in {head!r}")
            # 'GHULAM YASIN', 'Yann LE GUEN': title-case all-caps name tokens
            name = " ".join(t.title() if t.isupper() and len(t) > 1 else t for t in name_tokens)
        grants.append({"head": head, "degree": degree or None, "grantee": name or None, "grantee_type": gtype,
                       "amount": float(m.group("amt").replace(".", "")), "title": title or None})
    return grants, problems


def slug(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-") or "x"


def main() -> None:
    p = argparse.ArgumentParser(description="Magnus Ehrnrooth Foundation awarded grants -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="parse only the first N English field-year posts (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp/magnus_ehrnrooth"))
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-mismatch", action="store_true", help="debug only: do not fail on total/translation mismatches")
    p.add_argument("--allow-shrink", action="store_true", help="override the 1.4 shrink guard")
    args = p.parse_args()

    years = {t["id"]: t["name"] for t in get_all("vuosi", "id,name")}
    fields = {t["id"]: t["name"] for t in get_all("tieteenala", "id,name")}
    posts = get_all("myonnetyt_apurahat")
    log(f"{len(posts)} field-year posts (all languages); years {sorted(set(years.values()))}")

    listing = get(LISTING).text
    totals = {int(m.group(1)): float(re.sub(r"[^\d]", "", m.group(2)))
              for m in re.finditer(r'class="[^"]*\bpost-(\d+) myonnetyt_apurahat[^"]*".*?<h3[^>]*>(.*?)</h3>', listing, flags=re.S)}
    log(f"listing page carries {len(totals)} field totals")

    # translation consistency: every (year, field-position) has the same grant count in fi/sv/en
    counts = {}
    for post in posts:
        lang = "en" if "/en/" in post["link"] else ("sv" if "/sv/" in post["link"] else "fi")
        y = years.get((post.get("vuosi") or [None])[0])
        counts.setdefault((y, lang), []).append(len(re.findall(r"<p[^>]*>", post["content"]["rendered"])))
    en_posts = [x for x in posts if "/en/" in x["link"]]
    en_posts.sort(key=lambda x: (years.get((x.get("vuosi") or [None])[0]) or "", x["id"]))
    if args.limit:
        en_posts = en_posts[: args.limit]

    rows, problems = [], []
    for post in en_posts:
        year = years.get((post.get("vuosi") or [None])[0])
        field = fields.get((post.get("tieteenala") or [None])[0]) or html.unescape(post["title"]["rendered"])
        got, probs = parse_post(post)
        problems += probs
        s = sum(g["amount"] for g in got)
        tot = totals.get(post["id"])
        recon = "no_total" if tot is None else ("ok" if abs(s - tot) < 0.5 else f"parsed {s:,.0f} vs printed {tot:,.0f}")
        if tot is not None and recon != "ok":
            problems.append(f"{year} {field} (post {post['id']}): {recon}")
        log(f"{year} {field:20s} {len(got):3d} grants  EUR {s:>12,.0f}  total check: {recon}")
        for g in got:
            given, family = split_name(g["grantee"] or "")
            if given and given.split()[-1].lower() in PARTICLES:     # 'Giulia de' / 'Meijere' -> 'Giulia' / 'de Meijere'
                *rest, particle = given.split()
                given, family = (" ".join(rest) or None), f"{particle} {family}"
            rows.append((post, year, field, recon, g, given, family))

    # A few listings print 'Family Given' (e.g. 'Eriksson-Rosenberg Ove' next to
    # 'Ove Eriksson-Rosenberg'). Flip a two-token name only when its mirror image
    # is listed elsewhere AND the other rows (mirror rows excluded) use its tokens
    # the other way round more often than this way. Two source orderings that no
    # other row can settle are fixed explicitly.
    as_given, as_family, pairs = {}, {}, {}
    for *_, given, family in rows:
        as_given[given] = as_given.get(given, 0) + 1
        as_family[family] = as_family.get(family, 0) + 1
        pairs[(given, family)] = pairs.get((given, family), 0) + 1
    recs, seen, flipped = [], set(), 0
    for post, year, field, recon, g, given, family in rows:
        if (g["grantee"] or "") in NAME_ORDER_OVERRIDES:
            given, family = NAME_ORDER_OVERRIDES[g["grantee"]]
            flipped += 1
        elif given and family and " " not in given and pairs.get((family, given), 0):
            mirror, same = pairs[(family, given)], pairs[(given, family)]
            flip = (as_given.get(family, 0) - mirror) + (as_family.get(given, 0) - mirror)
            keep = (as_given.get(given, 0) - same) + (as_family.get(family, 0) - same)
            if flip > keep:
                given, family = family, given
                flipped += 1
        if True:
            if g["grantee_type"] == "organisation":
                base = f"MEF-{year}-{slug(field)}-{slug(g['head'])[:60]}"
            else:
                base = f"MEF-{year}-{slug(field)}-{slug(family)[:40]}-{slug(given)[:24]}"
            fid, i = base, 0
            while fid.lower() in seen:
                i += 1
                fid = f"{base}-{chr(ord('a') + i)}"
            seen.add(fid.lower())
            recs.append({
                "funder_award_id": fid,
                "grant_year": year,
                "field": field,
                "grantee_raw": g["head"],
                "grantee_degree": g["degree"],
                "grantee": g["grantee"],
                "grantee_type": g["grantee_type"],
                "lead_given_name": given,
                "lead_family_name": family,
                "title": g["title"],
                "amount": g["amount"],
                "currency": "EUR",
                "wp_post_id": str(post["id"]),
                "field_total_check": recon,
                "landing_page_url": post["link"],
            })
    log(f"  name order flipped to given-first on {flipped} rows (evidence from other rows)")
    for (y, lang), c in sorted(counts.items(), key=lambda kv: (str(kv[0][0]), kv[0][1])):
        log(f"  translation check {y} {lang}: {sum(c)} paragraphs in {len(c)} posts")
    by_year = {}
    for (y, lang), c in counts.items():
        by_year.setdefault(y, {})[lang] = sum(c)
    for y, d in by_year.items():
        if len(set(d.values())) > 1:
            log(f"  NOTE translations differ for {y}: {d} (English is used)")
    for pr in problems:
        log(f"  PROBLEM {pr}")
    if problems and not args.allow_mismatch:
        raise SystemExit(f"{len(problems)} problems; fix the parser (or --allow-mismatch to inspect)")

    df = pd.DataFrame(recs)
    log(f"{len(df)} grants, years {sorted(df['grant_year'].unique())}, EUR {df['amount'].sum():,.0f}")
    for c in ["title", "lead_family_name", "lead_given_name", "grantee_degree"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")
    log(f"  fields {df['field'].value_counts().to_dict()}")
    top = df.groupby(["lead_given_name", "lead_family_name"]).size().sort_values(ascending=False).head(6)
    log(f"  6.4a top grantees: {top.to_dict()}")

    df = df.astype("string")
    df["amount"] = pd.to_numeric(df["amount"])
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out = args.output_dir / "magnus_ehrnrooth_projects.parquet"
    df.to_parquet(out, index=False)
    log(f"Wrote {len(df)} rows to {out}")
    if args.skip_upload:
        return
    if args.limit:
        raise SystemExit("refusing to upload a --limit smoke file to S3")

    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    previous = args.output_dir / "_previous_magnus_ehrnrooth_projects.parquet"
    try:  # runbook 1.4: never shrink the corpus on re-ingest
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev}, new {len(df)}")
        if len(df) < prev and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(out), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
