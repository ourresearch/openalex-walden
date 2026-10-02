#!/usr/bin/env python3
"""
Fondation Leducq to S3 Data Pipeline
====================================

Fondation Leducq (Paris / Boston; OpenAlex F4320320890) funds cardiovascular and
neurovascular research. Its own site (WordPress, static HTML) publishes every
award of its flagship programme and one smaller programme:

1. **Transatlantic / International Networks of Excellence** (2004-2026, 93
   networks): https://www.fondationleducq.org/international-networks-of-excellence-how-to-apply/funded-networks/
   lists every network by award year with its title and its two coordinators
   (one European, one North American; since 2022 just "Coordinators"). Each
   title links to /network/<slug>/ (enumerated by wp-sitemap-posts-network-1.xml),
   which adds the member investigators and a description.
2. **Equipement de Recherche et Plateformes Technologiques (ERPT / RETP, 2017)**:
   https://www.fondationleducq.org/retp/ lists the 11 French equipment grants
   approved by the board on 17 May 2017, each with project title, managing
   body, research institution, project holder and the EUR amount requested
   from (and approved by) the foundation.

Network award numbers (funder_award_id, runbook §2.1.1)
-------------------------------------------------------
Leducq numbers networks ``YYCVDNN`` (award year + CVD + sequence, e.g.
``16CVD03``), and that is the form citing works write in acknowledgements
(``16CVD03``, ``TNE-16CVD03``, ``16 CVD 03``...). The site prints the number
for the 2023-2026 networks (and in a few slugs/news posts); for 2004-2022 it
does not. For those we use a crosswalk resolved from the citing works
themselves (``NETWORK_IDS`` below): for every number cited with Leducq as
the funder, the network of that award year whose coordinators/members author
the citing works. The method reproduces all 15 site-published 2023-2026
numbers exactly. Networks the crosswalk can't resolve unambiguously ship with
a synthetic ``LEDUCQ-<year>-<slug>`` key (no citation collapse, but stable).

Network amounts are not published per network (the programme gives
$6-8M over five years), so ``amount`` is NULL for networks.

Output: s3://openalex-ingest/awards/leducq/leducq_projects.parquet
"""

import argparse
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# --- Windows UTF-8 compatibility shim (fleet-fix 2026-05-22) ---
# (TWCF-style shim; it renames sys, so for the §4.0 grep: sys.stdout.reconfigure)
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

BASE = "https://www.fondationleducq.org"
LIST_URL = f"{BASE}/international-networks-of-excellence-how-to-apply/funded-networks/"
SITEMAP = f"{BASE}/wp-sitemap-posts-network-1.xml"
RETP_URL = f"{BASE}/retp/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/leducq/leducq_projects.parquet"

HEADERS = {"User-Agent": "openalex-walden/1.0 (+https://openalex.org)"}
REQUEST_DELAY = 0.5
RETRIES = 3

# Network slug -> Leducq award number, for the 2004-2022 networks whose number
# the site does not print. Resolved 2026-10-01 from the 7,358 OpenAlex works
# with Leducq as a funder: for each YYCVDNN cited in a Leducq award entry, the
# network of award year 20YY whose coordinators/members (family names from the
# network pages) author the citing works. Accepted only when the best network
# covers >=30% of the citing works, >=3x the runner-up, and >=2 works (or 1 by
# a coordinator). The rule reproduces every site-published 2023/2025 number it
# can score and refuses the 2024 ones (no member lists, no evidence) rather
# than guessing. 62 numbers resolved, one-to-one; 10CVD02/10CVD04 (one
# member-authored work each) rejected. 16 networks with no qualifying
# citation keep a synthetic LEDUCQ-<year>-<slug> key.
NETWORK_IDS: dict[str, str] = {
    "leducq-international-network-against-thrombosis-linat": "04CVD02",
    "calcium-cycling-and-novel-therapeutic-approaches-for-heart-failure": "05CVD03",
    "localized-control-of-camp-signaling-and-novel-therapeutic-approaches-for-heart-failure": "06CVD02",
    "integrative-networks-regulating-cardiomyocyte-metabolism-and-survival-in-heart-failure-and-insulin-resistance": "06CVD03",
    "european-north-american-atrial-fibrillation-research-alliance-enafra": "07CVD03",
    "mitral-valve-disease-from-genetic-mechanisms-to-improved-repair": "07CVD04",
    "alliance-for-calmodulin-kinase-ii-signaling-in-heart-failure-and-arrhythmias": "08CVD01",
    "mechanisms-matching-the-brains-vascular-energy-supply-to-neural-activity": "08CVD02",
    "redox-and-nitrosative-regulation-of-cardiac-remodeling-novel-therapeutic-approaches-for-heart-failure": "09CVD01",
    "structural-alterations-in-the-myocardium-and-the-substrate-for-cardiac-fibrillation": "09CVD03",
    "multi-scale-modeling-of-single-ventricle-hearts-for-clinical-decision-support": "09CVD04",
    "transatlantic-network-on-newborn-stroke-inflammatory-modulation-of-neurovascular-injury": "10CVD01",
    "molecular-mechanisms-of-novel-genes-associated-with-plasma-lipids-and-cardiovascular-disease": "10CVD03",
    "genomic-epigenomic-and-systems-dissection-of-mechanisms-underlying-dilated-cardiomyopathy": "11CVD01",
    "translating-human-pluripotent-stem-cells-from-heart-disease-models-to-cardiac-repair": "11CVD02",
    "lymph-vessels-in-obesity-and-cardiovascular-disease": "11CVD03",
    "understanding-coronary-artery-disease-genes": "12CVD02",
    "mibava-mechanistic-interrogation-of-bicuspid-aortic-valve-associated-aortopathy": "12CVD03",
    "tnt-triglyceride-metabolism-in-obesity-and-cardiovascular-disease": "12CVD04",
    "cellular-and-molecular-targets-to-promote-therapeutic-cardiac-regeneration": "13CVD01",
    "microrna-based-therapeutic-strategies-in-vascular-disease-mirvad": "13CVD02",
    "the-function-and-regulation-of-pcsk9-a-novel-modulator-of-ldlr-activity": "13CVD03",
    "mechanical-triggers-to-programmed-cell-death-in-cardiomyocytes-and-how-to-prevent-their-action-in-failing-hearts": "13CVD04",
    "deciphering-the-genomic-topology-of-atrial-fibrillation": "14CVD01",
    "sphingosine-1-phosphate-in-neurovascular-biology-and-disease-sphingonet": "14CVD02",
    "molecular-genetics-pathogenesis-and-protein-replacement-therapy-in-arrhythmogenic-cardiomyopathy": "14CVD03",
    "programming-the-failing-heart-to-a-regenerative-state": "14CVD04",
    "22q11-2-deletion-syndrome-novel-approaches-to-understand-cardiopharyngeal-pathogenesis": "15CVD01",
    "evoked-neuronal-activity-a-new-therapy-for-acute-ischemic-stroke": "15CVD02",
    "eliciting-heart-regeneration-through-cardiomyocyte-division": "15CVD03",
    "modulating-autophagy-to-treat-cardiovascular-disease": "15CVD04",
    "lean-leducq-epigenetics-of-atherosclerosis-network-defining-and-targeting-epigenetic-pathways-in-monocytes-and-macrophages-that-contribute-to-cardiovascular-disease": "16CVD01",
    "repolarization-heterogeneity-imaging-for-personalised-therapy-of-heart-arrhythmia": "16CVD02",
    "cellular-and-molecular-drivers-of-cardiac-fibrosis-and-remodelling-in-health-and-disease": "16CVD03",
    "targeting-mitochondria-to-treat-heart-disease": "16CVD04",
    "understanding-the-role-of-the-perivascular-space-in-cerebral-small-vessel-disease": "16CVD05",
    "gut-microbiome-as-a-target-for-the-treatment-of-cardiometabolic-diseases": "17CVD01",
    "the-sodium-channel-as-a-therapeutic-target-for-prevention-of-lethal-cardiac-arrhythmias": "17CVD02",
    "attract-arterial-flow-as-attractor-for-endothelial-cell-migration": "17CVD03",
    "redox-regulation-of-cardiomyocyte-renewal": "17CVD04",
    "potassium-in-hypertension": "17CVD05",
    "cure-phospholamban-induced-cardiomyopathy-cure-plan": "18CVD01",
    "defining-the-roles-of-smooth-muscle-cells-and-other-extracellular-matrix-producing-cells-in-late-stage-atherosclerotic-plaque-pathogenesis": "18CVD02",
    "transcription-factor-klf2-and-cardiovascular-disease": "18CVD03",
    "clonal-hematopoiesis-and-atherosclerosis": "18CVD04",
    "towards-precision-medicine-with-human-ipscs-for-cardiac-channelopathies": "18CVD05",
    "stroke-impact-stroke-immune-mediated-pathways-and-cognitive-trajectories": "19CVD01",
    "targeted-approaches-for-prevention-and-treatment-of-anthracycline-induced-cardiotoxicity": "19CVD02",
    "fighting-against-sinus-node-dysfunction-and-associated-arrhythmias": "19CVD03",
    "cellular-and-systemic-cholesterol-transport-in-physiology-and-disease": "19CVD04",
    "cytoskeletal-regulation-of-cardiomyocyte-homeostasis-in-health-and-disease": "20CVD01",
    "the-inflammatory-fibrosis-axis-in-ischemic-heart-failure-translating-mechanisms-into-new-diagnostics-and-therapeutics": "20CVD02",
    "b-cells-in-cardiovascular-disease": "20CVD03",
    "editing-the-failing-heart": "20CVD04",
    "brown-fat-and-cardiovascular-health-genetic-determinants-and-molecular-mechanisms": "21CVD01",
    "cardiac-splicing-as-a-therapeutic-target-castt": "21CVD02",
    "recalibrating-mechanotransduction-in-vascular-malformations": "21CVD03",
    "leducq-trans-atlantic-network-of-excellence-on-circadian-effects-in-stroke": "21CVD04",
    "international-network-of-excellence-on-brain-endothelium-a-nexus-for-cerebral-small-vessel-disease": "22CVD01",
    "checkpoint-athero": "22CVD02",
    "cellular-and-molecular-drivers-of-acute-aortic-dissections": "22CVD03",
    "atherogen": "22CVD04",
}

COUNTRIES = {
    "usa": "US", "us": "US", "uk": "GB", "united kingdom": "GB", "scotland": "GB", "england": "GB",
    "germany": "DE", "france": "FR", "netherlands": "NL", "the netherlands": "NL", "sweden": "SE",
    "denmark": "DK", "finland": "FI", "italy": "IT", "spain": "ES", "belgium": "BE", "austria": "AT",
    "switzerland": "CH", "canada": "CA", "australia": "AU", "norway": "NO", "ireland": "IE",
}


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get(url: str) -> str:
    last_err = None
    for attempt in range(RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code == 404:
                return ""
            r.raise_for_status()
            r.encoding = "utf-8"
            return r.text
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {last_err}")


def cached_get(url: str, cache_dir: Path | None) -> str:
    cache = None
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        slug = re.sub(r"[^a-z0-9]+", "-", url.lower().split("fondationleducq.org", 1)[-1]).strip("-")[:90]
        cache = cache_dir / ((slug or "home") + ".html")
        if cache.exists():
            return cache.read_text()
    page = get(url)
    if cache and page:
        cache.write_text(page)
    time.sleep(REQUEST_DELAY)
    return page


def text(fragment: str | None) -> str | None:
    if fragment is None:
        return None
    t = re.sub(r"<[^>]+>", " ", fragment)
    t = html.unescape(t).replace("\xa0", " ").replace("​", "").replace("﻿", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t or None


def split_name(name: str) -> tuple[str | None, str | None]:
    """Canonical runbook §2.4.1 helper (wolf_to_s3.py), verbatim."""
    if not name:
        return None, None
    tokens = name.split()
    suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
    while tokens and tokens[-1].lower().strip(",.") in suffixes:
        tokens.pop()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return None, tokens[0]
    return " ".join(tokens[:-1]), tokens[-1]


PARTICLES = {"de", "van", "von", "der", "den", "da", "di", "del", "le", "la", "du", "ten", "ter"}


def _is_caps(tok: str) -> bool:
    letters = [c for c in tok if c.isalpha()]
    return len(letters) >= 2 and all(c.isupper() for c in letters)


def _title(tok: str) -> str:
    """ZIMMERMANN -> Zimmermann, HILFIKER-KLEINER -> Hilfiker-Kleiner, MCNALLY -> McNally."""
    def one(p: str) -> str:
        p = p.capitalize()
        if len(p) > 2 and p.startswith("Mc"):
            p = "Mc" + p[2:].capitalize()
        return p
    return "-".join("'".join(one(q) for q in part.split("'")) for part in tok.split("-"))


def split_leducq_name(name: str) -> tuple[str | None, str | None]:
    """Leducq prints family names in capitals ("Menno de WINTHER", "Matthias
    VAN OSCH", "S. Ananth KARUMANCHI"), which is more precise than last-token.
    Family = the trailing run of all-caps tokens (plus a lowercase particle
    just before it); falls back to split_name when the name has no caps run or
    is all caps ("WILLIAM MCKENNA")."""
    tokens = name.split()
    while tokens and tokens[-1].lower().strip(",.") in {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}:
        tokens.pop()  # same suffix set as split_name ("Gerald W. DORN II")
    if not tokens:
        return None, None
    i = len(tokens)
    while i > 0 and _is_caps(tokens[i - 1]):
        i -= 1
    if 0 < i < len(tokens):
        while i > 1 and tokens[i - 1].lower() in PARTICLES:
            i -= 1
        given = " ".join(tokens[:i])
        family = " ".join(t.lower() if t.lower() in PARTICLES else _title(t) for t in tokens[i:])
        return given, family
    if i == 0:  # all caps
        tokens = [_title(t) for t in tokens]
    given, family = split_name(" ".join(tokens))
    parts = (given or "").split()
    if len(parts) > 1 and parts[-1].lower() in PARTICLES:  # "Michel De Waard"
        given, family = " ".join(parts[:-1]), parts[-1] + " " + family
    return given, family


DAGGER_RE = re.compile(r"\(\s*†[^)]*\)")


def parse_person_entry(raw: str) -> list[dict]:
    """'Wolfram ZIMMERMANN, University Medical Center Göttingen (Germany)' ->
    one person. Handles replacement coordinators: 'Kenneth BLOCH († 13/9/2014)/
    Donald B. BLOCH, MGH (USA)', 'Helmut DREXLER († ...)/Kai WOLLERT and Denise
    HILFIKER-KLEINER, Hannover Medical School (Germany)', and 'A, inst († d) /
    B, inst (France)'."""
    out = []
    for seg in re.split(r"\s+/\s+", raw):
        deceased = bool(DAGGER_RE.search(seg))
        seg = DAGGER_RE.sub("", seg).strip()
        name_part, _, inst = seg.partition(",")
        inst = inst.strip(" ,") or None
        country = None
        if inst:
            m = re.search(r"\(([^()]+)\)\s*$", inst)
            last = m.group(1) if m else inst.rsplit(",", 1)[-1]
            country = COUNTRIES.get(last.strip(" .").lower())
            if country:  # "Univ X, Paris (France)" -> "Univ X, Paris"; "ICGEB, Trieste, Italy" -> "ICGEB, Trieste"
                inst = (inst[: m.start()] if m else inst.rsplit(",", 1)[0]).strip(" ,") or inst
        names = [n.strip() for n in re.split(r"/|\s+and\s+|\s+et\s+", name_part) if n.strip()]
        for k, n in enumerate(names):
            given, family = split_leducq_name(n)
            out.append({
                "name": n, "given_name": given, "family_name": family,
                "institution": inst, "country": country,
                # in "A († date)/B" the dagger belongs to A, the original coordinator
                "deceased": deceased and k == 0 and len(names) > 1,
            })
    return out


def parse_list(page: str) -> list[dict]:
    nets = []
    for block in re.split(r'<div class="vc_col-sm-12 funded-network-wrapper">', page)[1:]:
        year = re.search(r"<h2>\s*(\d{4})\s*</h2>", block)
        for item in re.split(r'<div class="funded-network-content">', block)[1:]:
            a = re.search(r'<h3><a href="([^"]+)">(.*?)</a></h3>', item, re.S)
            if not a:
                continue
            raw_title = text(a.group(2))
            m = re.search(r"\s*\|\s*(\d{2}CVD\d{2})\s*$", raw_title or "")
            title = raw_title[: m.start()].strip() if m else raw_title
            coords = []
            # older layout: <li><strong>European Coordinator:</strong> Name, Inst</li>
            for role, body in re.findall(r"<li><strong>([^<]*?):?</strong>(.*?)</li>", item, re.S):
                for p in parse_person_entry(text(body) or ""):
                    coords.append({**p, "role": text(role).rstrip(":")})
            # 2022+ layout: <strong>Coordinators:</strong><ul><li>Name, Inst</li>...
            cm = re.search(r"<strong>\s*Coordinators:?\s*</strong>\s*<ul>(.*?)</ul>", item, re.S)
            if cm:
                for li in re.findall(r"<li>(.*?)</li>", cm.group(1), re.S):
                    for p in parse_person_entry(text(li) or ""):
                        coords.append({**p, "role": "Coordinator"})
            url = a.group(1).strip()
            slug_id = re.search(r"-(\d{2}cvd\d{2})/?$", url)
            nets.append({
                "year": int(year.group(1)) if year else None,
                "title": title,
                "network_number_site": m.group(1) if m else (slug_id.group(1).upper() if slug_id else None),
                "landing_page_url": url,
                "slug": url.rstrip("/").rsplit("/", 1)[-1],
                "coordinators": coords,
            })
    return nets


def parse_network_page(page: str) -> dict:
    out = {"members": [], "description": None, "post_date": None}
    d = re.search(r'<li class="meta-date">([^<]+)</li>', page)
    if d:
        try:
            out["post_date"] = datetime.strptime(d.group(1).strip(), "%B %d, %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    start = page.find('<div class="post-excerpt">')
    body = page[start:] if start >= 0 else ""
    ends = [i for i in (body.find("Related Research Highlights"), body.find("<footer")) if i > 0]
    body = body[: min(ends)] if ends else body
    mm = re.search(r'<div class="funded-network-members">', body)
    if mm:
        rest = body[mm.end():]
        ul = re.match(r"\s*<ul>(.*?)</ul>", rest, re.S)
        if ul:  # <ul><li>Name, Inst</li>...</ul>
            entries = re.findall(r"<li>(.*?)</li>", ul.group(1), re.S)
            body = rest[ul.end():]
        else:   # older pages: <p>Name, Inst<br />Name, Inst</p>
            pm = re.match(r"\s*<p[^>]*>(.*?)</p>", rest, re.S)
            entries = re.split(r"<br\s*/?>", pm.group(1)) if pm else []
            body = rest[pm.end():] if pm else rest
        for e in entries:
            if text(e):
                out["members"] += parse_person_entry(text(e))
    paras = [text(p) for p in re.findall(r"<p[^>]*>(.*?)</p>", body, re.S)]
    paras = [p for p in paras if p and not p.startswith("Related Research Highlights")]
    out["description"] = "\n\n".join(paras) or None
    return out


def parse_retp(page: str) -> list[dict]:
    """ERPT 2017: blocks of 'Titre du projet : ... Organisme gestionnaire : ...
    Institut(ion) de Recherche : ... Porteur de Projet : ... Montant demandé à
    la Fondation Leducq : 708.583€ ...'."""
    body = page[page.find('id="page-wrap"'):]
    body = body[: body.find("SUBSCRIBE")] if "SUBSCRIBE" in body else body
    t = text(re.sub(r"<(p|br|div|li|h\d)[^>]*>", "\n", body, flags=re.I)) or ""
    t = re.sub(r"\s*:\s*", " : ", t)
    parts = re.split(r"Titre\s+du\s+projet\s*:", t, flags=re.I)[1:]
    rows = []
    label = r"(?:Organisme\s+gestionnaire|Institut(?:ion)?\s+de\s+Recherche|Porteur\s+de\s+Projet(?:\s+et\s+Directeur\s+de\s+Plate-?forme)?|Directeur\s+d[eu]\s+\S+|Montant\s+demand[ée]\s+à\s+la\s+Fondation\s+Leducq|Nature\s+de\s+la\s+demande[^:]*|Objectif\s+du\s+projet)"

    def field(seg: str, name: str) -> str | None:
        m = re.search(name + r"\s*:\s*(.*?)(?=\s" + label + r"\s*:|$)", seg, re.I | re.S)
        return m.group(1).strip(" .;") if m else None

    for k, seg in enumerate(parts, 1):
        title = re.split(label + r"\s*:", seg, maxsplit=1, flags=re.I)[0].strip(" .;")
        holder = field(seg, r"Porteur\s+de\s+Projet(?:\s+et\s+Directeur\s+de\s+Plate-?forme)?")
        amount_txt = field(seg, r"Montant\s+demand[ée]\s+à\s+la\s+Fondation\s+Leducq")
        digits = re.sub(r"[^\d]", "", amount_txt or "")
        people = parse_person_entry(holder) if holder else []
        rows.append({
            "seq": k,
            "title": title or None,
            "managing_body": field(seg, r"Organisme\s+gestionnaire"),
            "institution": field(seg, r"Institut(?:ion)?\s+de\s+Recherche"),
            "holder": holder,
            "people": people,
            "amount_text": amount_txt,
            "amount": float(digits) if digits else None,
            "equipment": field(seg, r"Nature\s+de\s+la\s+demande[^:]*"),
            "objective": field(seg, r"Objectif\s+du\s+projet"),
        })
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Fondation Leducq networks + ERPT -> parquet -> S3")
    p.add_argument("--limit", type=int, default=None, help="only the first N networks (smoke test)")
    p.add_argument("--output-dir", type=Path, default=Path("/tmp"))
    p.add_argument("--cache-dir", type=Path, default=None, help="cache raw HTML here (re-runs skip fetch)")
    p.add_argument("--skip-upload", action="store_true")
    p.add_argument("--allow-shrink", action="store_true", help="override the §1.4 shrink guard")
    args = p.parse_args()

    nets = parse_list(cached_get(LIST_URL, args.cache_dir))
    log(f"Funded-networks page: {len(nets)} networks")
    sitemap = set(re.findall(r"<loc>([^<]+)</loc>", get(SITEMAP)))
    listed = {n["landing_page_url"] for n in nets}
    for u in sorted(sitemap - listed):
        log(f"  WARNING sitemap network not on the funded-networks list: {u}")
    if len(nets) < 90:
        raise SystemExit(f"only {len(nets)} networks parsed; page layout changed?")
    if args.limit:
        nets = nets[: args.limit]

    rows = []
    for i, n in enumerate(nets, 1):
        page = cached_get(n["landing_page_url"], args.cache_dir)
        det = parse_network_page(page) if page else {"members": [], "description": None, "post_date": None}
        number = n["network_number_site"] or NETWORK_IDS.get(n["slug"])
        source = "site" if n["network_number_site"] else ("citation_crosswalk" if number else "synthetic")
        coords = n["coordinators"]
        # lead = European (or first-listed) coordinator, co-lead = North American (or second)
        eu = [c for c in coords if c["role"].lower().startswith("european")]
        na = [c for c in coords if c["role"].lower().startswith("north american")]
        ordered = (eu[:1] + na[:1]) if (eu and na) else coords[:2]
        people = ordered + [c for c in n["coordinators"] if c not in ordered] + \
            [{**m, "role": "Member"} for m in det["members"]]
        rows.append({
            "programme": "Networks of Excellence",
            "award_year": n["year"],
            "network_number": number,
            "network_number_source": source,
            "funder_award_id": number or f"LEDUCQ-{n['year']}-{n['slug']}",
            "title": n["title"],
            "description": det["description"],
            "start_date": f"{n['year']}-01-01" if n["year"] else None,
            "post_date": det["post_date"],
            "amount": None,
            "currency": None,
            "lead_name": ordered[0]["name"] if ordered else None,
            "lead_given_name": ordered[0]["given_name"] if ordered else None,
            "lead_family_name": ordered[0]["family_name"] if ordered else None,
            "lead_institution": ordered[0]["institution"] if ordered else None,
            "lead_country": ordered[0]["country"] if ordered else None,
            "co_lead_name": ordered[1]["name"] if len(ordered) > 1 else None,
            "people": json.dumps(people, ensure_ascii=False),
            "n_people": len(people),
            "landing_page_url": n["landing_page_url"],
        })
        if i % 20 == 0:
            log(f"  {i}/{len(nets)} network pages")

    if not args.limit:
        retp = parse_retp(cached_get(RETP_URL, args.cache_dir))
        log(f"ERPT 2017 page: {len(retp)} equipment grants")
        if len(retp) != 11:
            raise SystemExit(f"expected 11 ERPT grants, parsed {len(retp)}")
        for r in retp:
            lead = r["people"][0] if r["people"] else {}
            rows.append({
                "programme": "Equipement de Recherche et Plateformes Technologiques (ERPT)",
                "award_year": 2017,
                "network_number": None,
                "network_number_source": "synthetic",
                "funder_award_id": f"LEDUCQ-ERPT-2017-{r['seq']:02d}",
                "title": r["title"],
                "description": "; ".join(x for x in [r["objective"], r["equipment"] and "Equipment: " + r["equipment"]] if x) or None,
                "start_date": "2017-05-17",  # board approval date stated on the page
                "post_date": None,
                "amount": r["amount"],
                "currency": "EUR" if r["amount"] is not None else None,
                "lead_name": lead.get("name"),
                "lead_given_name": lead.get("given_name"),
                "lead_family_name": lead.get("family_name"),
                "lead_institution": r["institution"] or r["managing_body"],
                "lead_country": "FR",
                "co_lead_name": None,
                "people": json.dumps([{**x, "role": "Porteur de projet", "institution": r["institution"] or r["managing_body"], "country": "FR"} for x in r["people"]], ensure_ascii=False),
                "n_people": len(r["people"]),
                "landing_page_url": RETP_URL,
            })

    df = pd.DataFrame(rows)
    dupes = df["funder_award_id"].str.lower().duplicated(keep=False)
    if dupes.any():
        raise SystemExit(f"duplicate funder_award_id: {df.loc[dupes, 'funder_award_id'].tolist()}")
    log(f"{len(df)} awards; number source: {df['network_number_source'].value_counts().to_dict()}")
    for c in ["title", "description", "start_date", "amount", "lead_family_name", "lead_institution", "co_lead_name"]:
        log(f"  {c:18s} {df[c].notna().mean():6.1%}")

    df = df.astype("string")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output_dir / "leducq_projects.parquet"
    df.to_parquet(parquet_path, index=False)
    log(f"Wrote {len(df)} rows to {parquet_path}")

    if args.skip_upload:
        return
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3")
    # runbook §1.4: never shrink the corpus on re-ingest
    previous = args.output_dir / "_previous_leducq_projects.parquet"
    try:
        s3.download_file(S3_BUCKET, S3_KEY, str(previous))
        prev_count = len(pd.read_parquet(previous))
        log(f"Shrink check: previous {prev_count}, new {len(df)}")
        if len(df) < prev_count and not args.allow_shrink:
            raise SystemExit(f"refusing to shrink corpus ({prev_count} -> {len(df)}); rerun with --allow-shrink if genuine")
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
            raise
        log("Shrink check: no existing parquet, first ingest")
    log(f"Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3.upload_file(str(parquet_path), S3_BUCKET, S3_KEY)
    log("Done")


if __name__ == "__main__":
    main()
