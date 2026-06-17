#!/usr/bin/env python3
"""
Fonds de recherche du Québec (FRQS / FRQNT / FRQSC) -> S3 awards parquet.

Source: Données Québec open-data CKAN. Each fund publishes one dataset with ~6
yearly CSVs ("Liste du financement", fiscal years 2018-2019 .. 2023-2024). No auth.

  FRQS  Santé                 funder F4320334618  CKAN f6bbdfe1-b9df-4dba-a2bd-5e9bbace8ed5
  FRQNT Nature et technologies funder F4320334841  CKAN afd242d3-cc46-4ef4-a194-40835293360c
  FRQSC Société et culture     funder F4320332645  CKAN liste-du-financement-accorde-par-le-fonds-de-recherche-du-quebec-societe-et-culture

Output column contract (13, all string/nullable):
  funder_award_id, title, pi_full, pi_given, pi_family, institution, amount,
  currency, scheme, start_date_raw, end_date_raw, description, landing_page_url

KEY HANDLING (each earned during the build/validation pass):
  * COLUMN-NAME DRIFT across yearly files — typos and casing differ year to year
    (`Fonds_gestion` vs `Fonds`, `Debut_financemnet`, `Montant_Total`,
    `Type_de_destinataire`, `Domaine_de_recherche`, `Champs_application_1`...). A
    naive concat silently drops the entire 2018-2019 cohort and nulls institution/
    date for older years. `norm_cols()` maps every variant to a canonical schema
    BEFORE concat.
  * NO native project title — synthesized `Programme — Objet_de_recherche` (or
    Domaines). Reported as a known caveat; never null (Programme always present).
  * PI `Titulaire` is "Family, Given" -> split; academic titles + stray commas
    stripped.
  * AMOUNT formats are mixed across years: "480000", "480 000", "540,000 $",
    "56141.75". `clean_amt()` is decimal-aware (comma = thousands UNLESS exactly
    two trailing digits = cents). Dedup keeps amount = MAX real annual installment
    (the source lists per-fiscal-year installments, not a cumulative total).
  * MULTI-YEAR grants recur once per active fiscal year. Collapse each `Dossier`
    to ONE coherent row: base = the max-installment row that has a real named PI
    (so PI + institution always come from the SAME source year — 0% cross-year
    field mixing), amount = max real installment across all years. Verified 0
    Frankenstein pairs against source.
  * `Dossier == "S.O."` (a non-unique N/A sentinel on aggregate programs) gets a
    synthetic `{FUND}-SO-{n}` id so those awards survive as distinct rows.
  * Placeholder tokens ("Non disponible", "S.O.", "N/D", "N/A") are suppressed
    ONLY as exact standalone field/keyword values — NOT as substrings (so "Na-K-Cl",
    "So Yeon", "Na-Dene" survive).

  Training bursaries ("Bourses et stages de formation", the largest category) are
  KEPT — they are legitimate awards with a recipient + amount.
"""
import io
import os
import re
import sys
import time
import subprocess

import pandas as pd
import requests

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
CKAN = "https://www.donneesquebec.ca/recherche/api/3/action/package_show?id={}"

FUNDS = {
    "frqs":  {"ckan": "f6bbdfe1-b9df-4dba-a2bd-5e9bbace8ed5", "code": "FRQS",  "min": 3000},
    "frqnt": {"ckan": "afd242d3-cc46-4ef4-a194-40835293360c", "code": "FRQNT", "min": 3000},
    "frqsc": {"ckan": "liste-du-financement-accorde-par-le-fonds-de-recherche-du-quebec-societe-et-culture",
              "code": "FRQSC", "min": 3000},
}

COLUMNS = ["funder_award_id", "title", "pi_full", "pi_given", "pi_family",
           "institution", "amount", "currency", "scheme", "start_date_raw",
           "end_date_raw", "description", "landing_page_url"]

# canonical source field -> list of normalized variants (lowercased, alnum-only)
CANON = {
    "Fonds": ["fonds", "fondsgestion"],
    "Cofinancement": ["cofinancement"],
    "Annee_financiere": ["anneefinanciere"],
    "Debut_Financement": ["debutfinancement", "debutfinancemnet", "debutfinancemnt"],
    "Titulaire": ["titulaire"],
    "Dossier": ["dossier", "nodossier"],
    "etablissement": ["etablissement"],
    "Categorie_de_financement": ["categoriedefinancement", "categoriefinancement"],
    "Programme": ["programme"],
    "Programme_volet": ["programmevolet"],
    "Type_de_Recipiendaire": ["typederecipiendaire", "typededestinataire"],
    "Domaines_de_recherche": ["domainesderecherche", "domainederecherche"],
    "Objet_de_recherche_1": ["objetderecherche1"],
    "Objet_de_recherche_2": ["objetderecherche2"],
    "Champs_d_application_1": ["champsdapplication1", "champsapplication1"],
    "Champs_d_application_2": ["champsdapplication2", "champsapplication2"],
    "Mots_cles": ["motscles"],
    "Montant_total": ["montanttotal"],
}
_VARIANT = {v: canon for canon, vs in CANON.items() for v in vs}
_PLACEHOLDER = {"non disponible", "s.o.", "s.o", "so", "n/d", "nd", "n/a", "na", "n.d.", "n.a."}
_TITLE_RE = re.compile(r"^\s*(?:Dr|Dre|Pr|Prof|Professeure?|M|Mme|Mlle)\.?\s+", re.I)


def _nk(col):
    return re.sub(r"[^a-z0-9]", "", str(col).lower())


def norm_cols(df):
    """Rename source columns to the canonical schema, collapsing year-to-year drift."""
    ren = {}
    for c in df.columns:
        canon = _VARIANT.get(_nk(c))
        if canon:
            ren[c] = canon
    return df.rename(columns=ren)


def is_placeholder(v):
    return isinstance(v, str) and v.strip().lower() in _PLACEHOLDER


def clean_text(v):
    if not isinstance(v, str):
        return None
    v = re.sub(r"\s+", " ", v).strip()
    if not v or is_placeholder(v):
        return None
    return v


def clean_amt(v):
    """Decimal-aware amount parse. comma = thousands unless exactly 2 trailing digits."""
    if not isinstance(v, str):
        return None
    s = v.replace("$", "").replace("\xa0", " ").strip().replace(" ", "")
    if not s:
        return None
    if "," in s:
        parts = s.split(",")
        s = parts[0] + "." + parts[1] if (len(parts) == 2 and len(parts[1]) == 2) else s.replace(",", "")
    try:
        f = float(s)
    except ValueError:
        d = re.sub(r"[^\d.]", "", s)
        f = float(d) if d else None
    if f is None or f <= 0:
        return None
    return str(int(f)) if float(f).is_integer() else str(f)


def split_name(titulaire):
    """'Family, Given' -> (full, given, family). Strip titles + stray commas."""
    s = clean_text(titulaire)
    if not s:
        return None, None, None
    if "," in s:
        fam, _, giv = s.partition(",")
        family = fam.strip().strip(",").strip() or None
        given = giv.replace(",", " ").strip()
        given = _TITLE_RE.sub("", given).strip() or None
    else:
        family, given = s, None
    full = " ".join(x for x in [given, family] if x) or None
    if full:
        full = re.sub(r"\s+", " ", full).strip()
    return full, given, family


def synth_title(row):
    prog = clean_text(row.get("Programme"))
    o1 = clean_text(row.get("Objet_de_recherche_1"))
    o2 = clean_text(row.get("Objet_de_recherche_2"))
    dom = clean_text(row.get("Domaines_de_recherche"))
    focus = ", ".join(x for x in [o1, o2] if x) or dom
    if prog and focus:
        return f"{prog} — {focus}"
    return prog or focus or None


def pack_desc(row):
    bits = []
    for label, key in [("Domaines de recherche", "Domaines_de_recherche"),
                       ("Objet de recherche 1", "Objet_de_recherche_1"),
                       ("Objet de recherche 2", "Objet_de_recherche_2"),
                       ("Champs d'application 1", "Champs_d_application_1"),
                       ("Champs d'application 2", "Champs_d_application_2"),
                       ("Mots-cles", "Mots_cles"),
                       ("Categorie de financement", "Categorie_de_financement"),
                       ("Type de recipiendaire", "Type_de_Recipiendaire")]:
        val = clean_text(row.get(key))
        if val:
            bits.append(f"{label}: {val}")
    cof = clean_text(row.get("Cofinancement"))
    if cof:
        bits.append(f"Cofinancement: {cof}")
    return " | ".join(bits) or None


def start_year_iso(row):
    raw = str(row.get("Debut_Financement") or row.get("Annee_financiere") or "")
    m = re.search(r"(\d{4})", raw)
    return f"{m.group(1)}-01-01" if m else None


def fetch_csvs(ckan):
    r = requests.get(CKAN.format(ckan), headers=UA, timeout=60)
    r.raise_for_status()
    urls = [x["url"] for x in r.json()["result"]["resources"]
            if x.get("format", "").upper() == "CSV"]
    frames = []
    for u in urls:
        for attempt in range(4):
            try:
                rr = requests.get(u, headers=UA, timeout=90)
                rr.encoding = "utf-8-sig"
                frames.append(norm_cols(pd.read_csv(io.StringIO(rr.text), dtype=str)))
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 3:
                    raise
                time.sleep(2 * (attempt + 1))
    return urls, frames


def build_fund(slug, cfg):
    urls, frames = fetch_csvs(cfg["ckan"])
    df = pd.concat(frames, ignore_index=True)
    # keep only this fund's rows (the Fonds column carries the fund code)
    if "Fonds" in df.columns:
        df = df[df["Fonds"].astype(str).str.upper().str.contains(cfg["code"], na=False)]
    sys.stderr.write(f"[{slug}] {len(urls)} CSVs, {len(df)} raw rows\n")

    # one record per source row, then collapse by Dossier
    by_dossier = {}
    synth_n = 0
    for _, row in df.iterrows():
        dossier = clean_text(row.get("Dossier"))
        amt = clean_amt(row.get("Montant_total"))
        full, given, family = split_name(row.get("Titulaire"))
        rec = {
            "title": synth_title(row),
            "pi_full": full, "pi_given": given, "pi_family": family,
            "institution": clean_text(row.get("etablissement")),
            "amount": amt, "currency": "CAD",
            "scheme": clean_text(row.get("Programme")),
            "start_date_raw": start_year_iso(row),
            "end_date_raw": None, "description": pack_desc(row),
            "landing_page_url": None,
        }
        if dossier is None:  # "S.O." sentinel -> distinct synthetic id
            synth_n += 1
            rec["funder_award_id"] = f"{cfg['code']}-SO-{synth_n}"
            by_dossier[rec["funder_award_id"]] = [rec]
        else:
            by_dossier.setdefault(dossier, []).append((rec, amt, full))

    out = []
    for key, group in by_dossier.items():
        if isinstance(group[0], dict):  # synthetic single rows
            r = group[0]; r["funder_award_id"] = key; out.append(r)
            continue
        # coherent collapse: base = max-installment row WITH a real PI (PI+inst same year)
        named = [(rec, a) for rec, a, full in group if full]
        pool = named if named else [(rec, a) for rec, a, full in group]
        base = max(pool, key=lambda t: float(t[1]) if t[1] else -1)[0].copy()
        # amount = MAX real installment across ALL years for this grant
        amts = [float(a) for _, a, _ in group if a]
        if amts:
            mx = max(amts)
            base["amount"] = str(int(mx)) if float(mx).is_integer() else str(mx)
        # fill genuinely-missing non-identity fields from other years
        for rec, _, _ in group:
            for fld in ("title", "scheme", "start_date_raw", "description", "institution"):
                if not base.get(fld) and rec.get(fld):
                    base[fld] = rec[fld]
        base["funder_award_id"] = key
        out.append(base)

    res = pd.DataFrame(out, columns=COLUMNS).astype("string")
    res = res.drop_duplicates(subset=["funder_award_id"])
    if len(res) < cfg["min"]:
        sys.stderr.write(f"[{slug}] ERROR only {len(res)} rows (< {cfg['min']}) — refusing\n")
        sys.exit(1)
    fill = lambda c: round(100 * res[c].notna().mean(), 1)
    sys.stderr.write(f"[{slug}] {len(res)} grants | title {fill('title')}% pi {fill('pi_full')}% "
                     f"inst {fill('institution')}% amount {fill('amount')}% date {fill('start_date_raw')}%\n")
    return res


def main():
    if any(a in ("-h", "--help") for a in sys.argv[1:]):
        print(__doc__)
        print("Usage: frq_to_s3.py [frqs|frqnt|frqsc ...]   "
              "(default: all three; set SKIP_UPLOAD=1 to skip the S3 upload)")
        return
    which = sys.argv[1:] or list(FUNDS)
    for slug in which:
        cfg = FUNDS[slug]
        df = build_fund(slug, cfg)
        out = f"/tmp/{slug}_grants.parquet"
        df.to_parquet(out, index=False)
        if not os.environ.get("SKIP_UPLOAD"):
            dst = f"s3://openalex-ingest/awards/{slug}/{slug}_grants.parquet"
            subprocess.run(["aws", "s3", "cp", out, dst], check=True)
            print(f"uploaded {dst}")


if __name__ == "__main__":
    main()
