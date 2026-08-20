#!/usr/bin/env python3
"""
FCT (Fundação para a Ciência e a Tecnologia, Portugal) -> S3 Data Pipeline
==========================================================================

Builds the FCT awards parquet from the SciPROJ CERIF dump (primary source) merged
with the legacy fct.pt XLSX rows, and uploads to S3 for Databricks ingestion.
FCT is OpenAlex funder F4320334779 (ROR 00snfqn58, DOI 10.13039/501100001871).

Primary source: SciPROJ (sciproj.ptcris.pt, FCT/FCCN's national CRIS), delivered as a
    bulk ZIP dump of per-project OpenAIRE CERIF 1.1 XML records (obtained directly from
    FCCN, 2026-08; CC BY 4.0). 106,753 records. SciPROJ is Portugal's national project
    registry, so it also carries projects funded by other agencies; every record gets a
    `funder_key` (all rows keep provenance 'fct' since the source is one dump):
      - fct  (98,807): funder OrgUnit ROR 00snfqn58, or no funder OrgUnit (FCT's own
              DB defaults to FCT). ~100% citable references (SFRH/PTDC/POCTI/...
              families plus modern YYYY.NNNNN.SFX), 93% EUR amounts, 90% PI.
      - ani  (3,713): Agência Nacional de Inovação business-R&D programs.
      - ec   (3,491): whole FP1..H2020 projects w/ Portuguese participation, EC grant
              numbers, EU-contribution amounts. 78% duplicate our CORDIS coverage
              (prio 27) — the notebook anti-joins those away so CORDIS keeps winning.
      - erdf (742): PT2020/QREN FEDER operation codes (EU structural funds).
    Archived raw zip: s3://openalex-ingest/awards/fct/raw/getDumpXML20260722_PRV.zip

Legacy source (merged in): the fct.pt "Lista-Projetos-ID" XLSX (7,569 projects; the
    public URL now 404s, so we read the archived parquet built from it —
    s3://openalex-ingest/awards/fct/archive/fct_grants_xlsx_2026-06.parquet).
    99.7% of its refs also exist in SciPROJ with identical titles; for those rows the
    XLSX field values win (they carry keyword descriptions and FCT-current amounts) and
    SciPROJ contributes institution ROR/country. 20 XLSX-only refs are appended as-is.

Output: s3://openalex-ingest/awards/fct/fct_grants.parquet
"""

import argparse
import io
import re
import subprocess
import sys
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fct/fct_grants.parquet"
XLSX_ARCHIVE_S3 = "s3://openalex-ingest/awards/fct/archive/fct_grants_xlsx_2026-06.parquet"
FCT_ROR = "https://ror.org/00snfqn58"
SCIPROJ_LANDING = "https://sciproj.ptcris.pt/"
_NULLISH = {"", "n/a", "na", "-", "—", "/", "nan", "none"}

NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "c": "https://www.openaire.eu/cerif-profile/1.1/",
}

# SciPROJ coordinator Country values are a mix of ISO-2 codes and Portuguese names
# (any case); normalize the common ones to English names for affiliation.country.
_COUNTRY = {
    "pt": "Portugal", "portugal": "Portugal",
    "gb": "United Kingdom", "reino unido": "United Kingdom",
    "es": "Spain", "espanha": "Spain",
    "us": "United States", "estados unidos": "United States",
    "fr": "France", "frança": "France",
    "it": "Italy", "itália": "Italy",
    "de": "Germany", "alemanha": "Germany",
    "be": "Belgium", "bélgica": "Belgium",
    "nl": "Netherlands", "holanda": "Netherlands", "países baixos": "Netherlands",
    "br": "Brazil", "brasil": "Brazil",
    "ch": "Switzerland", "suíça": "Switzerland",
    "at": "Austria", "áustria": "Austria",
    "se": "Sweden", "suécia": "Sweden",
    "dk": "Denmark", "dinamarca": "Denmark",
    "no": "Norway", "noruega": "Norway",
    "fi": "Finland", "finlândia": "Finland",
    "ie": "Ireland", "irlanda": "Ireland",
    "pl": "Poland", "polónia": "Poland",
    "gr": "Greece", "grécia": "Greece",
}

# Subject classifications are shipped commented-out in the CERIF XML; recover them.
_SUBJECT_COMMENT = re.compile(r"<!--<Subject[^>]*>([^<]+)</Subject>-->")


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return None if s.lower() in _NULLISH else s


def split_pi(name):
    name = clean(name)
    if not name or not re.search(r"[A-Za-zÀ-ÿ]{2}", name):
        return None, None, None
    parts = name.split()
    if len(parts) < 2:
        return name, None, name
    return name, " ".join(parts[:-1]), parts[-1]   # family = last token (PT order)


def norm_country(v):
    v = clean(v)
    if not v:
        return None
    return _COUNTRY.get(v.lower(), v.title())


def funding_type(ref, program):
    r = (ref or "").lower()
    if (r.startswith("sfrh/") or re.search(r"\.(bd|bpd|bl|bi)$", r)
            or (program or "").upper() == "FARH"):
        return "fellowship"
    return "grant"


def classify_funder(proj):
    """Map the record's Funded/By OrgUnit to a funder_key ('fct'|'ani'|'ec'|'erdf')."""
    funder = proj.find(".//c:Funded/c:By/c:OrgUnit", NS)
    if funder is None:
        return "fct"               # SciPROJ is FCT's own DB; missing funder = FCT
    ror = funder.findtext("c:RORID", default=None, namespaces=NS)
    name = (funder.findtext("c:Name", default=None, namespaces=NS) or "")
    acr = (funder.findtext("c:Acronym", default=None, namespaces=NS) or "")
    if ror == FCT_ROR or (not ror and not name):
        return "fct"
    if ror == "https://ror.org/01mvsby80" or acr == "ANI":
        return "ani"
    if acr == "EC" or name == "European Commission":
        return "ec"
    if acr.startswith("UE -") or name.startswith("União Europeia"):
        return "erdf"              # PT2020 / QREN FEDER operations
    return "fct"                   # unknown variants: FCT's registry, default FCT


def parse_cerif(xml_bytes):
    """One SciPROJ CERIF record -> dict, or None if no citable reference."""
    root = ET.fromstring(xml_bytes)
    proj = root.find(".//c:Project", NS)
    if proj is None:
        return None

    funder_key = classify_funder(proj)

    ref = program = None
    for ident in proj.findall("c:Identifier", NS):
        t = (ident.get("type") or "").rsplit("#", 1)[-1]
        if t == "ProjectReference" and ref is None:
            ref = clean(ident.text)
        elif t == "FundingProgram" and program is None:
            program = clean(ident.text)
    if not ref:
        return None

    titles = {}
    for t in proj.findall("c:Title", NS):
        lang = t.get("{http://www.w3.org/XML/1998/namespace}lang") or "und"
        titles[lang] = clean(t.text)
    title = titles.get("pt") or titles.get("en") or next(iter(titles.values()), None)

    funding = proj.find(".//c:Funded/c:As/c:Funding", NS)
    amount_el = funding.find("c:Amount", NS) if funding is not None else None
    amount = clean(amount_el.text) if amount_el is not None else None
    if amount:
        try:
            amount = f"{float(amount):.2f}" if float(amount) > 0 else None
        except ValueError:
            amount = None

    coord = proj.find(".//c:Consortium/c:Coordinator/c:OrgUnit", NS)
    inst = inst_ror = inst_country = None
    if coord is not None:
        inst = clean(coord.findtext("c:Name", default=None, namespaces=NS))
        inst_ror = clean(coord.findtext("c:RORID", default=None, namespaces=NS))
        inst_country = norm_country(coord.findtext("c:Country", default=None, namespaces=NS))

    pi = proj.find(".//c:Team/c:PrincipalInvestigator/c:Person/c:PersonName", NS)
    pf, pg, pfam = split_pi(pi.text if pi is not None else None)

    subjects = [clean(s) for s in _SUBJECT_COMMENT.findall(xml_bytes.decode("utf-8", "replace"))]
    description = "; ".join(dict.fromkeys(s for s in subjects if s)) or None

    start = clean(proj.findtext("c:StartDate", default=None, namespaces=NS))
    end = clean(proj.findtext("c:EndDate", default=None, namespaces=NS))
    # a handful of records carry corrupt years like 0007/0140 — drop those dates
    start = start if start and re.match(r"^(19|20)\d{2}-", start) else None
    end = end if end and re.match(r"^(19|20)\d{2}-", end) else None

    return {
        "funder_key": funder_key,
        "funder_award_id": ref,
        "title": title,
        "pi_full": pf, "pi_given": pg, "pi_family": pfam,
        "institution": inst,
        "institution_ror": inst_ror,
        "institution_country": inst_country,
        "amount": amount,
        "currency": "EUR" if amount else None,
        "scheme": program,
        "start_date_raw": start,
        "end_date_raw": end,
        "description": description,
        "landing_page_url": SCIPROJ_LANDING,
    }


def load_sciproj(zip_path: Path, limit=None):
    recs, skipped, errors = {}, 0, 0
    with zipfile.ZipFile(zip_path) as z:
        names = [n for n in z.namelist() if n.endswith(".xml")]
        print(f"SciPROJ zip: {len(names)} XML records")
        for i, n in enumerate(names):
            if limit and len(recs) >= limit:
                break
            try:
                r = parse_cerif(z.read(n))
            except Exception:
                errors += 1
                continue
            if r is None:
                skipped += 1
                continue
            recs[(r["funder_key"], r["funder_award_id"].lower())] = r
            if i and i % 20000 == 0:
                print(f"  ...{i}/{len(names)} parsed ({len(recs)} kept)")
    print(f"SciPROJ parsed: kept {len(recs)}, skipped {skipped} (no-ref), errors {errors}")
    return recs


def load_xlsx_archive(path: Path | None):
    if path is None:
        path = Path("/tmp/fct_data") / "fct_grants_xlsx_archive.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Downloading XLSX-era archive: {XLSX_ARCHIVE_S3}")
        subprocess.run(["aws", "s3", "cp", XLSX_ARCHIVE_S3, str(path)],
                       capture_output=True, text=True, check=True)
    df = pd.read_parquet(path)
    print(f"XLSX-era archive: {len(df)} rows")
    return df


def merge(sciproj: dict, xlsx: pd.DataFrame):
    out, overlap = [], 0
    seen = set()
    for _, row in xlsx.iterrows():           # XLSX rows are all FCT-funded
        ref = clean(row["funder_award_id"])
        if not ref or ("fct", ref.lower()) in seen:
            continue
        seen.add(("fct", ref.lower()))
        rec = {c: clean(row.get(c)) for c in (
            "funder_award_id", "title", "pi_full", "pi_given", "pi_family", "institution",
            "amount", "currency", "scheme", "start_date_raw", "end_date_raw",
            "description", "landing_page_url")}
        rec["funder_key"] = "fct"
        rec.setdefault("institution_ror", None)
        rec.setdefault("institution_country", None)
        sp = sciproj.get(("fct", ref.lower()))
        if sp:
            overlap += 1
            for k, v in sp.items():           # XLSX wins; SciPROJ fills the gaps
                if rec.get(k) is None and v is not None:
                    rec[k] = v
        rec["funding_type"] = funding_type(ref, rec.get("scheme"))
        if rec.get("institution_country") is None and rec.get("institution"):
            rec["institution_country"] = "Portugal"   # XLSX rows are host institutions in PT
        out.append(rec)
    for key, sp in sciproj.items():
        if key in seen:
            continue
        sp["funding_type"] = (funding_type(sp["funder_award_id"], sp.get("scheme"))
                              if sp["funder_key"] == "fct" else "grant")
        out.append(sp)
    print(f"Merged: {len(out)} rows ({overlap} XLSX∩SciPROJ, "
          f"{len(xlsx) - overlap} XLSX-only, {len(out) - len(xlsx)} SciPROJ-only)")
    return out


def upload_to_s3(local_path: Path) -> bool:
    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"\nUploading to {s3_uri}...")
    try:
        subprocess.run(["aws", "s3", "cp", str(local_path), s3_uri],
                       capture_output=True, text=True, check=True)
        print(f"Upload complete: {s3_uri}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e.stderr}")
        return False


def main():
    ap = argparse.ArgumentParser(description="FCT (SciPROJ dump + legacy XLSX) to S3")
    ap.add_argument("--sciproj-zip", type=Path, required=True,
                    help="SciPROJ CERIF dump zip (getDumpXML*.zip)")
    ap.add_argument("--xlsx-parquet", type=Path, default=None,
                    help="archived XLSX-era parquet (default: fetch from S3 archive)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/fct_data"))
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--allow-shrink", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("FCT (Fundação para a Ciência e a Tecnologia) -> S3")
    print("=" * 60)

    sciproj = load_sciproj(args.sciproj_zip, args.limit)
    xlsx = load_xlsx_archive(args.xlsx_parquet)
    recs = merge(sciproj, xlsx)

    out_df = pd.DataFrame(recs).astype("string")
    print(f"\nDataFrame: {len(out_df)} rows, {len(out_df.columns)} columns")
    print("funder_key breakdown:", out_df["funder_key"].value_counts().to_dict())
    for c in ("title", "pi_family", "institution", "institution_ror", "amount",
              "scheme", "start_date_raw", "end_date_raw", "description", "funding_type"):
        nn = out_df[c].notna().sum()
        print(f"  {c:20}: {nn}/{len(out_df)} ({round(100 * nn / max(len(out_df), 1))}%)")

    if len(out_df) < 90000 and not args.allow_shrink and not args.limit:
        print(f"[ERROR] only {len(out_df)} rows — expected ~99k; source changed/truncated?")
        sys.exit(1)

    out = args.output_dir / "fct_grants.parquet"
    out_df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e6:.1f} MB)")

    if not args.skip_upload:
        if not upload_to_s3(out):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
