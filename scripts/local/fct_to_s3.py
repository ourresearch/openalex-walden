#!/usr/bin/env python3
"""
FCT (Fundação para a Ciência e a Tecnologia, Portugal) -> S3 Data Pipeline
==========================================================================

Downloads FCT's R&D-projects list (XLSX) and uploads a parquet to S3 for
Databricks ingestion. FCT is OpenAlex funder F4320334779 (ROR 00snfqn58, DOI
10.13039/501100001871); existing award coverage is Crossref-derived only, so this
dedicated ingest adds canonical project metadata.

Data source: the "Projetos de I&D" list on fct.pt (linked from
https://www.fct.pt/en/sobre/a-fct-em-numeros/projetos-de-id/):
    https://www.fct.pt/wp-content/uploads/2023/04/Lista-Projetos-ID.xlsx
    Single sheet "Export", 7,569 projects, 13 columns (Portuguese), all core fields
    ~100% filled: Referência FCT (native id, e.g. 2025.00366.AZO), Título, Investigador
    Responsável (PI, a person), Instituição Proponente (host), Financiamento Concedido
    (EUR), Concurso (call), Domínio Científico, Data início / Data fim, Palavras-chave,
    Data aprovação, DOI (28%).
    NOTE: the server truncates the file over curl (HTTP2/chunked-encoding issue) — use
    python `requests`, which receives the full ~1.5 MB body.

Output: s3://openalex-ingest/awards/fct/fct_grants.parquet
"""

import argparse
import io
import re
import subprocess
import sys
from pathlib import Path

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
XLSX_URL = "https://www.fct.pt/wp-content/uploads/2023/04/Lista-Projetos-ID.xlsx"
LANDING = "https://www.fct.pt/en/sobre/a-fct-em-numeros/projetos-de-id/"
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fct/fct_grants.parquet"
_NULLISH = {"", "n/a", "na", "-", "—", "/", "nan", "none"}


def clean(v):
    if v is None:
        return None
    s = re.sub(r"\s+", " ", str(v)).strip()
    return None if s.lower() in _NULLISH else s


def norm(name):
    return re.sub(r"\s+", " ", str(name)).strip().lower()


def col(df, *cands):
    cols = {norm(c): c for c in df.columns}
    for cand in cands:
        if cand in cols:
            return df[cols[cand]]
    for cand in cands:
        for n, orig in cols.items():
            if cand in n:
                return df[orig]
    return pd.Series([None] * len(df))


def parse_amount(v):
    s = clean(v)
    if not s:
        return None
    num = re.sub(r"[^\d.]", "", s.replace(",", ""))
    try:
        f = float(num)
        return f"{f:.2f}" if f > 0 else None
    except ValueError:
        return None


def parse_date(v):
    s = clean(v)
    if not s:
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        return None if pd.isna(ts) else ts.strftime("%Y-%m-%d")
    except Exception:
        return None


def split_pi(name):
    name = clean(name)
    if not name or not re.search(r"[A-Za-zÀ-ÿ]{2}", name):
        return None, None, None
    parts = name.split()
    if len(parts) < 2:
        return name, None, name
    return name, " ".join(parts[:-1]), parts[-1]   # family = last token (PT order)


def download_xlsx(input_path):
    if input_path and input_path.exists():
        print(f"Reading local XLSX: {input_path}")
        return pd.ExcelFile(input_path)
    print(f"Downloading: {XLSX_URL}")
    last = None
    for attempt in range(4):
        try:
            r = requests.get(XLSX_URL, headers={"User-Agent": UA}, timeout=180)
            r.raise_for_status()
            print(f"  got {len(r.content)/1e6:.1f} MB")
            return pd.ExcelFile(io.BytesIO(r.content))
        except Exception as e:
            last = e
            print(f"  [retry {attempt+1}] {e}")
    raise RuntimeError(f"download failed: {last}")


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
    ap = argparse.ArgumentParser(description="FCT R&D projects to S3")
    ap.add_argument("--input", type=Path, default=None, help="local XLSX (optional)")
    ap.add_argument("--output-dir", type=Path, default=Path("/tmp/fct_data"))
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("FCT (Fundação para a Ciência e a Tecnologia) -> S3")
    print("=" * 60)

    xl = download_xlsx(args.input)
    sheet = xl.sheet_names[0]
    df = pd.read_excel(xl, sheet_name=sheet)
    print(f"Parsed {len(df)} rows, {len(df.columns)} columns (sheet {sheet!r})")

    c_ref = col(df, "referência fct", "referencia fct")
    c_title = col(df, "titulo", "título")
    c_pi = col(df, "investigador responsável", "investigador responsavel")
    c_inst = col(df, "instituição proponente", "instituicao proponente")
    c_amt = col(df, "financiamento concedido")
    c_call = col(df, "concurso")
    c_domain = col(df, "domínio científico", "dominio cientifico")
    c_kw = col(df, "palavras-chave", "palavras chave")
    c_start = col(df, "data início", "data inicio")
    c_end = col(df, "data fim")

    recs, seen = [], set()
    for i in range(len(df)):
        ref = clean(c_ref.iloc[i])
        if not ref or ref in seen:
            continue
        seen.add(ref)
        pf, pg, pfam = split_pi(c_pi.iloc[i])
        kw = clean(c_kw.iloc[i])
        if kw:
            kw = kw.replace("-*-", "; ")   # FCT keyword separator
        recs.append({
            "funder_award_id": ref,
            "title": clean(c_title.iloc[i]),
            "pi_full": pf, "pi_given": pg, "pi_family": pfam,
            "institution": clean(c_inst.iloc[i]),
            "amount": parse_amount(c_amt.iloc[i]),
            "currency": "EUR",
            "scheme": clean(c_call.iloc[i]),
            "start_date_raw": parse_date(c_start.iloc[i]),
            "end_date_raw": parse_date(c_end.iloc[i]),
            "description": kw,
            "landing_page_url": LANDING,
        })

    out_df = pd.DataFrame(recs).astype("string")
    print(f"\nDataFrame: {len(out_df)} rows, {len(out_df.columns)} columns")
    for c in ("title", "pi_family", "institution", "amount", "scheme",
              "start_date_raw", "end_date_raw", "description"):
        nn = out_df[c].notna().sum()
        print(f"  {c:16}: {nn}/{len(out_df)} ({round(100 * nn / max(len(out_df), 1))}%)")

    if len(out_df) < 6000:
        print(f"[ERROR] only {len(out_df)} rows — expected ~7,569; source changed/truncated?")
        sys.exit(1)

    out = args.output_dir / "fct_grants.parquet"
    out_df.to_parquet(out, index=False)
    print(f"\nWrote {out} ({out.stat().st_size / 1e3:.0f} KB)")

    if not args.skip_upload:
        if not upload_to_s3(out):
            print(f"\n[WARNING] manual: aws s3 cp {out} s3://{S3_BUCKET}/{S3_KEY}")
            sys.exit(1)
    print("\nPipeline complete!")


if __name__ == "__main__":
    main()
