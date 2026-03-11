#!/usr/bin/env python3
"""
FAPESP (São Paulo Research Foundation) to S3 Data Pipeline
===========================================================

This script downloads all research grants from FAPESP's Virtual Library,
processes them into a parquet file, and uploads to S3 for Databricks ingestion.

Data Source: https://bv.fapesp.br/en/
- Ongoing grants: https://media.fapesp.br/bv/uploads/gera_csv_assincrono/csv/auxilios_em_andamento.csv
- Completed grants: https://media.fapesp.br/bv/uploads/gera_csv_assincrono/csv/auxilios_concluidos.csv

Output: s3://openalex-ingest/awards/fapesp/fapesp_projects.parquet

What this script does:
1. Downloads both ongoing and completed grants CSVs from FAPESP
2. Combines and parses the semicolon-delimited CSVs
3. Maps Portuguese column names to English
4. Converts to parquet format with Spark-compatible types
5. Uploads to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/fapesp/

Usage:
    python fapesp_to_s3.py

    # Or with options:
    python fapesp_to_s3.py --output-dir /path/to/output --skip-upload

Author: OpenAlex Team
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# =============================================================================
# Configuration
# =============================================================================

# FAPESP CSV download URLs
FAPESP_ONGOING_URL = "https://media.fapesp.br/bv/uploads/gera_csv_assincrono/csv/auxilios_em_andamento.csv"
FAPESP_COMPLETED_URL = "https://media.fapesp.br/bv/uploads/gera_csv_assincrono/csv/auxilios_concluidos.csv"

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/fapesp/fapesp_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_csv(url: str, output_path: Path, description: str) -> int:
    """
    Download a CSV file from FAPESP.

    Args:
        url: URL to download from
        output_path: Path to save the downloaded file
        description: Description for logging

    Returns:
        Number of rows (excluding header)
    """
    print(f"  [DOWNLOAD] {description}")
    print(f"    URL: {url}")

    response = requests.get(url, timeout=300)
    response.raise_for_status()

    # Save to file
    with open(output_path, 'wb') as f:
        f.write(response.content)

    size_mb = output_path.stat().st_size / (1024 * 1024)

    # Count rows (subtract 1 for header)
    row_count = response.content.count(b'\n')

    print(f"    Size: {size_mb:.1f} MB, Rows: {row_count:,}")
    return row_count


def download_all_csvs(output_dir: Path) -> tuple[Path, Path]:
    """
    Download both FAPESP CSV files.

    Args:
        output_dir: Directory to save the downloaded files

    Returns:
        Tuple of paths to (ongoing, completed) CSV files
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading FAPESP grants CSVs")
    print(f"{'='*60}")

    ongoing_path = output_dir / "auxilios_em_andamento.csv"
    completed_path = output_dir / "auxilios_concluidos.csv"

    ongoing_rows = download_csv(FAPESP_ONGOING_URL, ongoing_path, "Ongoing grants")
    completed_rows = download_csv(FAPESP_COMPLETED_URL, completed_path, "Completed grants")

    print(f"\n  [SUCCESS] Downloaded {ongoing_rows + completed_rows:,} total rows")

    return ongoing_path, completed_path


# =============================================================================
# Processing Functions
# =============================================================================

def process_csvs(ongoing_path: Path, completed_path: Path, output_dir: Path) -> Path:
    """
    Process the FAPESP CSVs into a combined parquet file.

    The CSVs are semicolon-delimited with Portuguese column names.

    Args:
        ongoing_path: Path to ongoing grants CSV
        completed_path: Path to completed grants CSV
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 2: Processing CSVs")
    print(f"{'='*60}")

    # Read CSVs (semicolon-delimited, UTF-8 with BOM)
    print(f"  [READ] {ongoing_path.name}")
    df_ongoing = pd.read_csv(ongoing_path, sep=';', dtype=str, encoding='utf-8-sig')
    df_ongoing['status'] = 'ongoing'
    print(f"    Rows: {len(df_ongoing):,}")

    print(f"  [READ] {completed_path.name}")
    df_completed = pd.read_csv(completed_path, sep=';', dtype=str, encoding='utf-8-sig')
    df_completed['status'] = 'completed'
    print(f"    Rows: {len(df_completed):,}")

    # Combine
    df = pd.concat([df_ongoing, df_completed], ignore_index=True)
    print(f"\n  Combined rows: {len(df):,}")

    # Map Portuguese column names to English
    column_mapping = {
        'N. Processo': 'process_number',
        'Título (Português)': 'title_pt',
        'Título (Inglês)': 'title_en',
        'Beneficiário': 'beneficiary',
        'Instituição': 'institution',
        'Cidade Instituição': 'institution_city',
        'Instituição Parceira': 'partner_institution',
        'Empresa': 'company',
        'Município': 'municipality',
        'Pesquisador Responsável': 'principal_investigator',
        'Pesquisadores Principais': 'main_researchers',
        'Pesquisadores Associados': 'associated_researchers',
        'Supervisor': 'supervisor',
        'Local de Pesquisa': 'research_location',
        'Pesquisador Visitante': 'visiting_researcher',
        'Instituição do Pesquisador Visitante': 'visiting_researcher_institution',
        'Modalidade de apoio': 'support_modality',
        'Grande Área do Conhecimento': 'major_knowledge_area',
        'Área do Conhecimento': 'knowledge_area',
        'Subárea do Conhecimento': 'knowledge_subarea',
        'Assuntos': 'subjects',
        'Data de Início': 'start_date',
        'Data de Término': 'end_date',
        'Acordo(s)/Convênio(s) de Cooperação com a FAPESP': 'cooperation_agreements',
        'Instituições no Exterior': 'foreign_institutions',
        'Pais de Origem': 'country_of_origin',
        'Pesquisador responsável no exterior': 'pi_abroad',
        'Resumo (Português)': 'abstract_pt',
        'Resumo (Inglês)': 'abstract_en',
        'Processos Vinculados': 'linked_processes',
    }
    df = df.rename(columns=column_mapping)

    # Parse dates (format: YYYY-MM-DD)
    print("  [INFO] Parsing dates...")
    for date_col in ['start_date', 'end_date']:
        if date_col in df.columns:
            # Try parsing as YYYY-MM-DD
            df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d', errors='coerce')
            df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
            df[date_col] = df[date_col].replace('NaT', None)

    # Extract PI name from beneficiary or principal_investigator
    # The beneficiary field seems to contain the main person's name
    print("  [INFO] Processing PI names...")
    df['pi_name'] = df['beneficiary'].fillna(df['principal_investigator'])

    # Try to split PI name into given/family (Brazilian names typically have family name last)
    def split_name(name):
        if pd.isna(name) or not name:
            return None, None
        parts = name.strip().split()
        if len(parts) == 1:
            return None, parts[0]
        elif len(parts) == 2:
            return parts[0], parts[1]
        else:
            # First name is given, rest is family (common for Brazilian names)
            return parts[0], ' '.join(parts[1:])

    df[['pi_given_name', 'pi_family_name']] = df['pi_name'].apply(
        lambda x: pd.Series(split_name(x))
    )

    # Remove duplicates by process_number (keep first)
    print("  [INFO] Deduplicating by process_number...")
    original_count = len(df)
    df = df.drop_duplicates(subset=['process_number'], keep='first')
    print(f"    Removed {original_count - len(df):,} duplicates")
    print(f"    Unique grants: {len(df):,}")

    # Add metadata
    df['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Select and order columns for output
    output_columns = [
        'process_number',
        'title_pt',
        'title_en',
        'abstract_pt',
        'abstract_en',
        'beneficiary',
        'pi_name',
        'pi_given_name',
        'pi_family_name',
        'principal_investigator',
        'main_researchers',
        'associated_researchers',
        'supervisor',
        'visiting_researcher',
        'visiting_researcher_institution',
        'institution',
        'institution_city',
        'partner_institution',
        'company',
        'municipality',
        'research_location',
        'support_modality',
        'major_knowledge_area',
        'knowledge_area',
        'knowledge_subarea',
        'subjects',
        'start_date',
        'end_date',
        'status',
        'cooperation_agreements',
        'foreign_institutions',
        'country_of_origin',
        'pi_abroad',
        'linked_processes',
        'ingested_at',
    ]

    # Only include columns that exist
    output_columns = [c for c in output_columns if c in df.columns]
    df = df[output_columns]

    # Define explicit schema for Spark compatibility (all strings)
    schema_fields = [(col, pa.string()) for col in output_columns]
    schema = pa.schema(schema_fields)

    # Save to parquet with explicit schema
    output_path = output_dir / "fapesp_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")

    # Convert to pyarrow table with schema
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, output_path)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total grants: {len(df):,}")
    print(f"    - With title (PT): {df['title_pt'].notna().sum():,}")
    print(f"    - With title (EN): {df['title_en'].notna().sum():,}")
    print(f"    - With abstract (PT): {df['abstract_pt'].notna().sum():,}")
    print(f"    - With abstract (EN): {df['abstract_en'].notna().sum():,}")
    print(f"    - With start_date: {df['start_date'].notna().sum():,}")
    print(f"    - With PI name: {df['pi_name'].notna().sum():,}")
    print(f"    - Ongoing: {(df['status'] == 'ongoing').sum():,}")
    print(f"    - Completed: {(df['status'] == 'completed').sum():,}")

    if 'support_modality' in df.columns:
        print(f"\n  Support Modalities (top 15):")
        print(df['support_modality'].value_counts().head(15).to_string())

    if 'major_knowledge_area' in df.columns:
        print(f"\n  Major Knowledge Areas:")
        print(df['major_knowledge_area'].value_counts().to_string())

    if 'institution' in df.columns:
        print(f"\n  Institutions (top 10):")
        print(df['institution'].value_counts().head(10).to_string())

    return output_path


# =============================================================================
# S3 Upload
# =============================================================================

def find_aws_cli() -> Optional[str]:
    """Find AWS CLI executable path."""
    import shutil

    # Check if aws is in PATH
    aws_path = shutil.which("aws")
    if aws_path:
        return aws_path

    # Check common installation locations
    common_paths = [
        Path.home() / "Library/Python/3.9/bin/aws",
        Path.home() / "Library/Python/3.10/bin/aws",
        Path.home() / "Library/Python/3.11/bin/aws",
        Path.home() / "Library/Python/3.12/bin/aws",
        Path("/usr/local/bin/aws"),
        Path("/opt/homebrew/bin/aws"),
    ]

    for path in common_paths:
        if path.exists():
            return str(path)

    return None


def upload_to_s3(local_path: Path) -> bool:
    """
    Upload the parquet file to S3.

    Args:
        local_path: Path to local parquet file

    Returns:
        True if upload succeeded
    """
    print(f"\n{'='*60}")
    print("Step 3: Uploading to S3")
    print(f"{'='*60}")

    s3_uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    print(f"  [UPLOAD] {local_path.name} -> {s3_uri}")

    # Find AWS CLI
    aws_cmd = find_aws_cli()
    if not aws_cmd:
        print("  [ERROR] AWS CLI not found. Install with: pip install awscli")
        return False

    print(f"  [INFO] Using AWS CLI: {aws_cmd}")

    try:
        result = subprocess.run(
            [aws_cmd, "s3", "cp", str(local_path), s3_uri],
            capture_output=True,
            text=True,
            check=True
        )
        print("  [SUCCESS] Upload complete!")
        return True

    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] Upload failed: {e.stderr}")
        return False


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Download FAPESP grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./fapesp_data"),
        help="Directory for downloaded/processed files (default: ./fapesp_data)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing CSVs)"
    )
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Skip S3 upload step"
    )
    args = parser.parse_args()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("FAPESP (São Paulo Research Foundation) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if not args.skip_download:
        ongoing_path, completed_path = download_all_csvs(args.output_dir)
    else:
        ongoing_path = args.output_dir / "auxilios_em_andamento.csv"
        completed_path = args.output_dir / "auxilios_concluidos.csv"
        if not ongoing_path.exists() or not completed_path.exists():
            print(f"[ERROR] CSVs not found. Run without --skip-download")
            sys.exit(1)
        print(f"\n  [SKIP] Using existing CSVs")

    # Step 2: Process
    parquet_path = process_csvs(ongoing_path, completed_path, args.output_dir)

    # Step 3: Upload to S3
    upload_success = True
    if not args.skip_upload:
        upload_success = upload_to_s3(parquet_path)
        if not upload_success:
            print("\n[WARNING] S3 upload failed. You can upload manually:")
            print(f"  aws s3 cp {parquet_path} s3://{S3_BUCKET}/{S3_KEY}")

    print(f"\n{'='*60}")
    if upload_success or args.skip_upload:
        print("Pipeline complete!")
    else:
        print("Pipeline FAILED - S3 upload unsuccessful")
    print(f"{'='*60}")
    print(f"\nNext step:")
    print(f"  In Databricks, run: notebooks/awards/CreateFAPESPAwards.ipynb")


if __name__ == "__main__":
    main()