#!/usr/bin/env python3
"""
ANR (Agence Nationale de la Recherche) to S3 Data Pipeline
===========================================================

This script downloads all grants from the French National Research Agency (ANR)
open data portal, processes them into a parquet file, and uploads to S3 for
Databricks ingestion.

Data Source: https://www.data.gouv.fr/organizations/agence-nationale-de-la-recherche/datasets
    - ANR_01: DGDS projects (2005-2009 and 2010+) - Science directorate
    - ANR_02: DGPIE projects (2010+) - Major investment directorate

Output: s3://openalex-ingest/awards/anr/anr_projects.parquet

What this script does:
1. Downloads CSV files for projects and partners from both ANR datasets
2. Joins projects with partners data to get PI information
3. Combines all sources into a unified DataFrame
4. Converts to parquet format
5. Uploads to S3

Requirements:
    pip install pandas pyarrow requests

    AWS CLI must be configured with credentials that have write access to:
    s3://openalex-ingest/awards/anr/

Usage:
    python anr_to_s3.py

    # Or with options:
    python anr_to_s3.py --output-dir /path/to/output --skip-upload

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

# ANR data source URLs (from data.gouv.fr)
# DGDS = Direction Générale Déléguée à la Science (Science directorate)
# DGPIE = Direction Générale des Programmes d'Investissement de l'État (Major investments)

DATA_SOURCES = {
    # DGDS 2010+ (main dataset, largest)
    "dgds_2010_projects": "https://static.data.gouv.fr/resources/anr-01-projets-anr-dos-et-dgds-detail-des-projets-et-des-partenaires/20260108-110947/anr-dgds-depuis-2010-projets-finances-20260108-projets.csv",
    "dgds_2010_partners": "https://static.data.gouv.fr/resources/anr-01-projets-anr-dos-et-dgds-detail-des-projets-et-des-partenaires/20260108-111007/anr-dgds-depuis-2010-projets-finances-20260108-partenaires.csv",
    # DGDS 2005-2009 (historical)
    "dgds_2005_projects": "https://static.data.gouv.fr/resources/anr-01-projets-anr-dos-et-dgds-detail-des-projets-et-des-partenaires/20251105-125442/anr-dgds-2005-2009-projets-finances-20251105-projets.csv",
    "dgds_2005_partners": "https://static.data.gouv.fr/resources/anr-01-projets-anr-dos-et-dgds-detail-des-projets-et-des-partenaires/20251105-125445/anr-dgds-2005-2009-projets-finances-20251105-partenaires.csv",
    # DGPIE 2010+ (major investments - PIA, etc.)
    "dgpie_projects": "https://static.data.gouv.fr/resources/anr-02-projets-anr-dgpie-detail-des-projets-et-des-partenaires/20260108-104403/anr-dgpie-depuis-2010-projets-finances-20260108-projets.csv",
    "dgpie_partners": "https://static.data.gouv.fr/resources/anr-02-projets-anr-dgpie-detail-des-projets-et-des-partenaires/20260108-104405/anr-dgpie-depuis-2010-projets-finances-20260108-partenaires.csv",
}

# S3 destination
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/anr/anr_projects.parquet"


# =============================================================================
# Download Functions
# =============================================================================

def download_file(url: str, output_path: Path, description: str) -> bool:
    """
    Download a file from URL.

    Args:
        url: URL to download from
        output_path: Path to save file
        description: Description for logging

    Returns:
        True if download succeeded
    """
    print(f"  [DOWNLOAD] {description}")
    print(f"    URL: {url}")

    try:
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()

        # Write to file
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"    [SUCCESS] Downloaded {size_mb:.1f} MB")
        return True

    except requests.exceptions.RequestException as e:
        print(f"    [ERROR] Download failed: {e}")
        return False


def download_all_data(output_dir: Path) -> dict[str, Path]:
    """
    Download all ANR data files.

    Args:
        output_dir: Directory to save files

    Returns:
        Dictionary mapping source name to file path
    """
    print(f"\n{'='*60}")
    print("Step 1: Downloading ANR data files")
    print(f"{'='*60}")

    files = {}
    for name, url in DATA_SOURCES.items():
        output_path = output_dir / f"{name}.csv"

        # Skip if already exists
        if output_path.exists():
            size_mb = output_path.stat().st_size / (1024 * 1024)
            print(f"  [SKIP] {name} already exists ({size_mb:.1f} MB)")
            files[name] = output_path
            continue

        success = download_file(url, output_path, name)
        if success:
            files[name] = output_path
        else:
            print(f"  [WARN] Skipping {name} due to download failure")

    print(f"\n  Downloaded {len(files)}/{len(DATA_SOURCES)} files")
    return files


# =============================================================================
# Processing Functions
# =============================================================================

def load_csv_safe(path: Path) -> pd.DataFrame:
    """
    Load CSV with safe encoding handling.

    ANR data uses semicolons as delimiters.

    Args:
        path: Path to CSV file

    Returns:
        DataFrame
    """
    # Try different encodings with semicolon delimiter (French CSV format)
    for encoding in ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1']:
        try:
            df = pd.read_csv(path, dtype=str, encoding=encoding, low_memory=False, sep=';')
            return df
        except UnicodeDecodeError:
            continue

    # Last resort
    return pd.read_csv(path, dtype=str, encoding='latin-1', errors='replace', low_memory=False, sep=';')


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names to lowercase with underscores.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with normalized column names
    """
    df.columns = (df.columns
                  .str.lower()
                  .str.strip()
                  .str.replace(' ', '_')
                  .str.replace('é', 'e')
                  .str.replace('è', 'e')
                  .str.replace('ê', 'e')
                  .str.replace('à', 'a')
                  .str.replace('ô', 'o')
                  .str.replace("'", '_')
                  .str.replace('(', '')
                  .str.replace(')', ''))
    return df


def process_projects_and_partners(
    projects_path: Path,
    partners_path: Optional[Path],
    source_name: str
) -> pd.DataFrame:
    """
    Process projects CSV and join with partners data.

    Args:
        projects_path: Path to projects CSV
        partners_path: Path to partners CSV (optional)
        source_name: Name of the data source for provenance

    Returns:
        DataFrame with projects and PI information
    """
    print(f"\n  Processing {source_name}...")

    # Load projects
    projects_df = load_csv_safe(projects_path)
    projects_df = normalize_column_names(projects_df)
    print(f"    Projects: {len(projects_df):,} rows, {len(projects_df.columns)} columns")
    print(f"    Columns: {list(projects_df.columns)}")

    # Find the project ID column (varies between datasets)
    id_col = None
    for col in ['code_decision_anr', 'code_projet_anr', 'projet_code_decision_anr', 'projet_id']:
        if col in projects_df.columns:
            id_col = col
            break

    if id_col is None:
        # Try partial match
        for col in projects_df.columns:
            if 'code' in col.lower() and ('decision' in col.lower() or 'projet' in col.lower()):
                id_col = col
                break

    if id_col is None:
        print(f"    [WARN] Could not find project ID column, using first column")
        id_col = projects_df.columns[0]

    print(f"    Using '{id_col}' as project ID column")

    # Load and join partners if available
    if partners_path and partners_path.exists():
        partners_df = load_csv_safe(partners_path)
        partners_df = normalize_column_names(partners_df)
        print(f"    Partners: {len(partners_df):,} rows")

        # Find matching ID column in partners
        partner_id_col = None
        for col in partners_df.columns:
            if col == id_col or (col.startswith('projet_') and id_col in col) or ('code_decision' in col):
                partner_id_col = col
                break

        if partner_id_col is None:
            for col in partners_df.columns:
                if 'code' in col.lower() and ('decision' in col.lower() or 'projet' in col.lower()):
                    partner_id_col = col
                    break

        if partner_id_col:
            print(f"    Using '{partner_id_col}' as partner join column")

            # Get lead PI (coordinateur or first partner)
            # Look for role columns
            role_col = None
            for col in partners_df.columns:
                if 'role' in col.lower() or 'type' in col.lower():
                    role_col = col
                    break

            if role_col:
                # Filter for coordinators/PIs
                lead_partners = partners_df[
                    partners_df[role_col].str.lower().str.contains('coord|principal|responsable|porteur', na=False)
                ].drop_duplicates(subset=[partner_id_col], keep='first')

                if len(lead_partners) == 0:
                    # Fall back to first partner per project
                    lead_partners = partners_df.drop_duplicates(subset=[partner_id_col], keep='first')
            else:
                # Just take first partner per project
                lead_partners = partners_df.drop_duplicates(subset=[partner_id_col], keep='first')

            print(f"    Lead partners: {len(lead_partners):,}")

            # Rename partner columns to avoid conflicts
            partner_cols = {col: f"partner_{col}" for col in lead_partners.columns if col != partner_id_col}
            lead_partners = lead_partners.rename(columns=partner_cols)

            # Join
            projects_df = projects_df.merge(
                lead_partners,
                left_on=id_col,
                right_on=partner_id_col,
                how='left'
            )
        else:
            print(f"    [WARN] Could not find matching ID column in partners")

    # Add source identifier
    projects_df['data_source'] = source_name

    return projects_df


def combine_and_transform(dataframes: list[pd.DataFrame], output_dir: Path) -> Path:
    """
    Combine all project dataframes and transform to output schema.

    Args:
        dataframes: List of project DataFrames
        output_dir: Directory to save output

    Returns:
        Path to output parquet file
    """
    print(f"\n{'='*60}")
    print("Step 2: Combining and transforming data")
    print(f"{'='*60}")

    # Combine all dataframes
    df = pd.concat(dataframes, ignore_index=True)
    print(f"  Combined rows: {len(df):,}")

    # Print all available columns for debugging
    print(f"\n  Available columns ({len(df.columns)}):")
    for col in sorted(df.columns):
        non_null = df[col].notna().sum()
        print(f"    - {col}: {non_null:,} non-null values")

    # Map to standardized schema based on actual ANR column names
    # Columns from ANR data:
    # - projet.code_decision: unique project ID (e.g., "ANR-25-W4A2-0004")
    # - projet.titre.francais / projet.titre.anglais: titles
    # - projet.resume.francais / projet.resume.anglais: abstracts
    # - projet.montant.af.aide_allouee.anr / projet.aide_allouee: funding amount
    # - projet.t0_scientifique / projet.date_debut: start date
    # - programme.acronyme / action.titre.francais: funding program
    # - partner_projet.partenaire.responsable_scientifique.nom/.prenom/.orcid: PI info
    # - partner_projet.partenaire.nom_organisme: institution
    # - partner_projet.partenaire.adresse.pays: country

    # Project ID (consistent across all datasets)
    df['project_id'] = df['projet.code_decision']

    # Acronym
    if 'projet.acronyme' in df.columns:
        df['acronym'] = df['projet.acronyme']

    # Title - prefer English, fallback to French
    if 'projet.titre.anglais' in df.columns and 'projet.titre.francais' in df.columns:
        df['title'] = df['projet.titre.anglais'].fillna(df['projet.titre.francais'])
    elif 'projet.titre.francais' in df.columns:
        df['title'] = df['projet.titre.francais']

    # Abstract - prefer English, fallback to French
    if 'projet.resume.anglais' in df.columns and 'projet.resume.francais' in df.columns:
        df['abstract'] = df['projet.resume.anglais'].fillna(df['projet.resume.francais'])
    elif 'projet.resume.francais' in df.columns:
        df['abstract'] = df['projet.resume.francais']

    # Amount - DGDS uses projet.montant.af.aide_allouee.anr, DGPIE uses projet.aide_allouee
    amount_cols = ['projet.montant.af.aide_allouee.anr', 'projet.aide_allouee']
    df['amount'] = None
    for col in amount_cols:
        if col in df.columns:
            # Clean and convert
            temp = df[col].astype(str).str.replace(' ', '', regex=False).str.replace(',', '.', regex=False)
            temp = pd.to_numeric(temp, errors='coerce')
            df['amount'] = df['amount'].fillna(temp)

    # Start date - DGDS uses projet.t0_scientifique (YYYY-MM-DD format), DGPIE uses projet.date_debut
    start_cols = ['projet.t0_scientifique', 'projet.date_debut']
    df['start_date'] = None
    for col in start_cols:
        if col in df.columns:
            temp = pd.to_datetime(df[col], errors='coerce', format='%Y-%m-%d')
            if df['start_date'] is None:
                df['start_date'] = temp
            else:
                df['start_date'] = df['start_date'].fillna(temp)
    df['start_date'] = df['start_date'].dt.strftime('%Y-%m-%d')
    df['start_date'] = df['start_date'].replace('NaT', None)

    # Year from edition column (backup for year info)
    year_cols = ['aap.edition', 'action.edition']
    df['edition_year'] = None
    for col in year_cols:
        if col in df.columns:
            temp = pd.to_numeric(df[col], errors='coerce')
            if df['edition_year'] is None:
                df['edition_year'] = temp
            else:
                df['edition_year'] = df['edition_year'].fillna(temp)

    # Funding program - DGDS uses programme.acronyme, DGPIE uses action.titre.francais
    program_cols = ['programme.acronyme', 'action.titre.francais']
    df['funding_program'] = None
    for col in program_cols:
        if col in df.columns:
            if df['funding_program'] is None:
                df['funding_program'] = df[col]
            else:
                df['funding_program'] = df['funding_program'].fillna(df[col])

    # PI information from partner data
    if 'partner_projet.partenaire.responsable_scientifique.nom' in df.columns:
        df['pi_family_name'] = df['partner_projet.partenaire.responsable_scientifique.nom']
    if 'partner_projet.partenaire.responsable_scientifique.prenom' in df.columns:
        df['pi_given_name'] = df['partner_projet.partenaire.responsable_scientifique.prenom']
    if 'partner_projet.partenaire.responsable_scientifique.orcid' in df.columns:
        df['pi_orcid'] = df['partner_projet.partenaire.responsable_scientifique.orcid']

    # Institution
    if 'partner_projet.partenaire.nom_organisme' in df.columns:
        df['institution'] = df['partner_projet.partenaire.nom_organisme']

    # Institution category
    if 'partner_projet.partenaire.categorie_organisme' in df.columns:
        df['institution_type'] = df['partner_projet.partenaire.categorie_organisme']

    # Country
    if 'partner_projet.partenaire.adresse.pays' in df.columns:
        df['country'] = df['partner_projet.partenaire.adresse.pays']
    else:
        df['country'] = 'France'

    # City
    if 'partner_projet.partenaire.adresse.ville' in df.columns:
        df['city'] = df['partner_projet.partenaire.adresse.ville']

    # RNSR code (French research structure identifier)
    if 'partner_projet.partenaire.code_rnsr' in df.columns:
        df['rnsr_code'] = df['partner_projet.partenaire.code_rnsr']

    # Remove duplicates by project_id
    print(f"\n  Deduplicating by project_id...")
    original_count = len(df)
    df = df.drop_duplicates(subset=['project_id'], keep='first')
    print(f"  Removed {original_count - len(df):,} duplicates")
    print(f"  Unique projects: {len(df):,}")

    # Select final columns
    output_columns = [
        'project_id',
        'acronym',
        'title',
        'abstract',
        'amount',
        'start_date',
        'edition_year',
        'funding_program',
        'institution',
        'institution_type',
        'city',
        'country',
        'pi_given_name',
        'pi_family_name',
        'pi_orcid',
        'rnsr_code',
        'data_source',
    ]

    # Only keep columns that exist
    output_columns = [col for col in output_columns if col in df.columns]
    df_output = df[output_columns].copy()

    # Add metadata
    df_output['ingested_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # Save to parquet
    output_path = output_dir / "anr_projects.parquet"
    print(f"\n  [SAVE] Writing to {output_path.name}...")
    df_output.to_parquet(output_path, index=False)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"  Output file size: {size_mb:.1f} MB")

    # Print summary stats
    print(f"\n  Summary:")
    print(f"    - Total projects: {len(df_output):,}")
    if 'title' in df_output.columns:
        print(f"    - With title: {df_output['title'].notna().sum():,}")
    if 'abstract' in df_output.columns:
        print(f"    - With abstract: {df_output['abstract'].notna().sum():,}")
    if 'amount' in df_output.columns:
        print(f"    - With amount: {df_output['amount'].notna().sum():,}")
        total_amount = df_output['amount'].sum()
        print(f"    - Total funding: €{total_amount:,.0f}")
    if 'start_date' in df_output.columns:
        print(f"    - With start_date: {df_output['start_date'].notna().sum():,}")
    if 'pi_family_name' in df_output.columns:
        print(f"    - With PI: {df_output['pi_family_name'].notna().sum():,}")
    if 'pi_orcid' in df_output.columns:
        print(f"    - With ORCID: {df_output['pi_orcid'].notna().sum():,}")
    if 'institution' in df_output.columns:
        print(f"    - With institution: {df_output['institution'].notna().sum():,}")

    if 'data_source' in df_output.columns:
        print(f"\n  Projects by source:")
        print(df_output['data_source'].value_counts().to_string())

    if 'funding_program' in df_output.columns:
        print(f"\n  Top funding programs:")
        print(df_output['funding_program'].value_counts().head(15).to_string())

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
        description="Download ANR grants and upload to S3"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./anr_data"),
        help="Directory for downloaded/processed files (default: ./anr_data)"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing CSV files)"
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
    print("ANR (Agence Nationale de la Recherche) to S3 Data Pipeline")
    print("=" * 60)
    print(f"Output directory: {args.output_dir.absolute()}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_KEY}")

    # Step 1: Download
    if not args.skip_download:
        files = download_all_data(args.output_dir)
    else:
        # Use existing files
        files = {}
        for name in DATA_SOURCES.keys():
            path = args.output_dir / f"{name}.csv"
            if path.exists():
                files[name] = path
        print(f"\n  [SKIP] Using {len(files)} existing CSV files")

    if not files:
        print("[ERROR] No data files available!")
        sys.exit(1)

    # Step 2: Process each dataset
    dataframes = []

    # Process DGDS 2010+
    if 'dgds_2010_projects' in files:
        df = process_projects_and_partners(
            files['dgds_2010_projects'],
            files.get('dgds_2010_partners'),
            'dgds_2010'
        )
        dataframes.append(df)

    # Process DGDS 2005-2009
    if 'dgds_2005_projects' in files:
        df = process_projects_and_partners(
            files['dgds_2005_projects'],
            files.get('dgds_2005_partners'),
            'dgds_2005'
        )
        dataframes.append(df)

    # Process DGPIE
    if 'dgpie_projects' in files:
        df = process_projects_and_partners(
            files['dgpie_projects'],
            files.get('dgpie_partners'),
            'dgpie'
        )
        dataframes.append(df)

    if not dataframes:
        print("[ERROR] No project data to process!")
        sys.exit(1)

    # Step 3: Combine and transform
    parquet_path = combine_and_transform(dataframes, args.output_dir)

    # Step 4: Upload to S3
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
    print(f"  In Databricks, run: notebooks/awards/CreateANRAwards.ipynb")


if __name__ == "__main__":
    main()
