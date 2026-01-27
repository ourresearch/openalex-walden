#!/usr/bin/env python3
"""
Download ANID/FONDECYT (Chile) grants and upload to S3.

Data source: https://github.com/ANID-GITHUB/Historico-de-Proyectos-Adjudicados
Historical projects 1982-2025, ~47,000 grants.

Output: s3://openalex-ingest/awards/anid/anid_projects.parquet
"""

import io
from datetime import datetime, timezone

import boto3
import pandas as pd
import requests

# Configuration
S3_BUCKET = "openalex-ingest"
S3_KEY = "awards/anid/anid_projects.parquet"
CSV_URL = "https://raw.githubusercontent.com/ANID-GITHUB/Historico-de-Proyectos-Adjudicados/master/BDH_HISTORICA.csv"
OUTPUT_FILE = "/tmp/anid_projects.parquet"


def download_csv() -> pd.DataFrame:
    """Download and parse the ANID historical CSV."""
    print(f"[Download] Fetching {CSV_URL}")

    response = requests.get(CSV_URL, timeout=120)
    response.raise_for_status()

    # Parse CSV with semicolon separator, handle BOM
    df = pd.read_csv(
        io.StringIO(response.text),
        sep=';',
        encoding='utf-8-sig',  # Handles BOM
        low_memory=False
    )

    print(f"[Download] Loaded {len(df)} rows, {len(df.columns)} columns")
    print(f"[Columns] {list(df.columns)}")

    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform to standardized schema."""
    print("[Transform] Processing data...")

    # Rename columns to English/standard names
    column_map = {
        'N': 'row_number',
        'CODIGO_PROYECTO': 'project_code',
        'SUBDIRECCION': 'subdivision',
        'PROGRAMA': 'program',
        'INSTRUMENTO': 'instrument',
        'NOMBRE_CONCURSO': 'competition_name',
        'AGNO_CONCURSO': 'competition_year',
        'AGNO_FALLO': 'award_year',
        'NOMBRE_PROYECTO': 'title',
        'AREA_OCDE': 'oecd_area',
        'DISCIPLINA_DETALLE': 'discipline',
        'GRUPO_DE_EVALUACION': 'evaluation_group',
        'DURACION_MESES': 'duration_months',
        'TIPO_BENEFICIARIO': 'beneficiary_type',
        'NOMBRE_RESPONSABLE': 'pi_name',
        'SEXO': 'pi_gender',
        'INSTITUCION_PRINCIPAL': 'institution',
        'MACROZONA_MINCIENCIA': 'macro_zone',
        'REGION_EJECUCION': 'region',
        'MONTO_ADJUDICADO': 'amount',
        'NOTA_MONTO': 'amount_note',
        'MONEDA': 'currency_note',
        'PALABRAS_CLAVES': 'keywords'
    }

    df = df.rename(columns=column_map)

    # Clean amount - convert to numeric
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Set currency (Chilean Pesos - CLP, though historical data may be in "Miles de pesos")
    df['currency'] = 'CLP'

    # Parse PI name into given/family
    def parse_name(name):
        if pd.isna(name) or not name:
            return None, None
        parts = str(name).strip().split()
        if len(parts) >= 2:
            # Spanish naming: given names first, family names last
            # Typically: GIVEN GIVEN FAMILY FAMILY
            # Take first part as given, rest as family
            return parts[0], ' '.join(parts[1:])
        return name, None

    df[['pi_given_name', 'pi_family_name']] = df['pi_name'].apply(
        lambda x: pd.Series(parse_name(x))
    )

    # Create start_date from award_year
    df['start_year'] = pd.to_numeric(df['award_year'], errors='coerce').astype('Int64')

    # Calculate end_year based on duration
    df['duration_months'] = pd.to_numeric(df['duration_months'], errors='coerce')
    df['end_year'] = df.apply(
        lambda r: int(r['start_year'] + (r['duration_months'] / 12))
        if pd.notna(r['start_year']) and pd.notna(r['duration_months']) and r['duration_months'] > 0
        else None,
        axis=1
    ).astype('Int64')

    # Add metadata
    df['source_url'] = 'https://github.com/ANID-GITHUB/Historico-de-Proyectos-Adjudicados'
    df['scraped_at'] = datetime.now(timezone.utc).isoformat()

    # Map program to funding_type
    def map_funding_type(program):
        if pd.isna(program):
            return 'grant'
        program = str(program).upper()
        if 'POSTDOC' in program or 'DOCTORADO' in program:
            return 'fellowship'
        if 'EQUIPAMIENTO' in program:
            return 'equipment'
        if 'INFRAESTRUCTURA' in program:
            return 'infrastructure'
        return 'research'

    df['funding_type'] = df['program'].apply(map_funding_type)

    print(f"[Transform] Complete. {len(df)} rows processed")

    return df


def upload_to_s3(df: pd.DataFrame):
    """Upload DataFrame to S3 as parquet."""
    print(f"[S3] Saving to {OUTPUT_FILE}")
    df.to_parquet(OUTPUT_FILE, index=False)

    print(f"[S3] Uploading to s3://{S3_BUCKET}/{S3_KEY}")
    s3 = boto3.client("s3")
    s3.upload_file(OUTPUT_FILE, S3_BUCKET, S3_KEY)
    print("[S3] Upload complete")


def main():
    start_time = datetime.now()
    print(f"[Start] {start_time.isoformat()}")
    print("=" * 60)

    # Download
    df = download_csv()

    # Transform
    df = transform_data(df)

    # Stats
    print(f"\n[Stats]")
    print(f"  Total grants: {len(df)}")
    print(f"  Years: {df['start_year'].min()} - {df['start_year'].max()}")
    print(f"  Programs: {df['program'].nunique()}")
    print(f"  Institutions: {df['institution'].nunique()}")
    if df['amount'].notna().any():
        print(f"  Total amount: {df['amount'].sum():,.0f} (in currency_note units)")

    print(f"\n[Sample]")
    print(df[['project_code', 'title', 'pi_name', 'institution', 'start_year', 'amount']].head(3).to_string())

    # Upload
    upload_to_s3(df)

    end_time = datetime.now()
    print(f"\n[Complete] Duration: {end_time - start_time}")


if __name__ == "__main__":
    main()
