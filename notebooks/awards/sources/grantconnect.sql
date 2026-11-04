-- Mapping from CreateGrantConnectAwards.ipynb (origin e22fdf7f): its awards-table SELECT over grantconnect_input (was the
-- staging table read from s3://openalex-ingest/awards/grantconnect/grantconnect_projects.parquet); created_date and updated_date are the run timestamp.
WITH
agency_funder_map AS (
    SELECT * FROM VALUES
        ('Attorney-General''s Department', 4320325650),
        ('Australian Communications and Media Authority', 4320333917),
        ('Australian Federal Police', 4320310990),
        ('Australian Renewable Energy Agency', 4320323119),
        ('Australian Trade and Investment Commission (Austrade)', 4320331701),
        ('Cancer Australia', 4320320501),
        ('Department of Agriculture', 4320320376),
        ('Department of Agriculture, Fisheries and Forestry', 4320320376),
        ('Department of Climate Change, Energy, the Environment and Water', 4216318126),
        ('Department of Communications and the Arts', 4320328982),
        ('Department of Defence', 4320320441),
        ('Department of Education', 4320321981),
        ('Department of Employment and Workplace Relations', 6050540351),
        ('Department of Employment, Skills, Small and Family Business', 6050540351),
        ('Department of Finance', 4320331702),
        ('Department of Foreign Affairs and Trade', 4320320432),
        ('Department of Health, Disability and Ageing', 4320322220),
        ('Department of Home Affairs', 4320331703),
        ('Department of Industry, Science and Resources', 4320328828),
        ('Department of Infrastructure, Transport, Regional Development, Communications, Sport and the Arts', 4320328982),
        ('Department of Social Services', 4320320374),
        ('Department of the Prime Minister and Cabinet', 4320323600),
        ('Department of Veterans'' Affairs', 4320320381),
        ('Great Barrier Reef Marine Park Authority', 4320314560),
        ('National Blood Authority', 4320315995),
        ('National Disability Insurance Agency (NDIA)', 4320331704),
        ('National Indigenous Australians Agency', 4320331928),
        ('National Mental Health Commission', 4320331162),
        ('NDIS Quality and Safeguards Commission', 1374002132),
        ('Office of National Intelligence', 4320337300),
        ('Organ and Tissue Authority', 4320316032),
        ('Safe Work Australia', 4320325649),
        ('Wine Australia', 4320324767)
        AS t(agency, funder_id)
),

funder_lookup AS (
    SELECT funder_id, display_name, ror_id, doi
    FROM openalex.funders.funders
),

scoped AS (
    SELECT
        g.*,
        COALESCE(m.funder_id, 4320315885) AS resolved_funder_id
    FROM grantconnect_input g
    LEFT JOIN agency_funder_map m ON g.agency = m.agency
    WHERE (
        g.category IN (
            'Academic Medical Research',
        'Health and Medical Research',
        'Humanities, Arts and Social Sciences (HASS) Research',
        'Medical Research',
        'Science, Technology, Engineering and Mathematics (STEM) Research',
        'Scientific Research',
        'Technology'
        )
        OR LOWER(g.grant_program) RLIKE 'research (grant|program|programme|fund|scheme)|cooperative research centre|medical research future fund'
    )
      AND g.ga_id IS NOT NULL AND TRIM(g.ga_id) != ''
),

awards_transformed AS (
    SELECT
        abs(xxhash64(CONCAT(s.resolved_funder_id, ':', LOWER(s.ga_id)))) % 9000000000 as id,

        COALESCE(
            NULLIF(TRIM(s.grant_activity), ''),
            NULLIF(TRIM(s.grant_program), ''),
            CASE
                WHEN LENGTH(s.purpose) > 150 THEN CONCAT(SUBSTRING(s.purpose, 1, 147), '...')
                ELSE s.purpose
            END
        ) as display_name,

        NULLIF(TRIM(s.purpose), '') as description,

        s.resolved_funder_id as funder_id,
        s.ga_id as funder_award_id,

        TRY_CAST(s.value_aud AS DOUBLE) as amount,
        'AUD' as currency,

        struct(
            CONCAT('https://openalex.org/F', f.funder_id) as id,
            f.display_name,
            f.ror_id,
            f.doi
        ) as funder,

        CASE
            WHEN LOWER(s.grant_program) LIKE '%fellowship%' THEN 'fellowship'
            WHEN LOWER(s.grant_program) LIKE '%scholarship%' THEN 'fellowship'
            ELSE 'research'
        END as funding_type,

        NULLIF(TRIM(s.grant_program), '') as funder_scheme,

        'grantconnect' as provenance,

        TRY_TO_DATE(SUBSTRING(s.start_date, 1, 10), 'yyyy-MM-dd') as start_date,
        TRY_TO_DATE(SUBSTRING(s.end_date, 1, 10), 'yyyy-MM-dd') as end_date,
        YEAR(TRY_TO_DATE(SUBSTRING(s.start_date, 1, 10), 'yyyy-MM-dd')) as start_year,
        YEAR(TRY_TO_DATE(SUBSTRING(s.end_date, 1, 10), 'yyyy-MM-dd')) as end_year,

        CASE
            WHEN NULLIF(TRIM(s.recipient_name), '') IS NOT NULL
                 AND UPPER(TRIM(s.recipient_name)) NOT IN ('ABN EXEMPT', 'CONFIDENTIAL', 'WITHHELD', 'N/A')
            THEN struct(
                CAST(NULL AS STRING) as given_name,
                CAST(NULL AS STRING) as family_name,
                CAST(NULL AS STRING) as orcid,
                CAST(NULL AS DATE) as role_start,
                struct(
                    TRIM(s.recipient_name) as name,
                    s.recipient_country as country,
                    CAST(NULL AS ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>) as ids
                ) as affiliation
            )
            ELSE CAST(NULL AS STRUCT<
                given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
                affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
            >)
        END as lead_investigator,

        CAST(NULL AS STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >) as co_lead_investigator,

        CAST(NULL AS ARRAY<STRUCT<
            given_name:STRING, family_name:STRING, orcid:STRING, role_start:DATE,
            affiliation:STRUCT<name:STRING, country:STRING, ids:ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>>
        >>) as investigators,

        CONCAT('https://www.grants.gov.au/Search/KeywordSearch?keyword=', s.ga_id) as landing_page_url,

        CAST(NULL AS STRING) as doi,

        concat('https://api.openalex.org/works?filter=awards.id:G',
               abs(xxhash64(CONCAT(s.resolved_funder_id, ':', LOWER(s.ga_id)))) % 9000000000) as works_api_url,

        session.awards_timestamp as created_date,
        session.awards_timestamp as updated_date

    FROM scoped s
    JOIN funder_lookup f ON f.funder_id = s.resolved_funder_id
)
