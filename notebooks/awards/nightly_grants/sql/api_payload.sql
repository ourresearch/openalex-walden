WITH
-- Country lookup (country/award_country_lookup.csv): the free-text `affiliation.country` of each source -> ISO 3166-1
-- alpha-2. `code`: 'provenance|value' or '*|value' -> the code ('' when the row gives none). `usable_source`: provenance ->
-- whether that source's country field holds the country of the named organisation (its '*' row in the file).
country_lookup AS (
  {COUNTRY_LOOKUP}
),
country_map AS (
  SELECT
    -- ARRAY_SORT: entry order never depends on the scan (keys are unique, award_country.read_lookup refuses duplicates).
    MAP_FROM_ENTRIES(ARRAY_SORT(COLLECT_LIST(CASE WHEN value <> '*' THEN STRUCT(
      CONCAT(provenance_scope, '|', value) AS k,
      CASE WHEN confidence IN ('high', 'medium') THEN iso2 ELSE '' END AS v) END))) AS code,
    MAP_FROM_ENTRIES(ARRAY_SORT(COLLECT_LIST(CASE WHEN value = '*' THEN STRUCT(
      provenance_scope AS k, confidence IN ('high', 'medium') AS v) END))) AS usable_source
  FROM country_lookup
),
-- Grant addresses: each person on the award whose affiliation Nightly Grants reads, and `name`, the affiliation name it matches to
-- an institution. This is the one definition of which award names are matcher input (oxjob #2032): PrepareAffiliationStrings sends
-- them to the matcher's lookup and the Affiliation Matcher Backlog takes them as pending, each with a copy of this CTE over
-- openalex.awards.awards_api (a SQL warehouse notebook cannot read this file; tests/test_grant_addresses.py fails when a copy
-- differs). Ten feeds are not read (oxjob #123.2). KAKEN is read for its lead only, whose affiliation is the host institution
-- (sources/kaken.sql); its co-investigators' strings ('institution, department, title') stay out. A blank or placeholder name is
-- no name (#123.6: 'no institution available' was a junk magnet); the person's RORs are still read (funder_rors).
-- `spelled`: a name of two or more words written in capitals (NIH, CONAHCYT, ANID ...) as papers write it: title case, small
-- words in lower case, the abbreviations NIH uses spelled out (one word is usually an acronym: PATH, IAS). The matcher answers
-- capitals worse: WASHINGTON UNIVERSITY [], Washington University -> Washington University in St. Louis; UNIVERSITY OF TEXAS
-- HLTH SCI CTR HOUSTON [], the spelled name -> UTHealth Houston. Read only when the matcher answered the name itself [].
grant_addresses AS (
  SELECT award_id, provenance, funder_id, funder_award_id, is_lead, aff, name,
    NULLIF(CONCAT(UPPER(SUBSTR(words, 1, 1)), SUBSTR(words, 2)), name) AS spelled
  FROM (
    SELECT *,
      CASE WHEN name = UPPER(name) AND name <> LOWER(name) AND TRIM(name) LIKE '% %'
           THEN ARRAY_JOIN(TRANSFORM(SPLIT(INITCAP(TRIM(name)), ' '), w -> COALESCE(TRY_ELEMENT_AT(MAP(
             'Of', 'of', 'The', 'the', 'And', 'and', 'For', 'for', 'In', 'in', 'At', 'at', 'On', 'on', 'De', 'de', 'Del', 'del',
             'Da', 'da', 'Do', 'do', 'Dos', 'dos', 'Das', 'das', 'Di', 'di', 'Du', 'du', 'Des', 'des', 'La', 'la', 'Le', 'le',
             'Les', 'les', 'Y', 'y', 'E', 'e', 'Hlth', 'Health', 'Sci', 'Science', 'Scis', 'Sciences', 'Ctr', 'Center',
             'Ctrs', 'Centers', 'Cntr', 'Center', 'Univ', 'University', 'Inst', 'Institute', 'Hosp', 'Hospital', 'Coll', 'College',
             'Natl', 'National', 'Dept', 'Department', 'Assn', 'Association', 'Fdn', 'Foundation'), w), w)), ' ') END AS words
    FROM (
  SELECT award_id, provenance, funder_id, funder_award_id, is_lead, aff,
    CASE WHEN TRIM(aff.name) <> ''
              AND aff.name NOT LIKE 'Institution abroad%'
              AND LOWER(TRIM(aff.name)) NOT IN ('grantee', 'autre', 'autres', 'n/a', 'na', 'null', 'unknown', 'none', 'tbd', 'tba', 'other',
                                                'individual', 'data not available', 'no institution available')
         THEN aff.name END AS name
  FROM (
    SELECT id AS award_id, provenance, funder_id, funder_award_id, true AS is_lead, lead_investigator.affiliation AS aff FROM {AWARDS}
    UNION ALL
    SELECT id, provenance, funder_id, funder_award_id, false, co_lead_investigator.affiliation FROM {AWARDS}
    UNION ALL
    SELECT id, provenance, funder_id, funder_award_id, false, person.affiliation FROM {AWARDS} LATERAL VIEW EXPLODE(investigators) inv AS person
  )
  WHERE aff IS NOT NULL
    AND (provenance NOT IN ('kaken', 'crossref_work.grants', 'crossref_work_funders', 'gateway_to_research', 'usaspending',
                            'rwjf_grants_explorer', 'argentina_mincyt', 'openaire_fwf', 'researchfi', 'nobelprize_api')
         OR (provenance = 'kaken' AND is_lead))
    )
  )
),
-- The country code recorded next to each grant address. A row for this provenance and value decides on its own (RWJF's 'MA' is
-- Massachusetts). Otherwise the source must be usable (IDRC stores the project's recipient country, NSF the place of
-- performance, Humboldt a constant 'Germany': none is) and the code is the '*'-scoped row for the value. A source the
-- file does not describe gives none.
-- NIH intramural projects (activity codes Z01, ZIA, ZIC, ...) run inside NIH's own institutes and the source leaves their
-- country empty: US. On 2026-10-02 all 116,926 such awards had either no country (59,975) or UNITED STATES.
people AS (
  SELECT
    g.award_id, g.provenance, g.funder_id, g.is_lead, g.aff, g.name, g.spelled,
    COALESCE(
      NULLIF(COALESCE(
        TRY_ELEMENT_AT(cm.code, CONCAT(g.provenance, '|', LOWER(TRIM(g.aff.country)))),
        CASE WHEN TRY_ELEMENT_AT(cm.usable_source, g.provenance)
             THEN TRY_ELEMENT_AT(cm.code, CONCAT('*|', LOWER(TRIM(g.aff.country)))) END), ''),
      CASE WHEN g.is_lead AND g.provenance = 'nih_exporter' AND g.funder_award_id RLIKE '^[0-9]?[Zz]'
                AND g.aff.name IS NOT NULL AND NULLIF(TRIM(g.aff.country), '') IS NULL THEN 'US' END
    ) AS record_country
  FROM grant_addresses g
  CROSS JOIN country_map cm
),
-- Each affiliation name on the award with the country code recorded next to it. A KAKEN award with no lead name takes its host
-- institution from the 07-08 page scrape (kaken_projects_v) with the lead's recorded country; where there is a lead name, the
-- scrape's string is that name (equal on all 707,489 on 2026-11-06). The scrape holds no blank or placeholder name (0 of 204,785
-- on 2026-11-06). walden-kaken-nightly retires the scrape and this branch. A Chinese or Japanese name with no recorded country from a funder in China, Japan or Taiwan carries the
-- funder's country for one check (guarded).
all_strings AS (
  SELECT DISTINCT
    s.award_id,
    s.provenance,
    s.raw,
    s.spelled,
    s.record_country,
    CASE WHEN s.record_country IS NULL AND f.country_code IN ('CN', 'JP', 'TW') AND s.raw RLIKE '[\\x{3400}-\\x{9fff}]'
         THEN f.country_code END AS han_funder_country,
    CASE WHEN s.provenance IN ('nih_exporter', 'nsf_award_search', 'kaken') THEN 0.1 ELSE 0.3 END AS thresh
  FROM (
    SELECT award_id, provenance, funder_id, name AS raw, spelled, record_country FROM people WHERE name IS NOT NULL
    UNION ALL
    SELECT oa.id, oa.provenance, oa.funder_id, kpr.institution, CAST(NULL AS STRING), lead.record_country
    FROM {AWARDS} oa
    JOIN kaken_projects_v kpr ON oa.funder_award_id = kpr.project_id
    LEFT JOIN people lead ON lead.award_id = oa.id AND lead.is_lead
    WHERE oa.provenance = 'kaken' AND oa.lead_investigator.affiliation.name IS NULL AND kpr.institution IS NOT NULL
  ) s
  LEFT JOIN funders_v f ON f.funder_id = s.funder_id
),
-- The funder's own id for the organisation: a ROR in the person's affiliation.ids (NHMRC, FCT, GACR, DataCite ...) names the
-- institution with no matching (oxjob #2032). It is checked by the country guard like a matched name. Only a ROR id counts:
-- https://ror.org/0xxxxxxxx, the same with http or no scheme, or the bare id.
funder_rors AS (
  SELECT DISTINCT r.award_id, r.provenance, r.record_country, r.aff_json, i.id AS institution_id
  FROM (
    SELECT p.award_id, p.provenance, p.record_country, TO_JSON(p.aff) AS aff_json,
           REGEXP_EXTRACT(LOWER(TRIM(x.id)), '^(?:(?:https?://)?ror[.]org/)?(0[0-9a-z]{6}[0-9]{2})$', 1) AS ror_key
    FROM people p
    LATERAL VIEW EXPLODE(p.aff.ids) ids AS x
    WHERE UPPER(x.type) = 'ROR'
  ) r
  JOIN institutions_api_v i ON i.ror = CONCAT('https://ror.org/', r.ror_key)
),
-- Institution ids for each name. First the affiliation matcher's answer for the string ({ANSWERS}: the table works have
-- read since 2026-09-28, oxjob #1386; an empty answer means "names no institution" and is final), or, when that answer is
-- empty and the funder recorded a country for the guard to check, its answer for the name as papers spell it
-- (grant_addresses.spelled). Strings it has not answered keep the legacy route: hand-rules override, else the 2023 model's
-- candidates at or above the threshold.
-- A NaN score never passes: in Spark NaN compares above every number, which gave 318 names (mostly Japanese and
-- Chinese) the same five institutions.
disambiguated AS (
  SELECT
    s.award_id,
    s.provenance,
    s.raw,
    s.record_country,
    s.han_funder_country,
    ans.raw_affiliation_string IS NOT NULL AS from_matcher,
    CASE
      WHEN ans.raw_affiliation_string IS NOT NULL AND SIZE(ans.institution_ids) = 0 AND sp.raw_affiliation_string IS NOT NULL
           AND s.record_country IS NOT NULL
        THEN COALESCE(sp.institution_ids, ARRAY())
      WHEN ans.raw_affiliation_string IS NOT NULL THEN COALESCE(ans.institution_ids, ARRAY())
      WHEN SIZE(asl.institution_ids_override) > 0
           AND NOT ARRAY_CONTAINS(asl.institution_ids_override, -1)
        THEN asl.institution_ids_override
      ELSE TRANSFORM(
        FILTER(asl.model_response, x -> x.score >= s.thresh AND NOT ISNAN(x.score)),
        x -> CAST(x.id AS BIGINT)
      )
    END AS ids
  FROM all_strings s
  LEFT JOIN {ANSWERS} ans
    ON s.raw = ans.raw_affiliation_string
  LEFT JOIN {ANSWERS} sp
    ON s.spelled = sp.raw_affiliation_string
  LEFT JOIN affiliation_lookup_v asl
    ON s.raw = asl.raw_affiliation_string
  WHERE ans.raw_affiliation_string IS NOT NULL OR asl.raw_affiliation_string IS NOT NULL
),
exploded AS (
  SELECT d.award_id, d.provenance, d.raw, d.record_country, d.han_funder_country, d.from_matcher, ie.institution_id
  FROM disambiguated d
  LATERAL VIEW EXPLODE(d.ids) ie AS institution_id
  WHERE ie.institution_id IS NOT NULL
  UNION ALL
  SELECT award_id, provenance, CAST(NULL AS STRING), record_country, CAST(NULL AS STRING), false, institution_id
  FROM funder_rors
),
-- Dependent territories count as their sovereign state for the guard: NIH records Puerto Rico's universities under
-- UNITED STATES, Erasmus+ records the Universite de La Reunion under FR. Palau, Micronesia and the Marshall Islands are
-- grouped with the US because US federal sources list them as domestic.
sovereign AS (
  SELECT * FROM VALUES
    ('PR', 'US'), ('VI', 'US'), ('GU', 'US'), ('AS', 'US'), ('MP', 'US'), ('UM', 'US'), ('PW', 'US'), ('FM', 'US'), ('MH', 'US'),
    ('GF', 'FR'), ('GP', 'FR'), ('MQ', 'FR'), ('RE', 'FR'), ('YT', 'FR'), ('NC', 'FR'), ('PF', 'FR'), ('PM', 'FR'), ('BL', 'FR'),
    ('MF', 'FR'), ('WF', 'FR'), ('TF', 'FR'),
    ('GI', 'GB'), ('BM', 'GB'), ('KY', 'GB'), ('VG', 'GB'), ('TC', 'GB'), ('MS', 'GB'), ('AI', 'GB'), ('FK', 'GB'), ('SH', 'GB'),
    ('PN', 'GB'), ('IO', 'GB'), ('GS', 'GB'), ('JE', 'GB'), ('GG', 'GB'), ('IM', 'GB'),
    ('AW', 'NL'), ('CW', 'NL'), ('SX', 'NL'), ('BQ', 'NL'),
    ('GL', 'DK'), ('FO', 'DK'), ('SJ', 'NO'), ('BV', 'NO'), ('AX', 'FI'),
    ('NF', 'AU'), ('CX', 'AU'), ('CC', 'AU'), ('HM', 'AU'), ('CK', 'NZ'), ('NU', 'NZ'), ('TK', 'NZ'),
    ('HK', 'CN'), ('MO', 'CN')
  AS sovereign(territory, state)
),
-- Country-consistency guard: when the source recorded the organisation's country next to the name, a matched
-- institution must be in that country (or in a territory of it, or the reverse). A match is kept when either side has no
-- country. An institution reached from two names on the same award is kept if either agrees. An ancestor in the right
-- country does not rescue a match: "FRED HUTCHINSON CANCER CENTER", UNITED STATES was matched to its Cape Town
-- laboratory that way.
-- A Chinese or Japanese name with no recorded country from a funder in China, Japan or Taiwan is not matched to an institution
-- in another of the three (oxjob #2032): the same characters name different universities there. On the 11-03 blind sheet all 4
-- wrong Han-script links were Taiwan names (國立中山大學, Taiwan's National Sun Yat-sen University -> Sun Yat-sen University CN;
-- 東海大學, Tunghai -> Tokai JP). An institution elsewhere is kept (a Taiwanese grant to 雪梨大學, the University of Sydney).
-- A match in the wrong country moves to the one institution of the same name in the recorded country, if there is exactly one
-- and the funder's text contains that name, case and accents aside (Universidad Técnica Federico Santa María has a Chilean and
-- an Ecuadorian record; Asia University a Japanese and a Taiwanese one). A text that only resembles the name stays dropped:
-- NORTHWESTERN STATE UNIVERSITY OF LA is neither Northwestern University in the Philippines nor the one in Illinois.
same_name AS (
  SELECT a.id AS institution_id, COALESCE(sb.state, b.country_code) AS state, MIN(b.id) AS twin_id, COUNT(*) AS twins,
    MAX(TRIM(REGEXP_REPLACE(TRANSLATE(LOWER(a.display_name), 'áàâäãåéèêëíìîïóòôöõúùûüçñ', 'aaaaaaeeeeiiiiooooouuuucn'), '[^\\p{L}\\p{N}]+', ' ')))
      AS name_key
  FROM institutions_api_v a
  JOIN institutions_api_v b ON LOWER(b.display_name) = LOWER(a.display_name) AND b.id <> a.id
  LEFT JOIN sovereign sb ON sb.territory = b.country_code
  WHERE b.country_code IS NOT NULL
  GROUP BY a.id, COALESCE(sb.state, b.country_code)
),
guarded AS (
  SELECT
    e.award_id,
    e.provenance,
    e.raw,
    e.from_matcher,
    COALESCE(t.twin_id, e.institution_id) AS institution_id,
    e.record_country,
    i.country_code AS institution_country,
    t.twin_id IS NOT NULL
    OR (e.record_country IS NULL OR i.country_code IS NULL
      OR COALESCE(si.state, i.country_code) = COALESCE(sr.state, e.record_country))
    AND (e.han_funder_country IS NULL OR i.country_code IS NULL
      OR COALESCE(si.state, i.country_code) NOT IN ('CN', 'JP', 'TW') OR COALESCE(si.state, i.country_code) = e.han_funder_country)
      AS country_ok
  FROM exploded e
  JOIN institutions_api_v i ON e.institution_id = i.id
  LEFT JOIN sovereign si ON si.territory = i.country_code
  LEFT JOIN sovereign sr ON sr.territory = e.record_country
  LEFT JOIN same_name t ON t.institution_id = e.institution_id AND t.twins = 1
    AND t.state = COALESCE(sr.state, e.record_country) AND t.state <> COALESCE(si.state, i.country_code)
    AND CONCAT(' ', REGEXP_REPLACE(TRANSLATE(LOWER(e.raw), 'áàâäãåéèêëíìîïóòôöõúùûüçñ', 'aaaaaaeeeeiiiiooooouuuucn'), '[^\\p{L}\\p{N}]+', ' '), ' ')
        LIKE CONCAT('% ', t.name_key, ' %')
),
deduped AS (SELECT DISTINCT award_id, institution_id FROM guarded WHERE country_ok),
institution_awarded_per_award AS (
  SELECT
    d.award_id,
    -- ARRAY_SORT (pa1014-det): COLLECT_LIST order follows the shuffle, and the content hash covers this array, so unsorted it
    -- re-stamped updated_date on ~116k awards a night (prod 10-11 -> 10-12: 115,700 order-only changes). id is the first field and
    -- unique per award (deduped + INSTITUTION_API_UNIQUE), so the sort is a total order on the whole element. The order is the id
    -- URL's string order (I10 before I9). create_api.previous_sorted hashes yesterday's arrays sorted the same way (no one-time re-stamp).
    ARRAY_SORT(COLLECT_LIST(STRUCT(
      CONCAT('https://openalex.org/I', CAST(i.id AS STRING)) AS id,
      i.display_name,
      i.ror,
      i.country_code,
      i.type,
      i.lineage
    ))) AS institution_awarded
  FROM deduped d
  JOIN institutions_api_v i ON d.institution_id = i.id
  GROUP BY d.award_id
),
-- Each person's institutions (oxjob #1847, Casey 11-09): investigators[].institutions, as works' authorships[].institutions, from the
-- same matched, country-guarded names and RORs institution_awarded reads, kept per affiliation as printed (two people printing the
-- same affiliation get the same institutions). The affiliation itself stays as printed, like works' raw_affiliation_strings. A
-- KAKEN lead with no name of its own (the page-scrape branch) and the feeds grant_addresses does not read get none.
affiliation_institution_ids AS (
  SELECT pa.award_id, pa.aff_json, g.institution_id
  FROM (SELECT DISTINCT award_id, TO_JSON(aff) AS aff_json, name, record_country FROM people WHERE name IS NOT NULL) pa
  JOIN guarded g ON g.award_id = pa.award_id AND g.raw = pa.name AND g.record_country <=> pa.record_country AND g.country_ok
  UNION
  SELECT fr.award_id, fr.aff_json, g.institution_id
  FROM funder_rors fr
  JOIN guarded g ON g.award_id = fr.award_id AND g.raw IS NULL AND g.institution_id = fr.institution_id
    AND g.record_country <=> fr.record_country AND g.country_ok
),
affiliation_institutions AS (
  SELECT award_id, MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(aff_json AS k, institutions AS v))) AS by_affiliation
  FROM (
    SELECT a.award_id, a.aff_json,
      -- sorted as institution_awarded is (id first, unique per person), so the content hash doesn't move on shuffle order
      ARRAY_SORT(COLLECT_LIST(NAMED_STRUCT(
        'id', CONCAT('https://openalex.org/I', CAST(i.id AS STRING)), 'display_name', i.display_name, 'ror', i.ror,
        'country_code', i.country_code, 'type', i.type, 'lineage', i.lineage
      ))) AS institutions
    FROM (SELECT DISTINCT award_id, aff_json, institution_id FROM affiliation_institution_ids) a
    JOIN institutions_api_v i ON a.institution_id = i.id
    GROUP BY a.award_id, a.aff_json
  )
  GROUP BY award_id
),
-- Investigators with their OpenAlex author (oxjob #1847; lib/award_investigators.py): {INVESTIGATOR_LINKS} holds the served slots.
-- The author is {id, display_name, orcid}, as works' authorships[].author has them.
-- The key is slot, position, the names and the ORCID as printed (cleaned as the job's people.sql does), so a reordered list or an
-- edited name or ORCID gets no author rather than someone else's.
-- With no links every author is NULL and to_json drops it, so no content hash (api_hash_expression.sql) moves.
investigator_links AS (
  {INVESTIGATOR_LINKS}
),
investigator_authors AS (
  SELECT award_id, MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(
    CONCAT_WS('|', slot, CAST(position AS STRING), LOWER(TRIM(COALESCE(family_name, ''))), LOWER(TRIM(COALESCE(given_name, ''))),
      COALESCE(grant_orcid, '')) AS k,
    STRUCT(author_id AS id, display_name, author_orcid AS orcid) AS v))) AS authors
  FROM investigator_links
  GROUP BY award_id
),
investigator_people AS (
  SELECT
    x.id AS award_id,
    CASE WHEN x.lead_investigator IS NOT NULL THEN NAMED_STRUCT(
      'given_name', x.lead_investigator.given_name,
      'family_name', x.lead_investigator.family_name,
      'orcid', x.lead_investigator.orcid,
      'role_start', x.lead_investigator.role_start,
      'affiliation', x.lead_investigator.affiliation,
      'author', TRY_ELEMENT_AT(x.authors, CONCAT_WS('|', 'lead_investigator', '0',
        LOWER(TRIM(COALESCE(x.lead_investigator.family_name, ''))), LOWER(TRIM(COALESCE(x.lead_investigator.given_name, ''))),
        COALESCE(REGEXP_EXTRACT(UPPER(x.lead_investigator.orcid), '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])', 1), ''))),
      'institutions', TRY_ELEMENT_AT(x.by_affiliation, TO_JSON(x.lead_investigator.affiliation))) END AS lead_investigator,
    CASE WHEN x.co_lead_investigator IS NOT NULL THEN NAMED_STRUCT(
      'given_name', x.co_lead_investigator.given_name,
      'family_name', x.co_lead_investigator.family_name,
      'orcid', x.co_lead_investigator.orcid,
      'role_start', x.co_lead_investigator.role_start,
      'affiliation', x.co_lead_investigator.affiliation,
      'author', TRY_ELEMENT_AT(x.authors, CONCAT_WS('|', 'co_lead_investigator', '0',
        LOWER(TRIM(COALESCE(x.co_lead_investigator.family_name, ''))), LOWER(TRIM(COALESCE(x.co_lead_investigator.given_name, ''))),
        COALESCE(REGEXP_EXTRACT(UPPER(x.co_lead_investigator.orcid), '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])', 1), ''))),
      'institutions', TRY_ELEMENT_AT(x.by_affiliation, TO_JSON(x.co_lead_investigator.affiliation)))
      END AS co_lead_investigator,
    -- i is Spark's 0-based index, the same position the job reads with the same transform (people.sql)
    TRANSFORM(x.investigators, (p, i) -> CASE WHEN p IS NOT NULL THEN NAMED_STRUCT(
      'given_name', p.given_name,
      'family_name', p.family_name,
      'orcid', p.orcid,
      'role_start', p.role_start,
      'affiliation', p.affiliation,
      'author', TRY_ELEMENT_AT(x.authors, CONCAT_WS('|', 'investigators', CAST(i AS STRING),
        LOWER(TRIM(COALESCE(p.family_name, ''))), LOWER(TRIM(COALESCE(p.given_name, ''))),
        COALESCE(REGEXP_EXTRACT(UPPER(p.orcid), '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X])', 1), ''))),
      'institutions', TRY_ELEMENT_AT(x.by_affiliation, TO_JSON(p.affiliation))) END) AS investigators
  FROM (
    SELECT
      oa.id, oa.lead_investigator, oa.co_lead_investigator, oa.investigators, ia.authors, ai.by_affiliation
    FROM {AWARDS} oa
    LEFT JOIN investigator_authors ia ON ia.award_id = oa.id
    LEFT JOIN affiliation_institutions ai ON ai.award_id = oa.id
  ) x
)
SELECT
  oa.id, oa.display_name, oa.description, oa.funder_id, oa.funder_award_id,
  oa.amount, oa.currency, oa.funder, oa.funding_type, oa.funder_scheme,
  oa.provenance, oa.start_date, oa.end_date, oa.start_year, oa.end_year,
  -- with author{id, display_name} on each person (investigator_people, above)
  pe.lead_investigator, pe.co_lead_investigator, pe.investigators,
  oa.landing_page_url, oa.doi, oa.works_api_url,
  DATE_TRUNC('SECOND', CAST(oa.created_date AS TIMESTAMP)) AS created_date,
  CAST(NULL AS TIMESTAMP) AS updated_date,
  oa.funded_outputs,
  oa.funded_outputs_count,
  oa.primary_topic,
  oa.topics,
  COALESCE(iap.institution_awarded, ARRAY()) AS institution_awarded,
  -- Denormalized siblings (object mapping in v4) for fast term filtering — see oxjob #123.2.
  -- Same content as the rich nested versions; v4 mapping declares the rich ones as nested
  -- and the _full ones as object with explicit keyword id fields. Mirrors the works
  -- authorships/authorships_full pattern.
  COALESCE(iap.institution_awarded, ARRAY()) AS institution_awarded_full,
  oa.primary_topic AS primary_topic_full,
  oa.topics AS topics_full,
  -- Sub-award links (award_relations.py). *_full = uncapped object siblings for search filters.
  oa.parent_awards,
  oa.sub_awards,
  oa.sub_awards_count,
  oa.parent_awards_full,
  oa.sub_awards_full,
  -- Funding-flows release (oxjob #1570): amount in USD (award_usd.py), recipient countries and regions (award_recipients.py),
  -- research flag (award_research_scope.py, added after this payload). *_full = uncapped object siblings for search filters.
  oa.amount_usd,
  oa.amount_usd_basis,
  oa.recipient_countries,
  oa.recipient_regions,
  oa.recipient_countries_full,
  oa.recipient_regions_full,
  -- One organisation object with sorted roles[]; source role rows remain in the side table.
  oa.recipients,
  oa.recipients_full,
  -- Every person on the award in one array, for the filter investigators.author.id (oxjob #1847; the lead and co-lead are mostly not
  -- in investigators[]). Object-mapped in awards-v4 like authorships_full; not in the content hash: it repeats the three above.
  FILTER(CONCAT(ARRAY(pe.lead_investigator, pe.co_lead_investigator), COALESCE(pe.investigators, ARRAY())), x -> x IS NOT NULL) AS investigators_full
  -- research_scope is added after this payload by award_research_scope.run (it reads institution_awarded).
FROM {AWARDS} oa
LEFT JOIN institution_awarded_per_award iap ON oa.id = iap.award_id
LEFT JOIN investigator_people pe ON pe.award_id = oa.id
