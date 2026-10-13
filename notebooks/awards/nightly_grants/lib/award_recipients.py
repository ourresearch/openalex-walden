"""Recipient organisations and landing countries/regions, from the side table `award_recipients_raw` (optional extra input),
resolved to stable ids every night and projected onto the award document (oxjob #1570, funding-flows work).

Input: one row per award and recipient, keyed by the producer's STAGING triple (staging_id, funder_id, award_key) and
named by (provenance, funder_award_id), never by a G id. Columns: provenance, funder_award_id, staging_id, funder_id, award_key,
kind (organisation | country | region), country_code (ISO 3166-1 alpha-2) or region_code + region_vocabulary (IATI: 1 = OECD DAC, 2 = UN M.49),
basis (funder_reported | place_of_performance | organisation_location), share (0..1 or NULL), source_field, asserted_by
(funder | openalex | curated), adapter_version, asserted_at. Organisation rows also carry name, role and optional ROR.
Served (extra columns on awards_candidate, nothing else changes):
  recipient_countries[] {country_code, share, amount_usd, basis}   recipient_regions[] {code, vocabulary, display_name, share}
  recipients[] {id, display_name, ror, country_code, role, share, basis, asserted_by} (first 100)
  plus *_full siblings for search filters. Geography rules: when an award has rows on several bases the best one wins (funder_reported,
  then place_of_performance, then organisation_location: a US organisation working in Kenya lands in Kenya). Shares are the
  funder's; when any kept row has none, the kept rows split equally. amount_usd here is the award's amount_usd times the share
  (award_usd.py runs first). institution_awarded is untouched.
Failure policy: malformed or duplicate rows, an EMPTY side table, an ambiguous key, or more rows dropped than the fuse allows stop
the run; rows whose award is not served tonight are dropped and counted. Against last night: the rows per (provenance, kind) the
last successful night read, the served memberships and the awards with regions may not fall beyond the fuse. Rollback:
`clear_funding_flows` (award_keys).
"""
import csv
from pathlib import Path

import award_keys
import award_recipient_orgs

KINDS = ("country", "region", "organisation")
BASES = ("funder_reported", "place_of_performance", "organisation_location")
ASSERTED_BY = ("funder", "openalex", "curated")
VOCABULARIES = {"1": "oecd_dac_region", "2": "un_m49"}        # anything else: the publisher's own list
EMPTY_RAW = """SELECT CAST(NULL AS STRING) provenance,CAST(NULL AS STRING) funder_award_id,CAST(NULL AS BIGINT) staging_id,
  CAST(NULL AS BIGINT) funder_id,CAST(NULL AS STRING) award_key,CAST(NULL AS STRING) kind,CAST(NULL AS STRING) country_code,
  CAST(NULL AS STRING) region_code,CAST(NULL AS STRING) region_vocabulary,CAST(NULL AS STRING) basis,CAST(NULL AS DOUBLE) share,
  CAST(NULL AS STRING) source_field,CAST(NULL AS STRING) asserted_by,CAST(NULL AS STRING) adapter_version,
  CAST(NULL AS TIMESTAMP) asserted_at, CAST(NULL AS STRING) name, CAST(NULL AS STRING) role,
  CAST(NULL AS STRING) ror WHERE false"""


def region_names(package_root):
    """regions/dac_regions.csv: OECD DAC region codes (IATI Region codelist, vocabulary 1) -> display name."""
    with open(Path(package_root) / "regions" / "dac_regions.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    seen = set()
    for row in rows:
        if not row["code"] or not row["name"] or row["code"] in seen:
            raise RuntimeError("DAC_REGION_ROW_INVALID: " + repr(row))
        seen.add(row["code"])
    return rows


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def region_relation_sql(package_root):
    values = ",".join(f"({sql_string(r['code'])},{sql_string(r['name'])})" for r in region_names(package_root))
    return f"SELECT * FROM VALUES {values} AS dac_regions(code,name)"


def raw_input(c):
    if award_keys.clearing(c):
        return c.artifact("recipients_raw_empty", EMPTY_RAW)
    if c.has_extra("award_recipients_raw"):
        award_keys.nonempty(c, "RECIPIENT_RAW_NONEMPTY", "award_recipients_raw_v")
        # Older country-only producers remain readable; new organisation rows must supply these columns.
        cols = award_keys.columns(c, "award_recipients_raw_v")
        missing = [f"CAST(NULL AS STRING) {x}" for x in ("name", "role", "ror") if x not in cols]
        if missing:
            return c.artifact("recipients_raw_compatible", "SELECT *," + ",".join(missing) + " FROM award_recipients_raw_v")
        return "award_recipients_raw_v"
    # No input configured: allowed only while nothing has been published, so recipient countries can never vanish silently.
    if "recipient_countries" in award_keys.columns(c, "previous_api_v"):
        c.zero("RECIPIENT_INPUT_REQUIRED", "SELECT id FROM previous_api_v WHERE size(recipient_countries)>0 OR size(recipient_regions)>0")
    if "recipients_full" in award_keys.columns(c, "previous_api_v"):
        c.zero("RECIPIENT_ORG_INPUT_REQUIRED", "SELECT id FROM previous_api_v WHERE size(recipients_full)>0")
    return c.artifact("recipients_raw_empty", EMPTY_RAW)


def run(c):
    c.guard(); r = c.r
    raw = raw_input(c)
    lit = lambda values: ",".join(repr(v) for v in values)
    c.zero("RECIPIENT_RAW_SHAPE", f"""SELECT * FROM {raw} WHERE NOT coalesce(length(provenance)>0 AND length(funder_award_id)>0
      AND staging_id IS NOT NULL AND funder_id IS NOT NULL AND length(award_key)>0
      AND kind IN ({lit(KINDS)}) AND basis IN ({lit(BASES)}) AND asserted_by IN ({lit(ASSERTED_BY)})
      AND (share IS NULL OR (share BETWEEN 0 AND 1 AND NOT isnan(share)))
      AND CASE kind WHEN 'country' THEN country_code RLIKE '^[A-Z]{{2}}$' AND region_code IS NULL
                    WHEN 'region' THEN length(region_code)>0 AND length(region_vocabulary)>0 AND country_code IS NULL
                    WHEN 'organisation' THEN length(trim(name))>0 AND role IN ('lead','partner','implementing','beneficiary','host')
                      AND (country_code IS NULL OR country_code RLIKE '^[A-Z]{{2}}$')
                      AND region_code IS NULL AND region_vocabulary IS NULL
                      AND (ror IS NULL OR ror RLIKE '^https://ror.org/[0-9a-z]{{9}}$') END
      AND length(adapter_version)>0 AND asserted_at IS NOT NULL,false)""")
    c.zero("RECIPIENT_RAW_UNIQUE", f"""SELECT provenance,staging_id,funder_id,award_key,kind,country_code,region_code,region_vocabulary,basis,name,role,ror
      FROM {raw} GROUP BY ALL HAVING count(*)<>1""")
    # Rows per (provenance, kind) tonight, against the last successful night's: a producer that lost a feed or a kind stops the run.
    last = award_keys.last_counts(c)
    fuse = float(c.config.get("recipients_drop_fuse", 0.01))
    for row in c.sql(f"SELECT provenance,kind,count(*) n FROM {raw} GROUP BY 1,2").collect():
        c.counts[f"recipients_rows_{row.provenance}_{row.kind}"] = int(row.n)
    if not award_keys.clearing(c):
        # Every (provenance, kind) last night or the bootstrap knew about must still be there tonight; a new one needs no baseline.
        known = {k for k in last if k.startswith("recipients_rows_")} | {k for k in c.config.get("bootstrap_counts", {}) if k.startswith("recipients_rows_")}
        c.require(known, "RECIPIENT_ROWS_BASELINE_REQUIRED: no recipients_rows_* count in last night's run or bootstrap_counts")
        for key in sorted(known):
            award_keys.drop_gate(c, "RECIPIENT_ROWS_DROP_" + key[len("recipients_rows_"):], key, c.counts.get(key, 0), fuse)
    c.artifact("recipient_endpoints", f"""SELECT DISTINCT provenance namespace,funder_award_id source_record_id,staging_id,funder_id,award_key
      FROM {raw}""")
    lookup = award_keys.endpoint_lookup(c, "RECIPIENT", f"{r}recipient_endpoints", "recipient_endpoint_lookup")
    c.artifact("recipients_resolved", f"""SELECT a.*,e.stable_id award_id,
        CASE WHEN e.stable_id IS NULL THEN 'UNRESOLVED' WHEN e.status<>'ACTIVE' OR d.id IS NULL THEN 'NOT_SERVED' END dropped
      FROM {raw} a JOIN {lookup} e ON e.namespace=a.provenance AND e.source_record_id=a.funder_award_id
      LEFT JOIN {r}awards_candidate d ON d.id=e.stable_id""")
    c.zero("RECIPIENT_NO_LOSS", f"SELECT 1 WHERE (SELECT count(*) FROM {raw})<>(SELECT count(*) FROM {r}recipients_resolved)")
    for reason in ("UNRESOLVED", "NOT_SERVED"):
        c.counts["recipients_dropped_" + reason.lower()] = int(c.sql(f"SELECT count(*) n FROM {r}recipients_resolved WHERE dropped='{reason}'").collect()[0].n)
    c.zero("RECIPIENT_DROP_FUSE", f"""SELECT count_if(dropped IS NOT NULL) dropped,count(*) total FROM {r}recipients_resolved
      HAVING count_if(dropped IS NOT NULL) > greatest(10, {fuse}*count(*))""")
    # The best basis on the award wins; one row per (award, kind, code) after that (two sources agreeing collapse to one).
    priority = " ".join(f"WHEN '{b}' THEN {i}" for i, b in enumerate(BASES))
    c.artifact("recipients_selected", f"""WITH ranked AS (SELECT *,CASE basis {priority} END basis_rank FROM {r}recipients_resolved WHERE dropped IS NULL AND kind IN ('country','region')),
      best AS (SELECT award_id,min(basis_rank) best_rank FROM ranked GROUP BY 1),
      kept AS (SELECT x.award_id,x.kind,x.country_code,x.region_code,x.region_vocabulary,x.basis,max(x.share) share
        FROM ranked x JOIN best b ON b.award_id=x.award_id AND b.best_rank=x.basis_rank
        GROUP BY x.award_id,x.kind,x.country_code,x.region_code,x.region_vocabulary,x.basis),
      shares AS (SELECT *,count(*) OVER (PARTITION BY award_id) n_rows,bool_or(share IS NULL) OVER (PARTITION BY award_id) any_missing FROM kept)
      SELECT award_id,kind,country_code,region_code,region_vocabulary,basis,
        round(CASE WHEN any_missing THEN 1.0/n_rows ELSE share END,4) share,any_missing equal_split FROM shares""")
    c.zero("RECIPIENT_SELECTED_UNIQUE", f"SELECT award_id,kind,country_code,region_code,region_vocabulary FROM {r}recipients_selected GROUP BY ALL HAVING count(*)<>1")
    c.count("recipients_awards_equal_split", f"(SELECT DISTINCT award_id FROM {r}recipients_selected WHERE equal_split) x")
    c.count("recipients_awards_shares_off", f"""(SELECT award_id FROM {r}recipients_selected GROUP BY 1
      HAVING abs(sum(share)-1.0) > 0.02) x""")                 # information: a publisher whose shares do not sum to 100%
    vocab = " ".join(f"WHEN '{k}' THEN '{v}'" for k, v in VOCABULARIES.items())
    order = "(l, r) -> CASE WHEN l.share > r.share THEN -1 WHEN l.share < r.share THEN 1 WHEN l.k < r.k THEN -1 WHEN l.k > r.k THEN 1 ELSE 0 END"
    c.artifact("awards_candidate_before_recipients", f"SELECT * FROM {r}awards_candidate")
    c.artifact("recipient_arrays", f"""WITH countries AS (SELECT s.award_id,
          array_sort(collect_list(named_struct('share',s.share,'k',s.country_code,'country_code',s.country_code,
            'amount_usd',round(a.amount_usd*s.share,2),'basis',s.basis)),{order}) items
        FROM {r}recipients_selected s JOIN {r}awards_candidate_before_recipients a ON a.id=s.award_id WHERE s.kind='country' GROUP BY s.award_id),
      regions AS (SELECT s.award_id,
          array_sort(collect_list(named_struct('share',s.share,'k',s.region_code,'code',s.region_code,
            'vocabulary',CASE s.region_vocabulary {vocab} ELSE 'publisher' END,
            'display_name',CASE WHEN s.region_vocabulary='1' THEN n.name END)),{order}) items
        FROM {r}recipients_selected s LEFT JOIN ({region_relation_sql(c.config['package_root'])}) n ON n.code=s.region_code AND s.region_vocabulary='1'
        WHERE s.kind='region' GROUP BY s.award_id)
      SELECT coalesce(c.award_id,g.award_id) award_id,
        transform(coalesce(c.items,array()),x -> named_struct('country_code',x.country_code,'share',x.share,'amount_usd',x.amount_usd,'basis',x.basis)) recipient_countries,
        transform(coalesce(g.items,array()),x -> named_struct('code',x.code,'vocabulary',x.vocabulary,'display_name',x.display_name,'share',x.share)) recipient_regions
      FROM countries c FULL JOIN regions g USING(award_id)""")
    award_recipient_orgs.run(c)
    c.artifact("awards_candidate", f"""SELECT a.*,
        slice(coalesce(o.recipients,array()),1,100) recipients,
        coalesce(o.recipients,array()) recipients_full,
        coalesce(p.recipient_countries,array()) recipient_countries,
        coalesce(p.recipient_regions,array()) recipient_regions,
        coalesce(p.recipient_countries,array()) recipient_countries_full,
        coalesce(p.recipient_regions,array()) recipient_regions_full
      FROM {r}awards_candidate_before_recipients a LEFT JOIN {r}recipient_arrays p ON p.award_id=a.id
      LEFT JOIN {r}recipient_org_arrays o ON o.award_id=a.id""")
    c.zero("RECIPIENT_AWARDS_SAME_ROWS", f"""(SELECT id FROM {r}awards_candidate EXCEPT ALL SELECT id FROM {r}awards_candidate_before_recipients)
      UNION ALL (SELECT id FROM {r}awards_candidate_before_recipients EXCEPT ALL SELECT id FROM {r}awards_candidate)""")
    now = c.count("awards_with_recipient_countries", f"(SELECT id FROM {r}awards_candidate WHERE size(recipient_countries)>0) x")
    regions = c.count("awards_with_recipient_regions", f"(SELECT id FROM {r}awards_candidate WHERE size(recipient_regions)>0) x")
    members = int(award_keys.one(c, f"SELECT coalesce(sum(size(recipient_countries)),0) n FROM {r}awards_candidate", n=0).n)
    c.counts["recipient_country_memberships"] = members
    has_prev = "recipient_countries" in award_keys.columns(c, "previous_api_v")
    prev = (award_keys.one(c, """SELECT count_if(size(recipient_countries)>0) countries,count_if(size(recipient_regions)>0) regions,
        coalesce(sum(size(recipient_countries)),0) members FROM previous_api_v""", countries=0, regions=0, members=0) if has_prev else None)
    award_keys.drop_gate(c, "RECIPIENT_COUNT_DROP", "awards_with_recipient_countries", now, fuse, int(prev.countries) if prev else None)
    award_keys.drop_gate(c, "RECIPIENT_REGIONS_DROP", "awards_with_recipient_regions", regions, fuse, int(prev.regions) if prev else None)
    award_keys.drop_gate(c, "RECIPIENT_MEMBERSHIP_DROP", "recipient_country_memberships", members, fuse, int(prev.members) if prev else None)

    award_recipient_orgs.check_counts(c, fuse)
