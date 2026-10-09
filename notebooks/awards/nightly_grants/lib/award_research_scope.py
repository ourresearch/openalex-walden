"""`research_scope`: is this award research funding? Kyle's rule (2026-10-08): funding that goes to a research organisation, or
is about research; development aid to non-research bodies is out. Built from reference files versioned with the code and one
optional side table (oxjob #1570):

  research/feed_defaults.csv    provenance, is_research (true | false | mixed), basis, tier, recipient_test (yes | no), note. A feed
                                whose awards are all research (a research council, a research charity) or all not; `mixed` leaves the
                                award to the programme rules. `recipient_test = yes` (foundations, charities, aid-adjacent funders):
                                an award research-true by the feed default alone whose matched recipients are all non-research
                                organisations becomes unknown, basis recipient_type: a research funder's grant to a city for a park.
                                A research organisation is one of ROR type education or facility, or one with at least
                                `recipient_research_min_works` OpenAlex works (config, default 50): the Wellcome Sanger Institute
                                and WHO are typed nonprofit and government, a city council is not a publisher. Research councils and agencies are exempt (`no`): every award of theirs is
                                a research instrument whatever the recipient. A provenance with no row (citation stubs known only
                                from paper metadata) gets no research_scope at all, so those awards do not move.
  research/dac_research_codes.csv  the OECD DAC purpose codes that mean research (11182, 12182, 43082 ...). With the optional side
                                table `award_classifications_raw` (provenance, funder_award_id, staging triple, scheme, code: IDRC's
                                own sector codes first), an award carrying one is research, basis sector_code, whatever its recipient.
  research/programme_rules.csv  rule_id, provenance_scope ('*' or one provenance), funder_scope ('*' or a regex on the funder's
                                display name: a feed that carries several agencies), field (funder_scheme | funding_type |
                                display_name | funder_award_id), pattern (Java regex, case-insensitive, against the trimmed value),
                                is_research (true | false | unknown: a programme the feed default must not decide), tier, note. The first matching rule in file order decides; a rule
                                beats the feed default (a public-programme line inside a research funder).
Served: research_scope {is_research BOOLEAN (NULL = unknown), basis (funder_default | programme_rule | sector_code |
recipient_type | unknown), tier INT} or NULL when the feed has no row. Order: programme rule, research sector code, mixed feed ->
unknown, recipient test, feed default. The step runs after the institution match (create_api.py), which it reads. The default listing of /awards is unchanged; the field is a filter.
Policy (which feeds, which programmes, the research boundary) is Kyle's; this file only applies the two tables.
Failure policy: an invalid reference row stops the run (read_defaults / read_rules); against last night (or the frozen
bootstrap_counts on the first night; neither -> stop), the awards with a scope and the awards flagged true may not fall beyond the fuse. Rollback: `clear_funding_flows` (award_keys).
"""
import csv
import re
from pathlib import Path

import award_keys

DEFAULT_COLUMNS = ("provenance", "is_research", "basis", "tier", "recipient_test")
RECIPIENT_RESEARCH_TYPES = ("education", "facility")
DEFAULT_MIN_WORKS = 50
EMPTY_CLASSIFICATIONS = """SELECT CAST(NULL AS STRING) provenance,CAST(NULL AS STRING) funder_award_id,CAST(NULL AS BIGINT) staging_id,
  CAST(NULL AS BIGINT) funder_id,CAST(NULL AS STRING) award_key,CAST(NULL AS STRING) scheme,CAST(NULL AS STRING) code,
  CAST(NULL AS STRING) source_field,CAST(NULL AS STRING) asserted_by,CAST(NULL AS STRING) adapter_version,CAST(NULL AS TIMESTAMP) asserted_at WHERE false"""
RULE_COLUMNS = ("rule_id", "provenance_scope", "funder_scope", "field", "pattern", "is_research", "tier")
DEFAULT_VALUES = {"true", "false", "mixed"}
BASES = {"funder_default", "sector_code"}
RULE_FIELDS = {"funder_scheme", "funding_type", "display_name", "funder_award_id"}


def read_defaults(package_root):
    with open(Path(package_root) / "research" / "feed_defaults.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    seen = set()
    for row in rows:
        ok = (row["provenance"] and row["provenance"] not in seen and row["is_research"] in DEFAULT_VALUES
              and (row["is_research"] == "mixed" or row["basis"] in BASES)
              and (row["tier"] == "" or row["tier"].isdigit()) and row["recipient_test"] in ("yes", "no"))
        if not ok:
            raise RuntimeError("RESEARCH_DEFAULT_ROW_INVALID: " + repr(row))
        seen.add(row["provenance"])
    if not rows:
        raise RuntimeError("RESEARCH_DEFAULTS_EMPTY")
    return rows


def read_rules(package_root):
    with open(Path(package_root) / "research" / "programme_rules.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    seen = set()
    for row in rows:
        try:
            re.compile(row["pattern"])
        except re.error:
            raise RuntimeError("RESEARCH_RULE_PATTERN_INVALID: " + repr(row))
        try:
            if row["funder_scope"] != "*":
                re.compile(row["funder_scope"])
        except re.error:
            raise RuntimeError("RESEARCH_RULE_FUNDER_SCOPE_INVALID: " + repr(row))
        ok = (row["rule_id"] and row["rule_id"] not in seen and row["provenance_scope"] and row["funder_scope"] and row["field"] in RULE_FIELDS
              and row["pattern"] and row["is_research"] in ("true", "false", "unknown") and (row["tier"] == "" or row["tier"].isdigit()))
        if not ok:
            raise RuntimeError("RESEARCH_RULE_ROW_INVALID: " + repr(row))
        seen.add(row["rule_id"])
    return rows


def read_research_organisations(package_root):
    """research/research_organisations.csv: openalex_id (I...), name, note. Organisations that are research organisations although
    ROR types them otherwise and they have fewer than the works floor (a drug-discovery company, a small institute)."""
    with open(Path(package_root) / "research" / "research_organisations.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    ids = [r["openalex_id"] for r in rows]
    if any(not re.fullmatch(r"I[0-9]+", i) for i in ids) or len(set(ids)) != len(ids):
        raise RuntimeError("RESEARCH_ORGANISATIONS_INVALID")
    return ids


def read_research_codes(package_root):
    """research/dac_research_codes.csv: code, name. The DAC purpose codes whose own name says research."""
    with open(Path(package_root) / "research" / "dac_research_codes.csv", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    codes = [r["code"] for r in rows]
    if not codes or any(not c.isdigit() for c in codes) or len(set(codes)) != len(codes):
        raise RuntimeError("RESEARCH_CODES_INVALID")
    return codes


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def defaults_relation_sql(package_root):
    values = ",".join("(" + ",".join(sql_string(row[c]) for c in DEFAULT_COLUMNS) + ")" for row in read_defaults(package_root))
    return f"SELECT * FROM VALUES {values} AS research_feed_defaults({','.join(DEFAULT_COLUMNS)})"


def rules_relation_sql(package_root):
    rows = read_rules(package_root)
    if not rows:
        return "SELECT CAST(NULL AS INT) rule_order,CAST(NULL AS STRING) rule_id,CAST(NULL AS STRING) provenance_scope,CAST(NULL AS STRING) funder_scope,CAST(NULL AS STRING) field,CAST(NULL AS STRING) pattern,CAST(NULL AS STRING) is_research,CAST(NULL AS STRING) tier WHERE false"
    values = ",".join(f"({i}," + ",".join(sql_string(row[c]) for c in RULE_COLUMNS) + ")" for i, row in enumerate(rows))
    return f"SELECT * FROM VALUES {values} AS research_programme_rules(rule_order,{','.join(RULE_COLUMNS)})"


def scope_in_python(defaults, rules, provenance, values, recipient_types=None, research_code=False, recipient_works=None, min_works=DEFAULT_MIN_WORKS):
    """The answer for one award in Python (values: field -> value; recipient_types / recipient_works: the matched institutions'
    types and works counts, [] when none matched; a listed research organisation counts as works >= min_works): the reference
    for the SQL below, used by the tests."""
    for rule in rules:
        if rule["provenance_scope"] in ("*", provenance) and (rule["funder_scope"] == "*" or re.search(rule["funder_scope"], values.get("funder_name") or "", re.IGNORECASE)):
            value = (values.get(rule["field"]) or "").strip()          # a missing value is '' so a '.*' fallback still applies
            if re.search(rule["pattern"], value, re.IGNORECASE):
                if rule["is_research"] == "unknown":
                    return dict(is_research=None, basis="unknown", tier=None)
                return dict(is_research=rule["is_research"] == "true", basis="programme_rule", tier=int(rule["tier"]) if rule["tier"] else None)
    if research_code:
        return dict(is_research=True, basis="sector_code", tier=2)
    for row in defaults:
        if row["provenance"] == provenance:
            if row["is_research"] == "mixed":
                return dict(is_research=None, basis="unknown", tier=None)
            research_org = any(t in RECIPIENT_RESEARCH_TYPES for t in recipient_types or []) or any(w >= min_works for w in recipient_works or [])
            if (row["is_research"] == "true" and row["basis"] == "funder_default" and row["recipient_test"] == "yes"
                    and recipient_types and not research_org):
                return dict(is_research=None, basis="recipient_type", tier=None)
            return dict(is_research=row["is_research"] == "true", basis=row["basis"], tier=int(row["tier"]) if row["tier"] else None)
    return None


def classifications_input(c):
    if award_keys.clearing(c):
        return c.artifact("classifications_raw_empty", EMPTY_CLASSIFICATIONS)
    if c.has_extra("award_classifications_raw"):
        award_keys.nonempty(c, "CLASSIFICATION_RAW_NONEMPTY", "award_classifications_raw_v")
        return "award_classifications_raw_v"
    return c.artifact("classifications_raw_empty", EMPTY_CLASSIFICATIONS)


def run(c, relation="awards_candidate"):
    """Adds research_scope to {r}{relation} (the API payload, after the institution match; awards_candidate in older harnesses)."""
    c.guard(); r = c.r
    root = c.config["package_root"]
    src = relation
    if award_keys.clearing(c):                                      # rollback: no feed has a row, every award's scope is NULL
        c.artifact("research_feed_defaults", "SELECT CAST(NULL AS STRING) provenance,CAST(NULL AS STRING) is_research,CAST(NULL AS STRING) basis,CAST(NULL AS STRING) tier,CAST(NULL AS STRING) recipient_test WHERE false")
        c.artifact("research_programme_rules", "SELECT CAST(NULL AS INT) rule_order,CAST(NULL AS STRING) rule_id,CAST(NULL AS STRING) provenance_scope,CAST(NULL AS STRING) funder_scope,CAST(NULL AS STRING) field,CAST(NULL AS STRING) pattern,CAST(NULL AS STRING) is_research,CAST(NULL AS STRING) tier WHERE false")
    else:
        c.artifact("research_feed_defaults", defaults_relation_sql(root))
        c.artifact("research_programme_rules", rules_relation_sql(root))
    raw = classifications_input(c)
    codes = ",".join(repr(x) for x in read_research_codes(root))
    c.zero("CLASSIFICATION_RAW_SHAPE", f"""SELECT * FROM {raw} WHERE NOT coalesce(length(provenance)>0 AND length(funder_award_id)>0
      AND staging_id IS NOT NULL AND funder_id IS NOT NULL AND length(award_key)>0 AND length(scheme)>0 AND length(code)>0
      AND asserted_by IN ('funder','openalex','curated') AND length(adapter_version)>0 AND asserted_at IS NOT NULL,false)""")
    c.zero("CLASSIFICATION_RAW_UNIQUE", f"SELECT provenance,staging_id,funder_id,award_key,scheme,code FROM {raw} GROUP BY ALL HAVING count(*)<>1")
    # Rows and research-coded rows per provenance tonight, against the last successful night's (or the frozen bootstrap): a producer
    # that lost a feed, or its research codes, stops the run (Codex 10-09 finding 3).
    fuse = float(c.config.get("research_drop_fuse", 0.01))
    for row in c.sql(f"""SELECT provenance,count(*) n,count_if(scheme='oecd_dac_purpose' AND code IN ({codes})) research_n FROM {raw} GROUP BY 1""").collect():
        c.counts[f"classifications_rows_{row.provenance}"] = int(row.n)
        c.counts[f"classifications_research_rows_{row.provenance}"] = int(row.research_n)
    if not award_keys.clearing(c):
        known = {k for k in list(award_keys.last_counts(c)) + list(c.config.get("bootstrap_counts", {})) if k.startswith("classifications_")}
        for key in sorted(known):
            award_keys.drop_gate(c, "CLASSIFICATION_DROP_" + key[len("classifications_"):], key, c.counts.get(key, 0), fuse)
    c.artifact("classification_endpoints", f"SELECT DISTINCT provenance namespace,funder_award_id source_record_id,staging_id,funder_id,award_key FROM {raw}")
    lookup = award_keys.endpoint_lookup(c, "CLASSIFICATION", f"{r}classification_endpoints", "classification_endpoint_lookup")
    # Awards carrying a research purpose code (OECD DAC vocabulary 1): research whatever the recipient.
    c.artifact("classifications_resolved", f"""SELECT a.provenance,a.scheme,a.code,e.stable_id award_id,
        CASE WHEN e.stable_id IS NULL THEN 'UNRESOLVED' WHEN e.status<>'ACTIVE' OR d.id IS NULL THEN 'NOT_SERVED' END dropped
      FROM {raw} a JOIN {lookup} e ON e.namespace=a.provenance AND e.source_record_id=a.funder_award_id
      LEFT JOIN {r}{src} d ON d.id=e.stable_id""")
    c.zero("CLASSIFICATION_NO_LOSS", f"SELECT 1 WHERE (SELECT count(*) FROM {raw})<>(SELECT count(*) FROM {r}classifications_resolved)")
    for reason in ("UNRESOLVED", "NOT_SERVED"):
        c.counts["classifications_dropped_" + reason.lower()] = int(award_keys.one(c, f"SELECT count(*) n FROM {r}classifications_resolved WHERE dropped='{reason}'", n=0).n)
    c.zero("CLASSIFICATION_DROP_FUSE", f"""SELECT count_if(dropped IS NOT NULL) dropped,count(*) total FROM {r}classifications_resolved
      HAVING count_if(dropped IS NOT NULL) > greatest(10, {fuse}*count(*))""")
    c.artifact("research_sector_awards", f"""SELECT DISTINCT award_id FROM {r}classifications_resolved
      WHERE dropped IS NULL AND scheme='oecd_dac_purpose' AND code IN ({codes})""")
    c.count("research_sector_awards", f"{r}research_sector_awards")
    c.artifact(f"{src}_before_research", f"SELECT * FROM {r}{src}")
    # The first rule (file order) whose pattern matches the award's field value, within the rule's provenance and funder scope.
    c.artifact("research_rule_hits", f"""SELECT award_id,min_by(named_struct('is_research',is_research,'tier',tier),rule_order) hit
      FROM (SELECT a.id award_id,p.rule_order,p.is_research,p.tier
        FROM {r}{src}_before_research a JOIN {r}research_programme_rules p
          ON (p.provenance_scope='*' OR p.provenance_scope=a.provenance)
          AND (p.funder_scope='*' OR a.funder.display_name RLIKE concat('(?i)',p.funder_scope))
          AND coalesce(trim(CASE p.field WHEN 'funder_scheme' THEN a.funder_scheme WHEN 'funding_type' THEN a.funding_type
                               WHEN 'display_name' THEN a.display_name WHEN 'funder_award_id' THEN a.funder_award_id END),'')
              RLIKE concat('(?i)',p.pattern)) GROUP BY award_id""")
    types = ",".join(repr(t) for t in RECIPIENT_RESEARCH_TYPES)
    min_works = int(c.config.get("recipient_research_min_works", DEFAULT_MIN_WORKS))
    listed = ",".join(repr(i) for i in read_research_organisations(root)) or "''"
    # Awards with at least one research-organisation recipient: ROR type education / facility, a publisher of research, or an
    # organisation on the reviewed list (research/research_organisations.csv).
    c.artifact("research_recipient_awards", f"""SELECT DISTINCT x.award_id FROM
      (SELECT a.id award_id,ia.type,CAST(regexp_replace(ia.id,'^https://openalex[.]org/I','') AS BIGINT) institution_id,
              regexp_replace(ia.id,'^https://openalex[.]org/','') short_id
         FROM {r}{src}_before_research a LATERAL VIEW explode(a.institution_awarded) e AS ia) x
      LEFT JOIN institutions_api_v i ON i.id=x.institution_id
      WHERE x.type IN ({types}) OR coalesce(i.works_count,0) >= {min_works} OR x.short_id IN ({listed})""")
    c.artifact(src, f"""SELECT a.*,
        CASE WHEN h.award_id IS NOT NULL AND h.hit.is_research='unknown' THEN named_struct('is_research',CAST(NULL AS BOOLEAN),'basis','unknown','tier',CAST(NULL AS INT))
             WHEN h.award_id IS NOT NULL THEN named_struct('is_research',h.hit.is_research='true','basis','programme_rule','tier',CAST(nullif(h.hit.tier,'') AS INT))
             WHEN s.award_id IS NOT NULL THEN named_struct('is_research',true,'basis','sector_code','tier',2)
             WHEN d.is_research='mixed' THEN named_struct('is_research',CAST(NULL AS BOOLEAN),'basis','unknown','tier',CAST(NULL AS INT))
             WHEN d.is_research='true' AND d.basis='funder_default' AND d.recipient_test='yes' AND size(a.institution_awarded)>0
                  AND rr.award_id IS NULL
                  THEN named_struct('is_research',CAST(NULL AS BOOLEAN),'basis','recipient_type','tier',CAST(NULL AS INT))
             WHEN d.provenance IS NOT NULL THEN named_struct('is_research',d.is_research='true','basis',d.basis,'tier',CAST(nullif(d.tier,'') AS INT))
        END research_scope
      FROM {r}{src}_before_research a
      LEFT JOIN {r}research_rule_hits h ON h.award_id=a.id
      LEFT JOIN {r}research_sector_awards s ON s.award_id=a.id
      LEFT JOIN {r}research_recipient_awards rr ON rr.award_id=a.id
      LEFT JOIN {r}research_feed_defaults d ON d.provenance=a.provenance""")
    c.zero("RESEARCH_AWARDS_SAME_ROWS", f"""(SELECT id FROM {r}{src} EXCEPT ALL SELECT id FROM {r}{src}_before_research)
      UNION ALL (SELECT id FROM {r}{src}_before_research EXCEPT ALL SELECT id FROM {r}{src})""")
    now = c.count("awards_with_research_scope", f"(SELECT id FROM {r}{src} WHERE research_scope IS NOT NULL) x")
    for row in c.sql(f"""SELECT concat_ws('_',coalesce(CAST(research_scope.is_research AS STRING),'unknown'),research_scope.basis) k,count(*) n
        FROM {r}{src} WHERE research_scope IS NOT NULL GROUP BY 1""").collect():
        c.counts["research_scope_" + row.k] = int(row.n)
    true_now = sum(c.counts.get("research_scope_true_" + b, 0) for b in ("funder_default", "programme_rule", "sector_code"))
    c.counts["research_true"] = true_now
    has_prev = "research_scope" in award_keys.columns(c, "previous_api_v")
    prev = award_keys.one(c, "SELECT count_if(research_scope.is_research) t,count(*) n FROM previous_api_v WHERE research_scope IS NOT NULL", n=0, t=0) if has_prev else None
    award_keys.drop_gate(c, "RESEARCH_SCOPE_DROP", "awards_with_research_scope", now, fuse, int(prev.n) if prev else None)
    # A reference-file edit that empties a research feed shows here; an intended re-policy runs with research_drop_fuse raised.
    award_keys.drop_gate(c, "RESEARCH_TRUE_DROP", "research_true", true_now, fuse, int(prev.t) if prev else None)
