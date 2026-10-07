"""`research_scope`: is this award research funding? Built from two reference files versioned with the code (oxjob #1570):

  research/feed_defaults.csv    provenance, is_research (true | false | mixed), basis, tier, note. A feed whose awards are all
                                research (a research council, a research charity) or all not; `mixed` leaves the award to the
                                programme rules. A provenance with no row (citation stubs known only from paper metadata) gets no
                                research_scope at all, so those awards do not move.
  research/programme_rules.csv  rule_id, provenance_scope ('*' or one provenance), field (funder_scheme | funding_type |
                                display_name | funder_award_id), pattern (Java regex, case-insensitive, against the trimmed value),
                                is_research (true | false), tier, note. The first matching rule in file order decides; a rule
                                beats the feed default (a public-programme line inside a research funder).
Served: research_scope {is_research BOOLEAN (NULL = unknown), basis (funder_default | programme_rule | sector_code | unknown),
tier INT} or NULL when the feed has no row. The default listing of /awards is unchanged; the field is a filter.
Policy (which feeds, which programmes, the research boundary) is Kyle's; this file only applies the two tables.
Failure policy: an invalid reference row stops the run (read_defaults / read_rules); against last night, the awards with a scope and
the awards flagged true may not fall beyond the fuse. Rollback: `clear_funding_flows` (award_keys).
"""
import csv
import re
from pathlib import Path

import award_keys

DEFAULT_COLUMNS = ("provenance", "is_research", "basis", "tier")
RULE_COLUMNS = ("rule_id", "provenance_scope", "field", "pattern", "is_research", "tier")
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
              and (row["tier"] == "" or row["tier"].isdigit()))
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
        ok = (row["rule_id"] and row["rule_id"] not in seen and row["provenance_scope"] and row["field"] in RULE_FIELDS
              and row["pattern"] and row["is_research"] in ("true", "false") and (row["tier"] == "" or row["tier"].isdigit()))
        if not ok:
            raise RuntimeError("RESEARCH_RULE_ROW_INVALID: " + repr(row))
        seen.add(row["rule_id"])
    return rows


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def defaults_relation_sql(package_root):
    values = ",".join("(" + ",".join(sql_string(row[c]) for c in DEFAULT_COLUMNS) + ")" for row in read_defaults(package_root))
    return f"SELECT * FROM VALUES {values} AS research_feed_defaults({','.join(DEFAULT_COLUMNS)})"


def rules_relation_sql(package_root):
    rows = read_rules(package_root)
    if not rows:
        return "SELECT CAST(NULL AS INT) rule_order,CAST(NULL AS STRING) rule_id,CAST(NULL AS STRING) provenance_scope,CAST(NULL AS STRING) field,CAST(NULL AS STRING) pattern,CAST(NULL AS STRING) is_research,CAST(NULL AS STRING) tier WHERE false"
    values = ",".join(f"({i}," + ",".join(sql_string(row[c]) for c in RULE_COLUMNS) + ")" for i, row in enumerate(rows))
    return f"SELECT * FROM VALUES {values} AS research_programme_rules(rule_order,{','.join(RULE_COLUMNS)})"


def scope_in_python(defaults, rules, provenance, values):
    """The answer for one award in Python (values: field -> value): the reference for the SQL below, used by the tests."""
    for rule in rules:
        if rule["provenance_scope"] in ("*", provenance):
            value = (values.get(rule["field"]) or "").strip()
            if value and re.search(rule["pattern"], value, re.IGNORECASE):
                return dict(is_research=rule["is_research"] == "true", basis="programme_rule", tier=int(rule["tier"]) if rule["tier"] else None)
    for row in defaults:
        if row["provenance"] == provenance:
            if row["is_research"] == "mixed":
                return dict(is_research=None, basis="unknown", tier=None)
            return dict(is_research=row["is_research"] == "true", basis=row["basis"], tier=int(row["tier"]) if row["tier"] else None)
    return None


def run(c):
    c.guard(); r = c.r
    root = c.config["package_root"]
    if award_keys.clearing(c):                                      # rollback: no feed has a row, every award's scope is NULL
        c.artifact("research_feed_defaults", "SELECT CAST(NULL AS STRING) provenance,CAST(NULL AS STRING) is_research,CAST(NULL AS STRING) basis,CAST(NULL AS STRING) tier WHERE false")
        c.artifact("research_programme_rules", "SELECT CAST(NULL AS INT) rule_order,CAST(NULL AS STRING) rule_id,CAST(NULL AS STRING) provenance_scope,CAST(NULL AS STRING) field,CAST(NULL AS STRING) pattern,CAST(NULL AS STRING) is_research,CAST(NULL AS STRING) tier WHERE false")
    else:
        c.artifact("research_feed_defaults", defaults_relation_sql(root))
        c.artifact("research_programme_rules", rules_relation_sql(root))
    c.artifact("awards_candidate_before_research", f"SELECT * FROM {r}awards_candidate")
    # The first rule (file order) whose pattern matches the award's field value, within the rule's provenance scope.
    c.artifact("research_rule_hits", f"""SELECT award_id,min_by(named_struct('is_research',is_research,'tier',tier),rule_order) hit
      FROM (SELECT a.id award_id,p.rule_order,p.is_research,p.tier
        FROM {r}awards_candidate_before_research a JOIN {r}research_programme_rules p
          ON (p.provenance_scope='*' OR p.provenance_scope=a.provenance)
          AND trim(CASE p.field WHEN 'funder_scheme' THEN a.funder_scheme WHEN 'funding_type' THEN a.funding_type
                               WHEN 'display_name' THEN a.display_name WHEN 'funder_award_id' THEN a.funder_award_id END)
              RLIKE concat('(?i)',p.pattern)) GROUP BY award_id""")
    c.artifact("awards_candidate", f"""SELECT a.*,
        CASE WHEN h.award_id IS NOT NULL THEN named_struct('is_research',h.hit.is_research='true','basis','programme_rule','tier',CAST(nullif(h.hit.tier,'') AS INT))
             WHEN d.is_research='mixed' THEN named_struct('is_research',CAST(NULL AS BOOLEAN),'basis','unknown','tier',CAST(NULL AS INT))
             WHEN d.provenance IS NOT NULL THEN named_struct('is_research',d.is_research='true','basis',d.basis,'tier',CAST(nullif(d.tier,'') AS INT))
        END research_scope
      FROM {r}awards_candidate_before_research a
      LEFT JOIN {r}research_rule_hits h ON h.award_id=a.id
      LEFT JOIN {r}research_feed_defaults d ON d.provenance=a.provenance""")
    c.zero("RESEARCH_AWARDS_SAME_ROWS", f"""(SELECT id FROM {r}awards_candidate EXCEPT ALL SELECT id FROM {r}awards_candidate_before_research)
      UNION ALL (SELECT id FROM {r}awards_candidate_before_research EXCEPT ALL SELECT id FROM {r}awards_candidate)""")
    now = c.count("awards_with_research_scope", f"(SELECT id FROM {r}awards_candidate WHERE research_scope IS NOT NULL) x")
    for row in c.sql(f"""SELECT concat_ws('_',coalesce(CAST(research_scope.is_research AS STRING),'unknown'),research_scope.basis) k,count(*) n
        FROM {r}awards_candidate WHERE research_scope IS NOT NULL GROUP BY 1""").collect():
        c.counts["research_scope_" + row.k] = int(row.n)
    if "research_scope" in award_keys.columns(c, "previous_api_v") and not award_keys.clearing(c):
        fuse = float(c.config.get("research_drop_fuse", 0.01))
        prev = award_keys.one(c, "SELECT count_if(research_scope.is_research) t,count(*) n FROM previous_api_v WHERE research_scope IS NOT NULL", n=0, t=0)
        award_keys.count_drop_check(c, "RESEARCH_SCOPE_DROP", int(prev.n), now, fuse)
        # A reference-file edit that empties a research feed shows here; an intended re-policy runs with research_drop_fuse raised.
        award_keys.count_drop_check(c, "RESEARCH_TRUE_DROP", int(prev.t), c.counts.get("research_scope_true_funder_default", 0)
                                    + c.counts.get("research_scope_true_programme_rule", 0) + c.counts.get("research_scope_true_sector_code", 0), fuse)
