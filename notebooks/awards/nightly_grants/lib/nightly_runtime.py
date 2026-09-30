"""Nightly grants runtime: one run row, one lock (with crash takeover), fixed scratch, inputs read once per run.

What the old release wrapper did and this keeps:
  - every input is bound ONCE per run at the version current when the run starts (no multi-day pins, no waiting);
    materialized views (no Delta history) are copied into scratch at start instead
  - the D0 derivations (crossref/datacite record projections, funder map, normalization) are rebuilt from those inputs
  - every write goes through write(), fenced to the configured prefixes
"""
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

from stable_award_ids import compact, ident, literal, sha, short
from capture_support import verify_body

# one completeness unit per producer: the raw row's provenance, else the adapter family (gtr, ...)
SOURCE_SQL = "coalesce(o.payload.provenance,o.family)"
MAINTENANCE = ("OPTIMIZE", "VACUUM START", "VACUUM END", "COMPUTE STATISTICS")
PARAM = re.compile(r":(rid|ts)\b")
WRITE_TARGET = re.compile(r"\s*(?:CREATE (?:OR REPLACE )?TABLE|MERGE INTO|UPDATE|INSERT INTO|DELETE FROM)\s+(\S+)", re.I)

# The WorkAwards legs (view, relation key in config["inputs"]) and every other input the build reads.
REQUIRED_INPUTS = {
    "raw", "gtr", "crossref", "datacite", "crossref_grants", "datacite_items", "funders", "awards", "aliases",
    "works", "topics", "work_funder", "grobid", "crossref_work_funders", "nwo_work_funders", "anr_work_funders",
    "datacite_work_funders", "europepmc_work_funders", "kaken_work_funders", "relink", "affiliation_lookup",
    "institutions_api", "previous_api", "merged_work_ids", "kaken_projects",
}
DERIVED = {"crossref_records", "datacite_records", "funder_map", "normalization", "normalization_work", "continuity",
           "receipts", "previous_bindings", "last_state", "merge_doi_pairs"}


class Nightly:
    def __init__(self, spark, config, databricks_run_id=None):
        self.spark, self.config = spark, config
        self.run_id = str(uuid.uuid4())
        self.databricks_run_id = str(databricks_run_id) if databricks_run_id else None
        self.started = datetime.now(timezone.utc).replace(microsecond=0)
        self.params = {"rid": self.run_id, "ts": self.started.strftime("%Y-%m-%d %H:%M:%S")}
        self.p = config["registry_prefix"]                    # award_entities, award_keys, run log, lock, carried state
        self.r = config["scratch_prefix"]                     # overwritten every night
        ident(self.p + "award_entities"); ident(self.r + "x")
        self.fence = tuple(config["write_fence"])             # every write target must start with one of these
        self.require(len(self.fence) > 0, "WRITE_FENCE_REQUIRED")
        self.outputs = {k: ident(v) for k, v in config["outputs"].items()}
        self.require(set(self.outputs) == {"awards", "aliases", "work_awards", "api"}, "OUTPUT_SET")
        self.versions, self.counts, self.checks, self.log = {}, {}, {}, []
        self.locked = False
        if config.get("require_databricks_run_id"):
            self.require(self.databricks_run_id, "DATABRICKS_RUN_ID_REQUIRED")

    # ---------- primitives ----------
    def require(self, condition, name):
        if not condition:
            raise RuntimeError(name)

    def sql(self, statement, params=None):
        wanted = set(PARAM.findall(statement))
        args = {k: v for k, v in self.params.items() if k in wanted}
        args.update(params or {})
        self.log.append(statement)
        return self.spark.sql(statement, args) if args else self.spark.sql(statement)

    def write(self, statement, params=None):
        match = WRITE_TARGET.match(statement)
        self.require(match is not None, "UNSUPPORTED_WRITE")
        target = ident(match.group(1))
        self.require(target.startswith(self.fence), "WRITE_OUTSIDE_FENCE: " + target)
        if self.locked:
            self.guard()
        return self.sql(statement, params)

    def artifact(self, name, query, params=None):
        relation = ident(self.r + short(name))
        self.write(f"CREATE OR REPLACE TABLE {relation} USING DELTA "
                   f"TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported') AS {query}", params)
        return relation

    def zero(self, name, query):
        rows = self.sql(f"SELECT * FROM ({query}) check_rows LIMIT 20").collect()
        self.checks[name] = not rows
        if rows:
            raise RuntimeError(name + ": " + repr(rows)[:3000])

    def count(self, name, relation):
        n = int(self.sql(f"SELECT count(*) n FROM {relation}").collect()[0].n)
        self.counts[name] = n
        return n

    def graph(self, source, label, idcol="stable_id", nextcol="redirect_to", statuscol="status"):
        """Pointer traversal to a terminal ACTIVE/GONE node; cycles, missing targets and depth > 100 fail (orig stable_award_ids.py:479)."""
        base = self.artifact(label + "_paths_0", f"SELECT {idcol} owner_id,{idcol} stable_id,{nextcol} next_id,{statuscol} status,array({idcol}) path FROM {source}")
        for depth in range(101):
            if not self.sql(f"SELECT 1 FROM {base} WHERE status='REDIRECTED' LIMIT 1").collect():
                break
            self.require(depth < 100, "GRAPH_DEPTH_FUSE")
            self.zero(label + "_cycle_missing", f"SELECT p.owner_id FROM {base} p LEFT JOIN {source} e ON e.{idcol}=p.next_id WHERE p.status='REDIRECTED' AND (e.{idcol} IS NULL OR array_contains(p.path,p.next_id))")
            base = self.artifact(label + f"_paths_{depth+1}", f"SELECT p.owner_id, CASE WHEN p.status='REDIRECTED' THEN e.{idcol} ELSE p.stable_id END stable_id, CASE WHEN p.status='REDIRECTED' THEN e.{nextcol} ELSE p.next_id END next_id, CASE WHEN p.status='REDIRECTED' THEN e.{statuscol} ELSE p.status END status, CASE WHEN p.status='REDIRECTED' THEN concat(p.path,array(e.{idcol})) ELSE p.path END path FROM {base} p LEFT JOIN {source} e ON p.status='REDIRECTED' AND e.{idcol}=p.next_id")
        result = self.artifact(label, f"SELECT owner_id,stable_id,status FROM {base}")
        self.zero(label + "_unique", f"SELECT owner_id FROM {result} GROUP BY owner_id HAVING COUNT(*)<>1")
        self.zero(label + "_shape", f"SELECT * FROM {base} WHERE status NOT IN ('ACTIVE','GONE') OR next_id IS NOT NULL")
        return result

    # ---------- run row + lock ----------
    def start(self):
        p = self.p
        self.write(f"INSERT INTO {p}award_nightly_runs (run_id,databricks_run_id,started_at,status) VALUES (:rid,:dbr,CAST(:ts AS TIMESTAMP),'RUNNING')",
                   {"dbr": self.databricks_run_id})
        rows = self.sql(f"SELECT holder,databricks_run_id FROM {p}award_nightly_lock WHERE lock_name='nightly'").collect()
        self.require(len(rows) == 1, "LOCK_ROW_MISSING")
        holder = rows[0].holder
        if holder is not None:
            self.require(self.holder_is_dead(holder, rows[0].databricks_run_id), "NIGHTLY_LOCK_HELD_BY_LIVE_RUN: " + str(holder))
            self.write(f"UPDATE {p}award_nightly_runs SET status='ABANDONED',finished_at=current_timestamp(),error=concat('lock taken over by ',:rid) WHERE run_id=:old AND status='RUNNING'",
                       {"old": holder})
        # compare-and-set: only one concurrent writer's UPDATE can match the old holder
        self.write(f"UPDATE {p}award_nightly_lock SET holder=:rid,databricks_run_id=:dbr,acquired_at=current_timestamp() WHERE lock_name='nightly' AND holder <=> :old",
                   {"dbr": self.databricks_run_id, "old": holder})
        self.locked = True
        self.guard()
        legacy = self.config.get("legacy_lock")              # old chain / MergeFunders lock: held by this run for its whole life (Codex r1/r2 #3)
        if legacy:
            rows = self.sql(f"SELECT holder FROM {ident(legacy)} WHERE lock_name='registry'").collect()
            self.require(len(rows) == 1, "LEGACY_LOCK_ROW_MISSING")
            old = rows[0].holder
            if old is not None:
                mine = self.sql(f"SELECT status FROM {p}award_nightly_runs WHERE run_id=:old", {"old": old}).collect()
                self.require(mine and mine[0].status != "RUNNING", "LEGACY_REGISTRY_LOCK_HELD: " + str(old))
            self.write(f"UPDATE {ident(legacy)} SET holder=:rid,acquired_at=current_timestamp() WHERE lock_name='registry' AND holder <=> :old", {"old": old})
            got = self.sql(f"SELECT holder FROM {ident(legacy)} WHERE lock_name='registry'").collect()
            self.require(got[0].holder == self.run_id, "LEGACY_REGISTRY_LOCK_CONTENTION")
            self.legacy_locked = True

    def holder_is_dead(self, holder, databricks_run_id):
        """A holder is dead when its run row is finished, or its Databricks run is no longer running."""
        row = self.sql(f"SELECT status FROM {self.p}award_nightly_runs WHERE run_id=:old", {"old": holder}).collect()
        if not row or row[0].status != "RUNNING":
            return True
        if not databricks_run_id:
            return False                                      # a manual holder with no run id is never taken over
        from databricks.sdk import WorkspaceClient
        state = WorkspaceClient().jobs.get_run(int(databricks_run_id)).state
        return state.life_cycle_state.value in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")

    def guard(self):
        rows = self.sql(f"SELECT holder FROM {self.p}award_nightly_lock WHERE lock_name='nightly'").collect()
        self.require(len(rows) == 1 and rows[0].holder == self.run_id, "NIGHTLY_LOCK_LOST")

    def finish(self, status, error=None):
        details = compact(dict(versions=self.versions, counts=self.counts, checks=self.checks))
        try:
            self.sql(f"UPDATE {self.p}award_nightly_runs SET finished_at=current_timestamp(),status=:status,details_json=:details,error=:error WHERE run_id=:rid",
                     {"status": status, "details": details, "error": error})
        finally:
            if getattr(self, "legacy_locked", False):
                self.require(self.config['legacy_lock'].startswith(self.fence), "WRITE_OUTSIDE_FENCE: legacy lock")
                self.sql(f"UPDATE {ident(self.config['legacy_lock'])} SET holder=NULL,acquired_at=NULL WHERE lock_name='registry' AND holder=:rid")
                self.legacy_locked = False
            if self.locked:
                self.sql(f"UPDATE {self.p}award_nightly_lock SET holder=NULL,databricks_run_id=NULL,acquired_at=NULL WHERE lock_name='nightly' AND holder=:rid")
                self.locked = False

    # ---------- inputs ----------
    def bind(self, view, relation):
        """Current version, read once: every later statement reads exactly this version (no drift inside a run)."""
        relation = ident(relation)
        try:
            history = self.sql(f"SELECT version,timestamp,operation FROM (DESCRIBE HISTORY {relation}) ORDER BY version DESC LIMIT 50").collect()
        except Exception as exc:
            if "EXPECT_TABLE_NOT_VIEW" not in str(exc) and "not a Delta table" not in str(exc):
                raise
            history = None
        row = None
        if history:
            row = history[0]
            for h in history:                                 # newest data commit decides; maintenance after it preserves content
                if h.operation in MAINTENANCE:
                    continue
                if h.operation == "DELETE":                   # producers rewrite as DELETE then WRITE: never read between the two
                    prior = [x for x in history if int(x.version) == int(h.version) - 1]
                    self.require(prior, "INPUT_ENDS_IN_DELETE_WITHOUT_PRIOR_VERSION: " + relation)
                    row = prior[0]
                    self.versions.setdefault("_skipped_trailing_delete", []).append(dict(relation=relation, delete_version=int(h.version)))
                break
        if history is None:                                   # a view / materialized view: copy it now
            copy = self.artifact("in_" + view, f"SELECT * FROM {relation}")
            self.versions[view] = dict(relation=relation, copied_to=copy, rows=self.count("input_" + view, copy))
            self.sql(f"SELECT * FROM {copy}").createOrReplaceTempView(view + "_v")
            return
        self.versions[view] = dict(relation=relation, version=int(row.version), timestamp=str(row.timestamp), operation=row.operation)
        self.sql(f"SELECT * FROM {relation} VERSION AS OF {int(row.version)}").createOrReplaceTempView(view + "_v")

    def bind_inputs(self):
        inputs = self.config["inputs"]
        self.require(set(inputs) == REQUIRED_INPUTS, "INPUT_SET: " + compact(sorted(set(inputs) ^ REQUIRED_INPUTS)))
        for view, relation in inputs.items():
            self.bind(view, relation)
        self.derive_inputs()

    def identity_file(self, name):
        return (Path(self.config["package_root"]) / "identity" / name).read_text()

    def verify_udfs(self):
        for fn, file in (("openalex.awards.award_norm_key", "udf_award_norm_key.body.sql"),
                         ("openalex.awards.award_id_is_weak", "udf_award_id_is_weak.body.sql")):
            verify_body(self.sql("DESCRIBE FUNCTION EXTENDED " + fn).collect(), sha(self.identity_file(file)))   # LIVE_UDF_BODY_CHANGED

    def derive_inputs(self):
        """Port of NightlyD0 01-06 + CaptureD0 normalization_work/continuity (deployed nightly/sql, CaptureD0.r1.py:170-200)."""
        r, p = self.r, self.p
        self.verify_udfs()
        sharp = ",".join(str(int(x)) for x in json.loads(self.identity_file("sharp_funders.json"))["sharp_funders"])
        live = self.config.get("live_funders")
        if live:
            n = self.sql(f"""SELECT count(*) n FROM funders_v b FULL JOIN {ident(live)} l ON l.funder_id=b.funder_id
                WHERE NOT(b.merge_into_id <=> l.merge_into_id) OR b.funder_id IS NULL OR l.funder_id IS NULL""").collect()[0].n
            self.counts["funder_merges_pending_basis_update"] = int(n)
        self.view("crossref_records", self.artifact("crossref_records", "SELECT DOI record_doi,URL record_url,type record_type FROM crossref_grants_v"))
        self.view("datacite_records", self.artifact("datacite_records", "SELECT id record_id,attributes.types.resourceTypeGeneral resource_type FROM datacite_items_v"))
        self.view("funder_map", self.artifact("funder_map", """WITH RECURSIVE p(source_funder_id,current_id,next_id,depth) AS (
            SELECT funder_id,funder_id,merge_into_id,0 FROM funders_v
            UNION ALL SELECT p.source_funder_id,f.funder_id,f.merge_into_id,p.depth+1 FROM p JOIN funders_v f ON f.funder_id=p.next_id
            WHERE p.next_id IS NOT NULL AND p.depth<20)
          SELECT source_funder_id,current_id canonical_funder_id,depth FROM p WHERE next_id IS NULL LIMIT ALL"""))
        self.view("normalization", self.artifact("normalization", f"""WITH obs AS (
            SELECT id staging_id,funder_id,lower(funder_award_id) award_key,funder_award_id award_raw,
              (priority>=3 OR (priority=1 AND provenance IN ('crossref_work','datacite'))) direct,true from_raw,false from_gtr FROM raw_v
            UNION ALL SELECT ABS(XXHASH64(id)) % 9000000000,funder_id,lower(funder_award_id),funder_award_id,true,false,true FROM gtr_v),
          t AS (SELECT staging_id,funder_id,award_key,bool_or(direct) is_direct,min(award_raw) award_sample,count(*) n_obs,
              bool_or(from_raw) has_raw,bool_or(from_gtr) has_gtr FROM obs GROUP BY staging_id,funder_id,award_key),
          c AS (SELECT t.*,COALESCE(m.canonical_funder_id,t.funder_id) canonical_funder_id,CASE WHEN t.is_direct THEN 'registry' ELSE 'deposited' END regime
              FROM t LEFT JOIN {r}funder_map m ON m.source_funder_id=t.funder_id),
          w AS (SELECT c.*,CASE WHEN c.is_direct THEN false ELSE COALESCE(openalex.awards.award_id_is_weak(c.canonical_funder_id,c.award_sample),false) END is_weak,
              c.canonical_funder_id IN ({sharp}) sharp_eligible FROM c)
          SELECT staging_id,funder_id,award_key,is_direct,CASE WHEN sharp_eligible AND NOT is_weak
            THEN openalex.awards.award_norm_key(canonical_funder_id,award_sample,regime) END sharp_key,
            sharp_eligible,is_weak,canonical_funder_id,regime,award_sample,n_obs,has_raw,has_gtr FROM w"""))
        self.d0_checks()
        from work_awards import leg_sql
        self.view("work_observations_in", self.artifact("work_observations_in", f"SELECT DISTINCT * FROM ({leg_sql()})"))
        self.view("normalization_work", self.artifact("normalization_work", f"""WITH inputs AS (
            SELECT DISTINCT f.canonical_funder_id funder_id,w.award_key,w.is_direct FROM work_observations_in_v w LEFT JOIN funder_map_v f ON f.source_funder_id=w.source_funder_id
            UNION SELECT f.canonical_funder_id,lower(a.old_funder_award_id),false FROM aliases_v a LEFT JOIN funder_map_v f ON f.source_funder_id=a.funder_id),
          seeded AS (SELECT DISTINCT canonical_funder_id funder_id,award_key,is_direct FROM normalization_v),
          gaps AS (SELECT i.* FROM inputs i LEFT ANTI JOIN seeded s ON s.funder_id <=> i.funder_id AND s.award_key <=> i.award_key AND s.is_direct=i.is_direct),
          flags AS (SELECT *,funder_id IN ({sharp}) sharp_eligible,
            CASE WHEN is_direct THEN false ELSE coalesce(openalex.awards.award_id_is_weak(funder_id,award_key),false) END is_weak FROM gaps)
          SELECT funder_id,award_key,is_direct,CASE WHEN sharp_eligible AND NOT is_weak THEN
            openalex.awards.award_norm_key(funder_id,award_key,CASE WHEN is_direct THEN 'registry' ELSE 'deposited' END) END sharp_key,
            sharp_eligible,is_weak FROM flags"""))
        self.verify_udfs()                                    # a definition change during materialization also fails
        self.zero("WORK_FUNDER_MAP_COVERAGE", "SELECT w.* FROM work_observations_in_v w LEFT ANTI JOIN funder_map_v f ON f.source_funder_id=w.source_funder_id WHERE w.source_funder_id IS NOT NULL")
        self.zero("ALIAS_FUNDER_MAP_COVERAGE", "SELECT a.* FROM aliases_v a LEFT ANTI JOIN funder_map_v f ON f.source_funder_id=a.funder_id WHERE a.funder_id IS NOT NULL")
        self.zero("NORMALIZATION_WORK_UNIQUE", "SELECT funder_id,award_key,is_direct FROM normalization_work_v GROUP BY ALL HAVING count(*)<>1")
        self.zero("NORMALIZATION_UNION_UNIQUE", """SELECT funder_id,award_key,is_direct FROM (
            SELECT DISTINCT canonical_funder_id funder_id,award_key,is_direct,sharp_key,sharp_eligible,is_weak FROM normalization_v
            UNION SELECT * FROM normalization_work_v) GROUP BY ALL HAVING count(*)<>1""")
        self.spark.sql("""SELECT CAST(NULL AS STRING) old_observation_key,CAST(NULL AS STRING) new_observation_key,
            CAST(NULL AS BIGINT) stable_id,CAST(NULL AS BIGINT) old_funder_id,CAST(NULL AS BIGINT) new_funder_id,
            CAST(NULL AS STRING) namespace,CAST(NULL AS STRING) source_record_id,CAST(NULL AS STRING) producer_code_sha,
            CAST(NULL AS STRING) evidence_uri WHERE false""").createOrReplaceTempView("continuity_v")   # empty: unproven corrections block
        # carried state (written by the previous successful run; seeded once from n20260925)
        for view, table in (("previous_bindings", "award_bindings_last"), ("merge_doi_pairs", "award_merge_doi_pairs")):
            self.bind(view, p + table)
        # withdrawal evidence is per SOURCE (raw provenance, or the adapter family): the deployed SQL joins on `family`
        self.sql("SELECT observation_key,stable_id,source family FROM previous_bindings_v").createOrReplaceTempView("previous_bindings")
        self.bind("state_transitions", p + "award_state_transitions")
        # optional approved inputs, e.g. {"migration_manifest": "<relation>"} for an approved duplicate-retirement batch
        for view, relation in self.config.get("extra_inputs", {}).items():
            self.require(view in ("migration_manifest",), "UNKNOWN_EXTRA_INPUT: " + view)
            self.bind(view, relation)
        # yesterday's public state: ids in the live awards table are ACTIVE; everything else as the registry says
        self.sql(f"""SELECT e.stable_id,CASE WHEN a.id IS NOT NULL THEN 'ACTIVE' WHEN e.status='ACTIVE' THEN 'UNPUBLISHED' ELSE e.status END status,e.redirect_to
            FROM {p}award_entities e LEFT JOIN (SELECT DISTINCT id FROM awards_v) a ON a.id=e.stable_id""").createOrReplaceTempView("last_state")
        self.zero("PUBLIC_ID_IN_REGISTRY", f"SELECT a.id FROM awards_v a LEFT ANTI JOIN {p}award_entities e ON e.stable_id=a.id")

    def has_extra(self, name):
        return name in self.config.get("extra_inputs", {})

    def view(self, name, relation):
        self.sql(f"SELECT * FROM {relation}").createOrReplaceTempView(name + "_v")

    def d0_checks(self):
        """The identity-relevant D0 gates (deployed nightly/sql/08_gates_registry.sql); info rows and pin checks dropped."""
        r = self.r
        z = self.zero
        z("FUNDER_MAP_ROWS", f"SELECT 1 WHERE (SELECT count(*) FROM {r}funder_map)<>(SELECT count(*) FROM funders_v)")
        z("FUNDER_MAP_UNIQUE", f"SELECT source_funder_id FROM {r}funder_map GROUP BY 1 HAVING count(*)<>1")
        z("FUNDER_MAP_CANONICAL_NOT_NULL", f"SELECT * FROM {r}funder_map WHERE canonical_funder_id IS NULL")
        z("FUNDER_MAP_TERMINAL", f"SELECT m.* FROM {r}funder_map m JOIN funders_v f ON f.funder_id=m.canonical_funder_id WHERE f.merge_into_id IS NOT NULL")
        z("FUNDER_MAP_COVERS_SOURCES", f"SELECT n.funder_id FROM (SELECT DISTINCT funder_id FROM {r}normalization WHERE funder_id IS NOT NULL) n LEFT ANTI JOIN {r}funder_map m ON m.source_funder_id=n.funder_id")
        z("NORMALIZATION_UNIQUE_TRIPLE", f"SELECT staging_id,funder_id,award_key FROM {r}normalization GROUP BY ALL HAVING count(*)<>1")
        z("NORMALIZATION_COVERS_RAW", f"SELECT DISTINCT id,funder_id,lower(funder_award_id) FROM raw_v x LEFT ANTI JOIN {r}normalization n ON n.staging_id=x.id AND n.funder_id <=> x.funder_id AND n.award_key <=> lower(x.funder_award_id)")
        z("NORMALIZATION_COVERS_GTR", f"SELECT DISTINCT id FROM gtr_v g LEFT ANTI JOIN {r}normalization n ON n.staging_id=ABS(XXHASH64(g.id)) % 9000000000 AND n.funder_id <=> g.funder_id AND n.award_key <=> lower(g.funder_award_id)")
        z("DIRECT_NEVER_WEAK", f"SELECT * FROM {r}normalization WHERE is_direct AND is_weak")
        z("WEAK_NOT_NULL", f"SELECT * FROM {r}normalization WHERE is_weak IS NULL")
        z("SHARP_ONLY_ELIGIBLE", f"SELECT * FROM {r}normalization WHERE sharp_key IS NOT NULL AND NOT (sharp_eligible AND NOT is_weak)")
        z("CROSSREF_RECORDS_GRANT", f"SELECT * FROM {r}crossref_records WHERE record_type<>'grant' OR record_type IS NULL")
        z("CROSSREF_RECORDS_DOI_DISTINCT", f"SELECT lower(record_doi) FROM {r}crossref_records GROUP BY 1 HAVING count(*)<>1")
        z("DATACITE_RECORDS_AWARD", f"SELECT * FROM {r}datacite_records WHERE resource_type<>'Award' OR resource_type IS NULL")
        z("DATACITE_RECORDS_ID_DISTINCT", f"SELECT lower(record_id) FROM {r}datacite_records GROUP BY 1 HAVING count(*)<>1")
        z("CROSSREF_DOI_IN_RECORDS", f"SELECT c.* FROM crossref_v c LEFT ANTI JOIN {r}crossref_records x ON lower(trim(c.doi))=lower(trim(x.record_url)) WHERE c.doi IS NOT NULL")
        z("DATACITE_DOI_IN_RECORDS", f"SELECT d.* FROM datacite_v d LEFT ANTI JOIN {r}datacite_records x ON lower(trim(d.doi))=concat('https://doi.org/',lower(trim(x.record_id))) WHERE d.doi IS NOT NULL")

    def family_receipts(self):
        """Replaces CaptureD0's hard-coded receipts (CaptureD0.r1.py:219): a source family counts as complete when today's
        observations are at least `family_min_ratio` of the observations bound for it by the last successful run."""
        r, ratio = self.r, float(self.config.get("family_min_ratio", 0.98))
        overrides = self.config.get("source_min_ratio_overrides", {})
        floor = " ".join(f"WHEN {literal(k)} THEN {float(v)}" for k, v in overrides.items())
        ratio_sql = f"CASE family {floor} ELSE {ratio} END" if overrides else str(ratio)
        self.view("receipts", self.artifact("receipts", f"""WITH cur AS (SELECT {SOURCE_SQL} family,count(*) cur_n FROM {r}observations o GROUP BY 1),
            prev AS (SELECT family,prev_n FROM {self.baseline_sql()})
          SELECT family,coalesce(cur_n,0) observations,coalesce(prev_n,0) previous_observations,
            true success,true parser_ok,coalesce(cur_n,0)>={ratio_sql}*coalesce(prev_n,0) coverage_complete FROM prev FULL JOIN cur USING(family)"""))
        for row in self.sql(f"SELECT * FROM {r}receipts").collect():
            self.counts["source_" + str(row.family)] = int(row.observations)
            if not row.coverage_complete:
                self.counts.setdefault("_sources_below_ratio", []).append(dict(source=row.family, now=int(row.observations), before=int(row.previous_observations)))

    def baseline_sql(self):
        rows = self.sql(f"""SELECT details_json FROM {self.p}award_nightly_runs WHERE status LIKE 'SUCCEEDED%' AND details_json IS NOT NULL
            ORDER BY started_at DESC LIMIT 1""").collect()
        if rows:
            counts = json.loads(rows[0].details_json).get("counts", {})
            vals = [f"({literal(k[len('source_'):])},{int(v)})" for k, v in counts.items() if k.startswith("source_")]
            if vals:
                return f"(SELECT * FROM VALUES {','.join(vals)} AS t(family,prev_n))"
        return "(SELECT source family,count(*) prev_n FROM previous_bindings_v GROUP BY 1)"

    # ---------- publish ----------
    def swap(self, mapping):
        """Each CREATE OR REPLACE is one atomic Delta commit; every gate precedes the first one."""
        self.require(self.checks and all(self.checks.values()), "CHECKS_NOT_PASSED")
        for destination, candidate in mapping.items():
            self.write(f"CREATE OR REPLACE TABLE {ident(destination)} USING DELTA TBLPROPERTIES ('delta.feature.allowColumnDefaults'='supported') AS SELECT * FROM {ident(candidate)}")
