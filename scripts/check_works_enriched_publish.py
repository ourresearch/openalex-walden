#!/usr/bin/env python3
"""Fail when openalex.works.openalex_works could be written before Guardrails accepts the night (oxjob #2004, #2052).

CreateWorksEnriched builds tonight's works in openalex_works_build and its last cell defines the candidate view,
openalex_works_candidate (the build with its stored dates). Guardrails checks the candidate; only after it succeeds does
Publish_Works (notebooks/end2end/PublishWorks) write the candidate to openalex_works, in one commit (INSERT OVERWRITE
openalex.works.openalex_works BY NAME SELECT * FROM openalex.works.openalex_works_candidate, with a DEEP CLONE fallback
in the same cell). So readers outside the End 2 End never see a half-built table (#2004) or a rejected one (#2052).

This checks:
- CreateWorksEnriched writes openalex.works.openalex_works nowhere (a write before the publish is thrown away by it,
  silently, and a write that changes the table before Guardrails is exactly what #2052 removed); its last code cell
  defines the candidate view over openalex_works_build. New enrichments go into openalex.works.openalex_works_build.
- PublishWorks is one code cell holding the publish, which reads the candidate.
- jobs/walden_end2end.yaml runs Works_Enriched -> Guardrails (reading the candidate) -> Publish_Works ->
  Optimize_Works_Enriched, and nothing else depends on Works_Enriched or on Guardrails, so no task can read
  openalex_works between tonight's build and its accepted publish.

usage: check_works_enriched_publish.py [REPO_ROOT]   (default: .)
       check_works_enriched_publish.py --self-test
"""
import json
import os
import re
import sys

LIVE = r"openalex\.works\.openalex_works(?![A-Za-z0-9_])"
CANDIDATE = "openalex.works.openalex_works_candidate"
BUILD = "openalex.works.openalex_works_build"
WRITE = re.compile(r"\b(?:MERGE\s+INTO|INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?|UPDATE|DELETE\s+FROM|TRUNCATE\s+TABLE|"
                   r"ALTER\s+TABLE|OPTIMIZE|RESTORE(?:\s+TABLE)?|CREATE\s+(?:OR\s+REPLACE\s+)?TABLE(?:\s+IF\s+NOT\s+EXISTS)?)"
                   r"\s+" + LIVE, re.I)
PUBLISH = re.compile(r"INSERT\s+OVERWRITE\s+(?:TABLE\s+)?" + LIVE + r"\s+BY\s+NAME\s+SELECT\s+\*\s+FROM\s+"
                     + re.escape(CANDIDATE) + r"\b", re.I)
VIEW = re.compile(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+" + re.escape(CANDIDATE) + r"\b[\s\S]*?\bFROM\s+"
                  r"openalex\.works\.openalex_works_build\b", re.I)
CWE = "notebooks/end2end/CreateWorksEnriched.ipynb"
PUB = "notebooks/end2end/PublishWorks.ipynb"
JOB = "jobs/walden_end2end.yaml"


def strip_comments(sql):
    return re.sub(r"--[^\n]*", "", sql)


def enriched_problems(cells):
    """cells: CreateWorksEnriched's SQL code-cell texts in order. Returns messages (empty = OK)."""
    code = [strip_comments(c) for c in cells]
    out = []
    for i, s in enumerate(code):
        for m in WRITE.finditer(s):
            out.append(f"{CWE} code cell {i} writes openalex.works.openalex_works ({m.group(0)!r}): only Publish_Works "
                       f"writes it, after Guardrails; write openalex.works.openalex_works_build")
    views = [i for i, s in enumerate(code) if VIEW.search(s)]
    if views != [len(code) - 1]:
        out.append(f"{CWE}: expected the candidate view (CREATE OR REPLACE VIEW {CANDIDATE} ... FROM "
                   f"openalex.works.openalex_works_build) as the last code cell; found it in code cells {views} of {len(code)}")
    return out


def publish_problems(cells):
    code = [strip_comments(c) for c in cells]
    if len(code) != 1 or not PUBLISH.search(code[0]):
        return [f"{PUB}: expected one code cell holding the publish (INSERT OVERWRITE openalex.works.openalex_works BY NAME "
                f"SELECT * FROM {CANDIDATE}); found {len(code)} code cells"]
    return []


def tasks(yaml_text):
    """{task_key: {"deps": [...], "notebook": str|None, "works_table": str|None}} from walden_end2end.yaml's task list:
    one chunk per 8-space '- task_key:' item, its 12-space '- task_key:' lines are its depends_on (regex, so CI needs no
    YAML library)."""
    out = {}
    for chunk in re.split(r"^        - task_key: ", yaml_text, flags=re.M)[1:]:
        key = chunk.split(None, 1)[0]
        nb = re.search(r"^\s+notebook_path: (\S+)", chunk, re.M)
        wt = re.search(r"^\s+works_table: (\S+)", chunk, re.M)
        ct = re.search(r"^\s+content_table: (\S+)", chunk, re.M)
        out[key] = {"deps": re.findall(r"^            - task_key: (\S+)", chunk, re.M), "notebook": nb and nb.group(1),
                    "works_table": wt and wt.group(1).strip("'\""), "content_table": ct and ct.group(1).strip("'\"")}
    return out


# Each task depends on exactly the one before it, and is the only task that does: nothing reads openalex_works between
# tonight's build and its accepted publish (every other reader depends on Optimize_Works_Enriched).
CHAIN = ["Works_Enriched", "Guardrails", "Publish_Works", "Optimize_Works_Enriched"]


def job_problems(yaml_text):
    t = tasks(yaml_text)
    out = [f"{JOB}: no task {k}" for k in CHAIN if k not in t]
    for up, down in zip(CHAIN, CHAIN[1:]):
        after = sorted(k for k, v in t.items() if up in v["deps"])
        if t.get(down, {}).get("deps") != [up] or after != [down]:
            out.append(f"{JOB}: {down} must depend on {up} alone, and be the only task that does (found {down} -> "
                       f"{t.get(down, {}).get('deps')}, tasks after {up}: {after}); other readers depend on "
                       f"Optimize_Works_Enriched")
    g = t.get("Guardrails", {})
    if g.get("works_table") != CANDIDATE or g.get("content_table") != BUILD:
        out.append(f"{JOB}: Guardrails must check tonight's candidate (base_parameters works_table: {CANDIDATE}, "
                   f"content_table: {BUILD}), not the published table")
    if t.get("Publish_Works", {}).get("notebook") != "notebooks/end2end/PublishWorks":
        out.append(f"{JOB}: Publish_Works must run notebooks/end2end/PublishWorks")
    return out


def code_cells(path):
    nb = json.load(open(path, encoding="utf-8"))
    return ["".join(c["source"]) if isinstance(c["source"], list) else c["source"]
            for c in nb["cells"] if c["cell_type"] == "code"]


def self_test():
    clone = "CREATE OR REPLACE TABLE openalex.works.openalex_works_build DEEP CLONE openalex.works.openalex_works_base"
    view = (f"CREATE OR REPLACE VIEW {CANDIDATE}\nCOMMENT 'x'\nAS\nSELECT b.* EXCEPT (updated_date)\n"
            "FROM openalex.works.openalex_works_build b LEFT JOIN openalex.works.openalex_works_hash h ON h.id = b.id")
    good = [clone, "MERGE INTO openalex.works.openalex_works_build AS t USING x ON t.id = x.id",
            "-- MERGE INTO openalex.works.openalex_works (comment only)\nSELECT 1", view]
    assert enriched_problems(good) == [], enriched_problems(good)
    assert len(enriched_problems([clone, "MERGE INTO openalex.works.openalex_works AS t USING x ON t.id = x.id", view])) == 1
    assert enriched_problems([clone, "UPDATE openalex.works.openalex_works SET x = 1", view])
    assert enriched_problems([clone, view, "SELECT 1"]), "view not last"
    assert enriched_problems([clone]), "no view"
    old_pub = ("INSERT OVERWRITE openalex.works.openalex_works BY NAME SELECT b.* FROM openalex.works.openalex_works_build b")
    assert enriched_problems([clone, old_pub, view]), "a publish in CreateWorksEnriched (#2004's place) fails"
    assert enriched_problems([clone, "SELECT * FROM openalex.works.openalex_works_hash", view]) == []

    pub = ("BEGIN\n  IF fast THEN\n    INSERT OVERWRITE openalex.works.openalex_works BY NAME\n    SELECT * FROM "
           f"{CANDIDATE};\n  ELSE\n    CREATE OR REPLACE TABLE openalex.works.openalex_works DEEP CLONE "
           "openalex.works.openalex_works_build;\n  END IF;\nEND")
    assert publish_problems([pub]) == []
    assert publish_problems([old_pub]), "the publish must read the candidate Guardrails checked"
    assert publish_problems([pub, "SELECT 1"]), "one cell"

    def job(guard_deps="Works_Enriched", wt=CANDIDATE, ct=BUILD, sync_dep="Optimize_Works_Enriched", extra=""):
        return f"""resources:
  jobs:
    Walden_End_2_End:
      tasks:
        - task_key: Works_Enriched
          depends_on:
            - task_key: Authorships
          notebook_task:
            notebook_path: notebooks/end2end/CreateWorksEnriched
        # a comment between tasks
        - task_key: Guardrails
          timeout_seconds: 1200
          depends_on:
            - task_key: {guard_deps}
          notebook_task:
            notebook_path: notebooks/end2end/Guardrails
            base_parameters:
              # a comment
              run_start: "{{{{job.start_time.iso_datetime}}}}"
              works_table: {wt}
              content_table: {ct}
            source: GIT
        - task_key: Publish_Works
          depends_on:
            - task_key: Guardrails
          notebook_task:
            notebook_path: notebooks/end2end/PublishWorks
        - task_key: Optimize_Works_Enriched
          depends_on:
            - task_key: Publish_Works
          notebook_task:
            notebook_path: notebooks/end2end/OptimizeWorksEnriched
        - task_key: Sync_to_Elasticsearch
          depends_on:
            - task_key: {sync_dep}  # after the publish
          notebook_task:
            notebook_path: notebooks/elastic/sync_works
{extra}      job_clusters:
        - job_cluster_key: es_sync_cluster
          new_cluster:
            num_workers: 1
"""
    assert job_problems(job()) == [], job_problems(job())
    assert job_problems(job(guard_deps="Optimize_Works_Enriched")), "Guardrails after Optimize (#2004's order)"
    assert job_problems(job(wt="openalex.works.openalex_works")), "Guardrails must read the candidate"
    assert job_problems(job(ct="openalex.works.openalex_works")), "checks 3-10 must read tonight's build"
    assert job_problems(job(sync_dep="Guardrails")), "a reader right after Guardrails runs before the publish"
    assert job_problems(job(sync_dep="Works_Enriched")), "a reader right after Works_Enriched"
    assert job_problems(job(sync_dep="Publish_Works")), "a reader beside Optimize"
    print("self-test OK")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        sys.exit(0)
    root = sys.argv[1] if len(sys.argv) > 1 else "."
    found = (enriched_problems(code_cells(os.path.join(root, CWE))) + publish_problems(code_cells(os.path.join(root, PUB)))
             + job_problems(open(os.path.join(root, JOB), encoding="utf-8").read()))
    for p in found:
        print(p)
    print(f"{'FAIL' if found else 'OK'} (oxjob #2004, #2052: openalex_works is written only by Publish_Works, after Guardrails "
          f"accepts the candidate)")
    sys.exit(1 if found else 0)
