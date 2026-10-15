"""Sample run of the openalex.org sitemap generator (oxjob #1689), from desk, read-only.

Runs the generator's own SQL (utils/sitemaps.py: quality rule, groups, fingerprints, lastmod rule, changed
group) on a Databricks SQL warehouse over a sample, simulating one incremental night: the "previous state"
is the seed state computed from Delta time travel `--prev-as-of` (lastmod = updated_date then), the
"current" records are today's. Then it writes the sitemap files with the generator's writers, validates
every file against the sitemaps.org XSDs and prints sizes.

SELECT statements only (Databricks SQL Statement API through the `databricks` CLI); writes only to --out.

  python3 scripts/local/sitemaps_sample.py --out ~/ox/oxdata/search-engines-see-record-pages/sitemaps-sample
"""
from __future__ import annotations

import argparse
import gzip
import json
import subprocess
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from utils import sitemaps as sm  # noqa: E402

WAREHOUSE = "ab72acf2b2d1ef92"   # mcp-agent (Small); any warehouse works
SAMPLE_WHERE = {
    "works": "id BETWEEN 2000000000 AND 2001000000",       # ~228K indexable works (2015-era ids)
    "authors": "id BETWEEN 5000000000 AND 5000250000",     # ~100K indexable authors
}                                                          # other entities: all of them


def run_sql(statement: str, warehouse: str = WAREHOUSE) -> list[list]:
    """Read-only statement via the Statement Execution API, EXTERNAL_LINKS so size doesn't matter."""
    assert statement.lstrip().upper().startswith(("SELECT", "WITH")), "read-only statements only"
    body = {"warehouse_id": warehouse, "statement": statement, "wait_timeout": "30s",
            "disposition": "EXTERNAL_LINKS", "format": "JSON_ARRAY"}
    r = json.loads(subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)],
                                  check=True, capture_output=True, text=True).stdout)
    sid = r["statement_id"]
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(5)
        r = json.loads(subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{sid}"],
                                      check=True, capture_output=True, text=True).stdout)
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"{sid}: {r['status']}")
    rows, chunk = [], r.get("result", {})
    while True:
        for link in chunk.get("external_links", []):
            with urllib.request.urlopen(link["external_link"]) as f:
                rows.extend(json.load(f))
            nxt = link.get("next_chunk_internal_link")
            if nxt:
                chunk = json.loads(subprocess.run(["databricks", "api", "get", nxt], check=True,
                                                  capture_output=True, text=True).stdout)
                break
        else:
            return rows


def records_union(as_of: str | None) -> str:
    return "\nUNION ALL\n".join(sm.current_records_sql(e, SAMPLE_WHERE.get(e.name, ""), as_of) for e in sm.ENTITIES)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--prev-as-of", default="2026-10-09T13:00:00Z", help="Delta time travel for the previous state")
    ap.add_argument("--run-ts", default=None, help="run time, UTC 'YYYY-MM-DD HH:MM:SS' (default now)")
    ap.add_argument("--changed-days", type=int, default=7)
    ap.add_argument("--changed-max", type=int, default=1_000_000)
    a = ap.parse_args()
    run_ts = a.run_ts or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    prev_ts = a.prev_as_of.replace("T", " ").replace("Z", "")
    a.out.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    with_sql = (f"WITH prev_raw AS ({records_union(a.prev_as_of)}),\n"
                f"sm_prev AS ({sm.next_state_sql('prev_raw', None, prev_ts)}),\n"
                f"sm_current AS ({records_union(None)}),\n"
                f"nxt AS ({sm.next_state_sql('sm_current', 'sm_prev', run_ts)})\n")
    rows = run_sql(with_sql + f"SELECT entity, CAST(id AS STRING), grp, {sm.LASTMOD_W3C_SQL}, change, "
                              f"date_format(created_date, 'yyyy-MM-dd') FROM nxt")
    print(f"{len(rows):,} records in {time.time() - t0:.0f}s", flush=True)
    removed = run_sql(with_sql + f"SELECT entity, count(*) FROM ({sm.removed_sql('sm_current', 'sm_prev')}) GROUP BY 1")
    changed = run_sql(with_sql + f"SELECT entity, CAST(id AS STRING), {sm.LASTMOD_W3C_SQL}, prio FROM ("
                                 f"{sm.changed_sql('nxt', run_ts, a.changed_days, a.changed_max)}) ORDER BY "
                                 f"prio, lastmod DESC, entity, id")

    # Pack and write, exactly as the notebook does (buckets of BUCKET_WIDTH ids, <= 50,000 URLs per file).
    by_group: dict[str, list] = {}
    for entity, rid, grp, lm, change, created in rows:
        by_group.setdefault(grp, []).append((entity, int(rid), lm))
    hist = Counter((g, int(r[1]) // sm.BUCKET_WIDTH) for g, rs in by_group.items() for r in rs)
    assign = sm.pack_buckets((g, b, c) for (g, b), c in hist.items())
    stats = []
    for g, rs in by_group.items():
        rs.sort(key=lambda r: r[1])
        files: dict[int, list] = {}
        for r in rs:
            files.setdefault(assign[(g, r[1] // sm.BUCKET_WIDTH)], []).append(r)
        for n, frs in sorted(files.items()):
            gz, st = sm.build_file(g, n, frs)
            (a.out / st.name).write_bytes(gz)
            stats.append(st)
    ch = [(e, int(i), lm) for e, i, lm, _ in changed]
    for n, part in enumerate(sm.chunks(ch, sm.MAX_URLS_PER_FILE), 1):
        gz, st = sm.build_file(sm.CHANGED_GROUP, n, part)
        (a.out / st.name).write_bytes(gz)
        stats.append(st)
    counts = {g: len(rs) for g, rs in by_group.items()} | {sm.CHANGED_GROUP: len(ch)}
    problems = sm.validate_published_set(stats, counts)
    for name, data in sm.build_indexes(stats).items():
        (a.out / name).write_bytes(data)

    # Validate every file against the sitemaps.org schemas.
    from lxml import etree
    xsd = REPO / "notebooks" / "sitemaps" / "tests"
    urlset = etree.XMLSchema(etree.parse(str(xsd / "sitemap.xsd")))
    index = etree.XMLSchema(etree.parse(str(xsd / "siteindex.xsd")))
    invalid = []
    for p in sorted(a.out.iterdir()):
        if p.name.endswith(".xml.gz"):
            ok = urlset.validate(etree.fromstring(gzip.decompress(p.read_bytes())))
        elif p.name.endswith(".xml"):
            ok = index.validate(etree.fromstring(p.read_bytes()))
        else:
            continue
        if not ok:
            invalid.append(p.name)

    changes = Counter(r[4] for r in rows)
    changes_by_group = Counter((r[2], r[4]) for r in rows)
    summary = {
        "run_ts_utc": run_ts, "prev_as_of": a.prev_as_of, "sample": SAMPLE_WHERE,
        "records": len(rows), "counts": counts, "changes": dict(changes),
        "changes_by_group": {f"{g}:{c or 'unchanged'}": n for (g, c), n in sorted(changes_by_group.items())},
        "removed": {e: int(n) for e, n in removed},
        "changed_group": {"urls": len(ch), "new_first": sum(1 for r in changed if r[3] == 0)},
        "files": len(stats), "bytes_gz": sum(s.bytes_gz for s in stats), "bytes_xml": sum(s.bytes_xml for s in stats),
        "bytes_gz_per_url": round(sum(s.bytes_gz for s in stats) / max(1, sum(s.urls for s in stats)), 2),
        "bytes_xml_per_url": round(sum(s.bytes_xml for s in stats) / max(1, sum(s.urls for s in stats)), 2),
        "largest_file": max(((s.name, s.urls, s.bytes_xml, s.bytes_gz) for s in stats), key=lambda x: x[1]),
        "schema_invalid_files": invalid, "problems": problems,
        "seconds": round(time.time() - t0),
    }
    (a.out / "summary.json").write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary, indent=1))
    sys.exit(1 if invalid or problems else 0)


if __name__ == "__main__":
    main()
