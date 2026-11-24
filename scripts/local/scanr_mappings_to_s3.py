#!/usr/bin/env python3
"""Pin a dated copy of scanR's two organisation-id -> ROR mappings in S3 (oxjob #123.5, ZD 27257).

scanR (French research ministry; Eric Jeangirard) keeps both files current at fixed URLs:
    pic_ror.json   EU participant code (PIC) -> ROR id                     {"999979500": "04zaypm56", ...}
    rnsr_ror.json  French lab code (RNSR) -> {"rors": [...], "type": ...}  type: self | institution | ambiguous | none

Nothing reads those URLs directly. This script downloads both, checks them, and copies them byte for byte to
    s3://openalex-ingest/awards/mappings/scanr/<YYYY-MM-DD>/{pic_ror.json, rnsr_ror.json, MANIFEST.json}
where the date is the files' Last-Modified day. notebooks/awards/CreateOrgIdRors.sql reads rnsr_ror.json from that folder
(pinned in the notebook) into openalex.awards.org_id_rors, which Nightly Grants reads (config key org_id_rors). pic_ror.json
is pinned beside it for the CORDIS participants step, which nothing loads yet.

Refresh: run this, put the folder it prints into CreateOrgIdRors.sql (both lines) in a reviewed commit, run the notebook.
A folder already pinned is never overwritten: the same bytes are reported as pinned, different bytes stop the run.
--previous <the last pinned folder, or a local copy> is required (--first-pin only for the very first pin): the script stops when
more than --max-changed (default 5%) of the previous ids changed RORs, changed type, or disappeared, unless --accept-changes.

Usage:
    python scripts/local/scanr_mappings_to_s3.py --output-dir /tmp/scanr --previous s3://openalex-ingest/awards/mappings/scanr/2026-11-13/ [--skip-upload]
"""
import argparse
import email.utils
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

import requests

BASE_URL = "https://scanr-data.s3.gra.io.cloud.ovh.net/production/"
FILES = ("pic_ror.json", "rnsr_ror.json")
S3_BUCKET, S3_KEY = "openalex-ingest", "awards/mappings/scanr/"
S3_PREFIX = f"s3://{S3_BUCKET}/{S3_KEY}"
ROR = re.compile(r"^0[0-9a-z]{6}[0-9]{2}$")
RNSR = re.compile(r"^[0-9]{9}[A-Z]$")
RNSR_TYPES = {"self", "institution", "ambiguous", "none"}
MIN_ROWS = {"pic_ror.json": 20000, "rnsr_ror.json": 9000}      # 13 Nov 2026: 29,450 and 12,127


def check(name, data):
    """Problems with one file, as text; empty when it is usable."""
    problems = []
    if not isinstance(data, dict):
        return [f"{name}: not a JSON object"]
    if len(data) < MIN_ROWS[name]:
        problems.append(f"{name}: {len(data)} ids, fewer than {MIN_ROWS[name]}")
    for key, value in data.items():
        if name == "pic_ror.json":
            if not isinstance(value, str) or not ROR.match(value):
                problems.append(f"{name}: {key!r} -> {value!r} is not a ROR id")
        else:
            ok = (RNSR.match(key) and isinstance(value, dict) and value.get("type") in RNSR_TYPES
                  and isinstance(value.get("rors"), list) and all(isinstance(r, str) and ROR.match(r) for r in value["rors"])
                  and len(set(value["rors"])) == len(value["rors"]))
            if not ok:
                problems.append(f"{name}: {key!r} -> {value!r} is not an RNSR entry")
        if len(problems) > 20:
            break
    return problems


def rors_of(name, data):
    """What a loader reads per id: its RORs, and for RNSR its type (a type change can drop a code from the load)."""
    return {k: (frozenset([v]) if name == "pic_ror.json" else (v["type"], frozenset(v["rors"]))) for k, v in data.items()}


def changed_share(name, old, new):
    a, b = rors_of(name, old), rors_of(name, new)
    shared = set(a) & set(b)
    changed = sum(1 for k in shared if a[k] != b[k])
    return changed, len(shared), len(set(b) - set(a)), len(set(a) - set(b))


def existing_pin(folder_key):
    """The MANIFEST already in that folder, {} when the folder is empty; any S3 error, or files without a MANIFEST, stops."""
    out = subprocess.run(["aws", "s3api", "list-objects-v2", "--bucket", S3_BUCKET, "--prefix", folder_key, "--output", "json"],
                         capture_output=True, text=True)
    if out.returncode:
        raise SystemExit("STOP, nothing uploaded: cannot list " + folder_key + ": " + out.stderr.strip()[:300])
    keys = [o["Key"] for o in (json.loads(out.stdout or "{}").get("Contents") or [])]
    if not keys:
        return {}
    if folder_key + "MANIFEST.json" not in keys:
        raise SystemExit(f"STOP, nothing uploaded: {folder_key} holds {keys} but no MANIFEST.json (a broken earlier run?); read it by hand")
    got = subprocess.run(["aws", "s3", "cp", f"s3://{S3_BUCKET}/{folder_key}MANIFEST.json", "-"], capture_output=True, check=True)
    return json.loads(got.stdout)


def read_previous(where, name):
    if where.startswith("s3://"):
        out = subprocess.run(["aws", "s3", "cp", where.rstrip("/") + "/" + name, "-"], check=True, capture_output=True).stdout
        return json.loads(out)
    return json.loads((Path(where) / name).read_bytes())


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--previous", help="the folder pinned last time (s3:// or local), to compare against")
    ap.add_argument("--first-pin", action="store_true", help="no previous pin exists yet (the first run only)")
    ap.add_argument("--max-changed", type=float, default=0.05)
    ap.add_argument("--accept-changes", action="store_true")
    args = ap.parse_args()
    if not args.previous and not args.first_pin:
        ap.error("--previous is required (the last pinned folder); --first-pin only for the very first pin")

    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    manifest, days, problems = {"source": BASE_URL, "files": {}}, set(), []
    for name in FILES:
        r = requests.get(BASE_URL + name, timeout=120)
        r.raise_for_status()
        body = r.content
        data = json.loads(body)                       # data only: never executed
        problems += check(name, data)
        modified = email.utils.parsedate_to_datetime(r.headers["Last-Modified"])
        days.add(modified.date().isoformat())
        (out / name).write_bytes(body)
        manifest["files"][name] = dict(bytes=len(body), sha256=hashlib.sha256(body).hexdigest(), ids=len(data),
                                       last_modified=modified.isoformat(), etag=r.headers.get("ETag"))
        if args.previous:
            changed, shared, added, removed = changed_share(name, read_previous(args.previous, name), data)
            manifest["files"][name]["vs_previous"] = dict(previous=args.previous, shared=shared, changed=changed, added=added, removed=removed)
            print(f"{name}: {changed} of {shared} shared ids changed RORs or type; {added} added, {removed} removed")
            before = shared + removed
            if before and (changed + removed) / before > args.max_changed and not args.accept_changes:
                problems.append(f"{name}: {changed} changed + {removed} removed of {before} previous ids (> {args.max_changed:.0%}); read them, then --accept-changes")
    if problems:
        print("STOP, nothing uploaded:\n  " + "\n  ".join(problems[:40]))
        return 1
    folder = S3_PREFIX + max(days) + "/"
    manifest["pinned_folder"] = folder
    (out / "MANIFEST.json").write_text(json.dumps(manifest, indent=1) + "\n")
    print(json.dumps(manifest, indent=1))
    if args.skip_upload:
        print(f"--skip-upload: would copy {out} to {folder}")
        return 0
    existing = existing_pin(S3_KEY + max(days) + "/")
    if existing:                                      # pinned before: the folder may already be loaded and reviewed
        old = {k: v["sha256"] for k, v in existing["files"].items()}
        if old == {k: v["sha256"] for k, v in manifest["files"].items()}:
            print(f"ALREADY PINNED {folder} (same bytes); nothing uploaded")
            return 0
        print(f"STOP, nothing uploaded: {folder} is already pinned with different bytes; scanR republished the same day.")
        return 1
    for name in FILES + ("MANIFEST.json",):
        subprocess.run(["aws", "s3", "cp", str(out / name), folder + name], check=True)
    print(f"PINNED {folder}\nNext: put this folder into notebooks/awards/CreateOrgIdRors.sql (both lines) and run it.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
