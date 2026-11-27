#!/usr/bin/env python3
"""The DOI registration agencies (RAs) walden ingests as the registry record of a DOI: one list (oxjob #1776 Step 0, C1).

A record from one of these provenances IS the DOI's registration record: its native_id is the DOI, it claims the DOI for
its work, and other provenances' DOIs are only candidates beside it (#1573, #1616). Today that is Crossref and DataCite.
Adding an RA (mEDRA, JaLC, KISTI: #1776, #2257) means adding it here and running `--write`, which rewrites every marked
site below; C2 (priority_table rows, CreateSuperLocations) and C3 (host_type, indexed_in) are separate steps.

How a site is marked (the SQL and Python stay literal, so nothing changes at run time):
  SQL     provenance IN (/*DOI_RA*/ 'crossref', 'datacite')            also NOT IN, and longer lists:
          provenance NOT IN (/*DOI_RA*/ 'crossref', 'datacite', 'pubmed', 'mag')   (the RA run comes first)
  Python  p in ("crossref", "datacite")  # DOI_RA                      (marker comment at the end of the line)

Every quoted 'crossref', 'datacite' pair in notebooks/ and libraries/ is either marked or listed in CROSSREF_DATACITE_ONLY
with the reason it must stay Crossref/DataCite only; the check fails on a new unmarked pair, so the next RA can't miss one.

  python3 notebooks/utils/doi_ra_provenances.py           # check (also: notebooks/end2end/tests/test_doi_ra_provenances.py)
  python3 notebooks/utils/doi_ra_provenances.py --list    # every site, marked or not
  python3 notebooks/utils/doi_ra_provenances.py --write   # rewrite marked sites to DOI_RA_PROVENANCES
No warehouse, no network.
"""
import re
import sys
from pathlib import Path

DOI_RA_PROVENANCES = ("crossref", "datacite")

ROOT = Path(__file__).resolve().parents[2]
SCAN_DIRS = ("notebooks", "libraries")
SKIP_PARTS = {"_deprecated", "_reference", "__pycache__", ".ipynb_checkpoints"}
SUFFIXES = {".py", ".sql", ".ipynb"}
THIS = Path(__file__).resolve()
TESTS = {THIS.parents[1] / "end2end" / "tests" / "test_doi_ra_provenances.py"}

PAIR = re.compile(r"""(['"])crossref\1\s*,\s*(['"])datacite\2""")
SQL_MARK = "/*DOI_RA*/"
QUOTED = re.compile(r"""\s*(?P<q>['"])(?P<name>[a-z_]+)(?P=q)\s*,?""")
PY_MARK = re.compile(r"#\s*DOI_RA\b")

# Unmarked pairs that must stay Crossref/DataCite only: {path: (count, why)}. Counted per file, so line moves don't matter.
CROSSREF_DATACITE_ONLY = {
    "notebooks/end2end/CreateWorksBase.ipynb": (
        2, "late re-post / late twin rules (#1573 family): Crossref posted-content and DataCite re-posts of a DOI suffix"),
    "notebooks/end2end/MergeDeclaredLinks.ipynb": (
        2, "declared preprint links, typed by Crossref and DataCite types; revisit if an RA declares such links (JaLC relation_list)"),
    "notebooks/location_matching/sql/frame.sql": (
        1, "location-matching model feature: provenance bucket the model was trained on (oxjob #1668)"),
    "notebooks/awards/nightly_grants/lib/capture_support.py": (
        1, "awards input names (Crossref and DataCite funder data), not provenances"),
    "notebooks/awards/nightly_grants/lib/nightly_runtime.py": (
        1, "awards input names (Crossref and DataCite funder data), not provenances"),
}


def files():
    for d in SCAN_DIRS:
        base = ROOT / d
        if not base.is_dir():
            continue
        for p in sorted(base.rglob("*")):
            if p.suffix in SUFFIXES and p.is_file() and not SKIP_PARTS.intersection(p.parts) and p != THIS and p not in TESTS:
                yield p


def ra_run(line, pos):
    """The RA run starting at pos: the quoted names in DOI_RA_PROVENANCES, up to the first other name (a longer list such
    as ('crossref', 'datacite', 'pubmed') keeps its tail). Returns (names, start, end, quote); end is after the last
    RA name's closing quote."""
    names, end, quote, i = [], pos, "'", pos
    while True:
        m = QUOTED.match(line, i)
        if not m or m.group("name") not in DOI_RA_PROVENANCES:
            break
        if not names:
            quote = m.group("q")
        names.append(m.group("name"))
        end = m.start() + len(m.group(0).rstrip().rstrip(",").rstrip())
        i = m.end()
    start = QUOTED.match(line, pos).start("q") if QUOTED.match(line, pos) else pos
    return tuple(names), start, end, quote


def sites(text):
    """(line_no, kind, ok, line) for every crossref/datacite pair; kind is 'sql', 'py' or None (unmarked)."""
    out = []
    for i, line in enumerate(text.splitlines(), 1):
        for m in PAIR.finditer(line):
            before = line[:m.start()]
            if before.rstrip().endswith(SQL_MARK):
                out.append((i, "sql", ra_run(line, m.start())[0] == DOI_RA_PROVENANCES, line.strip()))
            elif PY_MARK.search(line):
                out.append((i, "py", ra_run(line, m.start())[0] == DOI_RA_PROVENANCES, line.strip()))
            else:
                out.append((i, None, None, line.strip()))
    return out


def check():
    errors, per_file = [], {}
    for p in files():
        rel = p.relative_to(ROOT).as_posix()
        for line_no, kind, ok, line in sites(p.read_text(encoding="utf-8")):
            if kind is None:
                per_file[rel] = per_file.get(rel, 0) + 1
            elif not ok:
                errors.append(f"{rel}:{line_no}: marked list is not DOI_RA_PROVENANCES {DOI_RA_PROVENANCES}: {line[:160]}")
    for rel, n in sorted(per_file.items()):
        allowed = CROSSREF_DATACITE_ONLY.get(rel, (0, ""))[0]
        if n > allowed:
            errors.append(f"{rel}: {n} unmarked 'crossref', 'datacite' pair(s), {allowed} listed in CROSSREF_DATACITE_ONLY: "
                          f"mark each (/*DOI_RA*/ in SQL, '# DOI_RA' in Python) or list it with the reason it stays")
    for rel, (allowed, _) in sorted(CROSSREF_DATACITE_ONLY.items()):
        if per_file.get(rel, 0) < allowed:
            errors.append(f"{rel}: CROSSREF_DATACITE_ONLY allows {allowed}, found {per_file.get(rel, 0)}: lower the count")
    return errors


def _sql_list(quote):
    return ", ".join(f"{quote}{p}{quote}" for p in DOI_RA_PROVENANCES)


def rewrite(text):
    """text with every marked RA run set to DOI_RA_PROVENANCES (a longer list keeps its tail). Lists only grow: to drop
    an RA, edit its sites by hand, since a run stops at the first name not in the list."""
    lines = text.split("\n")
    for i, line in enumerate(lines):
        for pos in reversed([m.end() for m in re.finditer(re.escape(SQL_MARK) + " ?", line)]):
            _, start, end, quote = ra_run(line, pos)
            line = line[:start] + _sql_list(quote) + line[end:]
        if PY_MARK.search(line):
            for m in reversed(list(PAIR.finditer(line))):
                _, start, end, quote = ra_run(line, m.start())
                line = line[:start] + _sql_list(quote) + line[end:]
        lines[i] = line
    return "\n".join(lines)


def write():
    """Rewrite every marked site in the repo. Returns the files changed."""
    changed = []
    for p in files():
        text = p.read_text(encoding="utf-8")
        new = rewrite(text)
        if new != text:
            p.write_text(new, encoding="utf-8")
            changed.append(p.relative_to(ROOT).as_posix())
    return changed


def main(argv):
    if "--write" in argv:
        for rel in write():
            print("rewrote", rel)
    if "--list" in argv:
        for p in files():
            for line_no, kind, ok, line in sites(p.read_text(encoding="utf-8")):
                print(f"{p.relative_to(ROOT).as_posix()}:{line_no}\t{kind or 'UNMARKED'}\t{line[:140]}")
    errors = check()
    for e in errors:
        print("FAIL", e)
    if not errors:
        print(f"ok: every crossref/datacite pair is marked as DOI_RA_PROVENANCES {DOI_RA_PROVENANCES} or listed as Crossref/DataCite only")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
