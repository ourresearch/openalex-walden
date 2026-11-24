"""Guardrail (Kyle + Rohan, 19 Nov 2026, oxjob #2373): no funding code may match a funder by its Crossref registry ID.

"Crossref registry ID" is the one plain name for: Crossref funder ID, FundRef, funder DOI, ids.crossref, funder.doi, 10.13039/...

Tests:
- test_no_new_crossref_registry_id_use passes today. It fails when a Crossref registry ID shows up in a file that is not
  listed below, or when a listed file gains more lines than its pin. Fix the code; raise a pin only for display or discard uses.
- test_crossref_registry_id_purge_complete FAILS until every file in LIVE_TODAY is clean. It is the acceptance test for the
  purge (#2373 Test 1). It is marked as an expected failure (strict) so the file can run on every change; when the purge
  is done it passes, pytest reports XPASS as a failure, and the marker is deleted in that same commit.
- test_pins_are_not_stale then asks for the cleaned file's LIVE_TODAY row to be deleted (same commit), so the first test
  holds the file at 0 from then on.
- The deposit-ROR tests (#2373 K10, Kyle's decision 6): a ROR in a Crossref deposit is never read to decide a funder, and a
  ROR in a DataCite deposit is kept and tried first.

The scan finds the ID by its names and by a few structural forms (an identifier picked because its type says DOI). It
cannot see a match whose variables were renamed to plain words, so passing it is necessary, not sufficient: every purge
step also gets a code review that looks for the join itself (#2373 ORDER.md).

What counts as a use: a CODE line (not a comment, not a markdown cell) that names the ID (TOKEN), or one of the SITES the
token search cannot see (they read a raw `DOI` field). The token also catches the prefix written with escaped dots
(`10\\.13039`, `10\\\\.13039`) inside SQL regexes. Showing a funder's stored `doi` on a record (`f.doi` in a SELECT) is
display and is not searched for.

Classes are Rohan's inventory (oxjobs working/purge-crossref-funder-ids/work/rohan-crossref-id-inventory.html):
A = the ID decides which funder a mention means; B = it decides whether a grant number is kept;
C = display, stored ids, checksums; D = dead or one-time code; E = filters that throw funder DOIs away.
"""
import json
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[3]
SCAN_DIRS = ("notebooks", "jobs", "libraries", "utils")
SUFFIXES = (".py", ".sql", ".ipynb", ".yaml", ".yml", ".csv")

TOKEN = re.compile(
    r"10(\\*\.|\[\.\]|_)13039|fundref|funder_doi|crossref_id|crossref_funder_id|ids\.crossref|crossref funder"
    r"|join_doi|owner_doi|doi_funder_id|trap_funder_id"
    # a funder identifier picked because its type says DOI (Crossref grant records, DataCite fundingReferences)
    r"|id-type[\"'\]]*\)?\s*==\s*(F\.lit\()?[\"']DOI[\"']|funderIdentifierType[^\n]*[\"']DOI[\"']", re.I)

# Uses the token search cannot see: file -> pattern of the line that carries a deposit's funder DOI on as a key.
SITES = {
    "notebooks/ingest/Crossref.py": re.compile(r"""fun\[["']DOI["']\]"""),
    "notebooks/ingest/DataCite.py": re.compile(r"""funderIdentifier.*doi\\?\.org"""),
}

# Class D: dead or one-time code and tests. Not scanned. The cleanup proposal archives or deletes these.
SKIP = {
    "notebooks/funders/release/": "D1 the one-time 16 Oct funders release; no job runs it",
    "notebooks/_deprecated/": "D4 deprecated notebooks",
    "notebooks/funders/SeedFunders.ipynb": "D3 one-time registry seed, run by hand, never again",
    "notebooks/funders/StageNoDoiMainCut.ipynb": "D2 one-time staging of the 16 Oct cut",
    "/tests/": "D7 tests decide nothing (they are rewritten with each fix)",
}

# Classes C and E (and D8): allowed for good. file -> (most lines allowed, class, why).
ALLOW = {
    "notebooks/funders/CreateFundersAPI.ipynb": (1, "C1", "serves ids.crossref on the funder record, display only"),
    "notebooks/awards/CreateOrUpdateAwards.sql": (4, "C2", "hard-coded ERC funder DOIs on raw award rows (generated from sources/); not served: the grant builder rebuilds the funder"),
    "notebooks/awards/sources/erc.sql": (4, "C2", "hard-coded ERC funder DOIs on raw award rows; not served: the grant builder rebuilds the funder"),
    "notebooks/awards/CreateCrossrefWorkFunders.ipynb#checksum": (1, "C4", "catalogue checksum over funder columns"),
    "notebooks/awards/CreateDataCiteWorkFunders.ipynb#checksum": (1, "C4", "catalogue checksum over funder columns"),
    "notebooks/funders/CreateOrUpdateFunders.ipynb#checksum": (4, "C4", "catalogue checksum over funder columns"),
    "notebooks/awards/CreateFunderReportedWorkFunders.ipynb": (1, "C4", "catalogue checksum over funder columns"),
    "notebooks/funders/PublishFunderNameRelease.ipynb": (1, "C4", "catalogue checksum over funder columns"),
    "notebooks/maintenance/MergeFunders.ipynb": (1, "C5", "shows crossref_id in the hand-run merge preflight"),
    "notebooks/parsing/parse_work_references.ipynb": (1, "E1", "a cited DOI starting 10.13039/ is not a cited work"),
    "notebooks/end2end/CreateLocationsMapped.ipynb": (1, "E5", "a location's DOI starting 10.13039/ is not the work's DOI"),
    "notebooks/awards/AwardNormKey.sql": (6, "E3", "strips a leading FUNDREF label from grant strings; a grant string that is a funder DOI is no grant"),
    "notebooks/awards/TagPdfAwardsIncremental.sql": (1, "E4", "a funder DOI is never the tail of a grant number (PDF award tagging)"),
    "notebooks/awards/BackfillPdfAwardMatches.sql": (1, "E4", "a funder DOI is never the tail of a grant number (PDF award tagging)"),
    "notebooks/awards/nightly_grants/lib/pdf_backfill.py": (1, "E4", "a funder DOI is never the tail of a grant number (PDF award tagging)"),
    "notebooks/sources/CreateSources.ipynb": (1, "D8", "crossref_id here is a journal's Crossref id, not a funder's"),
}
CHECKSUM = re.compile(r"xxhash64\(funder_id")   # the C4 line inside the two readers, which also hold class A lines

# Classes A and B: live matching uses (main 6f0958e5, 19 Nov 2026; recounted with the escaped-dot token and after purge
# step 1, 23 Nov 2026). file -> (lines today, class, what to remove). The purge empties this table. Do not add rows.
# Order and measured effect of each step: #2373 ORDER.md.
LIVE_TODAY = {
    "notebooks/ingest/Crossref.py": (1, "A1", "carries the deposit's funder DOI on as a key (and writes the ROR as NULL)"),
    "notebooks/ingest/DataCite.py": (1, "A1", "carries the deposit's funder DOI on as a key"),
    # Added 23 Nov after review (the first scan missed it): no reader uses IRDB's funder DOI today.
    "notebooks/ingest/IRDB.py": (1, "A1", "carries a Crossref-Funder-typed identifier on as the funder's doi"),
    "notebooks/awards/CreateCrossrefWorkFunders.ipynb": (14, "A2 B1 B2", "backup key (funder_fundref_owners, fk), numbers kept only on Crossref-ID matches, holds skipped; its ROR step is dead (the ingest writes NULL) and goes with it"),
    "notebooks/awards/CreateDataCiteWorkFunders.ipynb": (15, "A4 B3", "Crossref ID tried before names; numbers dropped"),
    "notebooks/awards/CreateCrossrefAwards.ipynb": (10, "A5", "a grant record's funder comes from the deposited funder DOI"),
    "notebooks/funders/CreateOrUpdateFunders.ipynb": (44, "A7", "attaches ROR records by Crossref ID and writes fundref keys"),
    "notebooks/funders/RorFunderProposals.py": (28, "A8", "ATTACH_FUNDREF / MINT_FUNDREF proposals"),
    "notebooks/funders/fundref_predecessor_bridge.csv": (2, "A9", "retired Crossref ID -> successor ROR bridge"),
    "notebooks/funders/fundref_predecessor_bridge_seed.sql": (4, "A9", "seed for the bridge"),
}


def _cells(path):
    """Yield (location, line) for code lines only."""
    text = path.read_text(errors="replace")
    if path.suffix == ".ipynb":
        for ci, cell in enumerate(json.loads(text).get("cells", [])):
            if cell.get("cell_type") != "code":
                continue
            src = cell.get("source") or ""
            src = "".join(src) if isinstance(src, list) else src
            for li, line in enumerate(src.split("\n"), 1):
                yield f"cell {ci} line {li}", line
    elif (path.suffix == ".py" and text.startswith("# Databricks notebook source")) or (
            path.suffix == ".sql" and text.startswith("-- Databricks notebook source")):
        c = "# " if path.suffix == ".py" else "-- "   # Databricks source notebooks: cells and MAGIC lines in comments
        n = 0
        for cell in text.split(c + "COMMAND ----------"):
            lines = cell.split("\n")
            magic = [l for l in lines if l.startswith(c + "MAGIC")]
            markdown = bool(magic) and magic[0].split(c + "MAGIC", 1)[1].strip().startswith("%md")
            for line in lines:
                n += 1
                if not markdown:
                    yield f"line {n}", line[len(c) + 5:] if line.startswith(c + "MAGIC") else line
            n -= 1
    elif path.suffix == ".csv":
        yield "header", text.split("\n", 1)[0]
    else:
        for li, line in enumerate(text.split("\n"), 1):
            yield f"line {li}", line


def _comment(line):
    s = line.strip()
    return s.startswith(("--", "#", "//")) or not s


def scan(root=ROOT):
    """{file: [(location, line)]} for every use outside the SKIP paths."""
    root = pathlib.Path(root)
    found = {}
    for top in SCAN_DIRS:
        for path in sorted((root / top).rglob("*")) if (root / top).is_dir() else ():
            rel = path.relative_to(root).as_posix()
            if not path.is_file() or path.suffix not in SUFFIXES or any(s in "/" + rel for s in SKIP):
                continue
            site = SITES.get(rel)
            hits = [(loc, line.strip()) for loc, line in _cells(path)
                    if not _comment(line) and (TOKEN.search(line) or (site and site.search(line)))]
            if TOKEN.search(path.name):
                hits.append(("file name", path.name))
            if hits:
                found[rel] = hits
    return found


def classify(found):
    """Split the uses into: over (more lines than a listed file may have, or an unlisted file) and live (LIVE_TODAY files)."""
    over, live = {}, {}
    for rel, hits in found.items():
        checksum = [h for h in hits if CHECKSUM.search(h[1])]
        rest = [h for h in hits if not CHECKSUM.search(h[1])]
        if rel + "#checksum" in ALLOW:
            if len(checksum) > ALLOW[rel + "#checksum"][0]:
                over[rel + "#checksum"] = checksum
            hits = rest
        if rel in ALLOW:
            if len(hits) > ALLOW[rel][0]:
                over[rel] = hits
        elif rel in LIVE_TODAY:
            if hits:
                live[rel] = hits
            if len(hits) > LIVE_TODAY[rel][0]:
                over[rel] = hits
        elif hits:
            over[rel] = hits
    return over, live


def _show(d):
    return "\n".join(f"{rel} ({len(h)} lines)\n" + "\n".join(f"    {loc}: {line[:160]}" for loc, line in h[:6]) for rel, h in sorted(d.items()))


def test_no_new_crossref_registry_id_use():
    over, _ = classify(scan())
    assert not over, ("A Crossref registry ID is used in funding code where it was not before. Match funders by name or ROR.\n"
                      "Only display (class C) and discard filters (class E) may be added to ALLOW.\n" + _show(over))


def test_pins_are_not_stale():
    """A pin never sits above what the file holds, so a cleaned file cannot quietly get its uses back."""
    found = scan()
    stale = {}
    for rel, (pin, cls, _why) in LIVE_TODAY.items():
        n = len([h for h in found.get(rel, []) if not (rel + "#checksum" in ALLOW and CHECKSUM.search(h[1]))])
        if pin and n < pin:
            stale[rel] = f"pin {pin}, file has {n}"
    for key, (pin, cls, _why) in ALLOW.items():
        rel = key.split("#")[0]
        hits = found.get(rel, [])
        n = len([h for h in hits if CHECKSUM.search(h[1])]) if "#" in key else len(
            [h for h in hits if not (rel + "#checksum" in ALLOW and CHECKSUM.search(h[1]))])
        if n < pin:
            stale[key] = f"pin {pin}, file has {n}"
    assert not stale, f"Lower these pins (or delete the row when it reaches 0): {stale}"


@pytest.mark.xfail(strict=True, reason="the purge is not finished (#2373 ORDER.md); delete this marker when it passes")
def test_crossref_registry_id_purge_complete():
    _, live = classify(scan())
    assert not live, (
        "Funding code still matches funders by Crossref registry ID (classes A and B). These files trip the test:\n"
        + "\n".join(f"  {rel}: {len(live.get(rel, []))} lines, class {cls}: {why}" for rel, (_n, cls, why) in LIVE_TODAY.items())
        + "\n\n" + _show(live))


# ---- deposit RORs (Kyle + Rohan, 19 Nov 2026, decision 6; #2373 K10) ----
# Crossref deposits: a ROR in a Crossref deposit is never used to decide a funder. Paper links see deposit funders only
# through the Crossref ingest, which writes each funder's ROR as NULL (0 of 43,384,097 deposit funder references carried
# one on 23 Nov 2026, dev). Of the files that read whole Crossref deposits, only the grant-record reader reads a funder's
# id list (End 2 End's enrichment step reads our own funder ids, never a deposit's). A grant DOI is registered by the funder itself, and Kyle's grant-DOI direction (DIRECTION-19NOV.md section 2) says to use
# a ROR in the record when present; whether decision 6 covers grant records too is open (#2373 ORDER.md).
# DataCite deposits: the ROR is trusted, so the DataCite ingest keeps ROR-typed funder ids and its reader tries them first.
CROSSREF_INGEST = "notebooks/ingest/Crossref.py"
RAW_CROSSREF = re.compile(r"crossref_items|crossref_exploded|crossref_deduplicated")   # tables that hold whole deposits
FUNDER_ID_READ = re.compile(r"""(?i)\bfun(der)?\w*["'\])\[\d\s]*(\[["']id["']\]|\.id\b|\[["']id-type)|\bfun(der)?\w*[^\n]*id-type""")
FUNDER_ID_READERS = {   # files that read raw Crossref tables AND read some funder's id list, and why that is allowed
    "notebooks/awards/CreateCrossrefAwards.ipynb": "grant records, registered by the funder itself (open question, see above)",
    "notebooks/end2end/CreateWorksEnriched.ipynb": "reads OpenAlex funder ids (g.funder.id, e.funder.id), never a deposit's",
}


def _code(path):
    return "\n".join(line for _loc, line in _cells(path) if not _comment(line))


def _blocks(code, name):
    """Every .withColumn("<name>", ...) call, each up to the next .withColumn( call."""
    found = []
    for m in re.finditer(r'\.withColumn\(\s*"' + name + '"', code):
        end = code.find(".withColumn(", m.end())
        found.append(code[m.start():end if end > 0 else len(code)])
    assert found, f'no .withColumn("{name}", ...) found'
    return found


def test_crossref_ingest_writes_no_funder_ror():
    code = _code(ROOT / CROSSREF_INGEST)
    for funders in _blocks(code, "funders"):
        assert re.findall(r'(\S+)\.alias\(\s*"ror"\s*\)', funders) == ['F.lit(None).cast("string")'], (
            "The Crossref ingest must write each funder's ROR as NULL: a ROR in a Crossref deposit never decides a funder "
            "(Kyle, 19 Nov 2026, decision 6).\n" + funders)
    assert not re.search(r"""withField\(\s*["']ror["']""", code), "The Crossref ingest sets a ROR field after the fact"


def test_only_known_files_read_a_raw_crossref_funder_id():
    readers = {}
    for top in SCAN_DIRS:
        for path in sorted((ROOT / top).rglob("*")) if (ROOT / top).is_dir() else ():
            rel = path.relative_to(ROOT).as_posix()
            if not path.is_file() or path.suffix not in SUFFIXES or any(s in "/" + rel for s in SKIP):
                continue
            lines = [line for _loc, line in _cells(path) if not _comment(line)]
            if RAW_CROSSREF.search("\n".join(lines)):
                hits = [line.strip() for line in lines if FUNDER_ID_READ.search(line)]
                if hits:
                    readers[rel] = hits
    assert set(readers) == set(FUNDER_ID_READERS), (
        "A file that reads raw Crossref records reads a funder's id list. A ROR in a Crossref deposit must never decide a "
        "funder (decision 6): read the ingest's funders instead, or add the file with Kyle's OK.\n"
        + "\n".join(f"  new: {rel}: {readers[rel][:3]}" for rel in sorted(set(readers) - set(FUNDER_ID_READERS)))
        + "\n".join(f"  gone: {rel}" for rel in sorted(set(FUNDER_ID_READERS) - set(readers))))


def test_datacite_deposit_ror_is_kept():
    funders = "\n".join(_blocks(_code(ROOT / "notebooks/ingest/DataCite.py"), "funders"))
    assert re.search(r'funderIdentifierType.*==\s*"ROR"', funders) and re.search(r'\.alias\(\s*"ror"\s*\)', funders), (
        "The DataCite ingest must keep ROR-typed funder ids: DataCite deposit RORs are trusted (decision 6).\n" + funders)
    reader = _code(ROOT / "notebooks/awards/CreateDataCiteWorkFunders.ipynb")
    assert "LEFT JOIN ror_owners r ON r.k=e.rk" in reader and "CASE WHEN r.owners=1 THEN 'ROR'" in reader, (
        "The DataCite paper-link reader must resolve a deposited ROR first (decision 6)")


# ---- the scanner itself, on small made-up trees ----
def _tree(tmp_path, files):
    for rel, text in files.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    return tmp_path


def test_scanner_passes_a_clean_tree_and_allowed_uses(tmp_path):
    nb = json.dumps({"cells": [{"cell_type": "markdown", "source": ["Funder DOI 10.13039/100000001"]},
                               {"cell_type": "code", "source": ["SELECT f.funder_id, f.doi FROM funders f\n", "-- old fundref join removed"]}]})
    root = _tree(tmp_path, {
        "notebooks/awards/CreateXAwards.ipynb": nb,
        "notebooks/parsing/parse_work_references.ipynb": json.dumps({"cells": [{"cell_type": "code", "source": ["x_doi NOT LIKE '10.13039/%'"]}]}),
        "notebooks/funders/release/old.sql": "JOIN f ON f.crossref_id = x.fundref",
        "notebooks/awards/tests/test_x.py": "assert 'fundref' in sql",
    })
    over, live = classify(scan(root))
    assert over == {} and live == {}


def test_scanner_catches_a_new_matching_key(tmp_path):
    root = _tree(tmp_path, {
        "notebooks/awards/CreateNewReader.sql": "SELECT * FROM refs r JOIN funders f ON CAST(f.crossref_id AS STRING) = r.fk",
        "notebooks/ingest/Crossref.py": 'x = F.struct(fun["DOI"].alias("doi"))',
        "notebooks/awards/CreateEscapedReader.sql": "SELECT regexp_extract(x, '10\\\\.13039/([0-9]+)', 1) k FROM refs",
        "notebooks/parsing/parse_work_references.ipynb": json.dumps({"cells": [{"cell_type": "code", "source": ["a 10.13039/1\n", "b fundref"]}]}),
    })
    over, live = classify(scan(root))
    assert set(over) == {"notebooks/awards/CreateNewReader.sql", "notebooks/parsing/parse_work_references.ipynb",
                         "notebooks/awards/CreateEscapedReader.sql"}
    assert set(live) == {"notebooks/ingest/Crossref.py"}
