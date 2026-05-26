"""Auto-resolve helper for merging Rohan's funder-ingest PRs.

Resolves conflicts in:
  - notebooks/awards/CreateAwards.ipynb (priority list cell — targeted text splice)
  - plans/awards/funder-ingestion-tracker.md (take theirs)

Uses text-level surgery (NOT json round-tripping) on the ipynb to preserve
the trailing-blank-line padding Databricks produces. The runbook explicitly
warns against ensure_ascii=False round-trips because they produce noisy diffs.

Splitting strategy: the source field is one big JSON-escaped string with
`\n` markers between markdown lines. We split on the literal two-char
sequence `\n` (backslash + n), process each markdown line, then rejoin.
This avoids regex pitfalls with `—` (em-dash escape) and `\"` (quote
escape) embedded in the priority descriptions.

Usage:
    py scripts/local/_merge_resolve.py
"""
import re
import subprocess
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
except Exception:
    pass

REPO = Path(__file__).resolve().parents[2]
CRA_REL = "notebooks/awards/CreateAwards.ipynb"
TRACKER_REL = "plans/awards/funder-ingestion-tracker.md"
CRA_PATH = REPO / CRA_REL
TRACKER_PATH = REPO / TRACKER_REL

# Literal two-char sequence used as line separator inside the JSON source string.
ESC_NL = "\\n"
PRIO_LINE_RE = re.compile(r"^-\s+(\d+):\s+")
TRAILING_MARKER_LINE_PREFIX = "When the same award appears in multiple sources"


def file_at_stage(rel_path, stage):
    try:
        return subprocess.check_output(
            ["git", "show", f"{stage}:{rel_path}"]
        ).decode("utf-8", errors="replace")
    except subprocess.CalledProcessError:
        return None


def find_source_string_bounds(text):
    """Return (open_quote_idx, close_quote_idx) of the cell-zero source string."""
    marker = '"source": "### Combine Award Sources to Create Single Awards Table'
    idx = text.find(marker)
    if idx == -1:
        return None, None
    o = text.index('"', idx + len('"source": '))
    i = o + 1
    while i < len(text):
        if text[i] == "\\":
            i += 2
            continue
        if text[i] == '"':
            return o, i
        i += 1
    return o, -1


def extract_priorities(src):
    """Return ordered list of (prio_int, line_text) and the index of the trailing
    marker line. line_text is the markdown line without the leading `\n`."""
    lines = src.split(ESC_NL)
    out = []
    marker_idx = -1
    for i, line in enumerate(lines):
        m = PRIO_LINE_RE.match(line)
        if m:
            out.append((int(m.group(1)), line))
        if line.startswith(TRAILING_MARKER_LINE_PREFIX):
            marker_idx = i
    return lines, out, marker_idx


def merge_create_awards():
    ours = file_at_stage(CRA_REL, "HEAD")
    theirs = file_at_stage(CRA_REL, "MERGE_HEAD")
    if ours is None or theirs is None:
        return False

    o_open, o_close = find_source_string_bounds(ours)
    t_open, t_close = find_source_string_bounds(theirs)
    if -1 in (o_open, o_close, t_open, t_close) or None in (o_open, t_open):
        print("!! Could not locate CreateAwards source bounds — manual resolution required.")
        return False

    ours_src = ours[o_open + 1 : o_close]
    theirs_src = theirs[t_open + 1 : t_close]

    ours_lines, ours_prios, ours_marker = extract_priorities(ours_src)
    theirs_lines, theirs_prios, _ = extract_priorities(theirs_src)

    ours_prio_set = {p for p, _ in ours_prios}
    theirs_prio_set = {p for p, _ in theirs_prios}
    new_in_theirs = sorted(theirs_prio_set - ours_prio_set)

    if not new_in_theirs:
        print("CreateAwards.ipynb: no new priorities in theirs — writing ours unchanged")
        CRA_PATH.write_text(ours, encoding="utf-8")
        return True

    if ours_marker == -1:
        print("!! Could not find trailing marker line in ours — manual resolution required.")
        return False

    # Look up the line text for each new priority from theirs
    theirs_by_prio = {p: line for p, line in theirs_prios}
    new_lines = [theirs_by_prio[p] for p in new_in_theirs]

    # Splice into ours: insert new lines just before the blank line that precedes the marker.
    # ours_lines[ours_marker] is the marker line; ours_lines[ours_marker - 1] is "" (blank).
    # We want: ...last priority line..., "", "When the same..." → ...last priority, NEW1, NEW2, "", "When the same..."
    # So insert at position ours_marker - 1 (before the blank).
    insert_at = ours_marker - 1
    if insert_at < 0:
        insert_at = ours_marker
    merged_lines = ours_lines[:insert_at] + new_lines + ours_lines[insert_at:]
    merged_src = ESC_NL.join(merged_lines)

    merged_file = ours[: o_open + 1] + merged_src + ours[o_close:]
    CRA_PATH.write_text(merged_file, encoding="utf-8")
    print(f"CreateAwards.ipynb: spliced in priorities {new_in_theirs}")
    return True


def merge_tracker():
    """Resolve tracker conflict by 3-way merge: take main's tracker, then re-apply
    just the rows added/modified by theirs (vs the merge base). Rejecting wholesale
    'take theirs' is critical — theirs' branch is forked from old-main and contains
    stale rows for funders that landed on main between branch-fork and now."""
    base_ref = subprocess.check_output(["git", "merge-base", "HEAD", "MERGE_HEAD"]).decode().strip()
    base = file_at_stage(TRACKER_REL, base_ref)
    ours = file_at_stage(TRACKER_REL, "HEAD")
    theirs = file_at_stage(TRACKER_REL, "MERGE_HEAD")
    if base is None or ours is None or theirs is None:
        return False

    base_lines = base.splitlines(keepends=True)
    theirs_lines = theirs.splitlines(keepends=True)

    # Find rows in theirs that are NEW (not in base) or MODIFIED (different in base).
    # Identify each row by the funder name (first cell). Rows look like:
    #     | <funder name>  | <step>  | <notes>  |
    # The funder name is unique per row.
    import re
    ROW_RE = re.compile(r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|")

    def index_by_funder(lines):
        out = {}
        for i, ln in enumerate(lines):
            m = ROW_RE.match(ln)
            if m:
                key = m.group(1).strip()
                out[key] = (i, ln)
        return out

    base_idx = index_by_funder(base_lines)
    theirs_idx = index_by_funder(theirs_lines)

    # Changes from base→theirs
    added_or_modified = []
    for k, (i, ln) in theirs_idx.items():
        if k not in base_idx:
            added_or_modified.append(("ADD", k, ln, i))
        elif base_idx[k][1] != ln:
            added_or_modified.append(("MOD", k, ln, i))

    if not added_or_modified:
        # theirs introduced nothing; just keep ours
        TRACKER_PATH.write_text(ours, encoding="utf-8")
        print("tracker: kept ours (theirs had no changes)")
        return True

    # Apply each change to ours
    ours_lines = ours.splitlines(keepends=True)
    ours_idx = index_by_funder(ours_lines)
    apply_log = []
    for action, k, line, theirs_pos in added_or_modified:
        if k in ours_idx:
            ours_pos, _ = ours_idx[k]
            ours_lines[ours_pos] = line
            apply_log.append(f"MOD {k!r}")
        else:
            # Insert at the position theirs put it, adjusted by the running offset.
            # Simple strategy: insert after the row that precedes it in theirs (if that row exists in ours), else append.
            inserted = False
            for j in range(theirs_pos - 1, -1, -1):
                m = ROW_RE.match(theirs_lines[j])
                if m:
                    anchor = m.group(1).strip()
                    if anchor in ours_idx and anchor != k:
                        anchor_pos = ours_idx[anchor][0]
                        ours_lines.insert(anchor_pos + 1, line)
                        # Rebuild index for subsequent inserts
                        ours_idx = index_by_funder(ours_lines)
                        inserted = True
                        apply_log.append(f"ADD {k!r} after {anchor!r}")
                        break
            if not inserted:
                ours_lines.append(line)
                ours_idx = index_by_funder(ours_lines)
                apply_log.append(f"ADD {k!r} (appended)")

    TRACKER_PATH.write_text("".join(ours_lines), encoding="utf-8")
    print(f"tracker: {len(apply_log)} change(s): " + "; ".join(apply_log))
    return True


def main():
    status = subprocess.check_output(["git", "status", "--porcelain"]).decode("utf-8", errors="replace")
    conflicted = []
    for line in status.splitlines():
        if line[:2] in ("UU", "AA", "DU", "UD"):
            conflicted.append(line[3:].strip())

    if not conflicted:
        print("No conflicts detected.")
        return

    print(f"Conflicted files: {conflicted}")
    handled = []
    for f in conflicted:
        if f == CRA_REL:
            if merge_create_awards():
                subprocess.check_call(["git", "add", f])
                handled.append(f)
        elif f == TRACKER_REL:
            if merge_tracker():
                subprocess.check_call(["git", "add", f])
                handled.append(f)

    remaining = [f for f in conflicted if f not in handled]
    if remaining:
        print(f"!! Manual resolution still needed for: {remaining}")
        sys.exit(1)
    print("All conflicts resolved. Run: git commit --no-edit")


if __name__ == "__main__":
    main()
