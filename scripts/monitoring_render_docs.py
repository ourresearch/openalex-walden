#!/usr/bin/env python3
"""Render docs/monitoring/<component>.md from monitoring/checks/<component>.yaml (oxjob #1116).

The YAML is the source of truth; the doc is what humans read. Run after editing a checks
file:  .venv/bin/python scripts/monitoring_render_docs.py [component ...]
"""

import glob
import os
import sys

import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CHECKS = os.path.join(ROOT, "monitoring", "checks")
DOCS = os.path.join(ROOT, "docs", "monitoring")


def rule_text(c: dict) -> str:
    parts = []
    r = c.get("rule", "relative")
    if r in ("absolute", "both"):
        th = []
        for k in ("critical_below", "critical_above", "watch_below", "watch_above"):
            if c.get(k) is not None:
                op = "<" if k.endswith("below") else ">"
                th.append(f"{k.split('_')[0]} if {op} {c[k]:g}")
        parts.append("absolute: " + ", ".join(th) if th else "absolute")
    if r in ("relative", "both"):
        d = c.get("direction", "both")
        parts.append(f"relative: {c.get('mad_watch', 3)}/{c.get('mad_critical', 6)} MAD"
                     + (f", {d} only" if d != "both" else "")
                     + f", {c.get('window_days', 14)}d window")
    mods = []
    if c.get("rolling", 1) > 1:
        mods.append(f"{c['rolling']}-day rolling")
    if c.get("delta"):
        mods.append("day-over-day delta")
    if c.get("capture_from") == "window":
        mods.append("dimensions from window")
    if c.get("min_denominator"):
        mods.append(f"n ≥ {c['min_denominator']}")
    if c.get("max_status"):
        mods.append(f"max {c['max_status']}")
    if c.get("page"):
        mods.append("**pages**")
    if not c.get("enabled", True):
        mods.append("*proposed, not evaluated*")
    return "; ".join(parts) + ("  (" + ", ".join(mods) + ")" if mods else "")


def render(spec: dict) -> str:
    out = [f"# Monitoring — {spec.get('title', spec['component'])}", "",
           "<!-- GENERATED from monitoring/checks/%s.yaml by scripts/monitoring_render_docs.py — edit the YAML -->" % spec["component"],
           "",
           f"Component `{spec['component']}`. Metrics: `{spec.get('metrics_table', 'openalex.monitoring.metrics')}`. "
           f"Owner: {spec.get('owner', '?')}. Evaluated nightly by `notebooks/monitoring/MonitoringFindings`; "
           "findings in `openalex.monitoring.findings`; the morning report in `openalex.monitoring.reports`.",
           ""]
    if spec.get("history_note"):
        out += ["> " + spec["history_note"].strip().replace("\n", " "), ""]
    out += ["## How to read this",
            "",
            "Each heading is the question we care about. Each row is one check the engine runs. "
            "**absolute** rules are fixed lines (critical on day one). **relative** rules compare today "
            "to the trailing median in units of MAD: watch at the first number, critical at the second, "
            "or at the first number two nights running. Status is deterministic; the LLM only narrates.",
            ""]
    for s in spec.get("sections", []):
        out += [f"## {s['question']}", "", "| check | expression | rule | known failure modes |", "|---|---|---|---|"]
        for c in s.get("checks", []):
            fm = (c.get("failure_modes") or c.get("note") or "").strip().replace("\n", " ").replace("|", "\\|")
            expr = c["expr"].replace("|", "\\|")
            out.append(f"| **{c['title']}** `{c['id']}` | `{expr}` | {rule_text(c)} | {fm} |")
        out.append("")
    if spec.get("known_states"):
        out += ["## Known states (true, uninteresting, not findings)", ""]
        out += [f"- {k.strip()}" for k in spec["known_states"]] + [""]
    if spec.get("open_decisions"):
        out += ["## Open decisions", ""]
        out += [f"{i}. {k.strip()}" for i, k in enumerate(spec["open_decisions"], 1)] + [""]
    return "\n".join(out)


def main(argv):
    os.makedirs(DOCS, exist_ok=True)
    paths = sorted(glob.glob(os.path.join(CHECKS, "*.yaml")))
    if argv:
        paths = [p for p in paths if os.path.splitext(os.path.basename(p))[0] in argv]
    for p in paths:
        with open(p) as fh:
            spec = yaml.safe_load(fh)
        dst = os.path.join(DOCS, f"{spec['component']}.md")
        with open(dst, "w") as fh:
            fh.write(render(spec))
        print(f"rendered {os.path.relpath(dst, ROOT)}")


if __name__ == "__main__":
    main(sys.argv[1:])
