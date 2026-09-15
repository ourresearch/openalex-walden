"""Monitoring findings engine (oxjob #1116).

Pure Python, no Spark: the notebook (Databricks) and the local dry-run driver both call
`evaluate_day`. Inputs are a checks spec (parsed YAML), the component's tall metrics as
plain dicts, and the findings already computed for earlier days (consecutive-day
confirmation and baseline exclusion read those). Output is the findings rows for one day.

Expression grammar and per-check fields are documented at the top of
monitoring/checks/author_matching.yaml.
"""

from __future__ import annotations

import ast
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Iterable

STATUS_ORDER = ["ok", "watch", "critical"]
NON_ALERT = {"known", "insufficient_history", "insufficient_n", "no_data"}

DEFAULTS = dict(
    rule="relative", rolling=1, delta=False, capture_from="today", missing_as_zero=False,
    min_denominator=0, mad_watch=4.0, mad_critical=8.0, confirm_days=2, direction="both",
    window_days=21, min_history=7, page=False, enabled=True, max_status="critical",
)

_SUM_TERM = re.compile(r"sum\(\s*([A-Za-z_][A-Za-z0-9_]*)(?:\[([^\]]*)\])?\s*\)")
_TERM = re.compile(r"(?<![A-Za-z0-9_])([A-Za-z_][A-Za-z0-9_]*)(?:\[([^\]]*)\])?")
_ALLOWED_NODES = (ast.Expression, ast.BinOp, ast.UnaryOp, ast.Constant, ast.Add, ast.Sub,
                  ast.Mult, ast.Div, ast.USub, ast.Name, ast.Load)


@dataclass
class Term:
    metric: str
    pattern: str | None      # None = NULL dimension
    is_sum: bool
    placeholder: str

    @property
    def has_capture(self) -> bool:
        return bool(self.pattern) and "*" in self.pattern and not self.is_sum

    def regex(self, capture: str | None = None) -> re.Pattern | None:
        if self.pattern is None or "*" not in self.pattern:
            return None
        if capture is not None:
            return re.compile("^" + re.escape(self.pattern.replace("*", capture)) + "$")
        return re.compile("^" + re.escape(self.pattern).replace(r"\*", "(.+)") + "$")


@dataclass
class Expr:
    template: str
    terms: list[Term] = field(default_factory=list)


def parse_expr(expr: str) -> Expr:
    terms: list[Term] = []

    def repl_sum(m: re.Match) -> str:
        ph = f"t{len(terms)}"
        terms.append(Term(m.group(1), m.group(2), True, ph))
        return f" {ph} "

    def repl(m: re.Match) -> str:
        metric, pattern = m.group(1), m.group(2)
        if re.fullmatch(r"t\d+", metric):      # already a placeholder
            return metric
        ph = f"t{len(terms)}"
        terms.append(Term(metric, pattern, False, ph))
        return ph

    template = _TERM.sub(repl, _SUM_TERM.sub(repl_sum, expr)).strip()
    tree = ast.parse(template, mode="eval")
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise ValueError(f"unsupported syntax in expr {expr!r}: {type(node).__name__}")
    return Expr(template, terms)


class MetricStore:
    """metrics rows -> {date: {metric: {dimension: value}}}"""

    def __init__(self, rows: Iterable[dict]):
        self.by_day: dict[date, dict[str, dict[str | None, float]]] = defaultdict(lambda: defaultdict(dict))
        for r in rows:
            d = r["snapshot_date"]
            if not isinstance(d, date):
                d = date.fromisoformat(str(d)[:10])
            dim = r.get("dimension")
            self.by_day[d][r["metric"]][dim] = float(r["value"])

    def dates(self) -> list[date]:
        return sorted(self.by_day)

    def dims(self, day: date, metric: str) -> dict[str | None, float]:
        return self.by_day.get(day, {}).get(metric, {})


def _lookup(store: MetricStore, day: date, term: Term, capture: str | None) -> float | None:
    """None when the METRIC has no rows that day. A present metric whose dimension is absent
    reads as 0.0 — a count that did not occur (e.g. no `different_person` verdicts tonight)."""
    dims = store.dims(day, term.metric)
    if not dims:
        return None
    if term.is_sum:
        rx = term.regex(capture) if (capture is not None and "*" in (term.pattern or "")) else term.regex()
        if rx is None:
            v = dims.get(term.pattern)
        else:
            vals = [v for k, v in dims.items() if k is not None and rx.match(k)]
            v = sum(vals) if vals else None
    elif term.has_capture:
        v = dims.get(term.pattern.replace("*", capture or ""))
    else:
        v = dims.get(term.pattern)
    return 0.0 if v is None else v


def _term_value(store: MetricStore, day: date, term: Term, capture: str | None, cfg: dict) -> float | None:
    """None = no data for this term (metric absent), unless missing_as_zero."""
    if cfg["delta"]:
        today = _lookup(store, day, term, capture)
        yday = _lookup(store, day - timedelta(days=1), term, capture)
        if today is None or yday is None:
            return 0.0 if cfg["missing_as_zero"] else None
        return today - yday
    if cfg["rolling"] <= 1:
        v = _lookup(store, day, term, capture)
    else:
        vals = [_lookup(store, day - timedelta(days=i), term, capture) for i in range(cfg["rolling"])]
        vals = [x for x in vals if x is not None]
        v = sum(vals) if vals else None
    if v is None and cfg["missing_as_zero"] and day in store.by_day:
        return 0.0     # the component reported that day; this metric simply had nothing to say
    return v


def _safe_eval(template: str, values: dict[str, float]) -> float | None:
    try:
        return float(eval(compile(ast.parse(template, mode="eval"), "<expr>", "eval"), {"__builtins__": {}}, values))
    except ZeroDivisionError:
        return None


def evaluate_expr(store: MetricStore, day: date, ex: Expr, capture: str | None, cfg: dict) -> tuple[float | None, str]:
    """Returns (value, reason). reason is '' when value is usable."""
    values: dict[str, float] = {}
    for t in ex.terms:
        v = _term_value(store, day, t, capture, cfg)
        if v is None:
            return None, "no_data"
        values[t.placeholder] = v
    # min_denominator: applies to the last term of a division (the divisor) — good enough for
    # the grammar we allow (ratios are `A / B` or `A / (B + C)`).
    if cfg["min_denominator"] and "/" in ex.template:
        divisor_ph = re.findall(r"t\d+", ex.template.split("/", 1)[1])
        if sum(values.get(p, 0.0) for p in divisor_ph) < cfg["min_denominator"]:
            return None, "insufficient_n"
    v = _safe_eval(ex.template, values)
    return (v, "") if v is not None else (None, "no_data")


def _captures(store: MetricStore, day: date, ex: Expr, cfg: dict) -> list[str | None]:
    cap_terms = [t for t in ex.terms if t.has_capture]
    if not cap_terms:
        return [None]
    t = cap_terms[0]
    rx = t.regex()
    days = [day]
    if cfg["capture_from"] == "window":
        days += [day - timedelta(days=i) for i in range(1, cfg["window_days"] + 1)]
    found: dict[str, None] = {}
    for d in days:
        for k in store.dims(d, t.metric):
            if k is None:
                continue
            m = rx.match(k)
            if m:
                found.setdefault(m.group(1), None)
    return sorted(found)


def _median_mad(xs: list[float]) -> tuple[float, float]:
    med = statistics.median(xs)
    mad = statistics.median([abs(x - med) for x in xs])
    return med, mad


def _worse(a: str, b: str) -> str:
    return a if STATUS_ORDER.index(a) >= STATUS_ORDER.index(b) else b


def _cap(status: str, max_status: str) -> str:
    return status if STATUS_ORDER.index(status) <= STATUS_ORDER.index(max_status) else max_status


def _absolute_status(v: float, cfg: dict) -> tuple[str, str]:
    notes = []
    st = "ok"
    if cfg.get("critical_below") is not None and v < cfg["critical_below"]:
        st, _ = "critical", notes.append(f"< {cfg['critical_below']}")
    if cfg.get("critical_above") is not None and v > cfg["critical_above"]:
        st, _ = "critical", notes.append(f"> {cfg['critical_above']}")
    if st == "ok":
        if cfg.get("watch_below") is not None and v < cfg["watch_below"]:
            st, _ = "watch", notes.append(f"< {cfg['watch_below']}")
        if cfg.get("watch_above") is not None and v > cfg["watch_above"]:
            st, _ = "watch", notes.append(f"> {cfg['watch_above']}")
    return st, " ".join(notes)


def evaluate_day(spec: dict, store: MetricStore, day: date, prior: list[dict], overrides: dict | None = None) -> list[dict]:
    """Findings for one component on one day.

    prior: findings rows for this component from earlier days (any order).
    overrides: {(check_id, dimension|None): accepted_from_date} — baseline window starts there.
    """
    overrides = overrides or {}
    prior_idx: dict[tuple[str, str | None, date], dict] = {
        (p["check_id"], p.get("dimension"), p["snapshot_date"]): p for p in prior
    }
    out: list[dict] = []
    component = spec["component"]

    for section in spec.get("sections", []):
        for raw in section.get("checks", []):
            cfg = {**DEFAULTS, **raw}
            if not cfg["enabled"]:
                continue
            ex = parse_expr(cfg["expr"])
            for capture in _captures(store, day, ex, cfg):
                base = dict(
                    snapshot_date=day, component=component, section_id=section["id"],
                    section=section["question"], check_id=cfg["id"], title=cfg["title"],
                    dimension=capture, rule=cfg["rule"], page=bool(cfg["page"]),
                    value=None, baseline=None, mad=None, deviation=None, status="ok", detail="",
                )
                if cfg.get("known_state"):
                    out.append({**base, "status": "known", "detail": cfg["known_state"]})
                    continue
                value, reason = evaluate_expr(store, day, ex, capture, cfg)
                if value is None:
                    out.append({**base, "status": reason, "detail": reason})
                    continue
                base["value"] = value
                status, detail = "ok", []

                if cfg["rule"] in ("absolute", "both"):
                    st, note = _absolute_status(value, cfg)
                    status = _worse(status, st)
                    if note:
                        detail.append(f"absolute {note}")

                if cfg["rule"] in ("relative", "both"):
                    start = day - timedelta(days=cfg["window_days"])
                    ov = overrides.get((cfg["id"], capture))
                    if ov and ov > start:
                        start = ov
                    hist: list[float] = []
                    d = day - timedelta(days=1)
                    while d >= start:
                        p = prior_idx.get((cfg["id"], capture, d))
                        if not (p and p.get("status") == "critical"):
                            hv, hr = evaluate_expr(store, d, ex, capture, cfg)
                            if hv is not None:
                                hist.append(hv)
                        d -= timedelta(days=1)
                    if len(hist) < cfg["min_history"]:
                        if status == "ok":
                            status = "insufficient_history"
                            detail.append(f"history {len(hist)}/{cfg['min_history']}")
                    else:
                        med, mad = _median_mad(hist)
                        if med == 0 and mad == 0:
                            # degenerate baseline (a counter that has always been 0): any
                            # nonzero is notable but not a measured deviation
                            base.update(baseline=0.0, mad=0.0, deviation=None)
                            if value != 0:
                                detail.append(f"first nonzero value ({value:.4g}) against an all-zero baseline")
                                status = _worse(status, "watch") if status in STATUS_ORDER else "watch"
                            base["status"] = _cap(status, cfg["max_status"]) if status in STATUS_ORDER else status
                            base["detail"] = "; ".join(detail)
                            out.append(base)
                            continue
                        mad_eff = max(mad, 0.05 * abs(med), 1e-9)
                        dev = (value - med) / mad_eff
                        base.update(baseline=med, mad=mad, deviation=dev)
                        signed = dev if cfg["direction"] == "both" else (dev if cfg["direction"] == "up" else -dev)
                        mag = abs(dev) if cfg["direction"] == "both" else max(signed, 0.0)
                        rel = "ok"
                        if mag >= cfg["mad_critical"]:
                            rel = "critical"
                        elif mag >= cfg["mad_watch"]:
                            rel = "watch"
                            if cfg["confirm_days"] <= 2:
                                y = prior_idx.get((cfg["id"], capture, day - timedelta(days=1)))
                                if y and y.get("status") in ("watch", "critical") and y.get("rule") in ("relative", "both"):
                                    rel = "critical"
                                    detail.append("2nd consecutive day")
                        if rel != "ok":
                            detail.append(f"{dev:+.1f} MAD vs median {med:.4g}")
                        if status in NON_ALERT:
                            status = rel
                        else:
                            status = _worse(status, rel)

                if status in STATUS_ORDER:
                    status = _cap(status, cfg["max_status"])
                base["status"] = status
                base["detail"] = "; ".join(detail)
                out.append(base)
    return out


def backfill(spec: dict, store: MetricStore, days: list[date], overrides: dict | None = None) -> list[dict]:
    """Evaluate a run of days in order so consecutive-day logic and baseline exclusion apply."""
    prior: list[dict] = []
    for d in days:
        prior.extend(evaluate_day(spec, store, d, prior, overrides))
    return prior


def summarize(findings: list[dict]) -> dict[str, int]:
    c: dict[str, int] = defaultdict(int)
    for f in findings:
        c[f["status"]] += 1
    return dict(c)
