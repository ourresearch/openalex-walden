"""Metered cost guard for bulk ai_query passes.

Standing rule (Casey, 2026-07-20): NEVER scale a bulk ai_query pass on an
estimate. Run a small metered chunk first, measure ACTUAL input+output tokens
from the returned rows, project the full cost, and require explicit approval
above a threshold.

Why: two misses this month, both from under-counting OUTPUT tokens (billed ~5x
input). Gemini reason-token pass ran 9x the naive estimate ($5.9K). An opus
name-pair pass with a per-row `reason` field ran ~3x a $90 estimate (~$270) —
the reason sentence alone was ~70% of the cost. Billing lags hours, so a low
system.billing figure right after a run is NOT confirmation of low cost.

Usage:
    from utils.ai_query_cost_guard import estimate_pass_cost
    est = estimate_pass_cost(
        build_chunk_sql=lambda n: f"SELECT ... LIMIT {n}",   # returns the ai_query
        total_rows=27200, model="databricks-claude-opus-4-8")
    # est.projected_usd is the number to bring to the user BEFORE the full run.
"""
from dataclasses import dataclass

# List prices per 1M tokens (input, output). Databricks may bill a lower DBU
# rate; treat these as a conservative CEILING, and always reconcile against
# system.billing.usage once it posts (hours later).
MODEL_PRICES = {
    "databricks-claude-opus-4-8":   (15.0, 75.0),
    "databricks-claude-sonnet-5":   (3.0, 15.0),
    "databricks-claude-haiku-4-5":  (1.0, 5.0),
    "databricks-gemini-3-5-flash":  (0.30, 2.50),  # reasoning tokens bill as output
    "databricks-gpt-oss-120b":      (0.15, 0.60),
}

# Any pass projected above this must be shown to the user with the measured
# number before the full run.
APPROVAL_THRESHOLD_USD = 50.0
MIN_CHUNK_ROWS = 500


@dataclass
class CostEstimate:
    model: str
    chunk_rows: int
    total_rows: int
    avg_input_chars: float
    avg_output_chars: float
    projected_usd: float
    needs_approval: bool
    note: str


def _tokens_from_chars(chars: float) -> float:
    # ~4 chars/token for English + short name strings; deliberately not clever.
    return chars / 4.0


def estimate_pass_cost(build_chunk_sql, total_rows, model,
                       chunk_rows=2000, run_query=None,
                       fixed_prompt_chars=0):
    """Run a metered chunk and project full-pass cost from MEASURED tokens.

    build_chunk_sql(n) -> a SQL string whose SELECT calls ai_query and returns
      one column `out` (the raw ai_query response text) plus, if convenient, the
      rendered input via a column `prompt_chars` (LENGTH of the full prompt). If
      `prompt_chars` is absent, pass fixed_prompt_chars = LENGTH of the static
      instruction block so input tokens aren't undercounted.
    Returns CostEstimate; caller shows projected_usd to the user when
      needs_approval is True.
    """
    if run_query is None:
        from utils.databricks_sql import run_query as _rq
        run_query = _rq
    if model not in MODEL_PRICES:
        raise ValueError(f"Unknown model {model}; add its (in,out) price to MODEL_PRICES")
    n = max(MIN_CHUNK_ROWS, min(chunk_rows, total_rows))

    rows = run_query(build_chunk_sql(n))
    if not rows:
        raise RuntimeError("metered chunk returned no rows")
    out_chars = sum(len(str(r.get("out", ""))) for r in rows) / len(rows)
    if "prompt_chars" in rows[0]:
        in_chars = sum(float(r["prompt_chars"]) for r in rows) / len(rows)
    else:
        in_chars = fixed_prompt_chars

    in_price, out_price = MODEL_PRICES[model]
    per_row = (_tokens_from_chars(in_chars) * in_price
               + _tokens_from_chars(out_chars) * out_price) / 1e6
    projected = per_row * total_rows
    return CostEstimate(
        model=model, chunk_rows=len(rows), total_rows=total_rows,
        avg_input_chars=round(in_chars, 1), avg_output_chars=round(out_chars, 1),
        projected_usd=round(projected, 2),
        needs_approval=projected >= APPROVAL_THRESHOLD_USD,
        note=("output tokens bill ~5x input — drop any per-row `reason`/explanation "
              "field unless needed for audit; reconcile vs system.billing once it posts."),
    )
