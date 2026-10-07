"""`amount_usd`: the native amount converted at the exchange rate of the award's start date (oxjob #689: start-date rate, USD only),
served next to `amount` and `currency`, which are never touched.

Inputs, both optional extra inputs: `exchange_rates` (rate_date, currency, usd_per_unit: daily reference rates, ECB and others) and
`exchange_rates_annual` (rate_year, currency, usd_per_unit: annual averages). Output: two columns on awards_candidate, amount_usd
(DOUBLE, 2 decimals) and amount_usd_basis:
  usd_native      currency is USD: amount_usd = amount;
  daily_rate      the last daily rate published on or before start_date, at most MAX_CARRY_DAYS before it (weekends, holidays);
  annual_average  start year only, a currency with no daily series, or a start date before the daily series begins.
No amount, no currency, no start year, or no rate -> NULL (a non-monetary prize stays NULL, never 0). An award with amount_usd NULL
hashes exactly as before (create_api.py), so nothing moves until a rate applies.
Failure policy: malformed, duplicate or EMPTY rate tables stop the run; so does a fall against last night beyond the fuse in the
number of awards converted, per basis (a lost daily table would otherwise downgrade millions of awards to annual averages) and
per currency; and running with no rate input once USD amounts have been published. Rollback: `clear_funding_flows` (award_keys).
"""
import award_keys

MAX_CARRY_DAYS = 10
EMPTY_DAILY = "SELECT CAST(NULL AS DATE) rate_date,CAST(NULL AS STRING) currency,CAST(NULL AS DOUBLE) usd_per_unit WHERE false"
EMPTY_ANNUAL = "SELECT CAST(NULL AS INT) rate_year,CAST(NULL AS STRING) currency,CAST(NULL AS DOUBLE) usd_per_unit WHERE false"
BASES = ("usd_native", "daily_rate", "annual_average")


def rate_inputs(c):
    if award_keys.clearing(c):
        return c.artifact("rates_daily_empty", EMPTY_DAILY), c.artifact("rates_annual_empty", EMPTY_ANNUAL)
    daily = "exchange_rates_v" if c.has_extra("exchange_rates") else None
    annual = "exchange_rates_annual_v" if c.has_extra("exchange_rates_annual") else None
    if daily:
        award_keys.nonempty(c, "RATES_DAILY_NONEMPTY", daily)
    if annual:
        award_keys.nonempty(c, "RATES_ANNUAL_NONEMPTY", annual)
    if daily is None and annual is None and "amount_usd" in award_keys.columns(c, "previous_api_v"):
        # No input configured: allowed only while no USD amount has been published, so amounts can never vanish silently.
        c.zero("USD_INPUT_REQUIRED", "SELECT id FROM previous_api_v WHERE amount_usd IS NOT NULL")
    return daily or c.artifact("rates_daily_empty", EMPTY_DAILY), annual or c.artifact("rates_annual_empty", EMPTY_ANNUAL)


def run(c):
    c.guard(); r = c.r
    daily, annual = rate_inputs(c)
    c.zero("RATES_DAILY_SHAPE", f"""SELECT * FROM {daily} WHERE NOT coalesce(rate_date IS NOT NULL AND currency RLIKE '^[A-Z]{{3}}$'
      AND usd_per_unit > 0 AND NOT isnan(usd_per_unit),false)""")
    c.zero("RATES_DAILY_UNIQUE", f"SELECT rate_date,currency FROM {daily} GROUP BY ALL HAVING count(*)<>1")
    c.zero("RATES_ANNUAL_SHAPE", f"""SELECT * FROM {annual} WHERE NOT coalesce(rate_year BETWEEN 1900 AND 2100 AND currency RLIKE '^[A-Z]{{3}}$'
      AND usd_per_unit > 0 AND NOT isnan(usd_per_unit),false)""")
    c.zero("RATES_ANNUAL_UNIQUE", f"SELECT rate_year,currency FROM {annual} GROUP BY ALL HAVING count(*)<>1")
    # One row per currency and calendar day from the first published rate to the run date, carrying the last published rate
    # forward over days without one (the ECB publishes on TARGET business days). A gap longer than MAX_CARRY_DAYS is left empty.
    c.artifact("rates_daily_filled", f"""WITH span AS (SELECT currency,min(rate_date) first_date FROM {daily} GROUP BY 1),
      days AS (SELECT currency,explode(sequence(first_date,CAST(:ts AS DATE))) rate_date FROM span),
      joined AS (SELECT d.currency,d.rate_date,x.usd_per_unit,CASE WHEN x.usd_per_unit IS NOT NULL THEN d.rate_date END source_date
        FROM days d LEFT JOIN {daily} x USING(currency,rate_date)),
      carried AS (SELECT currency,rate_date,
          last_value(usd_per_unit,true) OVER (PARTITION BY currency ORDER BY rate_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) usd_per_unit,
          last_value(source_date,true) OVER (PARTITION BY currency ORDER BY rate_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) source_date
        FROM joined)
      SELECT currency,rate_date,usd_per_unit,source_date FROM carried
      WHERE usd_per_unit IS NOT NULL AND datediff(rate_date,source_date) <= {MAX_CARRY_DAYS}""")
    c.artifact("awards_candidate_before_usd", f"SELECT * FROM {r}awards_candidate")
    clear = "true" if award_keys.clearing(c) else "false"           # rollback: the native-USD path needs no rate, so it is cleared explicitly
    c.artifact("awards_candidate", f"""SELECT a.*,
        CASE WHEN {clear} OR a.amount IS NULL OR a.currency IS NULL THEN CAST(NULL AS DOUBLE)
             WHEN a.currency='USD' THEN CAST(a.amount AS DOUBLE)
             WHEN d.usd_per_unit IS NOT NULL THEN round(a.amount*d.usd_per_unit,2)
             WHEN y.usd_per_unit IS NOT NULL THEN round(a.amount*y.usd_per_unit,2) END amount_usd,
        CASE WHEN {clear} OR a.amount IS NULL OR a.currency IS NULL THEN CAST(NULL AS STRING)
             WHEN a.currency='USD' THEN 'usd_native'
             WHEN d.usd_per_unit IS NOT NULL THEN 'daily_rate'
             WHEN y.usd_per_unit IS NOT NULL THEN 'annual_average' END amount_usd_basis
      FROM {r}awards_candidate_before_usd a
      LEFT JOIN {r}rates_daily_filled d ON a.currency<>'USD' AND d.currency=a.currency AND d.rate_date=CAST(a.start_date AS DATE)
      LEFT JOIN {annual} y ON a.currency<>'USD' AND y.currency=a.currency AND y.rate_year=CAST(a.start_year AS INT)""")
    c.zero("USD_AWARDS_SAME_ROWS", f"""(SELECT id FROM {r}awards_candidate EXCEPT ALL SELECT id FROM {r}awards_candidate_before_usd)
      UNION ALL (SELECT id FROM {r}awards_candidate_before_usd EXCEPT ALL SELECT id FROM {r}awards_candidate)""")
    c.zero("USD_BASIS_SHAPE", f"""SELECT id FROM {r}awards_candidate WHERE NOT ((amount_usd IS NULL) = (amount_usd_basis IS NULL))
      OR (amount_usd_basis IS NOT NULL AND amount_usd_basis NOT IN ({",".join(repr(b) for b in BASES)}))""")
    c.count("awards_with_amount", f"(SELECT id FROM {r}awards_candidate WHERE amount IS NOT NULL) x")
    now = c.count("awards_with_amount_usd", f"(SELECT id FROM {r}awards_candidate WHERE amount_usd IS NOT NULL) x")
    for row in c.sql(f"SELECT amount_usd_basis basis,count(*) n FROM {r}awards_candidate WHERE amount_usd_basis IS NOT NULL GROUP BY 1").collect():
        c.counts["amount_usd_" + row.basis] = int(row.n)
    fuse = float(c.config.get("usd_drop_fuse", 0.01))
    has_prev = "amount_usd" in award_keys.columns(c, "previous_api_v")
    prev_total = int(award_keys.one(c, "SELECT count(*) n FROM previous_api_v WHERE amount_usd IS NOT NULL", n=0).n) if has_prev else None
    award_keys.drop_gate(c, "USD_COVERAGE_DROP", "awards_with_amount_usd", now, fuse, prev_total)
    # Per basis: a lost or stale daily table would move awards from daily_rate to annual_average with the total unchanged.
    prev_basis = ({row.basis: int(row.n) for row in c.sql("SELECT amount_usd_basis basis,count(*) n FROM previous_api_v WHERE amount_usd_basis IS NOT NULL GROUP BY 1").collect()}
                  if has_prev else {})
    for basis in BASES:
        award_keys.drop_gate(c, "USD_BASIS_DROP_" + basis, "amount_usd_" + basis, c.counts.get("amount_usd_" + basis, 0), fuse, prev_basis.get(basis) if has_prev else None)
    # Per currency: one currency's series going missing is invisible in the total. Baseline: yesterday's column, else last night's
    # recorded counts, else the frozen bootstrap; every currency any baseline knows must still convert tonight (fail closed).
    for row in c.sql(f"SELECT currency,count(*) n FROM {r}awards_candidate WHERE amount_usd IS NOT NULL GROUP BY 1").collect():
        c.counts["amount_usd_currency_" + row.currency] = int(row.n)
    if not award_keys.clearing(c):
        prev_ccy = ({row.currency: int(row.n) for row in c.sql("SELECT currency,count(*) n FROM previous_api_v WHERE amount_usd IS NOT NULL GROUP BY 1").collect()}
                    if has_prev else {})
        known = {k[len("amount_usd_currency_"):] for k in list(award_keys.last_counts(c)) + list(c.config.get("bootstrap_counts", {})) if k.startswith("amount_usd_currency_")}
        known |= set(prev_ccy)
        c.require(known, "USD_CURRENCY_BASELINE_REQUIRED: no amount_usd_currency_* count in yesterday's table, last night's run or bootstrap_counts")
        for ccy in sorted(known):
            award_keys.drop_gate(c, "USD_CURRENCY_DROP_" + ccy, "amount_usd_currency_" + ccy, c.counts.get("amount_usd_currency_" + ccy, 0), fuse, prev_ccy.get(ccy))
