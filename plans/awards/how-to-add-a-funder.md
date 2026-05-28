# How to Add a Funder

**Location:** `plans/awards/how-to-add-a-funder.md`
**Purpose:** Instructions for Claude instances to add awards from a new funder to OpenAlex.
**Human involvement:** Minimal - just running the Databricks notebook and final approval.
**Parallelization:** Multiple Claude instances can work on different funders simultaneously.
**Tracker:** `plans/awards/funder-ingestion-tracker.md`

---

## Two ingest patterns: GRANT vs PRIZE

This document is written primarily around the **grant pattern** (most
funders fit this). The **prize pattern** is a smaller variant — see
[CreateNobelAwards.ipynb](../../notebooks/awards/CreateNobelAwards.ipynb)
as the canonical example. The deltas:

| concept                  | grant                                    | prize                                                           |
| ------------------------ | ---------------------------------------- | --------------------------------------------------------------- |
| `lead_investigator`      | the PI / project leader (often via affiliation) | **the laureate themselves** (given_name + family_name populated) |
| `funder` mapping         | the org issuing the grant                | the **awarding body** — may differ by category (Nobel Physics → Royal Swedish Academy; Nobel Medicine → Karolinska) |
| `amount` / `currency`    | required (>50% pct_amount per Step 6.7)  | **may be NULL** for non-monetary prizes (Fields Medal). Waive the Step 6.7 amount check explicitly with a note. |
| Multiple winners?        | one row per grant                        | one row per **(prize × laureate)**. Apportion amount via the `portion` field. |
| `funding_type`           | `grant`, `research`, `fellowship`        | `prize`                                                         |
| `funder_award_id`        | grant ID from the source                 | a synthetic key like `{category}-{year}-{laureate_id}`. **Collision detection in the upload script MUST `raise`, not warn** — duplicate `funder_award_id` silently merges rows in the awards table. |

**If the prize-awarding body is not a funder in OpenAlex**, stop and
flag it — same rule as for grants. Don't fabricate a funder. (Nobel
Foundation itself isn't in OpenAlex; we map to Royal Swedish Academy
of Sciences and Karolinska Institutet because those are the actual
awarding bodies for the science Nobels and they ARE in OpenAlex.)

**Prize-pattern source authority: prefer the awarding body's own site,
even when it publishes less data than Wikipedia.** Wikipedia/Wikidata
often has a cleaner-looking laureate table than the funder itself, and
it can be tempting to scrape from there. Don't. Every existing prize
source in this repo pulls from the awarding body directly (Nobel =
nobelprize.org API, Wolf = wolffund.org.il, Lasker = laskerfoundation.org,
Kavli = kavliprize.org, Fields = mathunion.org). Some funder sites
publish less per-laureate detail than Wikipedia does — e.g. IMU does
not publish affiliation-when-awarded for Fields medalists, and only
publishes citation text for the 2014/2018/2022 cohorts. **NULL is the
correct value for fields the funder doesn't publish.** Do not backfill
those fields from Wikipedia/Wikidata unless you also split provenance
across two sources (and even then, prefer to file the gap as a
follow-up rather than mix sources in one row).

### Ingest method ladder

When choosing how to fetch a funder's data, prefer methods higher in this
list — they're cheaper, faster, and more stable than what's below.

1. **CKAN open-data API** (e.g. Argentina MINCYT, IDRC) — `package_show`
   gives you all resource URLs and bumps automatically when the funder
   publishes new years.
2. **WordPress REST API** (e.g. Templeton, Rockefeller) — `/wp-json/wp/v2/{type}`
   endpoints with `X-WP-Total` headers. Often returns ACF custom fields
   directly. Auth-walled in some cases (Ford Foundation = 401).
3. **Search/index APIs** (e.g. Arnold Ventures via Algolia) — ApplicationId
   + public search-only key are usually exposed in the page bundle. Watch
   for `paginationLimitedTo: 1000` on Algolia and slice by facets.
4. **Bulk file downloads** (CSV/JSON/XML) — IATI XML for IDRC, NIH ExPORTER
   bulk files. Fragile when URLs change yearly; prefer CKAN-style discovery
   when the same files are published behind one.
5. **Static HTML scrapes** (e.g. Carl-Zeiss-Stiftung) — sitemap → detail
   pages → BeautifulSoup. Works when pages render server-side. Use
   structured markup (`<table><th><td>`) before regexing rendered text.
6. **agent-browser scrapes** (e.g. HHMI) — required when the site uses
   Shadow DOM, JS-rendered content, or has Cloudflare/anti-bot challenges.
   Slower (~5-10s per page) but works on anything the user can see in a
   browser. See [hhmi_to_s3.py](../../scripts/local/hhmi_to_s3.py) for the
   shadow-DOM-aware extraction pattern. Existing `sloan_scrape_click.py`
   covers click-based pagination through a list view.

   **Caveat for sites whose `load` event never fires** (RWJF, OSF, etc.):
   agent-browser CLI's `open` command has a 25s default timeout but on
   Windows the subprocess wrapper doesn't reliably terminate child Chrome
   processes when the page keeps network busy. **Use Playwright Python
   directly instead** when you hit this — see
   [rwj_to_s3.py](../../scripts/local/rwj_to_s3.py) and
   [osf_to_s3.py](../../scripts/local/osf_to_s3.py) for the Playwright
   pattern (catch `PWTimeout` from `goto`, sleep 5-8s for late renders,
   then query). Both reuse agent-browser's bundled Chrome via
   `executable_path` so you don't need a second Chromium download.
7. **990-PF / regulatory filings** — fallback for US private foundations
   that publish nothing else. Lags ~2 years, so only use when no other
   path exists.

When you escalate to method 6 (agent-browser), the smoke-test gate from
Step 1 still applies — fetch 1-3 records first, confirm the extraction
JS pulls real data, only then start the full ~hour-long crawl.

---

## Using the Funder Ingestion Tracker

The tracker (`funder-ingestion-tracker.md`) maintains the status of all funder ingestion jobs. **Always keep it updated.**

### Handling User Commands

**"Get the next funder"** or **"Start a new funder"**:
1. Read the tracker file
2. Find funders at Step 0 (not yet started)
3. Pick the first one
4. **IMMEDIATELY update the tracker to Step 1** before doing any other work (this acts as a lock—see below)
5. Begin working on it

**"Do the next one"** or **"Continue"**:
1. Read the tracker file
2. Check for in-progress funders (any step between 1-6)
3. **If multiple funders are in progress at different steps**: Ask the user for clarification:
   > "I see multiple funders in progress:
   > - {Funder A} at Step {N}: {description}
   > - {Funder B} at Step {M}: {description}
   > Which one should I continue with?"
4. **If only one funder is in progress**: Continue from where it left off
5. **If no funders are in progress**: Treat as "get the next funder"

### Updating the Tracker

#### Locking: Mark Progress Immediately

**CRITICAL:** When you start working on a funder, update the tracker **immediately**—before doing any actual work. This serves as a **lock** to prevent other Claude instances from picking the same funder.

Multiple agents may be working in parallel. If you read the tracker, pick a funder at Step 0, and then spend 10 minutes downloading data before updating the tracker, another agent could pick the same funder during that window. By marking Step 1 immediately, you claim the funder and other agents will skip it.

**When starting a new funder:**
1. Read the tracker, pick a funder at Step 0
2. **IMMEDIATELY** update the tracker to Step 1 (this is your lock)
3. Commit and push the tracker update
4. Now begin the actual work

**After completing each step**, update the tracker:
1. Read the current tracker file
2. Update the funder's status to the new step (e.g., "Step 1" → "Step 2")
3. Add any relevant notes (e.g., grant counts, issues encountered)
4. Commit the tracker update along with other changes

Example tracker update:
```markdown
| KAKEN (Japan Grant-in-Aid for Scientific Research) | Step 2 | Download complete: 450,000 grants. Creating notebook. |
```

---

## Prerequisites

Before starting, gather the following information:
- **Funder name:** Get from the user (e.g., "NSF", "DFG", "Wellcome Trust")
- **Data source:** URL to API or data dump. **Make every effort to locate this yourself first:**
  1. Search the web for "{funder name} grants API", "{funder name} awards data", or "{funder name} open data"
  2. Check the funder's official website for developer/API documentation
  3. Look for data portals or public data dumps (many funders publish to data.gov or similar)
  4. Search for existing open-source projects that fetch data from this funder
  5. **Only ask the user as a last resort** if you cannot find a suitable data source
- **OpenAlex funder_id:** Look this up using Databricks MCP (see Step 0)

---

## Step 0: Look Up funder_id in OpenAlex

**⚠️ LOCK FIRST:** Before doing any work on this funder, update the tracker from "Step 0" to "Step 1" and commit/push immediately. This locks the funder so other agents won't pick it. Then proceed with the lookup below.

Use the Databricks MCP to find the funder in OpenAlex:

```sql
SELECT funder_id, display_name, ror_id, doi
FROM openalex.common.funder
WHERE LOWER(display_name) LIKE '%{funder_name}%'
   OR LOWER(alternate_titles) LIKE '%{funder_name}%';
```

Record these values - you'll need them for the notebook:
- `funder_id`: (e.g., 4320332161)
- `display_name`: (e.g., "National Institutes of Health")
- `ror_id`: (e.g., "https://ror.org/01cwqze88")
- `doi`: (e.g., "10.13039/100000002")

If the lookup returns 0 rows, **do not stop yet** — `openalex.common.funder` is a Databricks mirror of F4320\* Crossref-registered funders only. Non-F4320\* OpenAlex funders (e.g., Abel Prize `F8651541334`, MinCiencias `F3277441329`, Schmidt Futures `F4026159580`) exist in OpenAlex but are not in this dim. For those, look up canonical values from the OpenAlex public API:

```
https://api.openalex.org/funders/F{funder_id}
```

Record `id` (strip the `F` prefix to get the numeric `funder_id`), `display_name`, `ror` (may be null), `doi`, and `country_code`. You'll use these as inline constants in §1.6 + Step 2 (see the non-F4320\* path below). If the funder doesn't exist in OpenAlex at all (no API record either), STOP and tell the user.

**→ Update tracker:** Change status to "Step 1" with funder_id in notes.

---

## Step 1: Download Data and Upload to S3

### ⚠️ CRITICAL: Avoid Silent Download Failures

**Problem:** Download scripts can hang silently for 15+ minutes with no output, wasting enormous time and momentum. Old APIs break, endpoints change, rate limits kick in, and network issues occur—you won't know unless you actively check.

**Required approach:**

1. **Test first (30 seconds):** Before attempting a full download, run a quick test to fetch just 1-2 items. Verify you actually get data back. Many data sources have changed URLs or gone offline.

2. **Verbose logging:** Scripts MUST log frequently:
   - Every API request (URL, status code, response size)
   - Items processed count (every 10-100 items)
   - Estimated time remaining / ETA
   - Any errors or retries

3. **Progress indicators:** Show running totals like:
   ```
   [00:30] Fetched 150/50000 projects (0.3%) - ETA: 2h 45m
   [01:00] Fetched 320/50000 projects (0.6%) - ETA: 2h 30m
   ```

4. **Fail fast:** If no data arrives within 30 seconds of starting, something is wrong. Stop and investigate immediately.

5. **Report to user frequently:** Don't go silent. Every 2-3 minutes, update the user on progress so they know whether to wait or cancel.

6. **Empty page ≠ end of corpus.** Anti-bot blips, transient timeouts, and
   rate-limit responses all produce single empty pages mid-run. Do **not** use
   "first empty response stops the loop" — log the empty page and continue. Use
   the source's reported total (a `meta.count`, a parsed "page X of Y" header,
   or an `args.max_pages` fallback) as the loop terminator. Confirmed incident
   on RWJF ([be5db1e](https://github.com/ourresearch/openalex-walden/commit/be5db1e)):
   a run bailed at page 321 with ~4,800 grants after one empty Playwright
   response; the site actually had ~31,717 grants across 2,116 pages. Probing
   page 321 immediately yielded 15 grants — the page wasn't past end-of-corpus,
   it was a flake.

**If you find yourself waiting >5 minutes with no output, STOP.** The script is likely hung. Check the source URL manually, test the API in a browser, and debug before continuing. Do not let downloads run silently—this wastes massive amounts of time.

---

### 1.1 Create the download script

Create a script at `openalex-walden/scripts/local/{funder_name}_to_s3.py`

Use these as templates (in order of preference based on API type):
- JSON API: `nwo_to_s3.py`
- XML API: `gtr_to_s3.py`
- File download: `nih_exporter_to_s3.py` or `gates_to_s3.py`

### 1.2 Script requirements

The script must:

1. **Preserve source fidelity** - Store data as close to the original as possible:
   - GOOD: `name → {"given":"john", "family":"smith"}` or split into `given_name`, `family_name`
   - BAD: `full_name → john smith` (loses structure)

2. **Use parquet format** with Spark-compatible types:
   - Dates as strings: `YYYY-MM-DD`
   - Timestamps as strings: `YYYY-MM-DD HH:MM:SS`
   - Never use pandas Timestamp with nanosecond precision

3. **Include checkpointing** for resumable downloads (see templates)

4. **Upload to S3** at: `s3://openalex-ingest/awards/{funder_name}/{funder_name}_projects.parquet`

5. **Force string dtype before `to_parquet`.** All source columns in awards
   scripts are strings (text fields + JSON-serialized substructures). End the
   script with `df = df.astype("string")` before `df.to_parquet(...)`. Without
   this, pyarrow infers null-heavy columns (e.g. `title_es`, `description_es`,
   `planned_start`) as **int**, and downstream `COALESCE(title_en, title_fr,
   title_es)` in the notebook resolves to int and blows up on the first
   non-numeric value (e.g. a French/Spanish title). **Smoke tests do not catch
   this** — 1–8 dense rows hide the inference. Confirmed incidents on IDRC
   ([0f8b891](https://github.com/ourresearch/openalex-walden/commit/0f8b891))
   and Rockefeller ([5f694b7](https://github.com/ourresearch/openalex-walden/commit/5f694b7)).

6. **If deduping stub-vs-real rows, sort by the column you ship as `amount`.**
   Some sources (e.g. MinCiencias' Socrata feed) emit two rows per project —
   a "registered" stub with zero amounts and a "funded approved" row with the
   real amount. Dedup by sorting on the *exact* column the notebook ships as
   `amount` in Step 2, not a sibling column. If you sort by a sibling that
   can diverge (e.g., `monto_total_ap` = funder + counterpart, while the
   notebook ships `monto_financiado_ap` = funder share only), you can keep
   the row with a high counterpart-funded value and zero funder share, then
   ship `amount = 0` and lose the real funder amount. Use the sibling as a
   tie-breaker if you want determinism on all-zero-funded pairs. Confirmed
   near-miss on MinCiencias (PR #82, May 2026).

7. **Install the Windows UTF-8 compatibility shim at the top of the script.**
   See `scripts/local/templeton_prize_to_s3.py` (or any other scraper
   added since 2026-05-22) for the canonical block. It does three things:

   ```python
   import sys
   try:
       sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
       sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
   except (AttributeError, ValueError):
       pass
   if sys.platform == "win32":
       # Monkey-patch Path.write_text / read_text / builtins.open to
       # default to utf-8 (Windows defaults to cp1252 without it)
       ...  # see fleet shim
   ```

   What each piece fixes:
   - **stdout / stderr reconfigure**: Windows pipes stdout as `cp1252`
     when captured by a subprocess (smoke runner, CI log redirection).
     `PYTHONIOENCODING=utf-8` in the parent env is silently **ignored**
     in this case. `print()` of a laureate name containing a non-ASCII
     char then crashes with `UnicodeEncodeError`.
   - **`line_buffering=True`**: Python uses full buffering (not line
     buffering) on pipe stdout. Without this, smoke runs that capture
     stdout to a file show **empty logs** even while the scraper is
     actively progressing, making timeouts indistinguishable from
     hangs. Confirmed empty-log symptom on nuffield and hewlett.
   - **Monkey-patch on `Path.write_text` / `Path.read_text` / `open`**:
     this is the actual cp1252 bug — `Path.write_text(json.dumps(...,
     ensure_ascii=False))` defaults to cp1252 on Windows and crashes on
     any non-ASCII content (laureate names like Mihály, Béla, Lech,
     Hubert Saint-Onge, μM, combining accents, zero-width spaces). The
     stdout reconfigure alone does **not** fix this — Python file I/O
     uses a different default. The monkey-patch makes `open()` /
     `write_text()` default to `encoding="utf-8"` on Windows; it is a
     no-op on Linux/Databricks. Confirmed crashes on 8 scrapers during
     the 2026-05-22 fleet smoke (blue_planet_prize, holberg,
     lemelson_mit, macarthur_fellows, packard_fellows, templeton_prize,
     vilcek_prizes, world_food_prize); fleet-fixed via the v2 shim.

   Alternative if you prefer cleaner code over a shim: pass
   `encoding="utf-8"` to **every** `Path.write_text()`, `Path.read_text()`,
   and `open()` call. The shim was chosen because it's localized to one
   block per script and impossible to forget on the next call site.

8. **Support `--limit N` for smoke testing.** A scraper with hundreds of
   detail-page fetches behind politeness throttling (`princess_asturias`
   = 353 × 6s = 35 min, `kyoto_prize` = 127 × 2s, `pew_biomedical_scholars`
   ≈ 200 detail pages) is unsmokable end-to-end on a contractor laptop.
   Provide a `--limit N` flag that truncates the laureate/grant list before
   the detail-page loop. Existing examples: `fields_medal_to_s3.py`,
   `abel_prize_to_s3.py`, `minciencias_to_s3.py`. Without `--limit`, smoke
   verification falls back to "did it crash in the first N seconds" — far
   weaker signal than "did it produce a parquet with expected coverage".

9. **Use `--output-dir DIR`, not `--output FILE`.** The fleet convention is
   `--output-dir` taking a directory (the parquet filename is derived from
   the funder name). One scraper drifted to `--output FILE` during template
   fork (`twas_awards_to_s3.py`); fleet-fixed 2026-05-22 by adding
   `--output-dir` as an alias. New scrapers must use `--output-dir` so the
   smoke runner can target all scrapers uniformly.

### 1.3 Run the script

```bash
cd openalex-walden/scripts/local
python {funder_name}_to_s3.py
```

Verify upload succeeded and note the row count.

### 1.4 Re-ingestion safety: never shrink the corpus

**When refreshing or re-ingesting an existing funder, do not overwrite the S3
parquet if the new file has fewer rows than the previous one.** A shrinking
corpus almost always means the source had a partial outage, an API/schema
change dropped records, a scrape bailed early on a flake (see Step 1's
"empty page ≠ end of corpus" note), or pagination broke — not that the funder
genuinely retracted grants. If you overwrite, the Step 3 DELETE-by-(provenance,
priority) wipes the old rows and you lose data that has no other source.

Required pattern for any re-ingest script:

1. Before uploading, read the existing parquet's row count from S3
   (`s3://openalex-ingest/awards/{funder}/{funder}_projects.parquet`).
2. Compare to the new dataframe's row count.
3. If `new_count < previous_count`, **abort the upload** and surface the diff
   to the user. Do not overwrite. Do not proceed to Step 2.
4. Only continue if `new_count >= previous_count` (equal is fine — sources
   often republish the same corpus between refreshes).

Allow an explicit override flag (e.g. `--allow-shrink`) for the rare
legitimate case where a funder genuinely removed records, but the default
must be fail-closed.

**→ Update tracker:** Change status to "Step 2" with row count in notes (e.g., "Downloaded 450,000 grants to S3").

---

## Step 2: Create Databricks Notebook

Create notebook at `openalex-walden/notebooks/awards/Create{FunderName}Awards.ipynb`

Use `CreateNIHAwards.ipynb` or `CreateNWOAwards.ipynb` as templates.

### 2.1 Required Award Schema

The output table MUST have exactly these columns (no additions or subtractions):

```sql
-- String fields
id                    -- Format: abs(xxhash64(CONCAT({funder_id}, ':', {lowercase_award_id}))) % 9000000000
display_name          -- Award/project title
description           -- Abstract or description
funder_award_id       -- The funder's native award ID
currency              -- "USD", "EUR", "GBP", etc.
funding_type          -- "research", "fellowship", "training", "grant", etc.
funder_scheme         -- The specific program/scheme name (nullable)
provenance            -- Source identifier, e.g., "nih_exporter", "nwopen".
                      -- For **shared aggregator sources** (USAspending, Crossref,
                      -- DataCite, IATI), suffix the agency so audit queries can
                      -- distinguish: "usaspending_epa", "usaspending_ahrq",
                      -- not bare "usaspending". The DELETE-by-(provenance, priority)
                      -- key in Step 3 still works either way, but a bare aggregator
                      -- name forces every audit to JOIN on funder_id.
landing_page_url      -- URL to award details page
doi                   -- DOI if available (usually NULL)

-- Numeric fields
funder_id             -- OpenAlex funder_id (from Step 0)
amount                -- Funding amount as DOUBLE (nullable)

-- Date fields
start_date            -- DATE type
end_date              -- DATE type
start_year            -- INTEGER (extracted from start_date)
end_year              -- INTEGER (extracted from end_date)
created_date          -- TIMESTAMP (use current_timestamp())
updated_date          -- TIMESTAMP (use current_timestamp())

-- Struct: funder
funder STRUCT<
    id: STRING,           -- "https://openalex.org/F{funder_id}"
    display_name: STRING,
    ror_id: STRING,
    doi: STRING
>

-- Struct: lead_investigator (nullable)
lead_investigator STRUCT<
    given_name: STRING,
    family_name: STRING,
    orcid: STRING,
    role_start: DATE,
    affiliation: STRUCT<
        name: STRING,
        country: STRING,
        ids: ARRAY<STRUCT<id:STRING, type:STRING, asserted_by:STRING>>
    >
>

-- Struct: co_lead_investigator (usually NULL)
co_lead_investigator  -- Same struct as lead_investigator

-- Array: investigators (usually NULL or empty)
investigators ARRAY<lead_investigator struct>

-- URL field
works_api_url -- concat('https://api.openalex.org/works?filter=awards.id:G', id)
```

### 2.2 Notebook structure

1. **Markdown header** with:
   - Funder name and description
   - Prerequisites (which script to run first)
   - Data source URL
   - S3 location
   - Funder details (funder_id, display_name, ror_id, doi)

2. **Step 1: Create staging table**
   ```sql
   CREATE OR REPLACE TABLE openalex.awards.{funder}_raw
   USING delta AS
   SELECT *, current_timestamp() as databricks_ingested_at
   FROM parquet.`s3a://openalex-ingest/awards/{funder}/{funder}_projects.parquet`;
   ```

3. **Step 1.5: INSPECT RAW DATA FIRST** (CRITICAL!)

   **Before writing ANY transformation SQL**, you MUST inspect the raw data using Databricks MCP:
   ```sql
   -- Check actual column names
   DESCRIBE openalex.awards.{funder}_raw;

   -- Sample the data to see actual values
   SELECT * FROM openalex.awards.{funder}_raw LIMIT 10;

   -- Check for unexpected values in key fields
   SELECT DISTINCT start_year FROM openalex.awards.{funder}_raw LIMIT 20;
   SELECT DISTINCT amount FROM openalex.awards.{funder}_raw WHERE amount IS NOT NULL LIMIT 20;
   ```

   This prevents errors from:
   - Wrong column names (e.g., `total` vs `grant_value`, `start_yr` vs `start_year`)
   - Unexpected data values (e.g., "TBC" instead of a year number)
   - Missing columns you assumed would exist

   **⚠️ Special attention to `amount` and `currency` — past ingests have shipped with NULL amount/currency because field-name search was too narrow.** Confirmed incidents: **DataCite Awards** (amount is nested inside `fundingReferences[].fundingAmount` — not a top-level column) and **Gateway to Research** (amount field was not named `amount`). Both required a follow-up fix branch. Do not let this happen again.

   Before writing any transformation SQL, scan EVERY column AND EVERY nested struct for money-shaped data:

   ```sql
   -- List every column with a money-flavored name (case-insensitive)
   SELECT column_name FROM (DESCRIBE openalex.awards.{funder}_raw)
   WHERE LOWER(column_name) RLIKE
       'amount|amt|total|value|sum|funded|fund_|funding|cost|budget|grant_offer|awarded|valeur|monto|importe|montant|betrag|valor|importo|kwota|belopp';

   -- List every column with a currency-flavored name
   SELECT column_name FROM (DESCRIBE openalex.awards.{funder}_raw)
   WHERE LOWER(column_name) RLIKE 'currenc|ccy|iso_4217';
   ```

   **Common amount field names — verify, don't assume:**
   - English: `amount`, `total`, `total_cost`, `value`, `funded_value`, `grant_offer`, `grant_value`, `funding_amount`, `funded_amount`, `awarded_amount`, `award_amount`, `budget`, `cost`, `project_cost`, `sum_awarded`
   - Currency-suffixed (currency is implicit in the column name): `valuePounds`, `valueGBP`, `valueEUR`, `valueUSD`, `amount_eur`, `funding_eur`
   - Localized: `monto` / `importe` (es), `montant` (fr), `betrag` (de), `valor` (pt), `importo` (it), `kwota` (pl), `belopp` (sv)

   **Common currency field names:** `currency`, `currencyCode`, `currency_code`, `ccy`, `fundingAmountCurrency`, `amountCurrency`. Currency may also be **implicit** — if the funder is single-country (GTR = GBP, FAPESP = BRL, ANID = CLP), hardcode it and document the choice in the notebook header.

   **Inspect nested structures explicitly.** DataCite-style sources hide amounts inside arrays of objects, and a top-level `DESCRIBE` won't surface them. For every array/struct column found:
   ```sql
   -- Expand each struct/array column to see what's inside
   SELECT fundingReferences.* FROM openalex.awards.{funder}_raw
   WHERE size(fundingReferences) > 0 LIMIT 5;
   -- Or for object structs:
   SELECT amounts.* FROM openalex.awards.{funder}_raw WHERE amounts IS NOT NULL LIMIT 5;
   ```

   **Sanity-check candidate amount columns before mapping:**
   ```sql
   SELECT
       MIN(TRY_CAST({col} AS DOUBLE)) AS min_val,
       MAX(TRY_CAST({col} AS DOUBLE)) AS max_val,
       AVG(TRY_CAST({col} AS DOUBLE)) AS avg_val,
       COUNT({col}) AS non_null,
       COUNT(*) AS total_rows
   FROM openalex.awards.{funder}_raw;
   ```
   A real grant-amount distribution sits in the thousands to millions. If avg is 1–100 or 1900–2030, you've grabbed a count, ID, or year — try another column.

   **Watch for unit encoding.** Some sources store amounts in minor units (GBP pence ×100, USD cents ×100). JPY/KRW use no minor unit. Convert to whole currency units before storing and document the conversion in the notebook header.

4. **Step 1.6: Funder existence check** (CRITICAL — but interpretation depends on funder_id prefix)

   The Step 2 transform joins your raw rows against a `funder` source CTE
   to populate the `funder` struct. If that CTE emits zero rows, the
   `CROSS JOIN` silently produces an empty table and the Step 3 INSERT
   looks like it succeeded — you'll discover the gap only when downstream
   queries return empty results. Always run the lookup, but **how you
   interpret a 0-row result depends on the funder_id prefix**:

   ```sql
   SELECT funder_id, display_name, ror_id, doi, country_code
   FROM openalex.common.funder
   WHERE funder_id = {funder_id};
   ```

   **Path A — `F4320*` Crossref-registered funders (the common case):**
   The Databricks `openalex.common.funder` dim mirrors F4320\* funders, so
   the lookup **must return exactly 1 row**. If 0 rows, STOP — the funder
   is unexpectedly missing from the dim. Flag back to the user; do not
   proceed. Step 2 selects from the dim as usual.

   **Path B — non-F4320\* funders (e.g., `F8651541334`, `F3277441329`, `F4026159580`):**
   The dim does **not** cover non-F4320\* funders, so 0 rows is **expected**
   and **not** an error. Treat the lookup as informational only. In Step 2,
   replace the dim-selecting CTE with an **inline `SELECT`-of-constants**
   carrying the canonical values you recorded from the OpenAlex API in
   Step 0:

   ```sql
   WITH funder_resolved AS (
       -- INLINED canonical funder row (non-F4320*, not in openalex.common.funder).
       -- Values from https://api.openalex.org/funders/F{funder_id} per Step 0.
       SELECT
           {funder_id} AS funder_id,
           '{display_name}' AS display_name,
           {ror_id_or_NULL} AS ror_id,    -- 'https://ror.org/...' or CAST(NULL AS STRING)
           '{doi}' AS doi,                 -- '10.13039/...'
           '{country_code}' AS country_code
   ),
   ...
   ```

   Precedents (all FIXED + CONFIRMED in the 2026-05-26/27 sweep): Abel Prize
   (`F8651541334`, `CreateAbelPrizeAwards.ipynb`), MinCiencias
   (`F3277441329`, `CreateMinCienciasAwards.ipynb`), Schmidt Sciences
   (`F4026159580`, `CreateSchmidtSciencesAwards.ipynb`). All three originally
   shipped with the dim-select pattern, silently emitted 0 rows, and were
   patched with this inline-constants shape.

   **Failure signature to watch for:** scraper uploads the expected row
   count to S3, raw table loads cleanly with the right `COUNT(*)`, but
   `openalex.awards.{funder}_awards` has 0 rows after Step 2. That's almost
   always a Path B funder using a Path A CTE.

   This check is mandatory for every ingest, not just the prize pattern.

5. **Step 2: Transform to award schema** (see Step 5 for details)
   - Map native fields to OpenAlex schema
   - Handle date parsing (try multiple formats)
   - Map funding types appropriately
   - Generate unique ID as `abs(xxhash64(CONCAT({funder_id}, ':', {lowercase_native_id}))) % 9000000000`

6. **Step 3: Delete old data and insert into openalex_awards_raw with priority**
   - Determine priority (lower = higher priority). Tier guide:
     - 0: GTR Project Awards (authoritative UK grants, full metadata)
     - 1: Crossref Awards
     - 2: Backfill Awards (from publication acknowledgements)
     - 3: NIH / NSF / NSERC / GTR (US/UK/CA flagship funders with full metadata)
     - 4+: All other direct-from-funder ingests, assigned in roughly the order
       they were added.
   - **For a new funder, take the next free integer.** As of 2026-05 the
     header of [CreateAwards.ipynb](../../notebooks/awards/CreateAwards.ipynb)
     lists priorities through 49. Read that header — it's the authoritative
     priority registry — and pick the next free number. Don't reuse a slot.
   - **⚠️ The priority integer lives in TWO places that MUST match:**
     (a) the per-funder notebook's INSERT cell (`N as priority`), and
     (b) the priority list in `CreateAwards.ipynb`.
     When forking a template (e.g., NASA → EPA → AHRQ), update **both**
     places, not just the trailing comment next to the integer. This bug
     shipped twice in May 2026 (PR #83 EPA, PR #84 AHRQ) — both copied
     `23 as priority` (NASA's slot) verbatim from the NASA template and
     only refreshed the `-- EPA priority` / `-- AHRQ priority` comment.
     The bug would have caused cross-source collisions with NASA in the
     dedup. Always grep the notebook for the literal `as priority` and
     verify the integer matches `CreateAwards.ipynb` before pushing.
   - Emit an SQL block that:
     1. Deletes previous data for this source (using provenance + priority as key)
     2. Inserts fresh data to `openalex.awards.openalex_awards_raw`
   ```sql
   -- Remove previous data for this source before inserting fresh data
   DELETE FROM openalex.awards.openalex_awards_raw
   WHERE provenance = '{provenance_value}' AND priority = N;

   -- Insert into openalex_awards_raw with priority
   INSERT INTO openalex.awards.openalex_awards_raw
   SELECT
       id,
       display_name,
       description,
       funder_id,
       funder_award_id,
       amount,
       currency,
       funder,
       funding_type,
       funder_scheme,
       provenance,
       start_date,
       end_date,
       start_year,
       end_year,
       lead_investigator,
       co_lead_investigator,
       investigators,
       landing_page_url,
       doi,
       works_api_url,
       created_date,
       updated_date,
       N as priority  -- Replace N with next available priority
   FROM openalex.awards.{funder}_awards;
   ```

7. **Verification queries**
   **⚠️ CRITICAL: Verify column names before writing SQL**

   Before writing any CTEs or intermediate queries that reference columns from the raw table, you MUST verify the actual column names present in the parquet file. Column names in the source data may differ from what you expect based on documentation or similar funders.

   Add a verification cell immediately after loading the raw data:
   ```sql
   -- Verify actual column names before writing transformation queries
   DESCRIBE openalex.awards.{funder}_raw;
   ```

   Or inspect the schema directly:
   ```sql
   SELECT * FROM openalex.awards.{funder}_raw LIMIT 1;
   ```

   **Common pitfalls:**
   - Column names may use different casing (e.g., `ProjectId` vs `project_id`)
   - Fields may be nested in structs differently than expected
   - Similar funders may use different field names for the same concept
   - Documentation may be outdated or incomplete

   Only after confirming the actual column names should you write the transformation SQL. Reference the exact column names from the `DESCRIBE` output in all subsequent CTEs and queries.

8. **Verification queries** (see Step 6 for details)

### 2.3 Defensive SQL Practices

**ALWAYS use these patterns to handle dirty data gracefully:**

```sql
-- For numeric conversions (handles "TBC", "N/A", empty strings, etc.)
TRY_CAST(amount_column AS DOUBLE) as amount

-- For date conversions from year strings
CASE
    WHEN TRY_CAST(year_column AS INT) IS NOT NULL
    THEN TRY_TO_DATE(CONCAT(year_column, '-01-01'), 'yyyy-MM-dd')
    ELSE NULL
END as start_date

-- For year extraction
TRY_CAST(year_column AS INT) as start_year

-- For date parsing with multiple formats
COALESCE(
    TRY_TO_DATE(date_col, 'yyyy-MM-dd'),
    TRY_TO_DATE(date_col, 'dd/MM/yyyy'),
    TRY_TO_DATE(date_col, 'MM/dd/yyyy')
) as parsed_date

-- For first/nth element of an array (handles empty arrays)
try_element_at(FILTER(arr, x -> x.field IS NOT NULL), 1).field
try_element_at(COALESCE(arr, ARRAY()), 1)
```

**NEVER use:**
- `CAST()` on external data - use `TRY_CAST()` instead
- `TO_DATE()` on external data - use `TRY_TO_DATE()` instead
- `ELEMENT_AT()` on any array that could be empty (`FILTER(...)` output,
  any nested array from the source) — it raises `SparkArrayIndexOutOfBoundsException`
  at runtime. Use `try_element_at(...)` instead; it returns NULL on out-of-bounds.
  This is the fix Spark's own error message recommends. Confirmed incident on
  IDRC ([16ab79a](https://github.com/ourresearch/openalex-walden/commit/16ab79a)):
  smoke test on 1 XML (205 activities) had no empty budgets/orgs/recipient_countries
  arrays; full corpus did, and `ELEMENT_AT` blew up the sanity cell.
- Assumed column names without verifying via `DESCRIBE`

### 2.3.0.1 Future-dated start/end years

Several funders publish records with `start_year` far in the future:
placeholder dates for not-yet-active projects (BMBF nuclear decommissioning
runs out to 2032), draft Gateway-to-Research entries pre-dated to 2028+, and
planning-stage Horizon Europe call IDs ingested as if they were grants. When
sorted by `start_year:desc` on the awards browse page, these dominate the top
and make OpenAlex look broken even though the underlying data is technically
what the funder publishes.

**Rule:** if `start_year > YEAR(current_date()) + 1`, set both `start_year`
and `end_year` to NULL. One year ahead is legitimate (grants announced for
next year). More than that is a placeholder.

**Where the rule is enforced:** centrally in
[CreateAwards.ipynb](../../notebooks/awards/CreateAwards.ipynb) in the
`cleaned_awards` CTE, so every priority-merged record gets the cap on every
refresh — including funders that pre-date this doc. Per-funder notebooks
should also apply the cap inline so the `openalex.awards.{funder}_awards`
raw table is clean and not just the final merged table. Pattern:

```sql
-- If you derive start_year via YEAR(...), wrap it:
CASE WHEN YEAR(b.start_date) > YEAR(current_date()) + 1
     THEN NULL
     ELSE YEAR(b.start_date)
END as start_year,
CASE WHEN YEAR(b.start_date) > YEAR(current_date()) + 1
     THEN NULL
     ELSE YEAR(b.end_date)
END as end_year

-- Or as a final cleanup CTE wrapping a prior `awards_transformed`:
SELECT
  * EXCEPT(start_year, end_year),
  CASE WHEN start_year > YEAR(current_date()) + 1 THEN NULL ELSE start_year END as start_year,
  CASE WHEN start_year > YEAR(current_date()) + 1 THEN NULL ELSE end_year END as end_year
FROM awards_transformed
```

`end_year` is nulled only when paired with a future `start_year`. A
legitimate multi-year grant (e.g. Wellcome 2024–2029) keeps its planned
end_year; only placeholder records where BOTH years are stamped to an
out-year (BMBF 2030–2030 nuclear, GTR 2028–2028 drafts) are cleaned.

Confirmed incidents (2026-05): BMBF (24 awards `start_year > 2027`), and
the UK research councils via Gateway to Research — BBSRC (9), AHRC (6),
EPSRC (6), ESRC (3), NERC (1), STFC (1) — all surfacing on
`https://openalex.org/awards?sort=start_year:desc`.

### 2.3.1 Composing display_name from scraped fields

When `display_name` concatenates a scraped role/title onto a fixed prefix (e.g.
`'Name — HHMI ' || role`), audit for two failure modes that are invisible until
you grep the output table:

1. **Placeholder values equal to the prefix.** If the scraper's `titleParts[1]`
   split has no role token between name and the organization, the placeholder
   captured can be the org name itself. Result: `"Name — HHMI HHMI"`. Defend
   with `NULLIF(role, 'HHMI')` (or whatever the placeholder string is) so the
   `COALESCE` falls back to a sensible default like `'Scientist'`.
2. **Role already contains the prefix.** Sources sometimes name programs with
   the org prefix baked in (`'HHMI Professor'`). Composing
   `'Name — HHMI ' || role` then yields `"Name — HHMI HHMI Professor"`. Defend
   with `REGEXP_REPLACE(role, '^HHMI ', '')` before composition. Keep
   `funder_scheme` set to the raw role — downstream consumers will want
   `'HHMI Professor'` as a queryable program name.

Confirmed on HHMI ([5085aa8](https://github.com/ourresearch/openalex-walden/commit/5085aa8),
[a624b73](https://github.com/ourresearch/openalex-walden/commit/a624b73)): 251
of 1,739 rows hit case 1, and another 42 rows hit case 2 after case 1 was
patched. Both showed up only when grepping the produced `display_name` column.

### 2.4 Common field mappings

| OpenAlex field | Common source fields |
|----------------|---------------------|
| display_name | title, project_title, name |
| description | abstract, summary, description |
| funder_award_id | grant_id, project_id, award_number, grant_reference |
| amount | `total_cost`, `amount`, `funding_amount`, `grant_offer`, `value`, `valuePounds`, `funded_value`, `awarded_amount`, `award_amount`, `total`, `budget`, `project_cost`, `monto`, `importe`, `montant`, `betrag`, `valor`, `kwota`, `belopp`. **May be nested** inside an array/struct (e.g., DataCite `fundingReferences[].fundingAmount`). Run the discovery scan in Step 1.6 before mapping. |
| currency | `currency`, `currencyCode`, `currency_code`, `ccy`, `fundingAmountCurrency`, `amountCurrency`. May be **implicit** — hardcode from funder country/region (e.g., GTR = GBP) and document in the notebook header. |
| funding_type | Map from activity codes or categories |
| funder_scheme | program_name, funding_scheme, activity_code |

### 2.4.1 Parsing PI names into given_name / family_name

Do **not** use `name.split(" ", 1)` to split "First Last" into given/family —
it mishandles middle initials and degree suffixes:

- `"Chad A. Mirkin"` → family=`"A. Mirkin"` (wanted `"Mirkin"`)
- `"Doris Ying Tsao"` → family=`"Ying Tsao"` (wanted `"Tsao"`)
- `"Robert S. Langer"` → family=`"S. Langer"` (wanted `"Langer"`)

Use the `split_name` helper instead. Canonical implementation in
[wolf_to_s3.py](../../scripts/local/wolf_to_s3.py); same pattern in
[hhmi_to_s3.py](../../scripts/local/hhmi_to_s3.py) and
[kavli_to_s3.py](../../scripts/local/kavli_to_s3.py). It strips trailing
degree/suffix tokens (`PhD`, `MD`, `DPhil`, `Jr.`, `Sr.`, `II`, `III`, `IV`)
and treats the last remaining token as family, everything before as given.
Port it verbatim — don't roll your own.

**→ Update tracker:** Change status to "Step 3" with notes (e.g., "Notebook created").

---

## Step 3: Add to CreateAwards.ipynb

**IMPORTANT:** This step integrates the new funder into the main awards table.

Edit `notebooks/awards/CreateAwards.ipynb` to add the new funder source with the priority determined in section 2.2.5

### 3.1 Update markdown header

Add the new funder to the priority list in the notebook header.

**→ Update tracker:** Change status to "Step 4" with notes (e.g., "Added to CreateAwards.ipynb at priority N").

---

## Step 4: Commit and Push

**IMPORTANT:** The Databricks GUI syncs from the git repo. You must commit and push before the user can see/run the notebooks.

### 4.1 Commit all new files

```bash
git add scripts/local/{funder}_to_s3.py notebooks/awards/Create{FunderName}Awards.ipynb notebooks/awards/CreateAwards.ipynb
git commit -m "Add {FunderName} awards pipeline

- Add {funder}_to_s3.py script to download and upload {FunderName} grants
- Add Create{FunderName}Awards.ipynb notebook to transform data to awards schema
- Update CreateAwards.ipynb to include {FunderName} at priority N

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

### 4.2 Push to remote

```bash
git pull --rebase && git push
```

If there are merge conflicts (especially in `CreateAwards.ipynb`), resolve them by keeping BOTH the remote changes AND your new funder addition.

### 4.3 Tracker sanity check (mandatory before push, and again after merge)

**The tracker is the file most likely to silently lose your edit during a
rebase.** `CreateAwards.ipynb` and `scripts/local/*.py` are separate files
per funder, so conflicts there look like "+++ ours" merges that a human
can review. `plans/awards/funder-ingestion-tracker.md` is a single shared
file every PR touches — a rebase-conflict resolution that takes "their
side" silently drops your tracker row, and your PR still compiles, ships,
and merges with the script + notebook + registry intact and the tracker
row gone. Confirmed incidents: PR #138 (lost the 7 codex-chain tracker
rows for #97-#103) and PR #139 (lost 22 more tracker rows across PRs
#85-#135 over ~3 weeks of activity).

Run this check **before `git push`** and again **after your PR merges**.
It cross-references the priority list in `CreateAwards.ipynb` against
`Priority N` entries in the tracker, and exits non-zero if any priority
in the registry has no matching tracker row.

```bash
python3 - <<'PY'
import json, re, sys
nb = json.load(open('notebooks/awards/CreateAwards.ipynb'))
hdr = ''.join(nb['cells'][0]['source']) if isinstance(nb['cells'][0]['source'], list) else nb['cells'][0]['source']
registry = {int(m.group(1)): m.group(2).strip()
            for m in re.finditer(r'-\s+(\d+):\s+(.+?)\s+\(', hdr)}
tracker = open('plans/awards/funder-ingestion-tracker.md').read()
have = set(int(m.group(1)) for m in re.finditer(r'Priority\s+(\d+)[\s\.\-]', tracker))
missing = sorted(p for p in registry if p >= 50 and p not in have)
if missing:
    print('TRACKER GAPS — these priorities have a CreateAwards.ipynb entry but no tracker row:')
    for p in missing:
        print(f'  prio {p}: {registry[p]}')
    sys.exit(1)
print(f'OK — tracker in sync with registry for {len([p for p in registry if p >= 50])} priorities (>=50).')
PY
```

If the check reports gaps **including your new funder**: your tracker
edit didn't survive the rebase. Re-add the tracker row and amend the
commit before pushing.

If the check reports gaps **for OTHER funders**, the drift is pre-existing
— don't try to fix it inside your funder PR. Open a separate restore PR
following the PR #138 / #139 pattern (verbatim row text pulled from the
originating PR's `/pulls/N/files` patch via the GitHub API).

### 4.4 Maintainer-side merge: never "take theirs" on the tracker

This section is for the maintainer (Kyle) running the batch merge — not for
the contractor preparing a PR. The auto-resolve helper in
`scripts/local/_merge_resolve.py` is the canonical implementation.

When merging a batch of N funder PRs locally, every PR after the first
conflicts on both `notebooks/awards/CreateAwards.ipynb` (priority list cell)
and `plans/awards/funder-ingestion-tracker.md`. The instinct to write a
helper that takes `--theirs` on the tracker — because Rohan's branch's row
is the new authoritative one for that funder — is **wrong** and clobbers
every prior merge in the batch. Confirmed incident: 2026-05-26 batch lost
all tracker rows for #143-#155 between merges before being caught and
re-resolved with a 3-way patch-apply.

The mistake: "the tracker conflict is between ours-has-newer-rows-from-PR-N-1
vs. theirs-has-the-NEW-funder-row." Taking theirs replaces the entire
file with theirs, dropping every row added by PR N-1, N-2, etc. in the
same batch. The 3-way merge base (what theirs branched from) does NOT
have those intermediate rows either, so git's conflict markers cover the
whole region.

Correct algorithm for the tracker (implemented in `_merge_resolve.py`):

1. Compute `base = git merge-base HEAD MERGE_HEAD`.
2. Index rows by funder name (the first `|`-delimited cell, which is
   unique per row).
3. Find rows in theirs that are **NEW vs. base** (funder name not in base
   index) or **MODIFIED vs. base** (line text differs from base's row for
   the same funder).
4. For each NEW row in theirs: insert it into ours, anchored after the
   row that precedes it in theirs (if that row exists in ours).
5. For each MODIFIED row in theirs: overwrite ours' line for that funder
   with theirs' line.
6. Write the result. Do NOT replace ours' entire file with theirs'.

This preserves all intermediate batch state while still landing the new
funder's row. Anchor placement is approximate (the tracker isn't strictly
sorted) — non-issue, but be aware that rows added during a batch may end
up near "AHRQ" or wherever the branch's tracker had its insertion point.

For `CreateAwards.ipynb` the splice is simpler: the priority list cell is
canonically sorted by priority integer, and each PR adds exactly one new
priority. The helper splits the cell-zero source by `\n` (the literal
JSON escape, not a real newline), finds priorities in theirs not in ours,
and splices them in before the trailing `When the same award appears in
multiple sources` marker. Do NOT round-trip the notebook through
`json.dumps(..., ensure_ascii=False)` — Databricks emits the source field
with trailing padding (~36 literal newlines between the closing `"` and
the next JSON token) that any naive re-serialize strips, producing a
massive noisy diff. Use text-level surgery on the raw bytes.

Watch out for these regex traps when parsing priority lines from the
source field:

- Descriptions contain `—` (em-dash JSON-escape) and `\"` (escaped
  quote), so a regex like `[^\\]*?` for the description body bails at the
  first backslash. Split on the line separator `\n` (literal 2-char
  sequence backslash + n) and treat each chunk as a markdown line; then
  match `^- (\d+):` against the chunk.
- Git Bash heredocs on Windows mangle backslashes — `r"[\\]"` written in
  a `<<'PYEND'` heredoc arrives as `[\]` (3 chars), not `[\\]` (4 chars).
  Write the helper to a file and `py scripts/local/_merge_resolve.py`, not
  via inline `py - <<EOF`.

**→ Update tracker:** Change status to "Step 5" with notes (e.g., "Code committed, waiting for human to run notebook").

---

## Step 5: Human Runs Notebook

**STOP: HUMAN ACTION REQUIRED**

Tell the user:
> The notebook is ready at `notebooks/awards/Create{FunderName}Awards.ipynb`.
> Please run it in Databricks and let me know when it completes.

Wait for confirmation before proceeding.

**⚠️ The notebook must run against the full S3 parquet, not a sample.** Three
classes of bug from Step 1.2 and Step 2.3 only appear at scale:

- **pyarrow int-inference on null-heavy columns** (Edit 1) — dense smoke-test
  rows have no nulls; full corpus does. Caused failures on IDRC and Rockefeller.
- **`ELEMENT_AT` on empty arrays** (Edit 3) — small samples often have no empty
  arrays. Caused failure on IDRC's full corpus.
- **Placeholder strings in scraped role/title** (Edit 4) — distribution-dependent.
  Caused two consecutive HHMI fixes after the notebook had already shipped.

If you ran the notebook on a `LIMIT 50` slice during dev, **rerun it without
the limit** before marking the funder Complete.

**→ Update tracker:** When user confirms notebook ran on the full corpus, change status to "Step 6".

---

## Step 6: Verify the Data

Use Databricks MCP to run these verification queries:

### 6.1 Basic counts
```sql
SELECT COUNT(*) as total FROM openalex.awards.{funder}_awards;
```
Should match expected count from Step 1.

### 6.2 Schema validation
```sql
DESCRIBE openalex.awards.{funder}_awards;
```
Verify all required columns exist with correct types.

### 6.3 Data completeness
```sql
SELECT
    COUNT(*) as total,
    COUNT(display_name) as has_title,
    COUNT(description) as has_description,
    COUNT(amount) as has_amount,
    COUNT(start_date) as has_start_date,
    COUNT(lead_investigator) as has_pi,
    ROUND(COUNT(display_name) * 100.0 / COUNT(*), 1) as pct_title,
    ROUND(COUNT(start_date) * 100.0 / COUNT(*), 1) as pct_dates
FROM openalex.awards.{funder}_awards;
```

### 6.4 Sample inspection
```sql
SELECT * FROM openalex.awards.{funder}_awards LIMIT 10;
```
Verify data looks reasonable.

### 6.5 Funder consistency
```sql
SELECT funder.display_name, COUNT(*)
FROM openalex.awards.{funder}_awards
GROUP BY funder.display_name;
```
Should show only the expected funder(s).

### 6.6 Year distribution
```sql
SELECT start_year, COUNT(*) as cnt
FROM openalex.awards.{funder}_awards
WHERE start_year IS NOT NULL
GROUP BY start_year
ORDER BY start_year DESC
LIMIT 20;
```
Verify reasonable year range. **Top values must be ≤ `YEAR(current_date()) + 1`.** Anything beyond means the future-year cap (§2.3.0.1) wasn't applied — go back and wrap the `start_year` / `end_year` derivations.

```sql
-- Confirm no records slipped past the cap:
SELECT COUNT(*) FROM openalex.awards.{funder}_awards
WHERE start_year > YEAR(current_date()) + 1;
-- Must be 0.
```

### 6.7 Amount and currency coverage (FAIL-FAST CHECK)

```sql
SELECT
    COUNT(*) AS total,
    COUNT(amount) AS has_amount,
    ROUND(COUNT(amount) * 100.0 / COUNT(*), 1) AS pct_amount,
    COUNT(DISTINCT currency) AS distinct_currencies,
    collect_set(currency) AS currencies,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    AVG(amount) AS avg_amount
FROM openalex.awards.{funder}_awards;
```

**STOP and re-run Step 1.6 if any of these hold:**
- `pct_amount < 50%` and the funder is known to publish amounts (i.e., not a research prize with implicit standard amounts)
- `distinct_currencies = 0` — currency was never set
- `avg_amount < 1000` or `> 1,000,000,000` — likely wrong unit (pence not pounds) or you mapped a year/count column
- Currencies don't match the funder's country/region (e.g., German funder showing only USD)

Reference baselines: NIH, GTR Project Awards, and ARC all have >95% amount coverage. **Both DataCite Awards and Gateway to Research initially shipped with broken amount/currency — do not skip this check.**

### Success criteria:
- Row count matches expected
- >90% have display_name
- >50% have start_date (varies by funder)
- >50% have amount (waive only if funder is a research prize or genuinely doesn't publish amounts)
- currency is populated and matches the funder's country/region
- Funder struct is populated correctly
- No obviously malformed data in samples

Report any concerns to the user before proceeding.

**→ Update tracker:** Change status to "Step 7" with verification results (e.g., "Verified: 450,000 grants, 95% with titles").

---

## Step 7: Final Human Approval

**STOP: HUMAN ACTION REQUIRED**

Tell the user:
> I've completed the integration. Please review:
> 1. The download script at `scripts/local/{funder}_to_s3.py`
> 2. The notebook at `notebooks/awards/Create{FunderName}Awards.ipynb`
> 3. The changes to `notebooks/awards/CreateAwards.ipynb`
>
> When you're ready, run CreateAwards.ipynb to merge everything.

**→ Update tracker:** When user approves and runs CreateAwards.ipynb, change status to "Step 8".

---

## Step 8: Wire into the CreateFunderSourcedAwards bundle

After the notebook has been verified and `CreateAwards.ipynb` has been
re-run successfully, wire the new funder into the
**CreateFunderSourcedAwards** orchestration job so it runs on every scheduled
refresh.

This job is defined as a Databricks Asset Bundle in
[jobs/create_funder_sourced_awards.yaml](../../jobs/create_funder_sourced_awards.yaml).
Each per-funder `Create{X}Awards` notebook is one task with `source: GIT`; the
downstream `Create_Awards` task depends on all of them and runs
`notebooks/awards/CreateAwards`; `Work_Awards` runs after that. **There is no
manual `databricks jobs reset` step anymore** — committing the YAML to `main`
triggers a GitHub Action that deploys via `databricks bundle deploy`.

### 8.1 Edit the bundle YAML

Two edits in [jobs/create_funder_sourced_awards.yaml](../../jobs/create_funder_sourced_awards.yaml):

1. **Add a per-funder task** (alphabetical within the task list):
   ```yaml
   - task_key: XYZ_Awards
     email_notifications: {}
     environment_key: Default
     notebook_task:
       notebook_path: notebooks/awards/CreateXYZAwards
       source: GIT
     run_if: ALL_SUCCESS
     timeout_seconds: 0
     webhook_notifications: {}
   ```

   Use `source: GIT` (not `WORKSPACE`) and a **relative** `notebook_path`
   (`notebooks/awards/CreateXYZAwards`, no leading `/Workspace/Shared/...`,
   no `.ipynb` suffix).

2. **Add the task_key under `Create_Awards.depends_on`** (anywhere in the list):
   ```yaml
   - task_key: XYZ_Awards
   ```

### 8.2 Validate the bundle locally

```bash
databricks bundle validate
```

Must exit clean. If validation fails, fix it before committing — the deploy
Action runs the same validator and will fail the push otherwise.

### 8.3 Commit and push

```bash
git add jobs/create_funder_sourced_awards.yaml
git commit -m "Wire {FunderName}_Awards into CreateFunderSourcedAwards (priority N) + tracker Complete

Co-Authored-By: Claude <noreply@anthropic.com>"
git pull --rebase && git push
```

On push, the GitHub Action deploys the bundle. Watch the Action's run for
green; if it fails, the job won't pick up the new task on the next schedule.

**→ Update tracker:** Change status to "Complete" with final grant count and
priority (e.g., `Complete | Priority 44 - 1,739 HHMI investigator awards (no
amount, by design). Wired into CreateFunderSourcedAwards 2026-05-14.`).

---

## Reference: Existing Examples

| Funder | Script                | Notebook                     | API Type      |
|--------|-----------------------|------------------------------|---------------|
| NIH    | nih_exporter_to_s3.py | CreateNIHAwards.ipynb        | File download |
| NWO    | nwo_to_s3.py          | CreateNWOAwards.ipynb        | JSON API      |
| GTR    | gtr_to_s3.py          | CreateGTRProjectAwards.ipynb | XML API       |
| Gates  | gates_to_s3.py        | CreateGatesAwards.ipynb      | CSV download  |

## Reference: S3 Paths

- Bucket: `openalex-ingest`
- Path pattern: `awards/{funder_name}/{funder_name}_projects.parquet`
- Access in Databricks: `s3a://openalex-ingest/awards/...`

## Reference: Databricks Tables

- Raw staging: `openalex.awards.{funder}_raw`
- Transformed: `openalex.awards.{funder}_awards`
- Combined: `openalex.awards.openalex_awards`
- Funder lookup: `openalex.common.funder`
