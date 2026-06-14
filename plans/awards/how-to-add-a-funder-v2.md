# How to Add a Funder (v2 — DRAFT, pending validation)

> ⚠️ **This is the v2 draft of Steps 4.5–9** (oxjobs #245). The authoritative doc is
> still [`how-to-add-a-funder.md`](how-to-add-a-funder.md). This version rewrites the
> downstream half for end-to-end CLI execution by an admin (no GUI), so a contractor's
> Step-4 funder can be pushed to Complete without the notebook's author. **Steps 0–4 are
> unchanged from v1.** The CLI commands in Steps 5/7/9 are written from repo evidence but
> have **not yet been run end-to-end** — once Kyle completes one funder (pilot: Holberg
> Prize) using this doc, fold in any fixes and promote v2 over the v1 file.

**Location:** `plans/awards/how-to-add-a-funder-v2.md`
**Purpose:** Instructions for Claude instances to add awards from a new funder to OpenAlex.
**Human involvement:** Steps 0–4 are agent/contractor work. Steps 5–9 (run the notebook, integrate, deploy, sync) need Databricks + S3 credentials and are run by an admin **entirely from the CLI — no Databricks GUI required**. If you're picking up someone else's funder that's already at Step 4, start at **Step 4.5**. The only human *decision* gate is the approval in Step 7.
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

> **0. ALWAYS CHECK FIRST — a bulk export the funder itself publishes.**
> Before any of the methods below (including scraping), look for a
> **"Download" / "Export" / "Download Results"** button, or a published
> **CSV / Excel** of the funder's whole grants database. A funder's own bulk
> export almost always carries **far more structured fields than its web
> pages** (PI, institution, exact amount, dates, status, etc.) and is **one
> download instead of hundreds/thousands of paginated + detail-page fetches.**
> Confirmed on **PCORI**: the "Download Results" CSV gave 2,683 projects × 16
> columns (PI, Organization, Budget, Award Date, …) — vastly richer than the
> JS portfolio pages, which would have needed ~107 list renders + 2,683 detail
> fetches for less. This sits **above everything else on the ladder**; do not
> write a page scraper until you've ruled out an export.
>
> If the export is behind a WAF/Cloudflare wall that blocks headless/plain
> HTTP (PCORI's `obolus` challenge ConnectionReset plain Bash), the **Chrome
> extension / a real browser can still fetch it** — `fetch(url,{credentials:
> 'include'})` in the page context, or trigger an `<a download>`; the file
> lands in Downloads and the build reads it locally. Govt funders: also check
> **data.gov / data.gouv / data.gov.au / CKAN** and **360Giving** (UK
> charities) for the same dataset as a clean file.

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

   **Grep-able forbidden patterns** (the v2 2026-05-27 batch review caught
   three of these across PRs #171 Searle, #175 Kauffman, #177 Carnegie):

   ```python
   # ❌ FORBIDDEN — bails on first empty page
   if not chunk: break
   if not posts: break
   if not cards: break

   # ❌ FORBIDDEN — bails on first non-200 (CDN/WAF blip)
   if r.status_code != 200: break
   ```

   **Correct pattern** (used by Carnegie/BBRF after fix):

   ```python
   consecutive_empty = 0
   consecutive_non200 = 0
   MAX_CONSECUTIVE_EMPTY = 3      # 2 is too low — see PR #173 BBRF baseline bump
   MAX_CONSECUTIVE_NON200 = 5
   while page <= MAX_PAGES:
       r = http_get(url, page=page)
       if r.status_code != 200:
           consecutive_non200 += 1
           print(f"page {page}: HTTP {r.status_code} ({consecutive_non200}/{MAX_CONSECUTIVE_NON200}); continuing")
           if consecutive_non200 >= MAX_CONSECUTIVE_NON200:
               raise RuntimeError(...)  # don't silently truncate
           page += 1
           continue
       consecutive_non200 = 0
       items = parse(r)
       if not items:
           consecutive_empty += 1
           if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
               break  # 3 in a row is probably real EOF
           page += 1
           continue
       consecutive_empty = 0
       ...
   ```

   For WP REST specifically, the cleaner approach is to read `X-WP-TotalPages`
   from the first response and use it as the authoritative terminator. See
   `searle_scholars_to_s3.py` after fix for the canonical pattern. Sources
   with WAF/Cloudflare history (Carnegie, RWJF) **must** treat non-200s as
   flakes rather than EOF.

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
   (`F4026159580`, `CreateSchmidtSciencesAwards.ipynb`), Smithsonian SARF
   (`F7230414656`, `CreateSmithsonianSARFAwards.ipynb` — PR #167, caught
   pre-merge in the 2026-05-27 batch review). All four originally shipped
   with the dim-select pattern, would have silently emitted 0 rows, and
   were patched with this inline-constants shape.

   **Failure signature to watch for:** scraper uploads the expected row
   count to S3, raw table loads cleanly with the right `COUNT(*)`, but
   `openalex.awards.{funder}_awards` has 0 rows after Step 2. That's almost
   always a Path B funder using a Path A CTE.

   This check is mandatory for every ingest, not just the prize pattern.

   **Forking guard rule.** When forking an existing notebook as a template,
   the most common mistake is keeping the Path A `assert_true(COUNT(*) = 1)`
   cell and the dim-selecting CTE while only changing the literal
   `funder_id`. That fails closed for Path B funders (the assert raises),
   but if the assert is silently removed during cleanup, the CTE then
   silently emits 0 rows. Before forking, **grep your new notebook**:

   ```bash
   # Both should return 0 hits for Path B funders.
   grep -E "FROM openalex.common.funder.*WHERE funder_id = {non_F4320_id}" notebooks/awards/Create{X}Awards.ipynb
   grep "assert_true.*funder_id = {non_F4320_id}" notebooks/awards/Create{X}Awards.ipynb
   ```

   If either hits, your funder is Path B but your notebook is Path A —
   replace with the inline-constants pattern above.

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

**Do the split in Python, not in SQL.** A SQL-side
`element_at(SPLIT(name, ' '), -1)` is equivalent to `name.split(" ", 1)`
in terms of bugs (no suffix strip), **and** has the additional flaw that
bare `element_at` on an empty array raises `SparkArrayIndexOutOfBoundsException`
at runtime per §2.3. The Python script already has the raw name string in
memory — it's the right place to do the parse. The script writes
`lead_given_name` and `lead_family_name` parquet columns; the notebook
just `TRIM`/`NULLIF`s them. Confirmed incident on MJFF (PR #165,
2026-05-27): the original notebook used
`ARRAY_JOIN(SLICE(SPLIT(lead_name_clean, ' '), 1, SIZE(...) - 1), ' ')` +
`element_at(SPLIT(lead_name_clean, ' '), -1)` — hit both bugs at once.

**Don't roll your own — even when it looks "close enough".** A custom
helper that strips only leading honorifics (Dr/Prof/Mr/Ms) but not
trailing degree suffixes (PhD/MD/Jr/Sr) will produce `family = "PhD"` on
names like `"Dr. Ahmed Ali PhD"`. Confirmed incident on HEC Pakistan
(PR #176, 2026-05-27): custom `split_pi_name` stripped only leading
titles. Either import `split_name` from `wolf_to_s3.py` directly, or
copy the function body **including the `suffixes` set**. The
2026-05-27 review found two contractors building variants that omitted
the suffix logic; the canonical helper is short enough (~18 lines) that
duplicating it is fine, but the suffix list is the load-bearing part —
not the leading-title regex.

**Grep check before push:**
```bash
# Both should return 0 for a new funder script:
grep "name\.split(" scripts/local/{funder}_to_s3.py        # forbidden naïve split
grep "element_at(SPLIT" notebooks/awards/Create*Awards.ipynb  # forbidden SQL split
```

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

### 4.0 Pre-PR self-check (run BEFORE `git commit`)

The 2026-05-27 fleet review (28 contractor PRs, #160–#187) found 7 bugs
that a single grep round would have caught. Run all of these on your new
files before opening the PR; each must return **0 hits** unless noted:

```bash
F={funder_name}             # e.g. mjff, smithsonian_sarf, kauffman
NB=notebooks/awards/Create*Awards.ipynb   # narrow to your file before grepping
SCRIPT=scripts/local/${F}_to_s3.py

# §1 — first-empty-page bail (caught 3 PRs in the 2026-05-27 batch)
grep -E "if not (chunk|posts|cards|items|results|grantees):\s*break" $SCRIPT
grep -E "if r\.status_code != 200:\s*break" $SCRIPT
grep -E "if resp\.status_code != 200:\s*break" $SCRIPT

# §1.6 — Path A applied to a Path B funder (caught 1 PR; same class as Abel/MinCiencias/Schmidt)
# Run if your funder_id does NOT start with 4320:
grep -E "assert_true.*COUNT\(\*\) = 1.*openalex\.common\.funder" $NB
grep -E "FROM openalex\.common\.funder.*WHERE funder_id" $NB

# §2.3 — bare element_at / ELEMENT_AT (raises on empty arrays)
grep -E "(^|[^_])element_at\(" $NB | grep -v "try_element_at"
grep -E "ELEMENT_AT\(" $NB | grep -v "TRY_ELEMENT_AT"

# §2.4.1 — naïve PI splitting (caught 2 PRs)
grep -E "name\.split\(" $SCRIPT             # naïve Python
grep -E "element_at\(SPLIT\(.*,\s*-1\)" $NB # SQL equivalent of name.split(' ',1)

# §1.2 — missing UTF-8 shim / astype / --limit / --output-dir
grep -q "sys.stdout.reconfigure" $SCRIPT || echo "MISSING: UTF-8 shim"
grep -q "astype(\"string\")\|astype(str)" $SCRIPT || echo "MISSING: df.astype before to_parquet"
grep -q '"--limit"' $SCRIPT || echo "MISSING: --limit flag"
grep -q '"--output-dir"' $SCRIPT || echo "MISSING: --output-dir flag"

# §2.4.1 — custom name splitter missing the canonical suffix set
# (look for any "def split_*name" function and verify it strips PhD/MD/Jr/Sr)
grep -E "def split_(pi_|investigator_|person_|lead_)?name" $SCRIPT
# If a match is found, manually verify the function includes:
#   suffixes = {"phd", "md", "dphil", "dsc", "scd", "jr.", "sr.", "ii", "iii", "iv", "jr", "sr"}
# Easier: just `from <canonical> import split_name` if your script imports work that way,
# or copy the wolf_to_s3.py implementation verbatim.
```

If anything trips, fix the script/notebook before continuing to §4.1.

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

## Step 4.5: Picking Up from Step 4 as the Admin

Steps 0–4 are the **contractor's** half of the pipeline: find the data, write the
download script, write the `Create{FunderName}Awards` notebook, note the funder's
priority in `CreateAwards.ipynb`, and commit/push. A funder sitting at **Step 4**
means all of that is done and on `main`. What's left is the half that needs
credentials the contractor doesn't have: **uploading the parquet to S3 and running
the notebook on Databricks.** That's your job.

You do **not** need to have written the notebook — everything you need is in the repo.
Steps 5–9 below run entirely from the CLI; you never open the Databricks GUI.

### 4.5.1 What you'll be running, and what each step produces

| Step | What it does | Writes |
|------|--------------|--------|
| 1.3  | Run the contractor's `{funder}_to_s3.py` | `s3://openalex-ingest/awards/{funder}/…parquet` |
| 5    | Run the `Create{FunderName}Awards` notebook | `{funder}_raw`, `{funder}_awards`, + rows in shared `openalex_awards_raw` |
| 6    | Verify the `{funder}_awards` table | — (read-only checks) |
| 7    | Approve, then trigger **RefreshWorkAwards** | combined `openalex_awards` + `work_awards` |
| 8    | Wire the notebook into the **CreateFunderSourcedAwards** bundle | future scheduled refreshes |
| 9    | Trigger **Sync Awards to Elasticsearch** (once per session) | the funder becomes visible in the API |

The key thing to understand: the per-funder notebook (Step 5) is what actually
integrates the data — it inserts the funder's rows into the shared
`openalex.awards.openalex_awards_raw` table (`DELETE` by provenance+priority, then
`INSERT`). Step 7 just rebuilds the user-facing combined table and work→award links
from that raw table. `CreateAwards.ipynb` is **generic**: there is no per-funder code
change in it beyond the priority note the contractor already added in Step 3.

### 4.5.2 Confirm your credentials before you start

You need two sets of credentials. Check both now — discovering a gap mid-run is annoying.

**1. Databricks CLI** — to run the notebook and trigger jobs:

```bash
databricks auth profiles
```

You want a line for `dbc-ce570f73-0362` showing **Valid: YES**. Confirm it actually works:

```bash
databricks current-user me -p dbc-ce570f73-0362
```

Expected: a JSON blob containing your `userName`. If you instead see `Valid: NO`, a
401/403, or "cannot resolve auth", (re)authenticate — this is the only Databricks
setup you should need:

```bash
databricks auth login --host https://dbc-ce570f73-0362.cloud.databricks.com -p dbc-ce570f73-0362
```

That opens a browser SSO flow and writes a token to `~/.databrickscfg`. Re-run the
`current-user me` check to confirm before continuing.

**2. AWS / S3** — to upload the parquet in Step 1.3:

```bash
aws sts get-caller-identity
aws s3 ls s3://openalex-ingest/awards/ | head
```

Expected: an identity, and a listing of existing funder folders. If you get
`Unable to locate credentials` or `AccessDenied`, set up AWS credentials with write
access to the `openalex-ingest` bucket (`aws configure`, or export
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`), then re-check.

> **If both checks pass on the first try, skip the setup commands above — you're ready.**
> Only run `auth login` / `aws configure` if a check fails or a later step throws a
> permissions error.

### 4.5.3 Get the contractor's code and upload the data

```bash
cd ~/work/openalex/openalex-walden
git pull --rebase          # pull the committed script + notebook
```

The S3 upload is **Step 1.3**, deferred until now because it needs credentials. First
check whether the parquet is already in S3 (the contractor may have uploaded before
running out of access):

```bash
aws s3 ls s3://openalex-ingest/awards/{funder}/
```

If it's there with a plausible size, skip straight to Step 5. Otherwise run the upload:

```bash
cd scripts/local
python {funder}_to_s3.py    # see Step 1 for smoke-testing slow scrapers with --limit first
```

Note the row count it reports, then confirm with the `aws s3 ls` above. Then continue
to Step 5.

---

## Step 5: Run the Notebook (via CLI)

Run the per-funder notebook straight from `main` as a one-off serverless job — no GUI,
no workspace import. Because it runs from the committed git branch, you execute
**exactly** what the scheduled bundle will run later.

**⚠️ Run against the full S3 parquet, not a sample.** Three classes of bug from
Step 1.2 and Step 2.3 surface *only* at scale, so a `--limit` smoke run will not catch
them:

- **pyarrow int-inference on null-heavy columns** (Step 1.2, item 5) — dense smoke-test
  rows have no nulls; the full corpus does. Caused failures on IDRC and Rockefeller.
- **`ELEMENT_AT` on empty arrays** (Step 2.3) — small samples often have no empty
  arrays. Caused failure on IDRC's full corpus.
- **Placeholder strings in scraped role/title** — distribution-dependent. Caused two
  consecutive HHMI fixes after the notebook had already shipped.

So make sure Step 1.3 uploaded the **full** parquet (no leftover `--limit` file in S3),
then run the notebook with no limit.

### 5.1 Submit the run

```bash
cd ~/work/openalex/openalex-walden
databricks jobs submit --profile dbc-ce570f73-0362 --json '{
  "run_name": "Create{FunderName}Awards (manual ingest)",
  "git_source": {
    "git_url": "https://github.com/ourresearch/openalex-walden.git",
    "git_provider": "gitHub",
    "git_branch": "main"
  },
  "tasks": [{
    "task_key": "run_notebook",
    "notebook_task": {
      "notebook_path": "notebooks/awards/Create{FunderName}Awards",
      "source": "GIT"
    },
    "environment_key": "Default"
  }],
  "environments": [{
    "environment_key": "Default",
    "spec": {"client": "1"}
  }]
}'
```

`jobs submit` blocks until the run finishes (typically 60–120s on serverless) and prints
a JSON result.

**Pass signal:** `"result_state": "SUCCESS"` and `"life_cycle_state": "TERMINATED"`.
Note the `run_id` in the output — you need it to view cell output.

> `source: GIT` runs what's on `main`, **not** your local working copy. The notebook must
> already be committed and pushed (Step 4). If you fix the notebook, push first, then
> re-submit.

### 5.2 View the notebook's cell output

```bash
python scripts/local/databricks_notebook_output.py <run_id> --profile dbc-ce570f73-0362
```

This renders each cell's result as a formatted table — the same counts you'd see in the
GUI. Skim it for the smoke-check counts the notebook prints (raw row count, transformed
row count, sample rows).

### 5.3 If the run fails

- Read `"state_message"` in the `jobs submit` output and find the failing cell in the
  5.2 dump — it usually names the column and operation.
- Cross-reference the three scale bugs above and the Step 1.2 / Step 2.3 fixes; most
  failures are one of those.
- To debug a single transform interactively, run that cell's SQL against the warehouse
  via the Databricks MCP rather than re-submitting the whole job.
- Fix in the notebook → commit & push → re-submit (5.1). Do **not** edit a workspace
  copy; nothing reads it (the job runs from git).

**→ Update tracker:** When the run succeeds on the full corpus, change status to "Step 6".

---

## Step 6: Verify the Data

Use Databricks MCP to run these verification queries:

### 6.1 Basic counts
```sql
SELECT COUNT(*) as total FROM openalex.awards.{funder}_awards;
```
Should match the row count Step 1.3 reported (the parquet). **On failure** — if it's
well below that count, the transform dropped rows: re-read the 5.2 cell output to see
which cell the count fell off at.

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
Eyeball, specifically: `display_name` is a real grant/award title (not a placeholder,
ID, or NULL); `lead_investigator` has a populated `given_name`/`family_name` (grant:
the PI; prize: the laureate); `funder` struct names the right organization; `amount`
and `currency` look sane together (not pence-as-pounds, not amount in a date column).
**On failure** — a systematically wrong field is a Step 2 mapping bug; fix the notebook
mapping, push, and re-run Step 5.

#### 6.4a PI / display_name frequency check (catches systematic scraper bugs)

100% PI coverage from §6.3 is **not** by itself a pass signal. A scraper that
captures the wrong DOM node on every page still scores 100% — the field is
populated, just wrong on every row. The 10-row sample inspection above will
catch the on-the-nose cases (literal `"Final report"` strings, etc.) but a
subtler bug (e.g. the scraper grabbing a department name instead of a person)
slips through.

Run a frequency check for both the PI and the display name:

```sql
-- PI names
SELECT lead_investigator.given_name AS given,
       lead_investigator.family_name AS family,
       COUNT(*) AS n
FROM openalex.awards.{funder}_awards
GROUP BY 1, 2
ORDER BY n DESC
LIMIT 20;

-- Award/grant titles
SELECT display_name, COUNT(*) AS n
FROM openalex.awards.{funder}_awards
GROUP BY 1
ORDER BY n DESC
LIMIT 20;
```

Top-20 must look like a real long-tail:

- No PI `given`+`family` combo should exceed ~5 rows. PI re-grants exist but
  are rare; a top combo with hundreds of rows is the canonical signature of
  the scraper landing on the same wrong DOM node every time.
- `given` and `family` must never equal a recognized institution name. An
  institution belongs in `lead_investigator.affiliation.name`. If it shows
  up as a person name, the scraper has confused two adjacent fields.
- `display_name` should likewise show distinct titles. Repeated identical
  titles usually mean the scraper is reading a page-template element (a
  section header, a button label) instead of the grant-specific title.

**On failure** — this is almost always a scraper bug, not a notebook
mapping bug. Open an oxjob, NULL the affected field in the notebook (with
the offending field's `affiliation` / structural anchors kept where they're
reliable — HHMI / Helmsley / Mott precedent), ship the funder with the
field NULL, and carry the scraper fix in the oxjob. Restore the field once
the scraper is patched and the rows re-ingest.

**Worked example.** RJ (Riksbankens Jubileumsfond) shipped via the v2 pilot
on 2026-05-27 with 1,211 rows whose `lead_investigator.given_name` was
`"Final"` and `family_name` was `"report"` — the scraper's forward-DOM-walk
landed on a nested `<div class="contentBox finalReport">` header on every
page (see [oxjobs #267](https://oxjobs.org/267)). §6.3 reported 100% PI
coverage. §6.4 sample inspection caught it before the funder went to Step
7. Without §6.4a's frequency check, a subtler version — say, the
grant_administrator string showing up as the family name on 70% of rows —
would have slipped through.

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
Verify reasonable year range.

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

**STOP and re-check your Step 2 amount/currency mapping (and, if the source itself is
missing the values, the Step 1 download) if any of these hold:**
- `pct_amount < 50%` and the funder is known to publish amounts (i.e., not a research prize with implicit standard amounts)
- `distinct_currencies = 0` — currency was never set
- `avg_amount < 1000` or `> 1,000,000,000` — likely wrong unit (pence not pounds) or you mapped a year/count column
- Currencies don't match the funder's country/region (e.g., German funder showing only USD)

Reference baselines: NIH, GTR Project Awards, and ARC all have >95% amount coverage. **Both DataCite Awards and Gateway to Research initially shipped with broken amount/currency — do not skip this check.**

### 6.8 Confirm the rows reached the shared raw table

The per-funder notebook also inserts into `openalex.awards.openalex_awards_raw` — that
insert is what Step 7 reads from, so verify it happened:

```sql
SELECT provenance, priority, COUNT(*) AS n
FROM openalex.awards.openalex_awards_raw
WHERE provenance = '{the provenance string set in the notebook}'
GROUP BY provenance, priority;
```

**Pass if** `n` matches your `{funder}_awards` count and the `priority` is the one the
contractor assigned. **On failure** — if `n` is 0, the notebook's final `DELETE`/`INSERT`
cell didn't run (re-check the 5.2 output); if `n` is doubled, you have a duplicate
provenance/priority from a stale earlier run — the `DELETE … WHERE provenance AND priority`
should have cleared it, so confirm the provenance string matches exactly.

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

## Step 7: Approve and Integrate (RefreshWorkAwards)

The funder's rows are already in `openalex.awards.openalex_awards_raw` (the per-funder
notebook inserted them in Step 5). Folding them into the user-facing combined table is
one job run — but because it touches the shared `openalex_awards` table, it gets an
explicit human go-ahead first.

### 7.1 Get human approval (the one human gate)

**STOP: HUMAN APPROVAL REQUIRED.** Summarize what you verified in Step 6 and what you're
about to do, e.g.:

> {FunderName} is ready: {N} awards in `{funder}_awards`, {pct}% with titles, {pct}%
> with amounts ({currency}). Rows are in `openalex_awards_raw` at priority {P}. I'd like
> to run **RefreshWorkAwards** to fold them into the combined `openalex_awards` table and
> rebuild the work→award links. OK to proceed?

Wait for an explicit yes. The *run* below is automated; the *decision* to merge into the
shared table is the gate.

### 7.2 Trigger RefreshWorkAwards from the CLI

`RefreshWorkAwards` is the bundle job ([jobs/refresh_work_awards.yaml](../../jobs/refresh_work_awards.yaml))
that runs `CreateAwards` (dedup `openalex_awards_raw` by priority → `openalex_awards`,
attach `funded_outputs`) then `WorkAwards` (rebuild `openalex.awards.work_awards`). It's
also scheduled daily at 03:00 UTC, so the funder *would* land tomorrow on its own — but
trigger it now to confirm integration while you're watching.

```bash
cd ~/work/openalex/openalex-walden
databricks bundle run Refresh_Work_Awards --profile dbc-ce570f73-0362
```

`bundle run` blocks and streams task status. **Pass signal:** both `Create_Awards` and
`Work_Awards` tasks end `SUCCESS`.

If `bundle run` can't resolve the resource in your context, run the job by name instead:

```bash
JOB_ID=$(databricks jobs list --profile dbc-ce570f73-0362 --output json \
  | jq -r '.[] | select(.settings.name=="RefreshWorkAwards") | .job_id')
databricks jobs run-now "$JOB_ID" --profile dbc-ce570f73-0362
```

### 7.3 Confirm the funder is in the combined table

```sql
SELECT funder.display_name, COUNT(*) AS awards
FROM openalex.awards.openalex_awards
WHERE funder.display_name = '{funder display_name from Step 0}'
GROUP BY funder.display_name;
```

**Pass if** the count is in the same ballpark as your `{funder}_awards` count from
Step 6. A small drop is normal dedup; a large drop means a priority collision with
another provenance writing the same awards — re-check the `priority` set in the
per-funder notebook.

**→ Update tracker:** Change status to "Step 8" with the integrated count.

---

## Step 8: Wire into the CreateFunderSourcedAwards bundle

After Step 7, the funder is already live in the combined `openalex_awards` table. This
step is about **keeping it that way**: wire the new funder's notebook into the
**CreateFunderSourcedAwards** orchestration job so its `{funder}_awards` table gets
regenerated on every scheduled refresh (otherwise it goes stale as the source publishes
new grants). It uses the same Databricks credentials as Steps 5–7; no new access needed.

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

**→ Update tracker:** Change status to "Step 9 (pending ES sync)" with final grant count
and priority (e.g., `Step 9 | Priority 44 - 1,739 HHMI investigator awards (no amount,
by design). Wired into CreateFunderSourcedAwards 2026-05-14; awaiting ES sync.`). It
becomes **Complete** in Step 9, after it's visible in the API.

---

## Step 9: Make It Live in the API (Sync Awards to Elasticsearch)

The data is in the combined table after Step 7, but it is **not visible in the
api.openalex.org `/awards` endpoint** until it's synced to Elasticsearch. That sync is
the **Sync Awards to Elasticsearch** job ([jobs/sync_awards_to_elasticsearch.yaml](../../jobs/sync_awards_to_elasticsearch.yaml)):
it runs `CreateAwardsAPI` then `notebooks/elastic/sync_awards`, and is scheduled nightly
at 05:00 UTC.

**The sync takes ~20 minutes and re-syncs *all* awards, not just your funder** — so it is
*not* a per-funder step. If you're working a batch of funders in series, do Steps 5–8 for
each one, then run this **once at the end**.

### 9.1 Ask before syncing

Because it's slow and batch-wide, treat it as a session-end decision. Ask the user:

> I've integrated {these N funders} into the awards table; they're at "Step 9 (pending
> ES sync)". Are you done adding funders for this session? If so, want me to run the
> Sync Awards to Elasticsearch job now (~20 min) to make them live in the API?

If they have more funders to do, **leave the sync** — the next funder's Steps 5–8 don't
need it, and the nightly 05:00 UTC run will catch everything if you never trigger it
manually.

### 9.2 Trigger the sync from the CLI

```bash
cd ~/work/openalex/openalex-walden
databricks bundle run Sync_Awards_to_Elasticsearch --profile dbc-ce570f73-0362
```

(An incremental sync — `is_full_sync` defaults to `false` — is correct for newly added
funders.) The run blocks for ~20 min. **Pass signal:** both `create_awards_api` and
`sync_awards_to_elasticsearch` tasks end `SUCCESS`.

### 9.3 Confirm in the API

```bash
curl -s 'https://api.openalex.org/awards?filter=funder.id:F{funder_id}&per_page=1' | jq '.meta.count'
```

**Pass if** the count is non-zero and roughly matches your Step 7.3 count.

**→ Update tracker:** For every funder included in this sync, change status to
**"Complete"** with its final grant count and priority.

---

## Step 10: (Optional) Funder-reported work↔award linkages

**When to do this — the trigger:** while ingesting a funder's awards (Steps 1–2) you notice
the source *also* lists, per grant, the **outputs the funded researchers reported** (DOIs,
URLs, or formatted citations). Funders increasingly publish these because grantees are
required to report outputs. That's a gift: an **authoritative, funder-reported** set of
work↔award edges you can materialize — additive to (and higher-trust than) OpenAlex's
text-mined funding acknowledgements. NWO is the canonical example (oxjobs #244): the NWOpen
API returns a `products` array per project. Do this **after** the award entities exist (the
linkage reuses them — it does **not** create awards).

**The shape:** a standalone job whose notebook resolves each reported product → an OpenAlex
work, writes a junction `openalex.awards.{funder}_work_funders (work_id, funder_id,
award_ids[])`, and a leg in `WorkAwards.ipynb` joins that junction to the **existing** award
entities. Mirror `crossref_work_funders` / `nwo_work_funders`. **This is a linkage job, not an
award-entity job — keep it out of `CreateFunderSourcedAwards`** (that's award entities); it
lives in its own job like `Crossref Work Funders` does.

### 10.1 Confirm the products are captured
The per-grant outputs should already be in the raw parquet from Step 1 (e.g. NWO keeps the
whole array as a `products_json` string on `{funder}_projects_raw`). If your Step-1 script
dropped them, add them — don't re-fetch from the funder API in this job.

### 10.2 Build `Create{Funder}WorkAwards` (canonical: `CreateNWOWorkAwards.ipynb`)
Writes the junction. Two precision-ordered work-resolution paths (a third optional):
- **Path 1 — DOI.** Reported identifiers are usually dirty (whitespace inside the DOI, leading
  junk). Salvage with `regexp_extract`/`regexp_replace`, rebuild the canonical
  `https://doi.org/…` form, join `openalex.works.openalex_works.doi`. High yield, ~96% match.
- **Path 2 — URL → unique work location.** For non-DOI URLs, match against
  `openalex.works.locations.urls[].url` and accept **only when the URL maps to exactly one
  work** (`best_doi`). High precision, low recall.
- **Path 3 (optional) — citation match.** Title + year, disambiguated by a funder-country
  author (`exists(authorships, a -> array_contains(a.countries, '<CC>'))`, e.g. `'NL'` for
  NWO). Precision-risky; build + measure in the notebook (title joins time out interactively).

Resolve the **award** by the funder's project/grant id → `(funder_id, funder_award_id)` on the
already-ingested entity. Output one row per `(work_id, funder_id)` with `ARRAY_DISTINCT` of
award ids. ⚠️ **Two SQL gotchas that bit #244:**
- **Double-escape regex backslashes** (`'\\s'`, `'\\.'`). The job cluster runs legacy
  `escapedStringLiterals` mode — single-escape (`'\s'`) silently collapses (`'\s'`→`'s'`) and
  *shrinks the result with no error* (warehouse runs the opposite mode, so it masks the bug).
- **No `JOIN` directly after `LATERAL VIEW`** — Spark `PARSE_SYNTAX_ERROR`. Explode in a
  subquery, then join.

### 10.3 Add a leg to `WorkAwards.ipynb`
Mirror the `nwo_work_funder_awards` CTE: explode the junction's `award_ids`, join
`openalex.awards.openalex_awards` on `(funder_id, funder_award_id)`, emit `(work_id, award)`,
and UNION it into `combined_awards` at **priority 3** (funder-reported — same tier as GTR/NWO,
beats crossref text-mined p4). Confirm the funder's awards are present in `openalex_awards`
with matching keys before trusting the join.

### 10.4 Standalone DAB job
Create `jobs/{funder}_work_funders.yaml` mirroring [jobs/nwo_work_funders.yaml](../../jobs/nwo_work_funders.yaml)
(`source: GIT`, `notebook_task`, `warehouse_id`) and add it to the `include:` list in
`databricks.yml`. If the funder's source is infrequently re-ingested, set
`schedule.pause_status: PAUSED` and run it **on-demand** (don't burn a daily rebuild on static
data). **Deploy ordering:** the junction table must exist before the `WorkAwards` leg
references it — push `Create{Funder}WorkAwards` + run it first, *then* push the `WorkAwards`
change, or the next `RefreshWorkAwards` run fails table-not-found.

### 10.5 Run, verify, make live
```bash
databricks bundle run {Funder}_Work_Funders --profile dbc-ce570f73-0362   # builds the junction
databricks bundle run RefreshWorkAwards    --profile dbc-ce570f73-0362   # folds it into work_awards
```
**Pass signal:** junction rows > 0 with unique `(work_id, funder_id)` keys; and
```sql
SELECT COUNT(*) FROM openalex.awards.work_awards
WHERE award.funder_id = 'https://openalex.org/F{funder_id}';
```
is non-zero and ≥ the pre-existing text-mined count for that funder.

⚠️ **Surfacing on the `/works` API needs the e2e run, not the awards sync.** `work_awards` and
the `/awards` index (Step 9) do **not** update the work records — `/works` serves from
`openalex_works.awards`, which is rebuilt from `work_awards` only by **`CreateWorksEnriched`**
(in `walden_end2end`), then works→ES. Until that runs, the works are *stale* and the new grants
won't appear on `/works` (they're correct in `work_awards`). Recheck a known net-new work after
the next e2e.

**Coverage is never 100% — don't gate on a round number.** Not every reported product carries a
resolvable DOI/URL (NWO landed ~76% of projects on a stale capture). Report the coverage you
got and move on; a re-ingest of fresher funder data is usually a better lever than fuzzy matching.

**→ Tracker:** note the funder has funder-reported linkages (junction + WorkAwards leg + job),
distinct from its award-entity "Complete" status.

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
