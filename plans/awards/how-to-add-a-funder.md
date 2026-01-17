# How to Add a Funder

**Location:** `plans/awards/how-to-add-a-funder.md`
**Purpose:** Instructions for Claude instances to add awards from a new funder to OpenAlex.
**Human involvement:** Minimal - just running the Databricks notebook and final approval.
**Parallelization:** Multiple Claude instances can work on different funders simultaneously.
**Tracker:** `plans/awards/funder-ingestion-tracker.md`

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

If the funder doesn't exist in OpenAlex, STOP and tell the user.

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

### 1.3 Run the script

```bash
cd openalex-walden/scripts/local
python {funder_name}_to_s3.py
```

Verify upload succeeded and note the row count.

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
provenance            -- Source identifier, e.g., "nih_exporter", "nwopen"
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

4. **Step 2: Transform to award schema** (see Step 5 for details)
   - Map native fields to OpenAlex schema
   - Handle date parsing (try multiple formats)
   - Map funding types appropriately
   - Generate unique ID as `abs(xxhash64(CONCAT({funder_id}, ':', {lowercase_native_id}))) % 9000000000`

5. **Step 3: Delete old data and insert into openalex_awards_raw with priority**
   - Determine priority (lower = higher priority):
     - 0: GTR Project Awards (authoritative UK grants)
     - 1: Crossref Awards
     - 2: Backfill Awards
     - 3: NIH, NSF, GTR Awards (legacy)
     - 4+: New funders (use next available number in notebooks/awards/CreateAwards.ipynb)
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

6. **Verification queries**
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

7. **Verification queries** (see Step 6 for details)

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
```

**NEVER use:**
- `CAST()` on external data - use `TRY_CAST()` instead
- `TO_DATE()` on external data - use `TRY_TO_DATE()` instead
- Assumed column names without verifying via `DESCRIBE`

### 2.4 Common field mappings

| OpenAlex field | Common source fields |
|----------------|---------------------|
| display_name | title, project_title, name |
| description | abstract, summary, description |
| funder_award_id | grant_id, project_id, award_number, grant_reference |
| amount | total_cost, amount, funding_amount, grant_offer |
| funding_type | Map from activity codes or categories |
| funder_scheme | program_name, funding_scheme, activity_code |


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

**→ Update tracker:** Change status to "Step 5" with notes (e.g., "Code committed, waiting for human to run notebook").

---

## Step 5: Human Runs Notebook

**STOP: HUMAN ACTION REQUIRED**

Tell the user:
> The notebook is ready at `notebooks/awards/Create{FunderName}Awards.ipynb`.
> Please run it in Databricks and let me know when it completes.

Wait for confirmation before proceeding.

**→ Update tracker:** When user confirms notebook ran, change status to "Step 6".

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
Verify reasonable year range.

### Success criteria:
- Row count matches expected
- >90% have display_name
- >50% have start_date (varies by funder)
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

## Step 8: Add to CreateWorkAwards Job

After the notebook has been verified and CreateAwards.ipynb has been run successfully, add the new funder notebook as a task in the CreateWorkAwards Databricks job.

### 8.1 Update the job JSON file

Edit `jobs/create_work_awards.json` to add the new funder:

1. **Add a new task** for the funder notebook in the `settings.tasks` array. Example for a new funder "XYZ":
   ```json
   {
     "email_notifications": {},
     "environment_key": "Default",
     "notebook_task": {
       "notebook_path": "/Workspace/Shared/openalex-walden/notebooks/awards/CreateXYZAwards",
       "source": "WORKSPACE"
     },
     "run_if": "ALL_SUCCESS",
     "task_key": "XYZ_Awards",
     "timeout_seconds": 0,
     "webhook_notifications": {}
   }
   ```

2. **Add the task_key to Create_Awards dependencies** in the `depends_on` array for the `Create_Awards` task:
   ```json
   {
     "task_key": "XYZ_Awards"
   }
   ```

### 8.2 Deploy the job update

Use the Databricks CLI to update the job:

```bash
databricks jobs reset 864794621551148 --profile dbc-ce570f73-0362 --json @jobs/create_work_awards.json
```

Verify the task was added:
```bash
databricks jobs get 864794621551148 --profile dbc-ce570f73-0362 --output json | grep -i "{funder}"
```

### 8.3 Commit the job JSON

```bash
git add jobs/create_work_awards.json
git commit -m "Add {FunderName} to CreateWorkAwards job

- Added {FunderName}_Awards task
- Added dependency in Create_Awards task

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
git pull --rebase && git push
```

**→ Update tracker:** Change status to "Complete" with final grant count (e.g., "Complete | Priority 18 - 450,000 grants | Added to job 864794621551148").

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
