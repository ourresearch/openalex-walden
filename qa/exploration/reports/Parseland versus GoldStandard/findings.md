# Should You Invest in Improving Parseland?

## The Bottom Line

Where does each record's data come from? (n=6,088, mutually exclusive categories)

| Category | Affiliations | Corresponding Authors |
|----------|-------------|----------------------|
| **OAX has it now** | 60.8% | 44.0% |
| + Pipeline fix | 2.3% | 4.5% |
| + Perfect Parseland | 3.6% | 12.1% |
| *Unavailable anywhere* | 33.3% | 39.3% |
| **TOTAL** | 100% | 100% |

**Key insights:**
- **Corresponding authors have the most room to grow**: 12.1% of records could be captured with better Parseland parsers
- **Pipeline fix is a quick win**: 2.3% + 4.5% = 6.8% of records are already parsed but not flowing through
- **~33-39% of records simply don't have this data anywhere**—not on landing pages, not in Crossref, not in PDFs

---

## Coverage by Year

### Affiliations (% of all records, mutually exclusive)

| Year | n | OAX Has | +Pipeline | +Parseland | Unavail |
|------|---|---------|-----------|------------|---------|
| ≤1999 | 1477 | 50.3% | 1.5% | 1.9% | 46.3% |
| 2000-05 | 526 | 65.8% | 1.3% | 1.9% | 31.0% |
| 2006-08 | 356 | 65.2% | 1.4% | 2.2% | 31.2% |
| 2009-11 | 420 | 63.1% | 2.1% | 1.7% | 33.1% |
| 2012-14 | 497 | 66.8% | 1.6% | 2.6% | 29.0% |
| 2015-17 | 632 | 62.7% | 0.8% | 5.4% | 31.2% |
| 2018-20 | 807 | 66.0% | 0.6% | 4.8% | 28.5% |
| 2021-23 | 908 | 68.2% | 1.5% | 3.6% | 26.7% |
| **2024-26** | 465 | **51.0%** | **13.8%** | **9.7%** | 25.6% |

**Notable:** 2024-26 stands out dramatically. Only 51% of records have affiliations in OAX right now, but **13.8% could be captured just by fixing the pipeline**—Parseland already has this data. Another 9.7% needs parser improvements. Recent works are being parsed but the data isn't flowing through.

### Corresponding Authors (% of all records, mutually exclusive)

| Year | n | OAX Has | +Pipeline | +Parseland | Unavail |
|------|---|---------|-----------|------------|---------|
| ≤1999 | 1477 | 43.7% | 3.6% | 5.2% | 47.5% |
| 2000-05 | 526 | 37.5% | 9.3% | 12.2% | 41.1% |
| 2006-08 | 356 | 37.4% | 11.8% | 11.8% | 39.0% |
| 2009-11 | 420 | 36.4% | 8.3% | 16.9% | 38.3% |
| 2012-14 | 497 | 36.0% | 10.3% | 17.3% | 36.4% |
| 2015-17 | 632 | 52.1% | 2.7% | 12.7% | 32.6% |
| 2018-20 | 807 | 52.3% | 0.9% | 12.5% | 34.3% |
| 2021-23 | 908 | 48.8% | 0.4% | 13.1% | 37.7% |
| 2024-26 | 465 | 38.3% | 3.7% | **21.3%** | 36.8% |

**Notable patterns:**
- **2000-2014 has big pipeline opportunities** (8-12% each year)—Parseland extracted corresponding authors but they're not in OAX
- **Parser improvements help most for 2009-2014 and 2024-26** (17-21% potential)
- **2015-2023 is relatively well-covered** (~50% OAX coverage) with modest improvement potential

---

## Recommendations

### 1. Fix the Pipeline First (Quick Win)

| Metric | % of Records |
|--------|-------------|
| Affiliations | 2.3% |
| Corresponding Authors | 4.5% |

Parseland already has this data. Investigate why it's not flowing to OAX. For 2024-26 affiliations specifically, this is **13.8%** of records—a huge win.

### 2. Invest in Corresponding Author Parsers

12.1% of all records could gain corresponding author data with better parsing. For 2024-26, this jumps to **21.3%**—Parseland isn't extracting corresponding authors from recent landing pages.

### 3. Affiliation Parsers: Lower Priority

Only 3.6% of records would benefit. OAX already has affiliations for 61% of records.

### 4. Accept the Data Availability Ceiling

~33% of affiliations and ~39% of corresponding author designations simply don't exist anywhere—not on landing pages, not in Crossref, not in PDFs. This is a publisher/author data availability issue.

---

## Methodology

- **Sample:** 6,088 records with DOIs where OpenAI extraction succeeded
- **Gold standard:** OpenAI extraction from HTML landing pages
- **Features:** Binary (has any affiliation? has any corresponding author?)
- **Categories are mutually exclusive:**
  - OAX has it: OpenAlex=Yes
  - +Pipeline: OpenAlex=No, Gold=Yes, Parseland=Yes
  - +Parseland: OpenAlex=No, Gold=Yes, Parseland=No
  - Unavailable: OpenAlex=No, Gold=No

- **Caveat on "Unavailable" category:** 32-64 gold standard records returned no JSON at all (likely extraction failures), and another ~750-800 returned valid JSON but with an empty authors list (possible extraction failures). In the worst case, this could mean we're understating the potential gains from parser improvements by up to ~13-14 percentage points—though the true impact is likely much smaller. Todo: check these "empty authors list" records to see if they're really empty or if they're just extraction failures.
