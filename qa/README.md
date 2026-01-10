# OpenAlex QA System

This directory contains the quality assurance infrastructure for OpenAlex data pipelines.

## For AI Agents

**Start here.** Read `AGENTS.md` for essential context on how OpenAlex data flows work and common pitfalls.

## Quick Links

| What you need | Where to find it |
|---------------|------------------|
| **Active issues** | `issues/open/` |
| **Issue template** | `issues/TEMPLATE.md` |
| **Evaluation datasets** | `exploration/datasets/` |
| **Data collection scripts** | `exploration/scripts/` |
| **Analysis reports** | `exploration/reports/` |

## QA Workflow

```
1. EXPLORE  →  Run scripts in exploration/ to identify problems
                ↓
2. DOCUMENT →  Create issue in issues/open/ using TEMPLATE.md
                ↓
3. INVESTIGATE → Fill in INVESTIGATION.md with root cause
                ↓
4. PLAN     →  Write fix approach in PLAN.md
                ↓
5. DEFINE   →  Write acceptance criteria in ACCEPTANCE.md
                ↓
6. FIX      →  Implement fix in fix/ subdirectory
                ↓
7. VERIFY   →  Run acceptance tests, document results
                ↓
8. CLOSE    →  Move to issues/closed/ when all tests pass
```

## Directory Structure

```
qa/
├── README.md                    # You are here
├── AGENTS.md                    # Context for AI agents
│
├── exploration/                 # Data collection & analysis
│   ├── datasets/               # Reusable evaluation datasets
│   │   ├── 10k-random/        # 10k random OpenAlex works
│   │   └── nees/              # Known problem DOIs
│   ├── scripts/               # Data collection tools
│   └── reports/               # Analysis reports
│
├── issues/                      # Issue tracking
│   ├── TEMPLATE.md            # Standard issue format
│   ├── open/                  # Active issues
│   └── closed/                # Resolved issues (for reference)
│
└── metrics/                     # Quality metrics over time
    └── dashboards/            # Generated reports
```

## Key Concepts

### Acceptance Criteria (ACCEPTANCE.md)

Every issue MUST have an `ACCEPTANCE.md` file with:
- Exact SQL queries or bash commands to verify the fix
- Specific numeric thresholds or conditions for pass/fail
- A verification log showing when tests were run and by whom

This is the most important file for agents - it provides unambiguous, runnable tests.

### Issue Lifecycle

| Status | Meaning |
|--------|---------|
| `open` | Issue documented, not yet investigated |
| `investigating` | Root cause analysis in progress |
| `fixing` | Fix being implemented |
| `verifying` | Fix deployed, running acceptance tests |
| `closed` | All acceptance tests pass |

## Getting Started

### For a new investigation

```bash
# 1. Run exploration scripts to collect data
python qa/exploration/scripts/compare_coverage.py

# 2. If you find an issue, create from template
cp qa/issues/TEMPLATE.md qa/issues/open/my-issue-name/README.md

# 3. Fill in the template and add supporting files
```

### For an existing issue

```bash
# 1. Check open issues
ls qa/issues/open/

# 2. Read the issue README
cat qa/issues/open/issue-name/README.md

# 3. Check what's been done
cat qa/issues/open/issue-name/INVESTIGATION.md
cat qa/issues/open/issue-name/ACCEPTANCE.md
```
