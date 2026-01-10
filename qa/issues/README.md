# QA Issues

This directory tracks quality issues discovered in OpenAlex data pipelines.

## Directory Structure

```
issues/
├── README.md       # You are here
├── TEMPLATE.md     # Copy this to create new issues
├── open/           # Active issues being worked on
└── closed/         # Resolved issues (kept for reference)
```

## Creating a New Issue

```bash
# 1. Create issue directory
mkdir -p qa/issues/open/my-issue-name-YYYY-MM/

# 2. Copy template
cp qa/issues/TEMPLATE.md qa/issues/open/my-issue-name-YYYY-MM/README.md

# 3. Create supporting files
touch qa/issues/open/my-issue-name-YYYY-MM/{INVESTIGATION.md,PLAN.md,ACCEPTANCE.md}
mkdir -p qa/issues/open/my-issue-name-YYYY-MM/{evidence,fix}

# 4. Fill in the README with issue summary
```

## Issue Lifecycle

```
┌──────────┐     ┌───────────────┐     ┌────────┐     ┌───────────┐     ┌────────┐
│   open   │ ──→ │ investigating │ ──→ │ fixing │ ──→ │ verifying │ ──→ │ closed │
└──────────┘     └───────────────┘     └────────┘     └───────────┘     └────────┘
```

| Status | What's happening | Next step |
|--------|------------------|-----------|
| `open` | Issue documented | Investigate root cause |
| `investigating` | Analyzing the problem | Write fix plan |
| `fixing` | Implementing the fix | Deploy and verify |
| `verifying` | Running acceptance tests | Close if all pass |
| `closed` | All tests pass | Move to `closed/` directory |

## Required Files

Every issue MUST have:

| File | Purpose |
|------|---------|
| `README.md` | Issue summary (from template) |
| `INVESTIGATION.md` | Root cause analysis |
| `PLAN.md` | Fix approach and implementation |
| `ACCEPTANCE.md` | **Critical**: Exact tests to verify fix |

Optional but recommended:

| File/Dir | Purpose |
|----------|---------|
| `evidence/` | SQL queries, screenshots, data files |
| `fix/` | Implementation (notebooks, scripts) |

## Closing an Issue

1. Ensure ALL acceptance criteria pass
2. Update `ACCEPTANCE.md` with actual results
3. Move the entire directory from `open/` to `closed/`

```bash
mv qa/issues/open/my-issue-name-YYYY-MM/ qa/issues/closed/
```

## Naming Convention

Use descriptive names with date suffix:

```
landing-page-regression-2026-01/    # Good
parser-fix-2025-12/                 # Good
issue1/                             # Bad - not descriptive
2026-01-15-fix/                     # Bad - date first is less scannable
```
