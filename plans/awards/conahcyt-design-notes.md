# CONAHCYT / CONACYT Awards Ingest Design Notes

Status: paused before implementation.

Branch: `codex/add-conahcyt-awards`
Initial tracker lock commit: `61bed11`
Date paused: 2026-05-20

This branch intentionally does not add a download script, Databricks notebook,
or `CreateAwards.ipynb` priority entry yet. The source is official and
promising, but the award model needs to be resolved before implementation.

## Confirmed OpenAlex Mapping

- OpenAlex funder: `F4320321739`
- Display name: `Consejo Nacional de Ciencia y Tecnologia`
- DOI: `10.13039/501100003141`
- ROR: `https://ror.org/059ex5q34`

Use this funder for the CONACYT / CONAHCYT / SECIHTI rename chain. Do not
invent a new SECIHTI or CONAHCYT funder row unless OpenAlex adds one later.

## Confirmed Source Shape

- CKAN host: `https://www.datos.gob.mx`
- Organization slug: `conahcyt`
- Direct file host: `https://repodatos.atdt.gob.mx`
- Official source package of interest:
  `programas_presupuestarios_conahcyt`

The package contains many CSV resources. The most visible high-volume data is
the S191 / SNII researcher program, published as annual CSV snapshots. Other
datasets under the same CKAN organization are not necessarily awards:

- `RENIECYT`: institutional registry, not awards.
- `Evaluadores`: peer reviewers, not awards.
- `Indicadores`: aggregate statistics, not awards.
- `Fondos Institucionales Mixtos y Sectoriales`: only a small/anemic resource
  set was observed; it may be aggregate or partial.

## Core Design Blocker

Do not treat annual SNII snapshot rows as award rows without an explicit design
decision.

SNII data appears to publish researcher-year status snapshots. A researcher may
appear in 2022, 2023, and 2024 for one continuing appointment or renewal cycle.
Publishing one award per researcher-year would be simple, but it may inflate the
corpus and misrepresent the award concept.

Preferred design to evaluate:

- One OpenAlex award per `(researcher, SNII level, contiguous appointment
  interval)`.
- Collapse contiguous annual rows into a start/end date interval.
- Use the source's stable researcher/person identifier if present; otherwise
  use a documented fallback based on normalized name + institution + level.

Fallback design only if explicitly approved:

- One award per `(researcher, year)` as an annual SNII disbursement/status row.
- The notebook description and tracker must say this is an annual snapshot /
  annual disbursement representation, not a full appointment-cycle model.

## Fields To Probe Before Coding

Before creating `scripts/local/conahcyt_to_s3.py`, inspect one recent S191 CSV
and confirm:

- Does it expose an SNII level/category column (`Nivel I`, `Nivel II`,
  `Nivel III`, `Emerito`, etc.)?
- Does it expose institution/affiliation?
- Does it expose a stable researcher/person identifier?
- Does it expose per-row `monto`, `importe`, or another amount field?
- Does it expose appointment start/end dates, validity years, or only snapshot
  year?
- Are there S-codes other than S191 in the same package that are award-like and
  should be included or explicitly deferred?

Concrete source discovery command:

```bash
curl -sL "https://www.datos.gob.mx/api/3/action/package_show?id=programas_presupuestarios_conahcyt" \
  | jq '.result.resources[] | select(.format=="CSV") | {name, url}'
```

## Amount Decision

If the CSV has per-row amount fields, use those after sanity-checking the
distribution.

If the CSV has SNII level but no per-row amount:

- Either set amount/currency to NULL and document a Step 6.7 waiver, following
  researcher-level precedents like HHMI.
- Or apply official monthly SNII rates by level and duration, but only if the
  rate source is official and the appointment-cycle/duration model is clear.

Do not silently fabricate amounts from unofficial summaries.

## Mapping Notes

- `funder_id`: `4320321739`
- `currency`: likely `MXN` if amount is used.
- `funder_scheme`: likely `SNII <level>` if the source has level; otherwise
  use `S191 / Sistema Nacional de Investigadoras e Investigadores`.
- `lead_investigator`: the researcher.
- `lead_investigator.affiliation.name`: institution column if present.
- `lead_investigator.affiliation.country`: `MX` when affiliation is Mexican or
  the source does not expose country-level variation.
- `provenance`: use a unique source string such as `conahcyt_snii`, not a bare
  CKAN/OpenData name.

## Priority

Do not claim a final priority until implementation resumes. At pause time,
Claude's review expected the next contiguous slot to be around `83`, but this
must be rechecked against all open/in-flight award PRs before editing
`CreateAwards.ipynb`.

## Do Not Do

- Do not include RENIECYT, Evaluadores, or Indicadores.
- Do not scope-creep into Becas/scholarships without explicit approval; that is
  a separate corpus and source path.
- Do not model fiscal/budget tranche rows as awards unless the PR explicitly
  documents that choice.
- Do not add job YAML. Contractor lane has no AWS/S3 or Databricks access; admin
  must upload, run, QA, and then wire the scheduled job later.
