Owner Rohan. Withdrawal of funders with no ROR id, no award, no work link in any source, no merge alias and no `mid.entity_link` role (batch `dd1016-b3`). Funder DOIs are NOT removed. Ships as its own push after the
Thursday 10-15 registry switch: Fri 10-16 daytime if fold-ship OKs, else Mon 10-19. No production action is authorized by these files.

Decisions: Kyle (1:1 10-08): "if they don't have any publications and they don't have any grants linked to them, let's get rid of them".
Round 4 (10-15) applies that literally: live, no ROR id, no award, and no work link (on the funder or an alias merged into it) in any
source table that feeds works' funders. Round 5 (Codex r4) also keeps every merge target (catalogue `merge_into_id` or registry
`merged_into_id` edge: its aliases' merge-funders redirects must not point at a deleted funder) and every funder named on either side of
`mid.entity_link`: 2,191 live funders, 0 aliases. The 10-14 rule (no ROR, no award: 6,593) is superseded. Standup 10-15: funder DOIs
stay in public output (`ids.doi`, `ids.crossref`, award `funder.doi`), demoted to a historical artifact: a side identifier and backup
crosswalk key, not the primary key (that is the OpenAlex id, with ROR as the preferred external id). Internal FundRef matching
(`openalex.funders.funders.doi/crossref_id`, `funder_id` fundref keys) is unchanged.

Code (one commit; pause-safe push):
- `CreateFundersAPI` (and the registry's dev API mirror): comment only. `ids.doi` / `ids.crossref` are built as today and marked as a
  historical side identifier / backup crosswalk key, not the primary key. Nightly Grants (`build_awards.py`) is not changed.
- `CreateWorksEnriched` funders cell: no leg may serve a funder in `openalex.funders.deleted_funders`; a work whose only funders were withdrawn
  gets its array stripped. With the round-4 rule no served work carries a dropped id (0 pairs), so this is a guard only.
- `sync_funders`: ledger-listed withdrawals do not count toward the 2,000 stale-delete cap (2,191 deletes > 2,000, still needed); anything else still does. Built on the
  merge-redirect cell (abbbb1eb): the withdrawn funders are not merge losers, so they get no redirect and their docs delete.
- `CreateEuropePmcWorkFunders`: dd1016-b3 names still count toward the matcher's ambiguity (round 4 re-measure on the 2,193 set:
  without it 118 agency strings / up to 269 mentions resolve to a different live funder; with it, 0).
- Works' funder objects have no DOI field; `openalex.funders.funders` (the served view, internal) keeps `doi`/`crossref_id`/`uri` because
  CreateCrossrefAwards / CreateDataCiteAwards match FundRef DOIs through it and the readers sign the catalogue with them.

Data (`FORWARD.sql`, after the code push has deployed, before 18:00 PT on the same day; readers start 19:00 PT):
F01 stages the drop set by rule (no ROR, no award, no link in mid.work_funder, works.fulltext_work_funders, the nine awards.*_work_funders
tables incl. crossref_no_doi_main_cut, datacite_work_funder_evidence, gtr_awards, work_awards, served works.openalex_works.funders
or mid.entity_link; not a merge target) and
refuses any drift from the reviewed 2,191 live funders (sha ca97a588...) and 0 merge aliases pointing at them. F02 before images. F03 ledger rows in `deleted_funders` (the empty-funder batch pattern). F04 registry `withdraw`
(the edits.csv statement). F05 the switch's compat republish, unchanged. F06 MERGE ... DELETE of the dropped ids' rows in the 13 link tables
(a guard: by the rule it removes 0 work-funder rows; funder_match_policy loses the dropped funders' 300 policy rows). F07/F08 the lookup keeps every
row except those targeting dropped funders and is re-signed for the new catalogue; the readers' own gate is checked.
`ROLLBACK.sql` is row-scoped: it reverses only FORWARD's rows for batch dd1016-b3 (the five registry columns of rows still in
FORWARD's state, their catalogue rows, the removed link rows, the removed lookup rows, the ledger rows) and re-signs the lookup for
the catalogue as it is then. Every gate also checks that ROLLBACK changed nothing else, so it holds after any number of Funders
Daily / reader / Nightly runs (dev: forward, a Funders Daily that minted 235 funders and reader replays, then rollback: all gates
pass, only the backed-up rows come back). It stops if anything else has since edited a drop-set registry row.
A re-run of FORWARD after ROLLBACK needs the `openalex.funders.dd1016_*` stage tables dropped first.
Dev wall clock: FORWARD about 4 min, ROLLBACK about 2 min (serverless).

Order of operations:
1. Push morning: `PRECHECK.sql` (read only). Push only if it prints 2191/true/0/true; otherwise re-cut the hashes first.
2. `push-walden.sh push TREE` (08:00-16:30 PT). Run FORWARD only after it printed the deploy run as `completed/success`.
3. `FORWARD.sql`, starting 08:00-16:30 PT; it stops itself after 17:30 PT before F05 (nothing public has changed by then).
4. To undo: `ROLLBACK.sql` first (data; `run-dd1016.py rollback` may start until 18:45 PT and is interrupted at 18:55, before
   the 19:00 readers; a ROLLBACK cut off there is idempotent and is re-run at the next window), then `push-walden.sh prepare inverse` / `push TREE inverse` (code). Never the other way
   round: the inverse code alone leaves the funders withdrawn, with the ES cap no longer exempting them.

Consequences to expect: no funder or award document changes apart from the drop (no DOI change, no award re-send); 0 works' funders change (no dropped
funder has a work link); ES funders deletes 2,191 docs; no merge-funders redirect is affected. Future ROR records whose FundRef
belongs to a withdrawn funder are held by the registry as NO_ATTACH_FUNDREF_OF_WITHDRAWN (existing rule for hand-deleted funders).
