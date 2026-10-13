"""NIH_PROJECT_YEAR_FOLD: fiscal-year / type-prefixed / spelling records of one NIH project retire into one project record.

Kyle 09-24: one grant per NIH project (activity + institute + serial, e.g. R01HL123456); the number shown is the form most papers
cite. Funder ruling (a), 09-28: the project keeps its institute funder; retirements may cross funders inside the NIH family.
The pairs come from the frozen, reviewed retirement list (extra input `migration_manifest`, one row per loser; 6f's merge step).
Everything that is a judgement lives in that list's evidence_json and is re-checked, never re-decided, at night:
  {"core": "R01HL123456", "primary_award_id": "R01HL123456", "title_from": "5R01HL123456-07", "project_funder_id": <institute funder id, in NIH_FAMILY>}
This module adds what the class needs on top of the generic merge step:
  - survivor funder pinned to the listed project_funder_id (the institute), else to its current funder;
  - number normalization for a survivor's records looked up under each record's own funder, not the pinned one;
  - a per-night proof (same parsed project number, both ends in the NIH family, survivor is a non-supplement year record of the
    project or, with none, its bare number);
  - the project projection: shown number, title/people from one chosen year, dates spanning all years, amount per switch.
When no NIH pair is in the list every function is a no-op and the build renders byte-identically.
"""
import json

from stable_award_ids import literal

NIH_CLASS = "NIH_PROJECT_YEAR_FOLD"
NIH_FAMILY = (4320332161, 4320337611, 4320337356, 4320337376, 4320337472, 4320337358, 4320337593, 4320337375, 4320337355, 4320337362,
              4320337363, 4320337364, 4320337365, 4320337357, 4320337361, 4320337354, 4320337346, 4320337359, 4320337353, 4320337337,
              4320337330, 4320337352, 4320337347, 4320337534, 4320337372, 4320337351, 4320337374, 4320337350, 4320337338, 4320337348)
FAMILY_SQL = "(" + ",".join(str(f) for f in NIH_FAMILY) + ")"
# Project number parse, identical to the list builder (nih-port/build_sql.py, from the NIH lane's c02/c05): unicode dashes -> '-',
# whitespace dropped, upper-cased; strict form = optional type digit + activity(3) + institute(2) + serial(6), then '-' or end;
# otherwise the letters+digits-only form with the serial left-padded to 6 (spelling variants such as 'U01 DK63790').
NORM_SQL = "upper(regexp_replace(regexp_replace(trim({x}),'[\\\\x{{2010}}-\\\\x{{2015}}]','-'),'\\\\s',''))"
STRICT_SQL = "regexp_extract({n},'^[1-9]?([A-Z][A-Z0-9]{{2}}[A-Z]{{2}}[0-9]{{6}})($|-)',1)"
LOOSE_SQL = ("CASE WHEN regexp_replace({n},'[^A-Z0-9]','') RLIKE '^[1-9]?[A-Z][A-Z0-9]{{2}}[A-Z]{{2}}[0-9]{{1,6}}$' THEN concat("
             "regexp_extract(regexp_replace({n},'[^A-Z0-9]',''),'^[1-9]?([A-Z][A-Z0-9]{{2}}[A-Z]{{2}})[0-9]+$',1),"
             "lpad(regexp_extract(regexp_replace({n},'[^A-Z0-9]',''),'([0-9]+)$',1),6,'0')) END")
AMOUNT_POLICIES = ("null", "sum", "elected")        # Q2; default null until Kyle rules
TITLE_POLICIES = ("chosen_year", "elected")         # Q5; default chosen_year (latest non-subproject, non-supplement year)


def norm(expr):
    return NORM_SQL.format(x=expr)


def core(expr):
    """Parsed project number of a grant number ('' when it does not parse)."""
    n = norm(expr)
    return f"coalesce(nullif({STRICT_SQL.format(n=n)},''),{LOOSE_SQL.format(n=n)},'')"


def multi(expr):
    """True when the text carries more than one project number (e.g. 'ZIAMH002786-15, ZIAMH002778-17'): never one project."""
    return (f"(size(regexp_extract_all(upper(regexp_replace({expr},'[^A-Za-z0-9]','')),"
            f"'([A-Z][0-9A-Z]{{2}}[A-Z]{{2}}[0-9]{{4,6}})',1))>1)")


def bare(expr):
    """True when the number is the project number itself: no fiscal-year suffix and no leading type digit."""
    n = norm(expr)
    return f"(NOT {n} RLIKE '[0-9]{{6}}-[0-9]{{2}}' AND NOT regexp_replace({n},'[^A-Z0-9]','') RLIKE '^[1-9]')"


def year_record(expr):
    """True when the number is one fiscal year of the project and not a supplement: [type digit]+project number+'-'+2-digit budget
    year, optionally amendments (A1...), never an S suffix (Rohan 10-07: the newest non-supplement year record survives)."""
    n = norm(expr)
    return f"(regexp_replace({n},'[^A-Z0-9-]','') RLIKE '^[1-9]?[A-Z][A-Z0-9]{{2}}[A-Z]{{2}}[0-9]{{6}}-[0-9]{{2}}(A[0-9]+)*$')"


def settings(c):
    s = {"amount": "null", "title": "chosen_year", **(c.config.get("nih_fold") or {})}
    c.require(s["amount"] in AMOUNT_POLICIES and s["title"] in TITLE_POLICIES, "NIH_FOLD_SETTINGS: " + json.dumps(s))
    return s


def active(c):
    if not c.has_extra("migration_manifest"):
        return False
    return bool(c.sql(f"SELECT 1 FROM migration_manifest_v WHERE evidence_class={literal(NIH_CLASS)} LIMIT 1").collect())


# ---------- plan(): ownership ----------
def pin_ownership(c):
    """Right after ownership_plan. Survivors (targets of the whole NIH list, applied or pending) take the funder the list gives them
    (evidence_json project_funder_id: the institute of the project's code, Rohan 10-07 option B) or, when it gives none, keep their current funder."""
    if not active(c):
        c.nih = False
        return
    c.nih = True; r = c.r; cls = literal(NIH_CLASS)
    c.artifact("nih_survivors", f"""SELECT DISTINCT m.target_id stable_id,
        coalesce(CAST(get_json_object(m.evidence_json,'$.project_funder_id') AS BIGINT),e.funder_id) pinned_funder_id
      FROM migration_manifest_v m JOIN {r}entities_before e ON e.stable_id=m.target_id WHERE m.evidence_class={cls}""")
    c.zero("NIH_SURVIVOR_ONE_FUNDER", f"SELECT stable_id FROM {r}nih_survivors GROUP BY stable_id HAVING count(*)<>1")
    c.zero("NIH_SURVIVOR_KNOWN", f"SELECT m.* FROM migration_manifest_v m LEFT ANTI JOIN {r}nih_survivors s ON s.stable_id=m.target_id WHERE m.evidence_class={cls}")
    c.zero("NIH_SURVIVOR_FUNDER_IN_FAMILY", f"SELECT * FROM {r}nih_survivors WHERE pinned_funder_id IS NULL OR pinned_funder_id NOT IN {FAMILY_SQL}")
    c.zero("NIH_SURVIVOR_BINDINGS_IN_FAMILY", f"""SELECT b.stable_id,f.canonical_funder_id FROM {r}bindings b JOIN {r}nih_survivors s USING(stable_id)
      LEFT JOIN {r}funder_canon f ON f.funder_id=b.source_funder_id WHERE f.canonical_funder_id IS NULL OR f.canonical_funder_id NOT IN {FAMILY_SQL}""")
    c.artifact("ownership_plan_unpinned", f"SELECT * FROM {r}ownership_plan")
    c.artifact("ownership_plan", f"""SELECT o.stable_id,coalesce(s.pinned_funder_id,o.funder_id) funder_id,o.entity_kind
      FROM {r}ownership_plan_unpinned o LEFT JOIN {r}nih_survivors s USING(stable_id)""")
    c.artifact("nih_obs_funder", f"""SELECT b.observation_key,f.canonical_funder_id funder_id FROM {r}bindings b
      JOIN {r}nih_survivors s USING(stable_id) JOIN {r}funder_canon f ON f.funder_id=b.source_funder_id""")
    c.zero("FAMILY_SURVIVOR_FUNDER_AS_LISTED", f"""SELECT o.* FROM {r}ownership_plan o JOIN {r}nih_survivors s USING(stable_id)
      WHERE NOT (o.funder_id <=> s.pinned_funder_id)""")


def exempt_survivors(c, alias):
    """SQL fragment appended to ENTITY_FUNDER_AMBIGUOUS / DIRECT_CORRECTION_UNPROVEN: a pinned survivor's family funders are expected."""
    return f" LEFT ANTI JOIN {c.r}nih_survivors nih_s ON nih_s.stable_id={alias}.stable_id" if getattr(c, "nih", False) else ""


def lookup_join(c, bindings_alias):
    """Join that supplies a survivor-bound record's own canonical funder (year records sit under the NIH parent, the survivor under
    its institute); number normalization must use the record's funder. Empty when no NIH pair is listed."""
    return f" LEFT JOIN {c.r}nih_obs_funder nof ON nof.observation_key={bindings_alias}.observation_key" if getattr(c, "nih", False) else ""


def unpinned(c, relation):
    """Matching (shell_targets, public-shell numbers, paper linking keys in work_awards) must see each observation's own funder, not a
    survivor's pinned one; the pin is for display only. Pinning a survivor to its
    institute would otherwise leave a shell under the NIH parent with one fewer same-funder target and make the job's own shell rule retire
    it into another year record (fold rehearsal 10-09 N1: MIGRATION_CONFLICT_WITH_RULE, 5,551 listed losers + 3,537 unlisted shells).
    Without NIH pairs this is the plain relation (byte-identical build)."""
    if not getattr(c, "nih", False):
        return f"{c.r}{relation}"
    return (f"(SELECT o.* EXCEPT(funder_id),coalesce(nof.funder_id,o.funder_id) funder_id FROM {c.r}{relation} o "
            f"LEFT JOIN {c.r}nih_obs_funder nof USING(observation_key))")


def plan_for_matching(c):
    """The ownership plan as matching must see it: before the survivor pin (public-shell FUNDER hold). Plain plan without NIH pairs."""
    return f"{c.r}ownership_plan_unpinned" if getattr(c, "nih", False) else f"{c.r}ownership_plan"


def unpinned_owner_join(c, alias):
    """Join supplying the pre-pin owner funder for legacy-alias eligibility (work_awards.legacy_aliases). Empty without NIH pairs."""
    return f" LEFT JOIN {c.r}ownership_plan_unpinned nih_u ON nih_u.stable_id={alias}.stable_id" if getattr(c, "nih", False) else ""


def owner_funder(c, document_alias):
    """Legacy-alias eligibility compares the alias funder with the record's funder before the pin (a pinned survivor shows its
    institute; its aliases were made under the NIH parent). Without NIH pairs: the served document's funder, unchanged."""
    return f"coalesce(nih_u.funder_id,{document_alias}.funder_id)" if getattr(c, "nih", False) else f"{document_alias}.funder_id"


def norm_funder(c, owner_alias):
    return f"coalesce(nof.funder_id,{owner_alias}.funder_id)" if getattr(c, "nih", False) else f"{owner_alias}.funder_id"


# ---------- plan(): proof on pending pairs (held-pair filter, same pattern as 6f's public_shell_filter) ----------
def filter(c, mm):
    """Each pending NIH pair is re-proved tonight: loser and target parse to the listed project number; the target is one of the
    project's non-supplement fiscal-year records (the newest one, chosen by the list) or, when the project has none, its bare number; both ends in the NIH family; the target is not being retired by a rule. Inputs move between nights, so a pair
    that fails is HELD (not retired tonight, counted by reason, re-proved next night). NIH_HELD_FUSE stops the run if more than
    max(100, 1%) of pending NIH pairs fail. A list that disagrees with itself (two choices for one project) stops the run.
    Returns the pending relation with held NIH pairs removed."""
    if not getattr(c, "nih", False):
        return mm
    r = c.r; cls = literal(NIH_CLASS)
    c.artifact("nih_pairs", f"""SELECT m.loser_id,m.target_id,get_json_object(m.evidence_json,'$.core') core,
        get_json_object(m.evidence_json,'$.primary_award_id') primary_award_id,get_json_object(m.evidence_json,'$.title_from') title_from
      FROM {mm} m WHERE m.evidence_class={cls}""")
    c.zero("NIH_ONE_CHOICE_PER_TARGET", f"""SELECT target_id FROM {r}nih_pairs GROUP BY target_id
      HAVING count(DISTINCT core)<>1 OR count(DISTINCT primary_award_id)<>1 OR count(DISTINCT coalesce(title_from,''))<>1""")
    c.zero("NIH_PRIMARY_IS_PROJECT_FORM", f"SELECT * FROM {r}nih_pairs WHERE NOT COALESCE({core('primary_award_id')}=core,false)")
    # yesterday's published number and funder of each end: the same field the list was built from (openalex_awards)
    c.artifact("nih_pair_numbers", f"""SELECT p.*,lo.funder_award_id loser_number,ta.funder_award_id target_number,
        lo.funder_id loser_funder_id,ta.funder_id target_funder_id
      FROM {r}nih_pairs p LEFT JOIN awards_v lo ON lo.id=p.loser_id LEFT JOIN awards_v ta ON ta.id=p.target_id""")
    # ids of tonight's listed pairs that are bound: an equality join (NOT IN over 18M bindings with OR hung, dev 10-01)
    c.artifact("nih_bound", f"""SELECT DISTINCT b.stable_id FROM {r}bindings b JOIN (SELECT loser_id id FROM {r}nih_pairs
      UNION SELECT target_id FROM {r}nih_pairs) i ON i.id=b.stable_id""")
    c.artifact("nih_held", f"""SELECT DISTINCT loser_id,reason FROM (
        SELECT loser_id,'NUMBER' reason FROM {r}nih_pair_numbers WHERE NOT COALESCE({core('loser_number')}=core AND {core('target_number')}=core,false)
        UNION ALL SELECT loser_id,'SUBPROJECT' FROM {r}nih_pair_numbers WHERE COALESCE(loser_number RLIKE ':[0-9]+$' OR target_number RLIKE ':[0-9]+$',false)
        UNION ALL SELECT loser_id,'MULTI_NUMBER' FROM {r}nih_pair_numbers WHERE COALESCE({multi('loser_number')} OR {multi('target_number')},true)
        UNION ALL SELECT loser_id,'TARGET_NOT_PROJECT_RECORD' FROM {r}nih_pair_numbers
          WHERE NOT COALESCE({year_record('target_number')} OR {bare('target_number')},false)
        UNION ALL SELECT loser_id,'NOT_IN_FAMILY' FROM {r}nih_pair_numbers
          WHERE NOT COALESCE(loser_funder_id IN {FAMILY_SQL} AND target_funder_id IN {FAMILY_SQL},false)
        UNION ALL SELECT p.loser_id,'TARGET_RETIRING' FROM {r}nih_pairs p JOIN {r}retirements_rule t ON t.old_id=p.target_id
        UNION ALL SELECT p.loser_id,'NOT_BOUND' FROM {r}nih_pairs p LEFT JOIN {r}nih_bound bl ON bl.stable_id=p.loser_id
          LEFT JOIN {r}nih_bound bt ON bt.stable_id=p.target_id WHERE bl.stable_id IS NULL OR bt.stable_id IS NULL
        UNION ALL SELECT p.loser_id,'NOT_ACTIVE' FROM {r}nih_pairs p LEFT JOIN {r}entities_allocated l ON l.stable_id=p.loser_id
          LEFT JOIN {r}entities_allocated t ON t.stable_id=p.target_id WHERE NOT COALESCE(l.status='ACTIVE' AND t.status='ACTIVE',false))""")
    pairs = c.count("nih_pending_pairs", r + "nih_pairs"); c.count("nih_held", f"(SELECT DISTINCT loser_id FROM {r}nih_held) x")
    for row in c.sql(f"SELECT reason,count(DISTINCT loser_id) n FROM {r}nih_held GROUP BY 1").collect():
        c.counts["nih_held_" + row.reason.lower()] = int(row.n)
    c.zero("NIH_HELD_FUSE", f"SELECT count(DISTINCT loser_id) n FROM {r}nih_held HAVING n>greatest(100,0.01*{pairs})")
    return c.artifact("migration_proven_nih", f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}nih_held h ON h.loser_id=m.loser_id")


def same_funder_exempt(c, retirements_alias):
    """Cross-funder retirements allowed only for this class and only inside the NIH family (checked by NIH_BOTH_IN_FAMILY); and a
    retirement touching a pinned survivor is compared on its owners' funders BEFORE the pin (the shell rule matched on those, see
    unpinned()): fold rehearsal 10-09 N2, 30,764 rule retirements of NIH-parent shells into institute-pinned survivors."""
    if not getattr(c, "nih", False):
        return ""
    r = c.r
    c.artifact("nih_unpinned_same_funder", f"""SELECT t.old_id,t.canonical_id FROM {r}retirements t
      JOIN {r}ownership_plan_unpinned ua ON ua.stable_id=t.old_id JOIN {r}ownership_plan_unpinned ub ON ub.stable_id=t.canonical_id
      LEFT JOIN {r}nih_survivors so ON so.stable_id=t.old_id LEFT JOIN {r}nih_survivors sc ON sc.stable_id=t.canonical_id
      WHERE ua.funder_id=ub.funder_id AND (so.stable_id IS NOT NULL OR sc.stable_id IS NOT NULL)""")
    return (f" AND NOT EXISTS (SELECT 1 FROM migration_manifest_v nm WHERE nm.evidence_class={literal(NIH_CLASS)}"
            f" AND nm.loser_id={retirements_alias}.old_id AND nm.target_id={retirements_alias}.canonical_id)"
            f" AND NOT EXISTS (SELECT 1 FROM {r}nih_unpinned_same_funder x WHERE x.old_id={retirements_alias}.old_id"
            f" AND x.canonical_id={retirements_alias}.canonical_id)")


def with_class(c, classes):
    """A class list with this class appended only when NIH pairs are listed (the build stays byte-identical otherwise)."""
    return tuple(classes) + ((NIH_CLASS,) if getattr(c, "nih", False) else ())


def exempt_where(c, clause, exemption):
    """The clause unchanged without NIH pairs (byte-identical build); with them, parenthesized and narrowed by the exemption."""
    return f"({clause}){exemption}" if getattr(c, "nih", False) else clause


def pair_same_funder_exempt(c, manifest_alias):
    return f" AND {manifest_alias}.evidence_class<>{literal(NIH_CLASS)}" if getattr(c, "nih", False) else ""


# ---------- project(): the project record ----------
def project(c):
    """After awards_candidate is built. Rewrites the survivors' number, title/people, dates and amount; nothing else, no other row."""
    if not getattr(c, "nih", False):
        return
    r = c.r; s = settings(c)
    c.artifact("nih_choice", f"""SELECT DISTINCT target_id stable_id,get_json_object(evidence_json,'$.primary_award_id') primary_award_id,
        upper(get_json_object(evidence_json,'$.title_from')) title_from
      FROM migration_manifest_v WHERE evidence_class={literal(NIH_CLASS)}""")
    # every observation now bound to a survivor (its own + the retired years'), with the fields the projection uses
    c.artifact("nih_obs", f"""SELECT b.stable_id,b.observation_key,b.is_direct,b.priority,upper(regexp_replace(b.payload.funder_award_id,'\\\\s','')) number,
        b.payload.display_name,b.payload.description,b.payload.lead_investigator,b.payload.co_lead_investigator,b.payload.investigators,
        b.payload.start_date,b.payload.end_date,b.payload.amount,b.payload.currency,b.payload.updated_date
      FROM {r}bindings_final b JOIN {r}nih_choice n USING(stable_id)""")
    c.artifact("nih_title_row", f"""SELECT o.* FROM {r}nih_obs o JOIN {r}nih_choice n USING(stable_id) WHERE o.number=n.title_from
      QUALIFY row_number() OVER (PARTITION BY o.stable_id ORDER BY o.is_direct DESC,o.priority DESC,o.updated_date DESC NULLS LAST,o.observation_key)=1""")
    c.artifact("nih_span", f"""SELECT stable_id,min(start_date) start_date,max(end_date) end_date,
        sum(CASE WHEN is_direct THEN amount END) amount_sum,count(DISTINCT currency) currencies FROM {r}nih_obs GROUP BY stable_id""")
    c.count("nih_title_from_missing", f"(SELECT n.stable_id FROM {r}nih_choice n LEFT ANTI JOIN {r}nih_title_row t USING(stable_id) WHERE n.title_from IS NOT NULL) x")
    title = s["title"] == "chosen_year"
    amount = {"null": "CAST(NULL AS DOUBLE)", "sum": "CASE WHEN sp.currencies<=1 THEN sp.amount_sum END", "elected": "a.amount"}[s["amount"]]
    pick = lambda col: f"CASE WHEN n.stable_id IS NOT NULL AND t.stable_id IS NOT NULL THEN t.{col} ELSE a.{col} END" if title else f"a.{col}"
    c.artifact("awards_candidate_before_nih", f"SELECT * FROM {r}awards_candidate")
    over = {
        "funder_award_id": "CASE WHEN n.stable_id IS NOT NULL THEN n.primary_award_id ELSE a.funder_award_id END",
        "display_name": pick("display_name"), "description": pick("description"),
        "lead_investigator": pick("lead_investigator"), "co_lead_investigator": pick("co_lead_investigator"), "investigators": pick("investigators"),
        "start_date": "CASE WHEN n.stable_id IS NOT NULL THEN coalesce(sp.start_date,a.start_date) ELSE a.start_date END",
        "end_date": "CASE WHEN n.stable_id IS NOT NULL THEN coalesce(sp.end_date,a.end_date) ELSE a.end_date END",
        "start_year": "CASE WHEN n.stable_id IS NOT NULL AND sp.start_date IS NOT NULL THEN year(sp.start_date) ELSE a.start_year END",
        "end_year": "CASE WHEN n.stable_id IS NOT NULL AND sp.end_date IS NOT NULL THEN year(sp.end_date) ELSE a.end_year END",
        "amount": f"CASE WHEN n.stable_id IS NOT NULL THEN {amount} ELSE a.amount END",
    }
    c.artifact("awards_candidate", f"""SELECT a.* EXCEPT({",".join(over)}),{",".join(f"{v} {k}" for k, v in over.items())}
      FROM {r}awards_candidate_before_nih a LEFT JOIN {r}nih_choice n ON n.stable_id=a.id
      LEFT JOIN {r}nih_title_row t ON t.stable_id=a.id LEFT JOIN {r}nih_span sp ON sp.stable_id=a.id""")
    c.zero("NIH_PROJECTION_SAME_ROWS", f"""(SELECT id FROM {r}awards_candidate EXCEPT ALL SELECT id FROM {r}awards_candidate_before_nih)
      UNION ALL (SELECT id FROM {r}awards_candidate_before_nih EXCEPT ALL SELECT id FROM {r}awards_candidate)""")
    c.zero("NIH_PRIMARY_PUBLISHED", f"""SELECT a.id,a.funder_award_id,n.primary_award_id FROM {r}awards_candidate a JOIN {r}nih_choice n ON n.stable_id=a.id
      WHERE NOT (a.funder_award_id <=> n.primary_award_id)""")
    c.zero("NIH_SURVIVOR_FUNDER_PUBLISHED", f"""SELECT a.id,a.funder_id,s.pinned_funder_id FROM {r}awards_candidate a JOIN {r}nih_survivors s ON s.stable_id=a.id
      WHERE NOT (a.funder_id <=> s.pinned_funder_id)""")
    c.count("nih_projects_published", f"(SELECT a.id FROM {r}awards_candidate a JOIN {r}nih_choice n ON n.stable_id=a.id) x")


# ---------- work_awards: link conservation on the night NIH pairs retire (same contract as uk_lineage.link_conservation) ----------
def link_conservation(c):
    """Tonight's NIH retirements: no paper keeps a link to a retired year record, and every paper that linked to one now links to
    its project (papers merged away tonight are counted as their surviving paper). Counts moved vs already-on-project."""
    if not getattr(c, "nih", False):
        return
    r = c.r; live = c.outputs["work_awards"]; g = "concat('https://openalex.org/G',{})"
    c.artifact("nih_applied", f"SELECT old_id loser_id,canonical_id target_id FROM {r}retirements_manifest WHERE evidence_class={literal(NIH_CLASS)}")
    # equality joins only: an IN(a,b) join against 60M links runs as a near cross product (dev 10-01: timed out at 9,000 s)
    c.artifact("nih_award_ids", f"""SELECT DISTINCT award_id FROM (SELECT {g.format('loser_id')} award_id FROM {r}nih_applied
      UNION ALL SELECT {g.format('target_id')} FROM {r}nih_applied)""")
    c.artifact("nih_links_before", f"""SELECT DISTINCT coalesce(sv.survivor_work_id,w.work_id) work_id,w.award.id award_id FROM {live} w
      JOIN {r}nih_award_ids i ON i.award_id=w.award.id
      LEFT JOIN {r}work_survivors sv ON sv.loser_work_id=w.work_id""")
    c.artifact("nih_candidate_edges", f"""SELECT DISTINCT w.work_id,w.award.id award_id FROM {r}work_awards_candidate w
      JOIN {r}nih_award_ids i ON i.award_id=w.award.id""")
    c.zero("NIH_NO_LOSER_LINKS", f"SELECT e.* FROM {r}nih_candidate_edges e JOIN {r}nih_applied m ON e.award_id={g.format('m.loser_id')}")
    c.artifact("nih_missing_mapped", f"""SELECT DISTINCT b.work_id,{g.format('m.target_id')} award_id FROM {r}nih_links_before b
      JOIN {r}nih_applied m ON b.award_id={g.format('m.loser_id')}
      EXCEPT SELECT work_id,award_id FROM {r}nih_candidate_edges""")
    # a paper whose acknowledgement vanished upstream tonight is an explained removal; only links tonight's producers still
    # tie to the retired record or its project must survive (same contract as uk_lineage's supported checks)
    c.artifact("nih_supported", f"""SELECT DISTINCT wm.work_id,{g.format('x.target_id')} award_id FROM {r}work_matches wm
      JOIN (SELECT loser_id id,target_id FROM {r}nih_applied UNION SELECT target_id,target_id FROM {r}nih_applied) x ON x.id=wm.stable_id""")
    c.zero("NIH_MAPPED_LINKS_PRESERVED", f"SELECT m.* FROM {r}nih_missing_mapped m LEFT SEMI JOIN {r}nih_supported s ON s.work_id=m.work_id AND s.award_id=m.award_id")
    c.counts["nih_explained_removals"] = int(c.sql(f"""SELECT count(*) n FROM {r}nih_missing_mapped m
      LEFT ANTI JOIN {r}nih_supported s ON s.work_id=m.work_id AND s.award_id=m.award_id""").collect()[0].n)
    row = c.sql(f"""WITH marks AS (
        SELECT b.work_id,m.target_id,1 l,0 t FROM {r}nih_links_before b JOIN {r}nih_applied m ON b.award_id={g.format('m.loser_id')}
        UNION ALL SELECT b.work_id,m.target_id,0,1 FROM {r}nih_links_before b JOIN {r}nih_applied m ON b.award_id={g.format('m.target_id')}),
      e AS (SELECT work_id,target_id,max(l) l,max(t) t FROM marks GROUP BY 1,2)
      SELECT (SELECT count(*) FROM {r}nih_applied) pairs,count_if(l=1 AND t=0) moved,count_if(l=1 AND t=1) deduplicated FROM e""").collect()[0]
    for k in ("pairs", "moved", "deduplicated"):
        c.counts["nih_" + k] = int(row[k])
