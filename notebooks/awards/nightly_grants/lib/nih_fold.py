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

EU_GRANT_FAMILY_FOLD (oxjob #690, design: that job's work/EU-FAMILY-FOLD-DESIGN.md), the second family class on the same hooks.
An EU grant agreement number (6 digits, or 101 + 6) is one numbering space across every EU programme. The registries (feeds `erc`,
`cordis`) file a grant under one programme funder; papers, DataCite and Europe PMC name the same number under another EU funder
name (ERC vs H2020 ERC, EC vs H2020 FP ...), and same-funder shell retirement never joins those copies. A listed paper-side copy
retires into the one registry record of its number: {"number": "101002724", "registry": "erc"}. The survivor keeps its own
funder (the registry's; no project_funder_id) and its registry row (the copies have no direct observation, so they never win
the election); the pin, the exemptions, the own-funder lookups and link conservation are shared with the NIH class; the proof
(_eu_filter) and the published-row checks are its own. With no EU pair listed every statement renders exactly as before.
The nih_* artifact names are kept for both classes (survivors, own-funder lookups); EU-only artifacts are eu_*.
"""
import json

import list_legs
from stable_award_ids import literal

EU_CLASS = "EU_GRANT_FAMILY_FOLD"
# The 86 EU programme funders: openalex.funders.funders whose display_name matches EU_FAMILY_RLIKE (frozen 2026-11-23, the same
# family as the ERC call-code guard, notebooks/awards/tests/test_erc_call_code_guard.py). It holds the canonical ids of its three
# merged names (Horizon 2020 -> Horizon 2020 Framework Programme, Seventh Framework Programme twin, EIT).
EU_FAMILY_RLIKE = ("^(European Commission|European Research Council|Seventh Framework Programme|Horizon 2020( Framework Programme)?"
                   "|(FP7|H2020|HORIZON EUROPE) .+)$")
EU_FAMILY = (4320314745, 4320320300, 4320332999, 4320333065, 4320334322, 4320334678, 4320334960, 4320335254, 4320337662, 4320337663,
             4320337664, 4320337665, 4320337668, 4320337669, 4320337670, 4320337671, 4320337755, 4320337994, 4320337995, 4320337996,
             4320337998, 4320338014, 4320338015, 4320338016, 4320338017, 4320338018, 4320338019, 4320338020, 4320338335, 4320338336,
             4320338337, 4320338338, 4320338339, 4320338340, 4320338341, 4320338342, 4320338343, 4320338344, 4320338345, 4320338348,
             4320338349, 4320338350, 4320338351, 4320338352, 4320338353, 4320338357, 4320338358, 4320338359, 4320338360, 4320338361,
             4320338362, 4320338364, 4320338365, 4320338366, 4320338367, 4320338368, 4320338369, 4320338370, 4320338371, 4320338372,
             4320338373, 4320338374, 4320338375, 4320338376, 4320338377, 4320338438, 4320338439, 4320338440, 4320338441, 4320338442,
             4320338444, 4320338445, 4320338446, 4320338447, 4320338448, 4320338449, 4320338450, 4320338451, 4320338452, 4320338453,
             4320338475, 4320338476, 4320338477, 4320338480, 4320338481, 4320338482)
EU_FAMILY_SQL = "(" + ",".join(str(f) for f in EU_FAMILY) + ")"
EU_NUMBER = "^([0-9]{6}|101[0-9]{6})$"   # an EU grant agreement number: FP7/H2020 6 digits, late H2020 and Horizon Europe 101 + 6
EU_REGISTRIES = ("erc", "cordis")
EU_HELD_REASONS = ("NUMBER", "NOT_IN_FAMILY", "TARGET_NOT_REGISTRY", "TARGET_NOT_UNIQUE", "LOSER_DIRECT", "LOSER_NATIVE",
                   "LOSER_IS_TARGET", "LOSER_RULE_RETIRING", "LOSER_RULE_TARGET", "TARGET_RETIRING", "NOT_BOUND", "NOT_ACTIVE")

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
AMOUNT_POLICIES = ("null", "sum", "elected")        # Q2; default null; prod picks via config nih_fold.amount (Rohan 11-16: sum;
                                                    # 11-19: a sum only when every bound year has a cost, option B)
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


IC_SERIAL_ONLY = "'^[A-Z]{2}[0-9]{5,6}$'"
EMBEDDED_NUMBER = "'([A-Z][0-9A-Z]{2}[A-Z]{2}[0-9]{4,6})'"
OTHER_IC_SERIAL = "'[A-Z]{2}[0-9]{4,6}'"


def ic_serial_names_core(expr, core_expr):
    """True when a number core() cannot read to core_expr still names that project: institute + serial alone ('CA148791',
    'HL-80682') or exactly one embedded project number ('NIH R35 GM143042', 'RO1 HL- 081823') and no other institute + serial
    beside it ('AI38022 R01-HL075822' names two grants; multi() misses it), whose institute + zero-padded serial (and activity
    code, letter O read as zero, when the text carries one) equal the core's. Rohan 11-02 (#1501, oxjob
    #1875 phase 2b, option (a)): core() stays a pure parse; that the institute + serial names only one ExPORTER project is
    checked separately (filter(): nih_exporter_ic_serial)."""
    an = f"upper(regexp_replace({expr},'[^A-Za-z0-9]',''))"
    emb = f"regexp_extract({an},{EMBEDDED_NUMBER},1)"
    return (f"(CASE WHEN {an} RLIKE {IC_SERIAL_ONLY} THEN concat(substr({an},1,2),lpad(substr({an},3),6,'0'))=substr({core_expr},4,8)"
            f" WHEN size(regexp_extract_all({an},{EMBEDDED_NUMBER},1))=1 AND NOT regexp_replace({an},{EMBEDDED_NUMBER},'') RLIKE {OTHER_IC_SERIAL}"
            f" THEN regexp_replace(substr({emb},1,3),'^([A-Z])O','$10')=substr({core_expr},1,3)"
            f" AND concat(substr({emb},4,2),lpad(substr({emb},6),6,'0'))=substr({core_expr},4,8) ELSE false END)")


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


def _listed(c, cls):
    if not c.has_extra("migration_manifest"):
        return False
    return bool(c.sql(f"SELECT 1 FROM migration_manifest_v WHERE evidence_class={literal(cls)} LIMIT 1").collect())


def active(c):
    return _listed(c, NIH_CLASS)


def eu_active(c):
    return _listed(c, EU_CLASS)


def _eu(c):
    """EU pairs listed tonight (set by pin_ownership; `is True` so a stand-in context that answers any attribute reads False)."""
    return getattr(c, "eu", False) is True


def on(c):
    """True when either family class is listed: the hooks both classes share (survivor pin, exemptions, own-funder lookups)."""
    return getattr(c, "nih", False) or _eu(c)


def classes(c):
    """The family classes listed tonight, NIH first."""
    return tuple(k for k, listed in ((NIH_CLASS, getattr(c, "nih", False)), (EU_CLASS, _eu(c))) if listed)


def class_is(c, col, negate=False):
    """col is (or, negated, is not) one of tonight's family classes. One class renders as before EU: col='X' / col<>'X'."""
    cl = classes(c)
    if len(cl) == 1:
        return f"{col}{'<>' if negate else '='}{literal(cl[0])}"
    return f"{col} {'NOT IN' if negate else 'IN'} ({','.join(literal(x) for x in cl)})"


def not_in_family(c, class_col, funder_col):
    """The funder is outside its class's family (NIH institutes, or the EU programme funders). NIH-only form unchanged."""
    if not _eu(c):
        return f"{funder_col} NOT IN {FAMILY_SQL}"
    return (f"NOT (CASE {class_col} WHEN {literal(NIH_CLASS)} THEN {funder_col} IN {FAMILY_SQL}"
            f" WHEN {literal(EU_CLASS)} THEN {funder_col} IN {EU_FAMILY_SQL} ELSE false END)")


# ---------- plan(): ownership ----------
def pin_ownership(c):
    """Right after ownership_plan. Survivors (targets of the whole NIH list, applied or pending) take the funder the list gives them
    (evidence_json project_funder_id: the institute of the project's code, oxjob #1875) or, when it gives none, keep their current funder.
    EU survivors (registry records) give none: they take the canonical funder of their own registry observation tonight (erc/cordis,
    direct), else their recorded funder made canonical. So the copies' observations that bind to them through the redirect (ERC
    parent, H2020 FP ...) never move the record off its registry funder, and a registry refiling the grant still moves it."""
    c.nih = active(c); c.eu = eu_active(c)
    if not on(c):
        return
    r = c.r; cls = class_is(c, "m.evidence_class")
    pin = "coalesce(CAST(get_json_object(m.evidence_json,'$.project_funder_id') AS BIGINT),e.funder_id)"
    fam = joins = ""
    if c.eu:
        registries = "(" + ",".join(literal(x) for x in EU_REGISTRIES) + ")"
        c.artifact("eu_registry_funder", f"""SELECT b.stable_id,min(f.canonical_funder_id) funder_id,count(DISTINCT f.canonical_funder_id) funders
          FROM {r}bindings b JOIN (SELECT DISTINCT target_id FROM migration_manifest_v WHERE evidence_class={literal(EU_CLASS)}) t
            ON t.target_id=b.stable_id JOIN {r}funder_canon f ON f.funder_id=b.source_funder_id
          WHERE b.is_direct AND b.payload.provenance IN {registries} GROUP BY b.stable_id""")
        # a survivor is exempt from ENTITY_FUNDER_AMBIGUOUS (its copies' funders are expected); its registry rows still name one funder
        c.zero("EU_SURVIVOR_REGISTRY_ONE_FUNDER", f"SELECT * FROM {r}eu_registry_funder WHERE funders<>1")
        pin = f"CASE WHEN m.evidence_class={literal(EU_CLASS)} THEN coalesce(rf.funder_id,fc.canonical_funder_id,e.funder_id) ELSE {pin} END"
        fam = ",m.evidence_class family_class"
        joins = (f" LEFT JOIN {r}eu_registry_funder rf ON rf.stable_id=m.target_id"
                 f" LEFT JOIN {r}funder_canon fc ON fc.funder_id=e.funder_id")
    c.artifact("nih_survivors", f"""SELECT DISTINCT m.target_id stable_id,
        {pin} pinned_funder_id{fam}
      FROM migration_manifest_v m JOIN {r}entities_before e ON e.stable_id=m.target_id{joins} WHERE {cls}""")
    c.zero("NIH_SURVIVOR_ONE_FUNDER", f"SELECT stable_id FROM {r}nih_survivors GROUP BY stable_id HAVING count(*)<>1")
    c.zero("NIH_SURVIVOR_KNOWN", f"SELECT m.* FROM migration_manifest_v m LEFT ANTI JOIN {r}nih_survivors s ON s.stable_id=m.target_id WHERE {cls}")
    c.zero("NIH_SURVIVOR_FUNDER_IN_FAMILY", f"SELECT * FROM {r}nih_survivors WHERE pinned_funder_id IS NULL OR {not_in_family(c, 'family_class', 'pinned_funder_id')}")
    c.zero("NIH_SURVIVOR_BINDINGS_IN_FAMILY", f"""SELECT b.stable_id,f.canonical_funder_id FROM {r}bindings b JOIN {r}nih_survivors s USING(stable_id)
      LEFT JOIN {r}funder_canon f ON f.funder_id=b.source_funder_id WHERE f.canonical_funder_id IS NULL OR {not_in_family(c, 's.family_class', 'f.canonical_funder_id')}""")
    if c.eu:
        # An EU survivor's funder is its registry's before and after the pin; only an NIH survivor moves (parent -> institute). Without
        # this, a survivor holding a copy's binding would read as min(funder ids) here (ERC parent < H2020 ERC), and legacy-alias
        # eligibility and the pre-pin same-funder checks would compare against that copy's funder.
        c.artifact("ownership_plan_unpinned", f"""SELECT o.stable_id,CASE WHEN s.family_class={literal(EU_CLASS)} THEN s.pinned_funder_id
            ELSE o.funder_id END funder_id,o.entity_kind FROM {r}ownership_plan o LEFT JOIN {r}nih_survivors s USING(stable_id)""")
    else:
        c.artifact("ownership_plan_unpinned", f"SELECT * FROM {r}ownership_plan")
    c.artifact("ownership_plan", f"""SELECT o.stable_id,coalesce(s.pinned_funder_id,o.funder_id) funder_id,o.entity_kind
      FROM {r}ownership_plan_unpinned o LEFT JOIN {r}nih_survivors s USING(stable_id)""")
    c.artifact("nih_obs_funder", f"""SELECT b.observation_key,f.canonical_funder_id funder_id FROM {r}bindings b
      JOIN {r}nih_survivors s USING(stable_id) JOIN {r}funder_canon f ON f.funder_id=b.source_funder_id""")
    c.zero("FAMILY_SURVIVOR_FUNDER_AS_LISTED", f"""SELECT o.* FROM {r}ownership_plan o JOIN {r}nih_survivors s USING(stable_id)
      WHERE NOT (o.funder_id <=> s.pinned_funder_id)""")


def exempt_survivors(c, alias):
    """SQL fragment appended to ENTITY_FUNDER_AMBIGUOUS / DIRECT_CORRECTION_UNPROVEN: a pinned survivor's family funders are expected."""
    return f" LEFT ANTI JOIN {c.r}nih_survivors nih_s ON nih_s.stable_id={alias}.stable_id" if on(c) else ""


def lookup_join(c, bindings_alias):
    """Join that supplies a survivor-bound record's own canonical funder (year records sit under the NIH parent, the survivor under
    its institute); number normalization must use the record's funder. Empty when no NIH pair is listed."""
    return f" LEFT JOIN {c.r}nih_obs_funder nof ON nof.observation_key={bindings_alias}.observation_key" if on(c) else ""


def unpinned(c, relation):
    """Matching (shell_targets, public-shell numbers, paper linking keys in work_awards) must see each observation's own funder, not a
    survivor's pinned one; the pin is for display only. Pinning a survivor to its
    institute would otherwise leave a shell under the NIH parent with one fewer same-funder target and make the job's own shell rule retire
    it into another year record (fold rehearsal 10-09 N1: MIGRATION_CONFLICT_WITH_RULE, 5,551 listed losers + 3,537 unlisted shells).
    Without NIH pairs this is the plain relation (byte-identical build)."""
    if not on(c):
        return f"{c.r}{relation}"
    return (f"(SELECT o.* EXCEPT(funder_id),coalesce(nof.funder_id,o.funder_id) funder_id FROM {c.r}{relation} o "
            f"LEFT JOIN {c.r}nih_obs_funder nof USING(observation_key))")


def plan_for_matching(c):
    """The ownership plan as matching must see it: before the survivor pin (public-shell FUNDER hold). Plain plan without NIH pairs."""
    return f"{c.r}ownership_plan_unpinned" if on(c) else f"{c.r}ownership_plan"


def unpinned_owner_join(c, alias):
    """Join supplying the pre-pin owner funder for legacy-alias eligibility (work_awards.legacy_aliases). Empty without NIH pairs."""
    return f" LEFT JOIN {c.r}ownership_plan_unpinned nih_u ON nih_u.stable_id={alias}.stable_id" if on(c) else ""


def owner_funder(c, document_alias):
    """Legacy-alias eligibility compares the alias funder with the record's funder before the pin (a pinned survivor shows its
    institute; its aliases were made under the NIH parent). Without NIH pairs: the served document's funder, unchanged."""
    return f"coalesce(nih_u.funder_id,{document_alias}.funder_id)" if on(c) else f"{document_alias}.funder_id"


def norm_funder(c, owner_alias):
    return f"coalesce(nof.funder_id,{owner_alias}.funder_id)" if on(c) else f"{owner_alias}.funder_id"


# ---------- plan(): proof on pending pairs (held-pair filter, same pattern as 6f's public_shell_filter) ----------
def filter(c, mm):
    """Pending pairs of each listed family class failing tonight's proof are held (_nih_filter, then _eu_filter)."""
    if getattr(c, "nih", False):
        mm = _nih_filter(c, mm)
    if _eu(c):
        mm = _eu_filter(c, mm)
    return mm


def _nih_filter(c, mm):
    """Each pending NIH pair is re-proved tonight: loser and target parse to the listed project number (or the loser names it by
    institute + serial / one embedded number, ic_serial_names_core, and that institute + serial is still one ExPORTER project's: oxjob
    #1875 phase 2b, Rohan 11-02); the target is one of the
    project's non-supplement fiscal-year records (the newest one, chosen by the list) or, when the project has none, its bare number; both ends in the NIH family; the target is not being retired by a rule. Inputs move between nights, so a pair
    that fails is HELD (not retired tonight, counted by reason, re-proved next night). NIH_HELD_FUSE stops the run if more than
    max(100, 1%) of pending NIH pairs fail. A list that disagrees with itself (two choices for one project) stops the run.
    Returns the pending relation with held NIH pairs removed."""
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
    # institute + serial -> how many ExPORTER projects carry it: an institute + serial spelling names the core only when it is the one
    c.artifact("nih_exporter_ic_serial", f"""SELECT substr(core,4,8) ic_serial,count(DISTINCT core) projects FROM (
        SELECT {core('funder_award_id')} core FROM awards_v WHERE provenance='nih_exporter' AND funder_id IN {FAMILY_SQL}) WHERE core<>'' GROUP BY 1""")
    c.artifact("nih_held", f"""SELECT DISTINCT loser_id,reason FROM (
        SELECT n.loser_id,'NUMBER' reason FROM {r}nih_pair_numbers n LEFT JOIN {r}nih_exporter_ic_serial x ON x.ic_serial=substr(n.core,4,8)
          WHERE NOT COALESCE(({core('n.loser_number')}=n.core OR ({ic_serial_names_core('n.loser_number','n.core')} AND x.projects=1))
            AND {core('n.target_number')}=n.core,false)
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


def _eu_filter(c, mm):
    """Each pending EU pair is re-proved tonight, on yesterday's served rows (awards_v, the table the list was built from) and
    tonight's identity build. A pair that fails is HELD (not retired tonight, counted by reason, re-proved next night):
      NUMBER              loser and target both serve the listed number (trimmed), an EU grant agreement number (EU_NUMBER);
      NOT_IN_FAMILY       both ends' served and plan funders are EU programme funders (the only cross-funder retirements allowed);
      TARGET_NOT_REGISTRY the target serves the listed registry's row (erc or cordis);
      TARGET_NOT_UNIQUE   the target is the one entity whose direct erc/cordis observations under an EU funder carry the number;
      LOSER_DIRECT, LOSER_NATIVE  the loser is a paper-side copy (no direct observation, no NATIVE key), so once its observations
                          bind to the survivor through the redirect the registry row still wins the election (direct first);
      LOSER_IS_TARGET, LOSER_RULE_RETIRING, LOSER_RULE_TARGET, TARGET_RETIRING  no chain with tonight's other retirements, and a
                          loser the shell rule retires elsewhere stays with the rule (the generic gates would stop the run);
      NOT_BOUND, NOT_ACTIVE  both ends bound tonight and ACTIVE.
    A list that disagrees with itself (a malformed row, two numbers or registries for one target, applied rows included) stops the
    run; EU_HELD_FUSE stops it if more than max(100, 1% of every listed EU pair) fail tonight (the whole list, not the pending rest,
    so a few hundred pairs still held after their batch applied never trip it the next night). A target that is another manifest
    row's loser is already a stop (MIGRATION_ACYCLIC reads the whole manifest). Returns the pending relation with held EU pairs removed."""
    r, reg_prefix = c.r, c.p; cls = literal(EU_CLASS); fam = EU_FAMILY_SQL
    registries = "(" + ",".join(literal(x) for x in EU_REGISTRIES) + ")"
    c.artifact("eu_pairs", f"""SELECT m.loser_id,m.target_id,get_json_object(m.evidence_json,'$.number') number,
        get_json_object(m.evidence_json,'$.registry') registry FROM {mm} m WHERE m.evidence_class={cls}""")
    c.artifact("eu_listed", f"""SELECT loser_id,target_id,get_json_object(evidence_json,'$.number') number,
        get_json_object(evidence_json,'$.registry') registry FROM migration_manifest_v WHERE evidence_class={cls}""")
    c.zero("EU_LISTED_SHAPE", f"SELECT * FROM {r}eu_listed WHERE NOT COALESCE(number RLIKE '{EU_NUMBER}' AND registry IN {registries},false)")
    c.zero("EU_ONE_NUMBER_PER_TARGET", f"""SELECT target_id FROM {r}eu_listed GROUP BY target_id
      HAVING count(DISTINCT number)<>1 OR count(DISTINCT registry)<>1""")
    c.artifact("eu_pair_numbers", f"""SELECT p.*,lo.funder_award_id loser_number,ta.funder_award_id target_number,
        lo.funder_id loser_funder_id,ta.funder_id target_funder_id,ta.provenance target_provenance,
        ul.funder_id loser_plan_funder_id,ut.funder_id target_plan_funder_id
      FROM {r}eu_pairs p LEFT JOIN awards_v lo ON lo.id=p.loser_id LEFT JOIN awards_v ta ON ta.id=p.target_id
      LEFT JOIN {r}ownership_plan_unpinned ul ON ul.stable_id=p.loser_id LEFT JOIN {r}ownership_plan_unpinned ut ON ut.stable_id=p.target_id""")
    # tonight's entities with a direct erc/cordis observation under an EU funder carrying a listed number (provenance read narrow)
    c.artifact("eu_number_owners", f"""WITH d AS (SELECT o.stable_id,o.nk FROM {r}resolved_observations o
        JOIN (SELECT observation_key,payload.provenance provenance FROM {r}bindings WHERE is_direct) b USING(observation_key)
        WHERE o.is_direct AND o.funder_id IN {fam} AND b.provenance IN {registries})
      SELECT p.loser_id,count(DISTINCT d.stable_id) owners,min(d.stable_id) owner_id FROM {r}eu_pairs p JOIN d ON d.nk=p.number
      GROUP BY p.loser_id""")
    c.artifact("eu_bound", f"""SELECT DISTINCT b.stable_id FROM {r}bindings b JOIN (SELECT loser_id id FROM {r}eu_pairs
      UNION SELECT target_id FROM {r}eu_pairs) i ON i.id=b.stable_id""")
    c.artifact("eu_direct", f"""SELECT DISTINCT o.stable_id FROM {r}resolved_observations o JOIN {r}eu_pairs p ON p.loser_id=o.stable_id
      WHERE o.is_direct""")
    c.artifact("eu_held", f"""SELECT DISTINCT loser_id,reason FROM (
        SELECT loser_id,'NUMBER' reason FROM {r}eu_pair_numbers
          WHERE NOT COALESCE(trim(loser_number)=number AND trim(target_number)=number AND number RLIKE '{EU_NUMBER}',false)
        UNION ALL SELECT loser_id,'NOT_IN_FAMILY' FROM {r}eu_pair_numbers WHERE NOT COALESCE(loser_funder_id IN {fam}
          AND target_funder_id IN {fam} AND loser_plan_funder_id IN {fam} AND target_plan_funder_id IN {fam},false)
        UNION ALL SELECT loser_id,'TARGET_NOT_REGISTRY' FROM {r}eu_pair_numbers WHERE NOT COALESCE(target_provenance=registry,false)
        UNION ALL SELECT p.loser_id,'TARGET_NOT_UNIQUE' FROM {r}eu_pairs p LEFT JOIN {r}eu_number_owners n USING(loser_id)
          WHERE NOT COALESCE(n.owners=1 AND n.owner_id=p.target_id,false)
        UNION ALL SELECT stable_id,'LOSER_DIRECT' FROM {r}eu_direct
        UNION ALL SELECT p.loser_id,'LOSER_NATIVE' FROM {r}eu_pairs p JOIN {reg_prefix}award_keys k ON k.stable_id=p.loser_id AND k.key_type='NATIVE'
        UNION ALL SELECT p.loser_id,'LOSER_IS_TARGET' FROM {r}eu_pairs p JOIN {r}migration_pending x ON x.target_id=p.loser_id
        UNION ALL SELECT p.loser_id,'LOSER_RULE_RETIRING' FROM {r}eu_pairs p JOIN {r}retirements_rule t ON t.old_id=p.loser_id
        UNION ALL SELECT p.loser_id,'LOSER_RULE_TARGET' FROM {r}eu_pairs p JOIN {r}retirements_rule t ON t.canonical_id=p.loser_id
        UNION ALL SELECT p.loser_id,'TARGET_RETIRING' FROM {r}eu_pairs p JOIN {r}retirements_rule t ON t.old_id=p.target_id
        UNION ALL SELECT p.loser_id,'NOT_BOUND' FROM {r}eu_pairs p LEFT JOIN {r}eu_bound bl ON bl.stable_id=p.loser_id
          LEFT JOIN {r}eu_bound bt ON bt.stable_id=p.target_id WHERE bl.stable_id IS NULL OR bt.stable_id IS NULL
        UNION ALL SELECT p.loser_id,'NOT_ACTIVE' FROM {r}eu_pairs p LEFT JOIN {r}entities_allocated l ON l.stable_id=p.loser_id
          LEFT JOIN {r}entities_allocated t ON t.stable_id=p.target_id WHERE NOT COALESCE(l.status='ACTIVE' AND t.status='ACTIVE',false))""")
    c.count("eu_pending_pairs", r + "eu_pairs"); c.count("eu_held", f"(SELECT DISTINCT loser_id FROM {r}eu_held) x")
    for row in c.sql(f"SELECT reason,count(DISTINCT loser_id) n FROM {r}eu_held GROUP BY 1").collect():
        c.counts["eu_held_" + row.reason.lower()] = int(row.n)
    listed = c.count("eu_listed_pairs", r + "eu_listed")
    c.zero("EU_HELD_FUSE", f"SELECT count(DISTINCT loser_id) n FROM {r}eu_held HAVING n>greatest(100,0.01*{listed})")
    return c.artifact("migration_proven_eu", f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}eu_held h ON h.loser_id=m.loser_id")


def same_funder_exempt(c, retirements_alias):
    """Cross-funder retirements allowed only for the family classes and only inside each family (NOT_IN_FAMILY holds a pair whose
    ends leave it); and a retirement touching a pinned survivor is compared on its owners' funders BEFORE the pin (the shell rule
    matched on those, see unpinned()): fold rehearsal 10-09 N2, 30,764 rule retirements of NIH-parent shells into institute-pinned
    survivors."""
    if not on(c):
        return ""
    r = c.r
    c.artifact("nih_unpinned_same_funder", f"""SELECT t.old_id,t.canonical_id FROM {r}retirements t
      JOIN {r}ownership_plan_unpinned ua ON ua.stable_id=t.old_id JOIN {r}ownership_plan_unpinned ub ON ub.stable_id=t.canonical_id
      LEFT JOIN {r}nih_survivors so ON so.stable_id=t.old_id LEFT JOIN {r}nih_survivors sc ON sc.stable_id=t.canonical_id
      WHERE ua.funder_id=ub.funder_id AND (so.stable_id IS NOT NULL OR sc.stable_id IS NOT NULL)""")
    return (f" AND NOT EXISTS (SELECT 1 FROM migration_manifest_v nm WHERE {class_is(c, 'nm.evidence_class')}"
            f" AND nm.loser_id={retirements_alias}.old_id AND nm.target_id={retirements_alias}.canonical_id)"
            f" AND NOT EXISTS (SELECT 1 FROM {r}nih_unpinned_same_funder x WHERE x.old_id={retirements_alias}.old_id"
            f" AND x.canonical_id={retirements_alias}.canonical_id)")


def with_class(c, base, shell_target=False):
    """A class list with tonight's family classes appended (unchanged when none is listed: byte-identical build). shell_target marks
    the gates that let a pair retire into a target with no direct observation (an NIH bare-number survivor may be a shell); an EU
    survivor is always a registry record, so the EU class never gets that exemption."""
    return tuple(base) + tuple(k for k in classes(c) if not (shell_target and k == EU_CLASS))


def exempt_where(c, clause, exemption):
    """The clause unchanged without family pairs (byte-identical build); with them, parenthesized and narrowed by the exemption."""
    return f"({clause}){exemption}" if on(c) else clause


def pair_same_funder_exempt(c, manifest_alias):
    return f" AND {class_is(c, manifest_alias + '.evidence_class', negate=True)}" if on(c) else ""


# ---------- project(): the project record ----------
def project(c):
    """After awards_candidate is built: the NIH project record (_nih_project), then the EU survivors' published-row checks."""
    if getattr(c, "nih", False):
        _nih_project(c)
    if _eu(c):
        _eu_published(c)


def _eu_published(c):
    """EU survivors keep the registry's row on every night after their fold. Published under their pinned (registry) funder, else a
    stop. Published as the listed registry's row with the listed number: a survivor missing tonight or showing another row (its
    registry feed dropped the grant and a copy's observation won the election, or another source's direct row outranked it) is
    listed in eu_survivors_off_registry; EU_SURVIVOR_ROW_FUSE stops the run above max(100, 1%) of survivors, so a feed outage can't
    quietly turn registry grants into paper-side copies, while one withdrawn grant never stops a night."""
    r = c.r; cls = literal(EU_CLASS)
    c.artifact("eu_survivors", f"""SELECT s.stable_id,s.pinned_funder_id,l.number,l.registry FROM {r}nih_survivors s
      JOIN (SELECT DISTINCT target_id,number,registry FROM {r}eu_listed) l ON l.target_id=s.stable_id WHERE s.family_class={cls}""")
    c.zero("EU_SURVIVOR_FUNDER_PUBLISHED", f"""SELECT a.id,a.funder_id,s.pinned_funder_id FROM {r}awards_candidate a
      JOIN {r}eu_survivors s ON s.stable_id=a.id WHERE NOT (a.funder_id <=> s.pinned_funder_id)""")
    survivors = c.count("eu_survivors_listed", r + "eu_survivors")
    c.count("eu_survivors_published", f"(SELECT a.id FROM {r}awards_candidate a JOIN {r}eu_survivors s ON s.stable_id=a.id) x")
    c.artifact("eu_survivors_off_registry", f"""SELECT s.stable_id,s.number,s.registry,a.id published_id,a.provenance,a.funder_award_id
      FROM {r}eu_survivors s LEFT JOIN {r}awards_candidate a ON a.id=s.stable_id
      WHERE NOT COALESCE(a.provenance=s.registry AND trim(a.funder_award_id)=s.number,false)""")
    c.count("eu_survivors_off_registry", r + "eu_survivors_off_registry")
    c.zero("EU_SURVIVOR_ROW_FUSE", f"SELECT count(*) n FROM {r}eu_survivors_off_registry HAVING n>greatest(100,0.01*{survivors})")
    # the frozen family against tonight's funders table: a new EU programme funder is outside the fold until EU_FAMILY lists it
    # (counted, never a stop: such a pair is only held, NOT_IN_FAMILY)
    named = f"SELECT funder_id FROM funders_v WHERE coalesce(display_name,'') RLIKE '{EU_FAMILY_RLIKE}'"
    c.count("eu_family_drift", f"""(({named} EXCEPT SELECT explode(array{EU_FAMILY_SQL}))
      UNION ALL (SELECT explode(array{EU_FAMILY_SQL}) EXCEPT {named})) x""")


def _nih_project(c):
    """After awards_candidate is built. Rewrites the survivors' number, title/people, dates and amount; nothing else, no other row."""
    r = c.r; s = settings(c)
    c.artifact("nih_choice", f"""SELECT DISTINCT target_id stable_id,get_json_object(evidence_json,'$.primary_award_id') primary_award_id,
        upper(get_json_object(evidence_json,'$.title_from')) title_from
      FROM migration_manifest_v WHERE evidence_class={literal(NIH_CLASS)}""")
    # every observation now bound to a survivor (its own + the retired years'), with the fields the projection uses
    c.artifact("nih_obs", f"""SELECT b.stable_id,b.observation_key,b.is_direct,b.priority,upper(regexp_replace(b.payload.funder_award_id,'\\\\s','')) number,
        b.payload.provenance,b.payload.display_name,b.payload.description,b.payload.lead_investigator,b.payload.co_lead_investigator,b.payload.investigators,
        b.payload.start_date,b.payload.end_date,b.payload.amount,b.payload.currency,b.payload.updated_date
      FROM {r}bindings_final b JOIN {r}nih_choice n USING(stable_id)""")
    c.artifact("nih_title_row", f"""SELECT o.* FROM {r}nih_obs o JOIN {r}nih_choice n USING(stable_id) WHERE o.number=n.title_from
      QUALIFY row_number() OVER (PARTITION BY o.stable_id ORDER BY o.is_direct DESC,o.priority DESC,o.updated_date DESC NULLS LAST,o.observation_key)=1""")
    # sum: the bound ExPORTER fiscal-year rows, one per number; supplements and amendments are their own awards and count; no other
    # source adds to it; a sum of zero or less is no amount (as HHS TAGGS); a project with any bound year that has no cost gets no
    # amount (Rohan 11-19, option B, complete totals only: ExPORTER's FY1985-1999 files and intramural years before FY2007 carry
    # no cost, and a short total would look complete). CreateNIHAwards writes one raw row per number, under one
    # funder, and a re-ingest keeps its observation key, so the one-per-number pick drops nothing today: nih_amount_twin_years
    # counts what it drops if a producer ever binds a number twice.
    c.artifact("nih_year_cost", f"""SELECT observation_key,amount,currency FROM {r}nih_obs WHERE is_direct AND provenance='nih_exporter'
      QUALIFY row_number() OVER (PARTITION BY stable_id,number ORDER BY priority DESC,updated_date DESC NULLS LAST,observation_key)=1""")
    c.artifact("nih_span", f"""SELECT o.stable_id,min(o.start_date) start_date,max(o.end_date) end_date,sum(y.amount) amount_sum,
        count(DISTINCT y.currency) currencies,count_if(y.observation_key IS NOT NULL AND y.amount IS NULL) years_without_cost
      FROM {r}nih_obs o LEFT JOIN {r}nih_year_cost y USING(observation_key) GROUP BY o.stable_id""")
    c.count("nih_title_from_missing", f"(SELECT n.stable_id FROM {r}nih_choice n LEFT ANTI JOIN {r}nih_title_row t USING(stable_id) WHERE n.title_from IS NOT NULL) x")
    c.count("nih_amount_twin_years", f"""(SELECT o.observation_key FROM {r}nih_obs o LEFT ANTI JOIN {r}nih_year_cost y USING(observation_key)
      WHERE o.is_direct AND o.provenance='nih_exporter') x""")
    # projects whose sum misses a bound year that has no cost (ExPORTER's FY1985-1999 project files carry none): under sum they get no
    # amount, so this counts the totals option B withholds
    c.count("nih_amount_partial", f"(SELECT stable_id FROM {r}nih_span WHERE amount_sum>0 AND years_without_cost>0) x")
    title = s["title"] == "chosen_year"
    amount = {"null": "CAST(NULL AS DOUBLE)", "sum": "CASE WHEN sp.currencies<=1 AND sp.amount_sum>0 AND sp.years_without_cost=0 THEN sp.amount_sum END", "elected": "a.amount"}[s["amount"]]
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
    # ExPORTER records of a folded project still served as their own award (not on the reviewed list, held tonight, or arrived after
    # it): their money is on their own record, not in the project's sum
    c.count("nih_amount_years_outside", f"""(SELECT DISTINCT a.id FROM {r}awards_candidate a
      JOIN (SELECT DISTINCT upper(get_json_object(evidence_json,'$.core')) core FROM migration_manifest_v WHERE evidence_class={literal(NIH_CLASS)}) m
        ON {STRICT_SQL.format(n='upper(a.funder_award_id)')}=m.core
      LEFT ANTI JOIN {r}nih_choice s ON s.stable_id=a.id
      WHERE a.provenance='nih_exporter' AND a.funder_id IN {FAMILY_SQL}) x""")


# ---------- work_awards: link conservation on the night family pairs retire (same contract as uk_lineage.link_conservation) ----------
def link_conservation(c):
    """Tonight's family retirements, per listed class (NIH: artifacts nih_*, gates NIH_*; EU: eu_*, EU_*): no paper keeps a link to
    a retired record, and every paper that linked to one now links to its survivor (papers merged away tonight are counted as their
    surviving paper). Counts moved vs already-on-survivor."""
    if getattr(c, "nih", False):
        _link_conservation(c, NIH_CLASS, "nih_", "NIH_")
    if _eu(c):
        _link_conservation(c, EU_CLASS, "eu_", "EU_")


def _link_conservation(c, cls, a, gate):
    r = c.r; live = c.outputs["work_awards"]; g = "concat('https://openalex.org/G',{})"
    c.artifact(a + "applied", f"SELECT old_id loser_id,canonical_id target_id FROM {r}retirements_manifest WHERE evidence_class={literal(cls)}")
    # equality joins only: an IN(a,b) join against 60M links runs as a near cross product (dev 10-01: timed out at 9,000 s)
    c.artifact(a + "award_ids", f"""SELECT DISTINCT award_id FROM (SELECT {g.format('loser_id')} award_id FROM {r}{a}applied
      UNION ALL SELECT {g.format('target_id')} FROM {r}{a}applied)""")
    c.artifact(a + "links_before", f"""SELECT DISTINCT coalesce(sv.survivor_work_id,w.work_id) work_id,w.award.id award_id FROM {live} w
      JOIN {r}{a}award_ids i ON i.award_id=w.award.id
      LEFT JOIN {r}work_survivors sv ON sv.loser_work_id=w.work_id""")
    c.artifact(a + "candidate_edges", f"""SELECT DISTINCT w.work_id,w.award.id award_id FROM {r}work_awards_candidate w
      JOIN {r}{a}award_ids i ON i.award_id=w.award.id""")
    c.zero(gate + "NO_LOSER_LINKS", f"SELECT e.* FROM {r}{a}candidate_edges e JOIN {r}{a}applied m ON e.award_id={g.format('m.loser_id')}")
    c.artifact(a + "missing_mapped", f"""SELECT DISTINCT b.work_id,{g.format('m.target_id')} award_id FROM {r}{a}links_before b
      JOIN {r}{a}applied m ON b.award_id={g.format('m.loser_id')}
      EXCEPT SELECT work_id,award_id FROM {r}{a}candidate_edges""")
    # a paper whose acknowledgement vanished upstream tonight is an explained removal; only links tonight's producers still
    # tie to the retired record or its project must survive (same contract as uk_lineage's supported checks)
    c.artifact(a + "supported", f"""SELECT DISTINCT wm.work_id,{g.format('x.target_id')} award_id FROM {r}work_matches wm
      JOIN (SELECT loser_id id,target_id FROM {r}{a}applied UNION SELECT target_id,target_id FROM {r}{a}applied) x ON x.id=wm.stable_id
      EXCEPT SELECT * FROM ({list_legs.removed_pairs_sql(c)})""")   # a pinned SUPPRESS row is an explained removal
    c.zero(gate + "MAPPED_LINKS_PRESERVED", f"SELECT m.* FROM {r}{a}missing_mapped m LEFT SEMI JOIN {r}{a}supported s ON s.work_id=m.work_id AND s.award_id=m.award_id")
    if cls == EU_CLASS:
        # the survivor's own links stay too (registry_twin's REGISTRY_TWIN_TARGET_LINKS_PRESERVED contract): a target link missing
        # tonight is explained only when no producer makes it any more or a pinned SUPPRESS row removes it
        c.artifact(a + "missing_target", f"""SELECT b.* FROM {r}{a}links_before b JOIN (SELECT DISTINCT target_id FROM {r}{a}applied) m
          ON b.award_id={g.format('m.target_id')} EXCEPT SELECT work_id,award_id FROM {r}{a}candidate_edges""")
        c.artifact(a + "target_producers", f"""SELECT wm.work_id,{g.format('wm.stable_id')} award_id FROM {r}work_matches wm
            JOIN (SELECT DISTINCT target_id FROM {r}{a}applied) t ON t.target_id=wm.stable_id
          UNION ALL SELECT x.work_id,x.award_id FROM ({list_legs.added_pairs_sql(c)}) x JOIN {r}{a}award_ids i ON i.award_id=x.award_id
          UNION ALL SELECT mv.work_id,{g.format('mv.stable_id')} FROM {r}work_links_moved mv
            JOIN (SELECT DISTINCT target_id FROM {r}{a}applied) t ON t.target_id=mv.stable_id WHERE mv.work_id<>mv.original_work_id""")
        c.zero(gate + "TARGET_LINKS_PRESERVED", f"""SELECT e.* FROM {r}{a}missing_target e LEFT SEMI JOIN {r}{a}target_producers p
          ON p.work_id=e.work_id AND p.award_id=e.award_id
          LEFT ANTI JOIN ({list_legs.removed_pairs_sql(c)}) s ON s.work_id=e.work_id AND s.award_id=e.award_id""")
    c.counts[a + "explained_removals"] = int(c.sql(f"""SELECT count(*) n FROM {r}{a}missing_mapped m
      LEFT ANTI JOIN {r}{a}supported s ON s.work_id=m.work_id AND s.award_id=m.award_id""").collect()[0].n)
    row = c.sql(f"""WITH marks AS (
        SELECT b.work_id,m.target_id,1 l,0 t FROM {r}{a}links_before b JOIN {r}{a}applied m ON b.award_id={g.format('m.loser_id')}
        UNION ALL SELECT b.work_id,m.target_id,0,1 FROM {r}{a}links_before b JOIN {r}{a}applied m ON b.award_id={g.format('m.target_id')}),
      e AS (SELECT work_id,target_id,max(l) l,max(t) t FROM marks GROUP BY 1,2)
      SELECT (SELECT count(*) FROM {r}{a}applied) pairs,count_if(l=1 AND t=0) moved,count_if(l=1 AND t=1) deduplicated FROM e""").collect()[0]
    for k in ("pairs", "moved", "deduplicated"):
        c.counts[a + k] = int(row[k])
