"""PDF grant-number backfill for funders with a new grant feed (replaces the manual BackfillPdfAwardMatches step, 10-01).

TagPdfAwardsIncremental only matches PDFs parsed inside its own window, so when a funder's grant list arrives later, papers
parsed earlier never get matched against it. Kyle closed that gap by hand after each funder batch
(notebooks/awards/BackfillPdfAwardMatches.sql). This step does the same pass every night AFTER the night is finished and public
(its own notebook cell): the pass can take hours for a big batch (the manual run for 58 funders ran 4+ hours on 10-01), so it
must not sit in front of the night. The matches it writes are read by the next night's link build, the same night the manual
step's matches would be read.

Which funders: a funder is due when its PUBLISHED direct grant sources (raw provenances with priority >= 3, as the last
successful run read them) include a source not yet recorded for it in the ledger. A brand-new funder counts the same way: its
first published direct source makes it due. Grants are matched only after they're published, exactly like the manual step.
Ledger: ONE table, {registry}award_nightly_pdf_backfill, one row per funder:
  funder_id, sources (direct sources this row's status covers), first_seen_run, backfill_run, pairs_found, status, detail, updated_at.
  status: SEEDED (present when the ledger was created, treated as done), DONE, HELD.
  HELD for a person (too big, or over the pair cap): sources are recorded, so the funder is NOT retried until a person deletes
  the row (retry) or runs the manual notebook and sets status DONE. HELD after an error: sources are not recorded, so the
  funder is retried the next night.
Matching: BackfillPdfAwardMatches.sql steps 1-3 (itself TagPdfAwardsIncremental steps 2 and 5) for the due funders; a test pins
the text token for token. One addition: the funder-name list is cut to the names that can matter (`pdfbf_names`: the due
funders' names and every name that contains one of them). A longer name can only cover a due funder's name if it contains it,
so the result is the same; the manual notebook tries all ~48,500 names against every paper, which is most of its run time.
New (paper, funder, number) rows are appended to the PDF match table (idempotent anti-join).
Fuses: a funder's cost is its papers x grant numbers (the regex checks the match runs). A funder over `max_checks_per_funder`
is HELD for a person without being matched. The others run cheapest first up to `max_checks_per_night`; the rest stay due for
the next night. A funder with more than `max_pairs_per_funder` new pairs is HELD unwritten for a person. Nothing here raises:
any error holds the funders being processed and is reported in counts.
"""
import json

LEDGER_COLUMNS = ("funder_id BIGINT, sources ARRAY<STRING>, first_seen_run STRING, backfill_run STRING, pairs_found BIGINT, "
                  "status STRING, detail STRING, updated_at TIMESTAMP")
# Budget: 4e9 checks per funder (measured 09-25 on the FCT pilot). The night's total is 4e9 too, not the pilot's 6e9: the match
# is one statement, serverless cancels a statement at 9,000 s, and the 10-01 dev run did 3.8e9 checks in 4,201 s (0.9M/s), so
# 6e9 would sit at about 6,600 s of the 9,000. A cancelled night holds every due funder and retries them all the next night.
DEFAULTS = dict(max_checks_per_funder=4_000_000_000, max_checks_per_night=4_000_000_000, max_pairs_per_funder=100000)


def ledger(c):
    return c.p + "award_nightly_pdf_backfill"


def settings(c):
    s = c.config.get("pdf_backfill")
    return None if not s else {**DEFAULTS, **s}


def published_raw(c):
    """The raw table at the version the last successful run read (what is public now); None before the first success."""
    rows = c.sql(f"""SELECT details_json FROM {c.p}award_nightly_runs WHERE status LIKE 'SUCCEEDED%' AND details_json IS NOT NULL
        ORDER BY started_at DESC LIMIT 1""").collect()
    if not rows:
        return None
    raw = json.loads(rows[0].details_json).get("versions", {}).get("raw") or {}
    return f"{raw['relation']} VERSION AS OF {int(raw['version'])}" if "version" in raw else None


def due_funders(c, s, raw):
    """Funders whose published direct sources are not all in the ledger yet; seeds the ledger on its first night."""
    L = ledger(c)
    try:
        c.sql(f"SELECT * FROM {L} LIMIT 0").collect()
    except Exception as exc:                              # first night: create the ledger (the write fence has no IF NOT EXISTS form)
        if "TABLE_OR_VIEW_NOT_FOUND" not in str(exc):
            raise
        c.write(f"CREATE TABLE {L} ({LEDGER_COLUMNS})")
    c.artifact("pdfbf_sources", f"""SELECT funder_id,sort_array(collect_set(provenance)) sources FROM {raw}
      WHERE priority>=3 AND funder_id IS NOT NULL GROUP BY funder_id""")
    if not c.sql(f"SELECT 1 FROM {L} LIMIT 1").collect():
        # seed_as_due: funders whose backfill had not been run by hand when the ledger was created (they start due)
        due_ids = ",".join(str(int(x)) for x in s.get("seed_as_due", [])) or "NULL"
        c.write(f"""INSERT INTO {L} SELECT funder_id,sources,:rid,:rid,CAST(NULL AS BIGINT),'SEEDED',
          'ledger created: sources published before this run count as done',current_timestamp() FROM {c.r}pdfbf_sources
          WHERE funder_id NOT IN ({due_ids})""")
        c.counts["pdf_backfill_seeded"] = c.count("pdfbf_seeded", L)
    return c.artifact("pdfbf_due", f"""SELECT s.funder_id,s.sources,coalesce(l.first_seen_run,:rid) first_seen_run
      FROM {c.r}pdfbf_sources s LEFT JOIN {L} l USING(funder_id)
      WHERE l.funder_id IS NULL OR size(array_except(s.sources,coalesce(l.sources,array())))>0""")


def match_sql(c, funders_view, t):
    """BackfillPdfAwardMatches.sql steps 1-3 for the funders in `funders_view` (column funder_id_numeric). Returns the SELECT of
    NEW rows (paper_id, funder_id, funder_award_id, funding_sections) for target table t['grobid']."""
    r = c.r
    # names that can matter: the due funders' own names, and any name containing one (only those can cover a due funder's name)
    c.artifact("pdfbf_names", f"""SELECT DISTINCT a.id,a.name FROM {t['funder_names_keep']} a
      JOIN (SELECT k.name FROM {t['funder_names_keep']} k
        JOIN {funders_view} bf ON CAST(regexp_extract(k.id,'F(\\\\d+)',1) AS BIGINT)=bf.funder_id_numeric) d
      ON contains(lower(a.name),lower(d.name))""")
    c.artifact("pdfbf_target_works", f"""SELECT DISTINCT wf.work_id,bf.funder_id_numeric FROM {t['fulltext_work_funders']} wf
      JOIN {funders_view} bf ON wf.funder_id=CONCAT('https://openalex.org/F',bf.funder_id_numeric)""")
    c.artifact("pdfbf_sections", f"""WITH work_native AS (
        SELECT DISTINCT tw.work_id,lm.native_id,lm.native_id_namespace FROM (SELECT DISTINCT work_id FROM {r}pdfbf_target_works) tw
        JOIN {t['locations_mapped']} lm ON lm.work_id=tw.work_id WHERE lm.native_id IS NOT NULL),
      xmls AS (SELECT DISTINCT wn.work_id,g.xml_content FROM work_native wn JOIN {t['grobid_processing_results']} g
        ON g.native_id=wn.native_id AND g.native_id_namespace=wn.native_id_namespace WHERE g.xml_content IS NOT NULL),
      raw_sections AS (SELECT work_id,
        array_join(flatten(transform(regexp_extract_all(xml_content,'<funder[^>]*>(.*?)</funder>',1),
          block -> regexp_extract_all(block,'<orgName[^>]*>([^<]+)</orgName>',1))),', ') AS funders,
        array_join(transform(regexp_extract_all(xml_content,'<div[^>]*type="acknowledgement"[^>]*>(.*?)</div>',1),
          block -> regexp_replace(block,'<[^>]+>',' ')),' ') AS acknowledgement,
        array_join(transform(regexp_extract_all(xml_content,'<div[^>]*type="funding"[^>]*>(.*?)</div>',1),
          block -> regexp_replace(block,'<[^>]+>',' ')),' ') AS funding
        FROM xmls)
      SELECT DISTINCT work_id,concat_ws(' ',funders,acknowledgement,funding) AS all_sections FROM raw_sections
      WHERE funders!='' OR acknowledgement!='' OR funding!=''""")
    return f"""WITH funder_regexes AS (
        SELECT fnk.name AS funder_name,fnk.id AS funder_id,CAST(regexp_extract(fnk.id,'F(\\\\d+)',1) AS BIGINT) AS funder_id_numeric,
          fa.display_name AS funder_display_name,fa.ids.ror AS ror_id,fa.ids.doi AS doi,
          CONCAT(
            CASE WHEN fnk.id='https://openalex.org/F4320306076' AND LOWER(TRIM(fnk.name))='national science foundation' THEN '(?i)(?<!\\\\bchinese\\\\s+)'
                 WHEN fnk.id='https://openalex.org/F4320306076' AND LOWER(TRIM(fnk.name))='nsf' THEN '(?<!(?i:\\\\bchinese)\\\\s+)'
                 ELSE '' END,
            CASE WHEN fnk.name RLIKE '^[A-Z0-9\\\\.\\\\-\\\\s]+$' AND LENGTH(fnk.name)<=10
                 THEN CONCAT('\\\\b',regexp_replace(fnk.name,'([\\\\[\\\\](){{}}+*?^$.|\\\\\\\\])','\\\\\\\\$1'),'\\\\b')
                 ELSE CONCAT('(?i)\\\\b',regexp_replace(fnk.name,'([\\\\[\\\\](){{}}+*?^$.|\\\\\\\\])','\\\\\\\\$1'),'\\\\b') END,
            CASE WHEN fnk.id='https://openalex.org/F4320324089' AND LOWER(TRIM(fnk.name)) IN ('centre for quantum technologies','center for quantum technologies')
                 THEN '(?i)(?!\\\\s*(?:,\\\\s*)?(?:and|&)\\\\s+applications\\\\b)'
                 WHEN fnk.id='https://openalex.org/F4320306076' AND LOWER(TRIM(fnk.name)) IN ('national science foundation','nsf')
                 THEN '(?i)(?!\\\\s*(?:,\\\\s*)?(?:\\\\(\\\\s*NSFC?\\\\s*\\\\)\\\\s*(?:,\\\\s*)?)?of\\\\s+china\\\\b)'
                 ELSE '' END) AS match_regex
        FROM {t['funder_names_keep']} fnk JOIN {t['funders_api']} fa ON CAST(regexp_extract(fnk.id,'F(\\\\d+)',1) AS BIGINT)=fa.id
        LEFT SEMI JOIN {r}pdfbf_names keep ON keep.id=fnk.id AND keep.name=fnk.name),
      matched_aliases AS (SELECT DISTINCT fs.work_id,fs.all_sections,fr.* FROM {r}pdfbf_sections fs CROSS JOIN funder_regexes fr
        WHERE fs.all_sections RLIKE fr.match_regex),
      alias_spans AS (SELECT m.*,LENGTH(m.all_sections)-LENGTH(tail)-LENGTH(m.funder_name)+1 AS span_start,LENGTH(m.all_sections)-LENGTH(tail)+1 AS span_end
        FROM matched_aliases m LATERAL VIEW explode(regexp_extract_all(m.all_sections,CONCAT('(?=(?:',m.match_regex,')([\\\\s\\\\S]*))'),1)) occurrences AS tail),
      parent_child AS (SELECT ror_id AS parent_ror,related_ror_id AS child_ror FROM {t['ror_relationships']} WHERE LOWER(relationship_type)='child'
        UNION SELECT related_ror_id AS parent_ror,ror_id AS child_ror FROM {t['ror_relationships']} WHERE LOWER(relationship_type)='parent'),
      suppressed_spans AS (SELECT s.work_id,s.all_sections,s.funder_id_numeric,s.span_start,s.span_end FROM alias_spans s
        JOIN alias_spans l ON s.work_id=l.work_id AND s.all_sections=l.all_sections AND s.funder_id_numeric<>l.funder_id_numeric
          AND l.span_start<=s.span_start AND l.span_end>=s.span_end AND l.span_end-l.span_start>s.span_end-s.span_start
        LEFT JOIN parent_child pc ON pc.parent_ror=REPLACE(s.ror_id,'https://ror.org/','') AND pc.child_ror=REPLACE(l.ror_id,'https://ror.org/','')
        GROUP BY s.work_id,s.all_sections,s.funder_id_numeric,s.span_start,s.span_end
        HAVING MAX(CASE WHEN pc.parent_ror IS NOT NULL OR (NULLIF(REPLACE(s.ror_id,'https://ror.org/',''),'')=NULLIF(REPLACE(l.ror_id,'https://ror.org/',''),'')) THEN 1 ELSE 0 END)=0),
      surviving_funder_windows AS (SELECT DISTINCT s.work_id,s.all_sections,s.funder_id_numeric FROM alias_spans s
        LEFT ANTI JOIN suppressed_spans x ON s.work_id=x.work_id AND s.all_sections=x.all_sections AND s.funder_id_numeric=x.funder_id_numeric
          AND s.span_start=x.span_start AND s.span_end=x.span_end),
      suppressed_funder_windows AS (SELECT DISTINCT m.work_id,m.all_sections,m.funder_id_numeric FROM matched_aliases m
        LEFT ANTI JOIN surviving_funder_windows k ON m.work_id=k.work_id AND m.all_sections=k.all_sections AND m.funder_id_numeric=k.funder_id_numeric),
      funder_alt_names AS (SELECT fa.id AS funder_id,fa.display_name AS alt_name FROM {t['funders_api']} fa JOIN {funders_view} bf ON fa.id=bf.funder_id_numeric
        UNION ALL SELECT fa.id AS funder_id,alt_name FROM {t['funders_api']} fa JOIN {funders_view} bf ON fa.id=bf.funder_id_numeric
        LATERAL VIEW explode(fa.alternate_titles) alt AS alt_name),
      candidate_awards AS (SELECT oa.funder_id,oa.funder_award_id,
          CONCAT('\\\\b',regexp_replace(oa.funder_award_id,'([\\\\[\\\\](){{}}+*?^$.|\\\\\\\\])','\\\\\\\\$1'),'\\\\b') AS award_match_pattern
        FROM {t['awards']} oa JOIN {funders_view} bf ON oa.funder_id=bf.funder_id_numeric
        WHERE openalex.common.is_usable_award_id(oa.funder_award_id)),
      usable_awards AS (SELECT ca.* FROM candidate_awards ca
        LEFT ANTI JOIN {t['award_id_guard']} g ON ca.funder_id=g.funder_id AND ca.funder_award_id=g.funder_award_id AND g.verdict='garbage'
          AND COALESCE(g.reason,'') NOT LIKE 'salvaged:%'
        LEFT ANTI JOIN funder_alt_names fan ON ca.funder_award_id=fan.alt_name),
      paper_funder_sections AS (SELECT /*+ REPARTITION(512, work_id) */ s.work_id,tw.funder_id_numeric,s.all_sections
        FROM {r}pdfbf_sections s JOIN {r}pdfbf_target_works tw ON tw.work_id=s.work_id
        LEFT ANTI JOIN suppressed_funder_windows x ON s.work_id=x.work_id AND s.all_sections=x.all_sections AND tw.funder_id_numeric=x.funder_id_numeric)
    SELECT /*+ BROADCAST(ua) */ DISTINCT pfs.work_id AS paper_id,ua.funder_id,ua.funder_award_id,pfs.all_sections AS funding_sections
    FROM usable_awards ua JOIN paper_funder_sections pfs ON pfs.funder_id_numeric=ua.funder_id AND pfs.all_sections RLIKE ua.award_match_pattern
    LEFT ANTI JOIN {t['grobid']} g ON pfs.work_id=g.paper_id AND ua.funder_id=g.funder_id AND ua.funder_award_id=g.funder_award_id"""


def run(c):
    """After the night is finished and public. Never raises: every failure is recorded on the ledger as HELD and counted."""
    s = settings(c)
    if not s:
        return
    try:
        raw = published_raw(c)
        if raw is None:
            c.counts["pdf_backfill"] = "skipped: no successful run yet"
            return
        due = due_funders(c, s, raw)
        n_due = c.count("pdf_backfill_due", due)
    except Exception as exc:                              # the run log or the ledger is unreadable: report, never raise
        c.counts["pdf_backfill"] = "error: " + str(exc)[:300]
        return
    if not n_due:
        return
    L, r, t = ledger(c), c.r, s["tables"]
    pair_cap, funder_cap = int(s["max_pairs_per_funder"]), int(s["max_checks_per_funder"])
    night_cap = max(int(s["max_checks_per_night"]), funder_cap)     # a funder under its own cap always fits a night
    done, held = 0, n_due
    try:
        # size first: papers that name the funder x its published grant numbers = the regex checks the match would run
        c.artifact("pdfbf_size", f"""SELECT d.funder_id,coalesce(w.works,0) works,coalesce(a.awards,0) awards,
            coalesce(w.works,0)*coalesce(a.awards,0) checks FROM {due} d
          LEFT JOIN (SELECT CAST(regexp_extract(funder_id,'F(\\\\d+)',1) AS BIGINT) funder_id,count(DISTINCT work_id) works
            FROM {t['fulltext_work_funders']} GROUP BY 1) w USING(funder_id)
          LEFT JOIN (SELECT funder_id,count(*) awards FROM {t['awards']} GROUP BY 1) a USING(funder_id)""")
        # tonight: every too-big funder (held below), and the cheapest others up to the night's budget; the rest stay due
        c.artifact("pdfbf_tonight", f"""SELECT funder_id,works,awards,checks FROM (SELECT *,
            sum(CASE WHEN checks<={funder_cap} THEN checks ELSE 0 END) OVER (ORDER BY checks,funder_id ROWS UNBOUNDED PRECEDING) running
          FROM {r}pdfbf_size) WHERE checks>{funder_cap} OR running<={night_cap}""")
        c.artifact("pdfbf_funders", f"SELECT funder_id funder_id_numeric FROM {r}pdfbf_tonight WHERE checks<={funder_cap}")
        c.artifact("pdfbf_new", match_sql(c, f"{r}pdfbf_funders", t))
        c.artifact("pdfbf_per_funder", f"""SELECT d.funder_id,d.sources,d.first_seen_run,z.works,z.awards,coalesce(p.pairs,0) pairs,
            CASE WHEN z.checks>{funder_cap} OR coalesce(p.pairs,0)>{pair_cap} THEN 'HELD' ELSE 'DONE' END status,
            CASE WHEN z.checks>{funder_cap} THEN concat('held for a person, too big for the nightly step: ',z.works,
                   ' papers x ',z.awards,' grant numbers > {funder_cap} checks; run BackfillPdfAwardMatches by hand, then set status DONE')
                 WHEN coalesce(p.pairs,0)>{pair_cap} THEN concat('held for a person, over the cap: ',p.pairs,
                   ' new pairs > {pair_cap}; nothing written; delete this row to retry') END detail
          FROM {due} d JOIN {r}pdfbf_tonight z USING(funder_id)
          LEFT JOIN (SELECT funder_id,count(*) pairs FROM {r}pdfbf_new GROUP BY 1) p USING(funder_id)""")
        c.write(f"""INSERT INTO {t['grobid']} (paper_id,funder_id,funder_award_id,funding_sections,batch_time)
          SELECT n.paper_id,n.funder_id,n.funder_award_id,n.funding_sections,now() FROM {r}pdfbf_new n
          JOIN {r}pdfbf_per_funder p ON p.funder_id=n.funder_id AND p.status='DONE'""")
        # sources are recorded for HELD rows too: held for a person means not retried until a person acts on the row
        c.write(f"""MERGE INTO {L} l USING {r}pdfbf_per_funder u ON l.funder_id=u.funder_id
          WHEN MATCHED THEN UPDATE SET sources=u.sources,backfill_run=:rid,pairs_found=u.pairs,status=u.status,detail=u.detail,
            updated_at=current_timestamp()
          WHEN NOT MATCHED THEN INSERT (funder_id,sources,first_seen_run,backfill_run,pairs_found,status,detail,updated_at)
            VALUES (u.funder_id,u.sources,u.first_seen_run,:rid,u.pairs,u.status,u.detail,current_timestamp())""")
        row = c.sql(f"""SELECT count_if(status='DONE') done,count_if(status='HELD') held,
          coalesce(sum(CASE WHEN status='DONE' THEN pairs END),0) written FROM {r}pdfbf_per_funder""").collect()[0]
        done, held = int(row.done), int(row.held)
        c.counts["pdf_backfill_pairs_written"] = int(row.written)
        c.counts["pdf_backfill_waiting"] = n_due - done - held      # over tonight's budget: still due tomorrow
    except Exception as exc:                              # hold every due funder; their sources stay unrecorded, so they retry
        detail = ("step failed: " + str(exc))[:1000]
        try:
            c.write(f"""MERGE INTO {L} l USING (SELECT funder_id,first_seen_run FROM {due}) u ON l.funder_id=u.funder_id
              WHEN MATCHED THEN UPDATE SET backfill_run=:rid,status='HELD',detail=:detail,updated_at=current_timestamp()
              WHEN NOT MATCHED THEN INSERT (funder_id,sources,first_seen_run,backfill_run,pairs_found,status,detail,updated_at)
                VALUES (u.funder_id,array(),u.first_seen_run,:rid,NULL,'HELD',:detail,current_timestamp())""", {"detail": detail})
        except Exception:
            pass
        done, held = 0, n_due
        c.counts["pdf_backfill_error"] = detail[:300]
    c.counts["pdf_backfill_done"], c.counts["pdf_backfill_held"] = done, held
