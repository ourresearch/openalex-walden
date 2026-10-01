"""UK GtR duplicate records (class PUBLIC_GTR_LINEAGE_TWIN), ported 10-01 from the 09-24 UK packet (uk_twins.py).

A published gtr_legacy record retires into the unique served, titled gateway_to_research record of the same GtR project.
Proof (unchanged from 09-24): both DIRECT; the loser was born from a GtR NATIVE key; same funder and award key; loser untitled
legacy, target titled current feed; award_keys is the only bridge (the loser's birth GtR id is a STAGING key the target owns).
Changes for the nightly: a pair failing the proof is HELD for the night (counted), not a stop; a target claimed by two losers
still stops the run. Link conservation stays strict (no loser links, every loser link mapped to the target, target links still
supported by a producer are kept, target extras must be explained). The 09-24 exact count contract becomes a report.
"""
from stable_award_ids import literal

LINEAGE_TWIN_CLASS = 'PUBLIC_GTR_LINEAGE_TWIN'


def enabled(c):
    return c.has_extra('migration_manifest') and bool(c.sql(
        f"SELECT 1 FROM migration_manifest_v WHERE evidence_class={literal(LINEAGE_TWIN_CLASS)} LIMIT 1").collect())


def filter(c, mm):
    """Runs right after the shell filter; returns mm without held lineage pairs."""
    if not enabled(c):
        return mm
    p, r = c.p, c.r
    cls = literal(LINEAGE_TWIN_CLASS)
    c.artifact('uk_pairs', f"SELECT loser_id,target_id FROM {mm} WHERE evidence_class={cls}")
    c.zero('UK_LINEAGE_TARGET_UNIQUE', f"SELECT target_id FROM {r}uk_pairs GROUP BY target_id HAVING count(DISTINCT loser_id)<>1")
    c.artifact('uk_held', f"""SELECT DISTINCT loser_id,reason FROM (
        SELECT m.loser_id,'PROOF' reason FROM {r}uk_pairs m
          LEFT JOIN {r}entities_allocated l ON l.stable_id=m.loser_id LEFT JOIN {r}entities_allocated t ON t.stable_id=m.target_id
          LEFT JOIN awards_v la ON la.id=m.loser_id LEFT JOIN awards_v ta ON ta.id=m.target_id
          WHERE NOT COALESCE(l.entity_kind='DIRECT' AND t.entity_kind='DIRECT'
            AND l.birth_key:type='NATIVE' AND l.birth_key:namespace='gtr'
            AND l.funder_id <=> t.funder_id AND l.award_key <=> t.award_key
            AND (la.display_name IS NULL OR trim(la.display_name)='')
            AND la.provenance='gtr_legacy' AND ta.provenance='gateway_to_research'
            AND la.funder_id <=> ta.funder_id AND la.funder_award_id <=> ta.funder_award_id
            AND ta.display_name IS NOT NULL AND trim(ta.display_name)<>'',false)
        UNION ALL SELECT m.loser_id,'KEYS' FROM {r}uk_pairs m JOIN {r}entities_allocated l ON l.stable_id=m.loser_id
          LEFT JOIN {p}award_keys n ON n.stable_id=m.loser_id AND n.key_type='NATIVE' AND n.namespace='gtr' AND n.source_record_id=l.birth_key:source_record_id
          LEFT JOIN {p}award_keys k ON k.key_type='STAGING' AND k.staging_id=try_cast(l.birth_key:source_record_id AS BIGINT)
            AND k.funder_id=l.funder_id AND k.award_key=l.award_key AND k.stable_id=m.target_id
          GROUP BY m.loser_id HAVING count(n.stable_id)<>1 OR count(k.stable_id)<>1
        UNION ALL SELECT m.loser_id,'NATIVE_COUNT' FROM {r}uk_pairs m LEFT JOIN
          (SELECT stable_id,count(*) n FROM {p}award_keys WHERE key_type='NATIVE' GROUP BY 1) k ON k.stable_id=m.loser_id WHERE coalesce(k.n,0)<>1
        UNION ALL SELECT m.loser_id,'HAS_ANCESTOR' FROM {r}uk_pairs m JOIN {r}entities_allocated x ON x.redirect_to=m.loser_id AND x.status='REDIRECTED')""")
    pairs = c.count('uk_pairs', r + 'uk_pairs')
    c.count('uk_held', r + 'uk_held')
    for row in c.sql(f"SELECT reason,count(DISTINCT loser_id) n FROM {r}uk_held GROUP BY 1").collect():
        c.counts['uk_held_' + row.reason.lower()] = int(row.n)
    c.zero('UK_LINEAGE_HELD_FUSE', f"SELECT count(DISTINCT loser_id) n FROM {r}uk_held HAVING n>greatest(100,0.01*{pairs})")
    return c.artifact('migration_proven_uk', f"SELECT m.* FROM {mm} m LEFT ANTI JOIN {r}uk_held h ON h.loser_id=m.loser_id")


def winner_metadata(c):
    """The surviving titled record keeps its own published title, funder and number."""
    if not enabled(c):
        return
    r = c.r
    c.zero('UK_LINEAGE_WINNER_METADATA', f"""SELECT m.* FROM {r}retirements_manifest m
      LEFT JOIN awards_v b ON b.id=m.canonical_id LEFT JOIN {r}awards_candidate a ON a.id=m.canonical_id
      WHERE m.evidence_class={literal(LINEAGE_TWIN_CLASS)} AND NOT COALESCE(
        a.provenance='gateway_to_research' AND a.display_name IS NOT NULL AND trim(a.display_name)<>''
        AND a.display_name=b.display_name AND a.funder_id=b.funder_id AND a.funder_award_id=b.funder_award_id,false)""")


def link_conservation(c):
    """Strict link gates for tonight's lineage retirements (09-24 contract, counts reported instead of pinned)."""
    if not enabled(c):
        return
    r = c.r
    live = c.outputs['work_awards']
    c.artifact('uk_applied', f"SELECT old_id loser_id,canonical_id target_id FROM {r}retirements_manifest WHERE evidence_class={literal(LINEAGE_TWIN_CLASS)}")
    # Last night's links, with each paper mapped to its surviving paper (the job moves links off merged papers the same night).
    c.artifact('uk_links_before', f"""SELECT DISTINCT coalesce(sv.survivor_work_id,w.work_id) work_id,w.award.id award_id FROM {live} w
      LEFT JOIN {r}work_survivors sv ON sv.loser_work_id=w.work_id""")
    c.artifact('uk_candidate_edges', f"SELECT DISTINCT work_id,award.id award_id FROM {r}work_awards_candidate")
    c.artifact('uk_links_expected', f"""SELECT DISTINCT work_id,award_id FROM (
        SELECT b.work_id,concat('https://openalex.org/G',m.target_id) award_id FROM {r}uk_links_before b JOIN {r}uk_applied m ON b.award_id=concat('https://openalex.org/G',m.loser_id)
        UNION ALL SELECT b.work_id,b.award_id FROM {r}uk_links_before b JOIN {r}uk_applied m ON b.award_id=concat('https://openalex.org/G',m.target_id))""")
    c.artifact('uk_loser_edges', f"""SELECT b.* FROM {r}uk_candidate_edges b JOIN {r}uk_applied m ON b.award_id=concat('https://openalex.org/G',m.loser_id)""")
    c.artifact('uk_mapped_expected', f"""SELECT DISTINCT b.work_id,concat('https://openalex.org/G',m.target_id) award_id
      FROM {r}uk_links_before b JOIN {r}uk_applied m ON b.award_id=concat('https://openalex.org/G',m.loser_id)""")
    c.artifact('uk_missing_mapped', f"SELECT * FROM {r}uk_mapped_expected EXCEPT SELECT * FROM {r}uk_candidate_edges")
    c.artifact('uk_missing_target', f"""SELECT b.* FROM {r}uk_links_before b JOIN {r}uk_applied m ON b.award_id=concat('https://openalex.org/G',m.target_id)
      EXCEPT SELECT * FROM {r}uk_candidate_edges""")
    c.artifact('uk_target_extra', f"""SELECT b.* FROM {r}uk_candidate_edges b JOIN {r}uk_applied m ON b.award_id=concat('https://openalex.org/G',m.target_id)
      EXCEPT SELECT * FROM {r}uk_links_expected""")
    c.artifact('uk_link_producers', f"""SELECT work_id,concat('https://openalex.org/G',stable_id) award_id,source FROM {r}work_matches WHERE stable_id IS NOT NULL
      UNION ALL SELECT g.work_id,concat('https://openalex.org/G',x.stable_id),'gtr_native' FROM gtr_v g
        JOIN {r}keys_candidate k ON k.key_type='NATIVE' AND k.namespace='gtr' AND k.source_record_id=cast(g.id AS STRING)
        JOIN {r}resolve_candidate x ON x.owner_id=k.stable_id AND x.status='ACTIVE' WHERE g.work_id IS NOT NULL""")
    c.artifact('uk_unaccounted_extra', f"SELECT e.* FROM {r}uk_target_extra e LEFT ANTI JOIN {r}uk_link_producers p ON p.work_id=e.work_id AND p.award_id=e.award_id")
    c.artifact('uk_supported_missing_target', f"SELECT e.* FROM {r}uk_missing_target e LEFT SEMI JOIN {r}uk_link_producers p ON p.work_id=e.work_id AND p.award_id=e.award_id")
    c.zero('UK_LINEAGE_NO_LOSER_LINKS', f"SELECT * FROM {r}uk_loser_edges")
    c.zero('UK_LINEAGE_MAPPED_LINKS_PRESERVED', f"SELECT * FROM {r}uk_missing_mapped")
    c.zero('UK_LINEAGE_TARGET_LINKS_PRESERVED', f"SELECT * FROM {r}uk_supported_missing_target")
    c.zero('UK_LINEAGE_TARGET_EXTRAS_UNACCOUNTED', f"SELECT * FROM {r}uk_unaccounted_extra")
    row = c.sql(f"""WITH marks AS (
        SELECT m.loser_id,b.work_id,1 l,0 t FROM {r}uk_applied m JOIN {r}uk_links_before b ON b.award_id=concat('https://openalex.org/G',m.loser_id)
        UNION ALL SELECT m.loser_id,b.work_id,0,1 FROM {r}uk_applied m JOIN {r}uk_links_before b ON b.award_id=concat('https://openalex.org/G',m.target_id)),
      e AS (SELECT loser_id,work_id,max(l) l,max(t) t FROM marks GROUP BY 1,2)
      SELECT (SELECT count(*) FROM {r}uk_applied) pairs,count_if(l=1 AND t=0) moved,count_if(l=1 AND t=1) deduplicated,count(*) final_links FROM e""").collect()[0]
    for k in ('pairs', 'moved', 'deduplicated', 'final_links'):
        c.counts['uk_' + k] = int(row[k])
    c.counts['uk_target_extra'] = c.count('uk_target_extra_n', r + 'uk_target_extra')
    c.counts['uk_explained_target_removals'] = int(c.sql(f"SELECT (SELECT count(*) FROM {r}uk_missing_target)-(SELECT count(*) FROM {r}uk_supported_missing_target) n").collect()[0].n)
