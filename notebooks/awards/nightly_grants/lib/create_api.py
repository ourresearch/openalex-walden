"""API hydration ported from deployed stableid-r1-prod lib/create_api.py; content-hash expression preserved verbatim."""
from pathlib import Path
from stable_award_ids import ident


def run(c):
    c.guard(); r=c.r
    c.zero('API_INPUT_RESOLUTION',f"SELECT a.* FROM {r}awards_candidate a LEFT ANTI JOIN {r}resolve_candidate e ON e.owner_id=a.id AND e.stable_id=a.id AND e.status='ACTIVE'")
    c.zero('API_INPUT_RELEASE',f'SELECT * FROM {r}awards_candidate WHERE release_id<>:rid OR release_id IS NULL')
    c.zero('INSTITUTION_LOOKUP_UNIQUE','SELECT raw_affiliation_string FROM affiliation_lookup_v GROUP BY raw_affiliation_string HAVING count(*)<>1')
    c.zero('INSTITUTION_API_UNIQUE','SELECT id FROM institutions_api_v GROUP BY id HAVING count(*)<>1')
    template=(Path(c.config['package_root'])/'sql/api_payload.sql').read_text()
    template=template.replace('{AWARDS}',r+'awards_candidate')
    c.artifact('api_payload',f'SELECT a.*,:rid release_id FROM ({template}) a')
    prev='previous_api_v'   # yesterday's published awards_api, bound at run start (was the release's api_final pin)
    base=(Path(c.config['package_root'])/'sql/api_hash_expression.sql').read_text().strip()
    # Sub-award links: an award with none hashes exactly as before; yesterday's table may predate the columns.
    expression=(f"CASE WHEN coalesce(size(parent_awards_full),0)=0 AND coalesce(size(sub_awards_full),0)=0 THEN {base} "
                f"ELSE xxhash64(concat_ws('|',CAST({base} AS STRING),to_json(parent_awards_full),to_json(sub_awards_full))) END")
    has_links=set(c.sql(f'SELECT * FROM {prev} LIMIT 0').columns)>={'parent_awards_full','sub_awards_full'}
    c.artifact('previous_api_hash',f'SELECT id,updated_date,{expression if has_links else base} content_hash FROM {prev}')
    c.zero('PREVIOUS_API_HASH_UNIQUE',f'SELECT id FROM {r}previous_api_hash GROUP BY id HAVING count(*)<>1')
    c.artifact('new_api_hash',f'SELECT id,{expression} content_hash FROM {r}api_payload')
    c.artifact('api_candidate',f"""SELECT a.* EXCEPT(updated_date),
      CASE WHEN p.id IS NULL OR NOT(n.content_hash <=> p.content_hash) THEN date_trunc('SECOND',CAST(:ts AS TIMESTAMP)) ELSE p.updated_date END updated_date
      FROM {r}api_payload a JOIN {r}new_api_hash n USING(id) LEFT JOIN {r}previous_api_hash p USING(id)""")
    c.zero('API_UNIQUE',f'SELECT id FROM {r}api_candidate GROUP BY id HAVING count(*)<>1 OR id IS NULL')
    c.zero('API_SET_EQUAL',f'(SELECT id FROM {r}api_candidate EXCEPT SELECT id FROM {r}awards_candidate) UNION ALL (SELECT id FROM {r}awards_candidate EXCEPT SELECT id FROM {r}api_candidate)')
    c.zero('API_UPDATED_DATE',f'SELECT id FROM {r}api_candidate WHERE updated_date IS NULL')
    c.zero('API_UNCHANGED_DATES',f'SELECT a.id FROM {r}api_candidate a JOIN {r}new_api_hash n USING(id) JOIN {r}previous_api_hash p USING(id) WHERE n.content_hash=p.content_hash AND NOT(a.updated_date <=> p.updated_date)')
    c.count('api',r+'api_candidate')
