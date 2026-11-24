"""A French lab code (RNSR), as ANR records it on its grants, to ROR: optional config key `org_id_rors` (oxjob #123.5, ZD 27257).

The table it names, openalex.awards.org_id_rors (id_type, org_id, ror, mapping_type, source), is loaded by
notebooks/awards/CreateOrgIdRors.sql from the dated copy of scanR's public rnsr_ror.json (French research ministry) that
scripts/local/scanr_mappings_to_s3.py pinned in S3: the lab's own ROR (mapping_type 'self') or, for a lab with none, its
supervising institutions' RORs ('institution'). scanR's 'ambiguous' and 'none' rows are not loaded. Hand-checked 23 Nov:
30 of 30 random codes right. Reader: sql/api_payload.sql (funder_rors), where a code's RORs join the grant's institutions
as a ROR the funder gives does, next to the name's match.
bind() runs in bind_inputs at run start, like list_legs: the table is read at one version for the whole run, and a missing,
malformed or truncated table stops the run before anything is built. Not configured = an empty relation and no change.
"""

EMPTY = ("SELECT CAST(NULL AS STRING) id_type,CAST(NULL AS STRING) org_id,CAST(NULL AS STRING) ror,"
         "CAST(NULL AS STRING) mapping_type WHERE false")
MIN_ROWS = 8500      # 23 Nov: 10,920 (2,256 self + 8,664 institution); fewer is a broken load, and would quietly drop labs


def bind(c):
    table = c.config.get('org_id_rors')
    if not table:
        return
    c.bind('org_id_rors', table)
    c.zero('ORG_ID_RORS_SHAPE', """SELECT * FROM org_id_rors_v WHERE NOT coalesce(id_type='rnsr' AND org_id RLIKE '^[0-9]{9}[A-Z]$'
        AND mapping_type IN ('self','institution') AND ror RLIKE '^https://ror.org/0[0-9a-z]{6}[0-9]{2}$',false)""")
    c.zero('ORG_ID_RORS_UNIQUE', 'SELECT id_type,org_id,ror FROM org_id_rors_v GROUP BY ALL HAVING count(*)<>1')
    c.zero('ORG_ID_RORS_ONE_TYPE_PER_CODE', 'SELECT id_type,org_id FROM org_id_rors_v GROUP BY ALL HAVING count(DISTINCT mapping_type)<>1')
    c.zero('ORG_ID_RORS_TOO_FEW', f'SELECT n FROM (SELECT count(*) n FROM org_id_rors_v) WHERE n < {MIN_ROWS}')
    c.count('org_id_rors_rows', 'org_id_rors_v')


def relation(c):
    if not c.config.get('org_id_rors'):
        return c.artifact('org_id_rors_empty', EMPTY)
    c.require('org_id_rors' in c.versions, 'ORG_ID_RORS_NOT_BOUND: bind_inputs binds it at run start')
    return 'org_id_rors_v'
