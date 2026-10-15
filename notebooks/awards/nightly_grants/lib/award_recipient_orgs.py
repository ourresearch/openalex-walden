"""Organisation recipients only; never feeds institution_awarded or geographic share selection.

Reuse stored matcher answers (including authoritative empty answers), explicit ROR ids,
and unambiguous exact institution names. A multi-institution answer is ambiguous for one
recipient: retain its source name rather than invent a split. Country means organisation
location, never place of performance. Missing shares stay null; roles need not partition
an award's money. Arrays are deterministic and uncapped internally, first 100 for display.
"""
from pathlib import Path
import csv
from award_country import sql_string
import award_keys
import funding_population

EMPTY_ANSWERS = "SELECT CAST(NULL AS STRING) raw_affiliation_string,CAST(NULL AS ARRAY<BIGINT>) institution_ids WHERE false"


def exclusion_relation_sql(package_root):
    """Reviewed wrong name/id pairs only; never changes the shared matcher or institution_awarded."""
    with open(Path(package_root) / 'recipients/match_exclusions.csv', newline='', encoding='utf-8') as fh:
        rows = list(csv.DictReader(fh))
    seen = set()
    values = []
    for row in rows:
        key = (row['name'].strip().lower(), row['institution_id'])
        if not key[0] or not key[1].isdigit() or key in seen or not row['reason'] or not row['evidence']:
            raise RuntimeError('RECIPIENT_MATCH_EXCLUSION_INVALID: ' + repr(row))
        seen.add(key)
        values.append(f"({sql_string(key[0])},{int(key[1])})")
    if not values:
        return 'SELECT CAST(NULL AS STRING) name,CAST(NULL AS BIGINT) institution_id WHERE false'
    return 'SELECT * FROM VALUES ' + ','.join(values) + ' AS excluded(name,institution_id)'


def run(c):
    r = c.r
    answers = 'affiliation_answers_v' if c.has_extra('affiliation_answers') else c.artifact('recipient_answers_empty', EMPTY_ANSWERS)
    c.zero('RECIPIENT_MATCHER_UNIQUE', f'SELECT raw_affiliation_string FROM {answers} GROUP BY 1 HAVING count(*)<>1')
    c.zero('RECIPIENT_INSTITUTIONS_UNIQUE', 'SELECT id FROM institutions_api_v GROUP BY 1 HAVING count(*)<>1')
    c.artifact('recipient_org_selected', f"""SELECT DISTINCT award_id,name,role,country_code,ror,share,basis,asserted_by
        FROM {r}recipients_resolved WHERE dropped IS NULL AND kind='organisation'""")
    # Share is not an identity field: conflicting source assertions fail, never pick an arbitrary share.
    c.zero('RECIPIENT_ORG_CONFLICT', f"""SELECT award_id,name,role,country_code,ror FROM {r}recipient_org_selected
        GROUP BY ALL HAVING count(*)>1""")
    c.artifact('recipient_org_names', f'SELECT DISTINCT name,country_code,ror FROM {r}recipient_org_selected')
    # Reuse the same sovereign/territory equivalences as institution_awarded without changing that path.
    payload = (Path(c.config['package_root']) / 'sql/api_payload.sql').read_text()
    sovereign = payload.split('sovereign AS (', 1)[1].split('\n),', 1)[0]
    c.artifact('recipient_org_matches', f"""WITH sovereign AS ({sovereign}), answers AS (
        SELECT s.*,ans.raw_affiliation_string answered,ans.institution_ids
        FROM {r}recipient_org_names s LEFT JOIN {answers} ans ON ans.raw_affiliation_string=s.name
      ), answer_ids AS (
        SELECT name,country_code,ror,explode(institution_ids) institution_id
        FROM answers WHERE ror IS NULL AND answered IS NOT NULL
      ), candidates AS (
        SELECT s.name,s.country_code,s.ror,i.id institution_id,i.display_name,
          i.ror institution_ror,i.country_code institution_country,'ror' match_basis
        FROM answers s JOIN institutions_api_v i ON i.ror=s.ror WHERE s.ror IS NOT NULL
        UNION ALL
        SELECT s.name,s.country_code,s.ror,i.id,i.display_name,i.ror,i.country_code,'affiliation_matcher'
        FROM answer_ids s JOIN institutions_api_v i ON i.id=s.institution_id
        UNION ALL
        SELECT s.name,s.country_code,s.ror,i.id,i.display_name,i.ror,i.country_code,'institution_name'
        FROM answers s JOIN institutions_api_v i ON lower(trim(i.display_name))=lower(trim(s.name))
        WHERE s.ror IS NULL AND s.answered IS NULL
      ), guarded AS (
        SELECT c.* FROM candidates c
        LEFT JOIN sovereign si ON si.territory=c.institution_country
        LEFT JOIN sovereign sr ON sr.territory=c.country_code
        WHERE (c.country_code IS NULL OR c.institution_country IS NULL
          OR coalesce(si.state,c.institution_country)=coalesce(sr.state,c.country_code))
          AND NOT EXISTS (SELECT 1 FROM ({exclusion_relation_sql(c.config['package_root'])}) ex
            WHERE ex.name=lower(trim(c.name)) AND ex.institution_id=c.institution_id)
      )
      SELECT name,country_code,ror,min(institution_id) institution_id,min(display_name) display_name,
        min(institution_ror) institution_ror,min(institution_country) institution_country,min(match_basis) match_basis
      FROM guarded GROUP BY name,country_code,ror HAVING count(DISTINCT institution_id)=1""")
    c.artifact('recipient_org_objects', f"""SELECT s.award_id,
        CASE WHEN m.institution_id IS NOT NULL THEN concat('https://openalex.org/I',CAST(m.institution_id AS STRING)) END id,
        coalesce(m.display_name,s.name) display_name,m.institution_ror ror,
        coalesce(s.country_code,m.institution_country) country_code,s.role,s.share,
        coalesce(m.match_basis,s.basis) basis,s.asserted_by
      FROM {r}recipient_org_selected s LEFT JOIN {r}recipient_org_matches m
        ON m.name=s.name AND m.country_code <=> s.country_code AND m.ror <=> s.ror""")
    # One object per identity/assertion. Unknown identities retain country; matching
    # methods are evidence for the same identity, ordered ROR > matcher > exact name.
    # Distinct monetary/attribution assertions are never silently combined or summed.
    c.artifact('recipient_org_grouped', f"""SELECT award_id,id,display_name,ror,country_code,share,asserted_by,
        sort_array(collect_set(role)) roles,
        CASE WHEN id IS NULL THEN min(basis) ELSE
          CASE min(CASE basis WHEN 'ror' THEN 0 WHEN 'affiliation_matcher' THEN 1 ELSE 2 END)
            WHEN 0 THEN 'ror' WHEN 1 THEN 'affiliation_matcher' ELSE 'institution_name' END END basis
      FROM {r}recipient_org_objects
      GROUP BY award_id,id,display_name,ror,country_code,share,asserted_by,CASE WHEN id IS NULL THEN basis END""")
    c.artifact('recipient_org_arrays', f"""SELECT award_id,sort_array(collect_set(named_struct(
        'id',id,'display_name',display_name,'ror',ror,'country_code',country_code,
        'roles',roles,'share',share,'basis',basis,'asserted_by',asserted_by))) recipients
      FROM {r}recipient_org_grouped GROUP BY award_id""")


def check_counts(c, fuse):
    rel = c.r + 'awards_candidate'
    now = c.count('awards_with_recipients', f'(SELECT id FROM {rel} WHERE size(recipients_full)>0) x')
    matched = c.count('awards_with_matched_recipients', f'(SELECT id FROM {rel} WHERE exists(recipients_full,x -> x.id IS NOT NULL)) x')
    members = int(award_keys.one(c, f'SELECT coalesce(sum(size(recipients_full)),0) n FROM {rel}', n=0).n)
    c.counts['recipient_org_memberships'] = members
    prev = (award_keys.one(c, '''SELECT count_if(size(recipients_full)>0) awards,
        count_if(exists(recipients_full,x -> x.id IS NOT NULL)) matched,
        coalesce(sum(size(recipients_full)),0) members FROM previous_api_v''', awards=0, matched=0, members=0)
        if 'recipients_full' in award_keys.columns(c, 'previous_api_v') else None)
    for gate, key, n, before in (
        ('RECIPIENT_ORG_COUNT_DROP','awards_with_recipients',now,prev.awards if prev else None),
        ('RECIPIENT_ORG_MATCH_DROP','awards_with_matched_recipients',matched,prev.matched if prev else None),
        ('RECIPIENT_ORG_MEMBERSHIP_DROP','recipient_org_memberships',members,prev.members if prev else None)):
        funding_population.gate(c, "orgs",gate,key,n,fuse,before)
