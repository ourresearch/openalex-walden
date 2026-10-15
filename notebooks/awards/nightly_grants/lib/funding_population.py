"""Loss baselines on the approved resolved population, independent of tonight's output.

Published rows are mapped through the checked resolve_candidate, never filtered by the
candidate being tested. Missing/GONE rows retain their original id so source loss is
not excused as a fold. Scalar fields use the survivor's previous row; membership
fields union/deduplicate at the survivor. NIH's explicit amount=null policy removes
only listed project amounts, matching nih_fold.project before USD conversion.

On the first release, scalar bootstrap counts cannot describe a fold. Supply a frozen
same-input pre-fold full candidate as extra input funding_flows_baseline. It is checked
against the bootstrap counts BEFORE any resolution adjustment. Subsequent nights use
previous_api_v. No source-row gate uses this population.
"""
import award_keys
import nih_fold

FIELDS = {
    'usd': {'amount_usd', 'amount_usd_basis', 'currency'},
    'research': {'research_scope'},
    'geography': {'recipient_countries_full', 'recipient_regions_full'},
    'orgs': {'recipients_full'},
}


def mapped_sql(source, resolver):
    return f"""SELECT p.*,CASE WHEN x.status='ACTIVE' THEN x.stable_id ELSE p.id END resolved_id
      FROM {source} p LEFT JOIN {resolver} x ON x.owner_id=p.id"""


def scalar_sql(mapped, null_ids=None):
    nulling = f"NOT EXISTS (SELECT 1 FROM {null_ids} n WHERE n.stable_id=p.resolved_id)" if null_ids else 'true'
    return f"""SELECT p.*,{nulling} amount_allowed FROM {mapped} p
      QUALIFY row_number() OVER (PARTITION BY resolved_id ORDER BY CASE WHEN id=resolved_id THEN 0 ELSE 1 END,id)=1"""


def metrics_sql(kind, mapped, scalar):
    if kind == 'usd':
        return f"""SELECT 'awards_with_amount_usd' k,count(*) n FROM {scalar} WHERE amount_usd IS NOT NULL AND amount_allowed
          UNION ALL SELECT concat('amount_usd_',amount_usd_basis),count(*) FROM {scalar}
            WHERE amount_usd IS NOT NULL AND amount_allowed GROUP BY amount_usd_basis
          UNION ALL SELECT concat('amount_usd_currency_',currency),count(*) FROM {scalar}
            WHERE amount_usd IS NOT NULL AND amount_allowed GROUP BY currency"""
    if kind == 'research':
        return f"""SELECT 'awards_with_research_scope' k,count(*) n FROM {scalar} WHERE research_scope IS NOT NULL
          UNION ALL SELECT 'research_true',count(*) FROM {scalar} WHERE research_scope.is_research=true"""
    if kind == 'orgs':
        # Count organisation identities after resolution, unioning overlapping roles.
        # Match methods do not split an identity; unmatched countries and genuine
        # share/source-basis/attribution differences remain distinct.
        return f"""WITH members AS (SELECT DISTINCT p.resolved_id,o.id institution_id,o.display_name,o.ror,o.country_code,
            o.share,o.asserted_by,CASE WHEN o.id IS NULL THEN o.basis END source_basis
            FROM {mapped} p LATERAL VIEW explode(p.recipients_full) e AS o)
          SELECT 'awards_with_recipients' k,count(DISTINCT resolved_id) n FROM members
          UNION ALL SELECT 'awards_with_matched_recipients',count(DISTINCT resolved_id) FROM members WHERE institution_id IS NOT NULL
          UNION ALL SELECT 'recipient_org_memberships',count(*) FROM members"""
    # Geography's best basis wins across the whole award (countries AND regions).
    # Regions have no served basis; their source policy is funder_reported.
    return f"""WITH members AS (
        SELECT p.resolved_id,'country' kind,o.country_code code,'' vocabulary,
          CASE o.basis WHEN 'funder_reported' THEN 0 WHEN 'place_of_performance' THEN 1 ELSE 2 END rank
        FROM {mapped} p LATERAL VIEW explode(p.recipient_countries_full) e AS o
        UNION ALL SELECT p.resolved_id,'region',o.code,o.vocabulary,0
        FROM {mapped} p LATERAL VIEW explode(p.recipient_regions_full) e AS o),
      best AS (SELECT *,min(rank) OVER (PARTITION BY resolved_id) best_rank FROM members),
      kept AS (SELECT DISTINCT resolved_id,kind,code,vocabulary FROM best WHERE rank=best_rank)
      SELECT 'awards_with_recipient_countries' k,count(DISTINCT resolved_id) n FROM kept WHERE kind='country'
      UNION ALL SELECT 'awards_with_recipient_regions',count(DISTINCT resolved_id) FROM kept WHERE kind='region'
      UNION ALL SELECT 'recipient_country_memberships',count(*) FROM kept WHERE kind='country'"""


def expected(c, kind):
    if award_keys.clearing(c):
        return None
    cache = getattr(c, '_funding_population', {})
    c._funding_population = cache
    if kind in cache:
        return cache[kind]
    cols = award_keys.columns(c, 'previous_api_v')
    source = 'previous_api_v' if FIELDS[kind] <= cols else None
    bootstrap = source is None
    if bootstrap and c.has_extra('funding_flows_baseline'):
        source = 'funding_flows_baseline_v'
        c.require(FIELDS[kind] <= award_keys.columns(c, source), 'FUNDING_BASELINE_COLUMNS_' + kind)
        c.zero('FUNDING_BASELINE_UNIQUE', f'SELECT id FROM {source} GROUP BY id HAVING count(*)<>1 OR id IS NULL')
        c.zero('FUNDING_BASELINE_FOLD_COVERAGE', f"""SELECT p.id FROM previous_api_v p
          JOIN {c.r}resolve_candidate x ON x.owner_id=p.id AND x.status='ACTIVE' AND x.owner_id<>x.stable_id
          LEFT ANTI JOIN {source} b ON b.id=p.id""")
        if getattr(c, 'nih', False):
            c.zero('FUNDING_BASELINE_NIH_COVERAGE', f"""SELECT p.id FROM previous_api_v p
              JOIN {c.r}nih_choice n ON n.stable_id=p.id
              LEFT ANTI JOIN {source} b ON b.id=p.id""")
    if source is None:
        # No silent scalar fallback on a fold night. A reviewed row baseline is required.
        c.zero('FUNDING_FOLD_BASELINE_REQUIRED_' + kind, f"""SELECT p.id FROM previous_api_v p
          JOIN {c.r}resolve_candidate x ON x.owner_id=p.id AND x.status='ACTIVE' AND x.owner_id<>x.stable_id""")
        c.require(not getattr(c, 'nih', False), 'FUNDING_NIH_BASELINE_REQUIRED_' + kind)
        cache[kind] = None
        return None
    source = '(SELECT id,' + ','.join(sorted(FIELDS[kind])) + ' FROM ' + source + ')'
    label = 'funding_previous_' + kind
    mapped = c.artifact(label, mapped_sql(source, c.r + 'resolve_candidate'))
    null_ids = None
    if kind == 'usd' and getattr(c, 'nih', False) and nih_fold.settings(c)['amount'] == 'null':
        null_ids = c.r + 'nih_choice'
    scalar = c.artifact(label + '_scalar', scalar_sql(mapped, null_ids)) if kind in ('usd','research') else mapped
    after = {row.k: int(row.n) for row in c.sql(metrics_sql(kind, mapped, scalar)).collect()}
    before = None
    if bootstrap:
        original = c.artifact(label + '_original', f'SELECT *,id resolved_id,true amount_allowed FROM {source}')
        before = {row.k: int(row.n) for row in c.sql(metrics_sql(kind, original, original)).collect()}
    result = (after, before)
    cache[kind] = result
    c._funding_population = cache
    return result


def gate(c, kind, name, key, now, fuse, previous=None):
    population = expected(c, kind)
    if population is None:
        return award_keys.drop_gate(c, name, key, now, fuse, previous)
    after, before = population
    if before is not None:
        award_keys.drop_gate(c, name + '_BOOTSTRAP', key, before.get(key, 0), fuse)
        c.counts['funding_before_' + key] = before.get(key, 0)
    c.counts['funding_expected_' + key] = after.get(key, 0)
    award_keys.drop_gate(c, name, key, now, fuse, after.get(key, 0))
