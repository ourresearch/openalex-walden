"""Small, release-free stable-award identity primitives.

The pure functions are the reference rules from orig/lib/stable_award_ids.py.
The SQL helpers deliberately know nothing about releases, pins, or journals.
"""
import hashlib
import json
import re

BOUNDARY = 9_000_000_000


def compact(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def sha(value):
    return hashlib.sha256(value if isinstance(value, bytes) else value.encode()).hexdigest()


def ident(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[A-Za-z_][A-Za-z_0-9]*(\.[A-Za-z_][A-Za-z_0-9]*){2}", value
    ):
        raise ValueError("Unsafe qualified identifier: " + repr(value))
    return value


def short(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value):
        raise ValueError("Unsafe artifact identifier")
    return value


def literal(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def native_root(namespace, source_record_id):
    return json.dumps(
        dict(v=2, type="NATIVE", namespace=namespace, source_record_id=source_record_id),
        separators=(",", ":"), ensure_ascii=False,
    )


def staging_root(staging_id, funder_id, award_key):
    return json.dumps(
        dict(v=2, type="STAGING", staging_id=staging_id, funder_id=funder_id, award_key=award_key),
        separators=(",", ":"), ensure_ascii=False,
    )


def resolve_graph(entities, depth=100):
    """Resolve redirects. ACTIVE/GONE are terminals; missing nodes and cycles fail."""
    result = {}
    for owner in entities:
        node, path = owner, set()
        for _ in range(depth + 1):
            if node in path or node not in entities:
                raise ValueError("BROKEN_ENTITY_GRAPH")
            path.add(node)
            status, target = entities[node]
            if status in ("ACTIVE", "GONE") and target is None:
                result[owner] = (node, status)
                break
            if status != "REDIRECTED" or target is None:
                raise ValueError("BAD_ENTITY_STATE")
            node = target
        else:
            raise ValueError("GRAPH_DEPTH_FUSE")
    return result


def component_roots(observations):
    """Union observations by full STAGING and producer-scoped NATIVE keys."""
    parent = {o["observation_key"]: o["observation_key"] for o in observations}

    def root(node):
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    keys = {}
    for observation in observations:
        candidates = [staging_root(observation["staging_id"], observation.get("source_funder_id"), observation.get("award_key"))]
        if observation.get("namespace") is not None:
            candidates.append(native_root(observation["namespace"], observation["source_record_id"]))
        for key in candidates:
            if key in keys:
                left, right = root(observation["observation_key"]), root(keys[key])
                parent[max(left, right)] = min(left, right)
            keys[key] = observation["observation_key"]
    groups = {}
    for observation in observations:
        groups.setdefault(root(observation["observation_key"]), []).append(observation)
    answer = {}
    for members in groups.values():
        owners = {o.get("existing_id") for o in members} - {None}
        if len(owners) > 1:
            raise ValueError("COMPONENT_OWNER_CONFLICT")
        native = [native_root(o["namespace"], o["source_record_id"]) for o in members if o.get("namespace")]
        staging = [staging_root(o["staging_id"], o.get("source_funder_id"), o.get("award_key")) for o in members]
        birth_key = min(native or staging)
        for observation in members:
            answer[observation["observation_key"]] = (birth_key, next(iter(owners), None))
    return answer


def shell_target(generic, sharp, eligible=True):
    generic, sharp = set(generic), set(sharp)
    if len(generic) == 1:
        return next(iter(generic))
    if not generic and eligible and len(sharp) == 1:
        return next(iter(sharp))
    return None


def norm_doi_sql(expr):
    value = f"regexp_replace(lower(trim({expr})), '^(doi:[ ]*|https?://(dx[.])?doi[.]org/)', '')"
    return f"CASE WHEN {value} RLIKE '^10[.][0-9]+/.+' THEN {value} END"


def generic_sql(expr):
    clean = f"regexp_replace(lower({expr}), '[^a-z0-9]', '')"
    return f"CASE WHEN length({clean}) >= 4 THEN {clean} ELSE lower(trim({expr})) END"


def key_match(left, right):
    return (
        f"{left}.key_type={right}.key_type AND {left}.staging_id <=> {right}.staging_id "
        f"AND {left}.funder_id <=> {right}.funder_id AND {left}.award_key <=> {right}.award_key "
        f"AND {left}.namespace <=> {right}.namespace AND {left}.source_record_id <=> {right}.source_record_id"
    )
