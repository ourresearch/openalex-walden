"""Investigator authors (oxjob #1847): the job's helpers. run_sql_file fills __KEY__ pins from the job's parameters and runs one
statement at a time (split on a line holding only `;`); a pin left unfilled stops the run."""
import csv
import datetime as dt
import itertools
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path

HERE = Path(__file__).resolve().parent
NAMES = ("prefix", "run_date", "max_change", "model", "alias", "jev_deadline_min", "jev_max_items")   # the job's parameters (YAML)
# The listwise columns the model was trained on: each candidate against the best other one on its list, and its rank
LISTWISE = ["nm_full_eq_any", "nm_mid", "yrs_inst_win", "yrs_exact_win", "frac_inst", "at_inst_start", "works_log", "works_win",
            "active_at_start"]
EXCLUDED_COLUMNS = ["funder_id", "country_code", "reason", "decided"]


def log(msg):
    print(f"{dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC  {msg}", flush=True)


def params(dbutils):
    p = {k: dbutils.widgets.get(k).strip() for k in NAMES}
    assert re.fullmatch(r"[a-z0-9_]+\.[a-z0-9_]+\.[a-z0-9_]+_", p["prefix"]), f"prefix must be catalog.schema.name_: {p['prefix']}"
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", p["run_date"]), p["run_date"]
    assert 0 < float(p["max_change"]) <= 1, p["max_change"]
    assert 0 < float(p["jev_deadline_min"]) <= 120, p["jev_deadline_min"]
    assert re.fullmatch(r"\d+", p["jev_max_items"]) and int(p["jev_max_items"]) <= 1_000_000, p["jev_max_items"]
    return p


def listwise(cols=LISTWISE):
    """features.sql's __LISTWISE__, written from one list: the training code's d_ (the value minus the best OTHER candidate's on the list; 0 for
    a list of one) and r_ (rank, highest first). Window functions instead of the training code's sorted arrays; tests check they agree."""
    over = "OVER (PARTITION BY entry_id ORDER BY {c} DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
    return {"LISTWISE": ",\n  ".join(
        f"CASE WHEN n_short > 1 THEN {c} - CASE WHEN {c} = max({c}) OVER (PARTITION BY entry_id) THEN nth_value({c}, 2) "
        f"{over.format(c=c)} ELSE max({c}) OVER (PARTITION BY entry_id) END ELSE 0.0 END AS d_{c},\n  "
        f"CASE WHEN {c} IS NOT NULL THEN CAST(rank() OVER (PARTITION BY entry_id ORDER BY {c} DESC NULLS LAST) AS DOUBLE) END AS r_{c}"
        for c in cols)}


def pins(p):
    return {"P": p["prefix"], "RUN": p["run_date"], "MAXCHG": p["max_change"], "JEVMAX": int(p["jev_max_items"]), "JEVMODEL": JEV_MODEL,
            "JEVSEED": jev_seed_source(p["prefix"]), **listwise()}


def fill(text, pins):
    for k, v in pins.items():
        text = text.replace(f"__{k}__", str(v))
    left = re.findall(r"__[A-Z0-9_]+__", "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("--")))
    assert not left, f"unfilled {sorted(set(left))}"
    return text


def statements(text):
    return [s.strip() for s in text.split("\n;\n") if s.strip()]


def run_sql_file(spark, name, pins):
    """Run every statement of sql/<name> with its pins filled; print up to 60 rows of those that return any."""
    for stmt in statements(fill((HERE / "sql" / name).read_text(), pins)):
        t = time.time()
        rows = spark.sql(stmt).take(60)
        body = "\n".join(l for l in stmt.splitlines() if not l.lstrip().startswith("--")).lstrip()
        log(f"{time.time() - t:.0f}s  {body[:90]!r}")
        for row in rows:
            print("   ", row.asDict(), flush=True)


def read_excluded(path=HERE / "excluded_funders.csv"):
    """excluded_funders.csv: one row per funder id OR country (never both), each with a reason and who decided it, when."""
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == EXCLUDED_COLUMNS, f"EXCLUDED_COLUMNS: {reader.fieldnames}"
        rows = list(reader)
    out, seen = [], set()
    for r in rows:
        fid, cc = r["funder_id"].strip(), r["country_code"].strip()
        ok = (bool(fid) != bool(cc) and (not fid or fid.isdigit()) and (not cc or re.fullmatch(r"[A-Z]{2}", cc))
              and r["reason"].strip() and r["decided"].strip() and (fid, cc) not in seen)
        assert ok, f"EXCLUDED_ROW_INVALID: {r}"
        seen.add((fid, cc))
        out.append((int(fid) if fid else None, cc or None, r["reason"].strip(), r["decided"].strip()))
    return out


# ---------- nsfc_name (routes.sql): NSFC prints its PI in Chinese only, so the route needs the PI's pinyin ----------
NSFC = 4320321001
PINYIN_CAP = 12   # readings kept per name part (heteronyms multiply)
NSFC_PEOPLE = f"funder_id = {NSFC} AND length(trim(given_name)) = 2"   # pinyin_readings drops a name with any non-Han character


def pinyin_readings(s):
    """Lower-case a-z pinyin readings of a Han string: phrase-aware first, then every reading per character (heteronyms: 曾 zeng and
    ceng); ü as u, yu and v. [] if any character has no reading. pypinyin 0.55.0 (the job's Default environment)."""
    from pypinyin import Style, pinyin
    ph = pinyin(s, style=Style.NORMAL, heteronym=True, errors="ignore")
    pc = [sum(pinyin(ch, style=Style.NORMAL, heteronym=True, errors="ignore"), []) for ch in s]
    if len(ph) != len(s) or any(not r for r in ph + pc):
        return []
    out = []
    for combo in itertools.islice(itertools.chain(itertools.product(*ph), itertools.product(*pc)), 4 * PINYIN_CAP):
        w = "".join(combo).lower()
        if w.isascii() and w.isalpha():
            out += [w] if "v" not in w else [w.replace("v", "u"), w.replace("v", "yu"), w]
    return list(dict.fromkeys(out))[:PINYIN_CAP]


def pinyin_keys(family, given):
    """Every (family, given) pinyin reading of a Han name."""
    f, g = (family or "").strip(), (given or "").strip()
    return [(a, b) for a in pinyin_readings(f) for b in pinyin_readings(g)] if f and g else []


def nsfc_pinyin_view(spark, pins):
    """The temp view routes.sql's nsfc_name reads, built each run from this run's people (no stored table): nsfc_pinyin(award_id, slot,
    position, f, g) for NSFC people with a Han family name and a two-character given name. Bounded: ~230k people, ~15 s on the driver."""
    people = spark.sql(f"SELECT award_id, slot, position, family_name, given_name FROM {pins['P']}people WHERE {NSFC_PEOPLE}").toPandas()
    rows = [(int(a), s, int(q), f, g) for a, s, q, fa, gi in people.itertuples(index=False) for f, g in pinyin_keys(fa, gi)]
    spark.createDataFrame(rows, "award_id BIGINT, slot STRING, position INT, f STRING, g STRING").createOrReplaceTempView("nsfc_pinyin")
    log(f"nsfc_pinyin: {len(people)} people, {len(rows)} readings")


VIEWS = {"nsfc_pinyin": nsfc_pinyin_view}   # RunSql's `views` parameter: built in its Spark session before its SQL files


# ---------- Judge_With_Jev: Jev on unserved slots with a same-name list of 1-20 (judge.sql builds the items, judge_merge.sql keeps the picks) ----
JEV_MODEL = "jev-1.13.0"   # utils.study_design.JevClient's pin; the graded runs (JEV55, JWI55/JWE55, J20S63/J20S2A/J20S2B) used it
JEV_P = 0.5                # a pick: Jev's choice is a candidate on the list with p >= 0.5, as graded
JEV_FROZEN = "openalex.awards.investigator_jev_links"         # the one-time seed (jev.sql): the 11-06 picks ...
JEV_FROZEN_UNLINKED = "openalex.awards.investigator_jev_unlinked"   # ... and the 11-06 none / unsure answers, read only while they exist
JEV_SEED_COLS = "award_id, slot, position, given_name, family_name, decision, author_id, p, list_hash, src"


def jev_seed_source(prefix, links_exist=False, unlinked_exist=False):
    """jev.sql's __JEVSEED__: the frozen 11-06 answers that still exist (picks as 'link' with no list yet; none / unsure with the list
    they were judged on), else an empty set (the job's own table, WHERE false)."""
    parts = ([f"SELECT award_id, slot, position, given_name, family_name, 'link', author_id, p, CAST(NULL AS STRING), 'investigator_jev_links' "
              f"FROM {JEV_FROZEN}"] if links_exist else []) + \
            ([f"SELECT award_id, slot, position, given_name, family_name, decision, CAST(NULL AS BIGINT), p, list_hash, 'investigator_jev_unlinked' "
              f"FROM {JEV_FROZEN_UNLINKED}"] if unlinked_exist else [])
    if not parts:
        return f"(SELECT {JEV_SEED_COLS} FROM (SELECT *, model AS src FROM {prefix}jev) AS z WHERE false)"
    return f"(SELECT * FROM ({' UNION ALL '.join(parts)}) AS s ({JEV_SEED_COLS}))"


def jev_questions(max_k=20, path=HERE / "jev_link_prompt.txt"):
    """The graded prompt (ai-tools questions_pt1102.json): one instruction text; stratum sp_kN offers c1..cN, none and unsure."""
    text = path.read_text(encoding="utf-8").rstrip("\n")
    out = {}
    for k in range(1, max_k + 1):
        crit = {f"c{i}": f"Candidate c{i} under CANDIDATES is the SAME PERSON as the investigator" for i in range(1, k + 1)}
        crit["none"] = "NOT THIS PERSON (rule 5): no candidate is the investigator"
        crit["unsure"] = "NOT ENOUGH EVIDENCE (rule 4): the cards cannot decide"
        out[f"sp_k{k}"] = {"link": {"type": "choice", "instructions": text, "criteria": crit}}
    return out


def jev_answer(item, result):
    """One judge_merge.sql row from Jev's result: decision 'link' (a candidate on this list at p >= JEV_P), 'none' or 'unsure'. None
    for a failed call or any malformed answer (the entry is asked again another night)."""
    try:
        link = result["answers"]["link"] if result.get("ok") else None
        choice, probs, ckeys = link["choice"], link["probabilities"], json.loads(item["ckeys"])
        assert isinstance(probs, dict) and (choice in ckeys or choice in ("none", "unsure"))
        p = float(probs[choice])
        assert 0.0 <= p <= 1.0
    except (AssertionError, AttributeError, KeyError, TypeError, ValueError):
        return None
    if choice in ckeys and p >= JEV_P:
        decision, author = "link", int(ckeys[choice])
    else:
        decision, author = ("none" if choice == "none" else "unsure"), None
    return {"entry_id": item["entry_id"], "list_hash": item["list_hash"], "decision": decision, "author_id": author, "p": round(p, 4),
            "model": result.get("model") or JEV_MODEL}


def ask(client, item, qs, late=lambda: False):
    """One Jev call (questions: the item's stratum, as the graded runs); not made once late(); an exception is a failed call."""
    if late():
        return {"ok": False, "error": "deadline"}
    try:
        return client.decide(json.loads(item["state_json"]), qs[item["stratum"]])
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": repr(e)[:300]}


def judge_items(client, items, qs, deadline_s, batch=2000, threads=64, clock=time.monotonic, on_answers=None, grace_s=120.0):
    """Ask Jev about the night's items (already capped by judge.sql), a batch at a time. No call starts after deadline_s seconds or once
    the client is out of credit (checked before each batch, after its rows arrive, and before each call); a call still running
    grace_s after the deadline is abandoned (no answer, asked again next night), so a retrying call cannot hold up decide.
    on_answers(answers) keeps each batch's answers as it ends. Returns (answers, n_asked). Never a fallback."""
    t0, answers, asked, it = clock(), [], 0, iter(items)
    late = lambda: clock() - t0 >= deadline_s or bool(getattr(client, "out_of_credit", None))   # noqa: E731
    while not late():
        chunk = list(itertools.islice(it, batch))
        if not chunk or late():
            break
        ex = ThreadPoolExecutor(threads)
        futs = [ex.submit(ask, client, i, qs, late) for i in chunk]
        done, _ = wait(futs, timeout=max(0.0, deadline_s + grace_s - (clock() - t0)))
        ex.shutdown(wait=False, cancel_futures=True)
        if len(done) < len(futs):   # calls abandoned: stop the client now, before saving answers, so none retries meanwhile
            client.out_of_credit = getattr(client, "out_of_credit", None) or "stopped: calls abandoned at the deadline"
        results = [f.result() if f in done else {"ok": False, "error": "deadline"} for f in futs]
        got = [a for a in (jev_answer(i, r) for i, r in zip(chunk, results)) if a]
        asked += sum(1 for r in results if r.get("error") != "deadline")
        answers += got
        if on_answers and got:
            on_answers(got)
    return answers, asked
