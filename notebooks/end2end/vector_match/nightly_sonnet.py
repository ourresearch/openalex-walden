"""oxjob #1401 Sonnet stage for #1348's vector matcher (replaces / follows Vector_Jev). The night's zone seats (rule 'zone' in the decisions
table; cards pre-built by #1348 vector_zone_cards.sql into <zone-cards-table>), minus seats the ORCID tier decides, judged by Claude
Sonnet 5.5 as a Choice over the top-3 vector candidates + new_person + unsure (prompt/schema = #1348 replay_judge.py, measured in #1401).
Auto-assign when the pick is a profile at confidence 'high' -> rule 'sonnet' + vec_author_id; else 'zone_unresolved' (cascade decides).
Runs on a small CPU cluster (network-bound); asyncio in its own thread (the Databricks driver has a running loop). Secret anthropic/api_key.
    argv: <home> --date 2026-09-25 [--out-table T --zone-cards-table Z --card compact|full --per-cand 5 --effort medium
          --concurrency 64 --limit 0 --only-ambiguous 0 --dry-run 0 --max-usd 1500]
    Spend cap: once --max-usd is spent, remaining seats are not sent (they stay 'zone' -> cascade). A normal night is ~$540."""
import json, os, sys, time, asyncio, threading, shutil
HOME = sys.argv[1]
def arg(k, d): return next((sys.argv[i + 1] for i, x in enumerate(sys.argv) if x == k), d)
DATE = arg("--date", None); D8 = DATE.replace("-", ""); A = "openalex.authors"
OUT = arg("--out-table", f"{A}.oxjob1348_nightly_decisions"); ZT = arg("--zone-cards-table", f"{A}.vector_match_zone_cards")
CARD = arg("--card", "compact"); PER = int(arg("--per-cand", 5)); EFFORT = arg("--effort", "medium"); CONC = int(arg("--concurrency", 64))
LIMIT = int(arg("--limit", 0)); MAX_USD = float(arg("--max-usd", 1500)); ONLY_AMB = arg("--only-ambiguous", "0") == "1"; DRY = arg("--dry-run", "0") == "1"
MODEL = "claude-sonnet-5-5"; PRICE = (2.0, 10.0)
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)
from pyspark.dbutils import DBUtils
KEY = DBUtils(spark).secrets.get("anthropic", "api_key")
import anthropic
PROMPT = """You are auditing author-name disambiguation for a scholarly database (OpenAlex). A NEW AUTHORSHIP (one author's seat on one newly ingested work) must be assigned to an existing author profile or become a new profile. Below are the new authorship and {k} CANDIDATE PROFILES with compatible names; each profile is shown as a sample of its existing authorships (name as printed, affiliation as deposited and as parsed, venue, year, field, nearest coauthors, title). Using ordinary bibliometric judgment (name forms and rarity, affiliations, field, coauthors, years, topic), decide which candidate profile (by its number) is the same real person as the new authorship, or "new_person" if none is. Note: the candidates were retrieved by similarity, so a plausible-looking near miss is common; "new_person" is a normal answer. Say "unsure" only if the evidence genuinely cannot decide.

NEW AUTHORSHIP:
{seat}

{cands}
"""
SCHEMA = {"type": "object", "properties": {"choice": {"type": "string", "enum": ["1", "2", "3", "new_person", "unsure"]}, "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
          "reason": {"type": "string"}}, "required": ["choice", "confidence", "reason"], "additionalProperties": False}
def show_full(c):
    return json.dumps({"name_as_printed": c.get("raw_name"), "affiliation_as_deposited": c.get("aff_strings"), "institution_parsed": c.get("inst_names"),
                       "venue": c.get("venue"), "year": c.get("publication_year"), "field": c.get("subfield"), "nearest_coauthors": c.get("coauthors"), "title": c.get("title")}, ensure_ascii=False)
def show_compact(s):
    c = {"name": s.get("raw_name")}
    aff = [x[:120] for x in (s.get("aff_strings") or []) if x][:2]; inst = [x for x in (s.get("inst_names") or []) if x][:2]
    if aff: c["affiliation"] = aff
    elif inst: c["affiliation"] = inst
    if s.get("title"): c["title"] = s["title"][:160]
    if s.get("publication_year"): c["year"] = str(s["publication_year"])
    if s.get("venue"): c["venue"] = s["venue"][:80]
    if s.get("subfield"): c["field"] = s["subfield"]
    co = [x for x in (s.get("coauthors") or []) if x][:5]
    if co: c["coauthors"] = co
    return json.dumps(c, ensure_ascii=False)
show = show_full if CARD == "full" else show_compact
def prompt_for(p):
    cands = sorted(p["cands"], key=lambda c: c["pos"])
    body = "\n\n".join(f"CANDIDATE {i + 1} ({c.get('wc') or len(c['seats'])} works on the profile; showing {len(c['seats'][:PER])}):\n" + "\n".join(show(s) for s in c["seats"][:PER]) for i, c in enumerate(cands))
    return [c["pid"] for c in cands], PROMPT.format(k=len(cands), seat=show(p["seat"]), cands=body)
t0 = time.time(); lim = f" LIMIT {LIMIT}" if LIMIT else ""
amb = " AND d.match_outcome = 'AMBIGUOUS'" if ONLY_AMB else ""
rows = [r.asDict(recursive=True) for r in spark.sql(f"""SELECT z.* FROM {ZT} z JOIN {OUT} d ON d.run_date = '{DATE}' AND d.work_id = z.work_id AND d.author_sequence = z.author_sequence
    WHERE d.rule IN ('zone', 'zone_unresolved') AND d.orcid_author_id IS NULL{amb}{lim}""").collect()]
import re
PLACEHOLDER = re.compile(r"^\W*(anonymous|anon\.?|unknown|n/?a|none|null|et al\.?|various|collective|group|author|authors|editor|editors|staff)\W*$", re.I)
n0 = len(rows); rows = [r for r in rows if r["seat"].get("raw_name") and not PLACEHOLDER.match(r["seat"]["raw_name"].strip()) and len(re.sub(r"\W", "", r["seat"]["raw_name"])) >= 3]
log(f"placeholder / empty names skipped (cascade decides them): {n0 - len(rows)}")
log(f"zone seats to judge (non-ORCID{', AMBIGUOUS only' if ONLY_AMB else ''}): {len(rows)} ({time.time()-t0:.0f}s)")
res_path = f"/tmp/sonnet_{D8}.jsonl"; fout = open(res_path, "w"); tot = {"usd": 0.0, "n": 0, "err": 0, "refusal": 0}
async def judge():
    client = anthropic.AsyncAnthropic(api_key=KEY, max_retries=6, timeout=120.0); sem = asyncio.Semaphore(CONC); t1 = time.time()
    async def one(p):
        pids, prompt = prompt_for(p); rec = {"work_id": int(p["work_id"]), "author_sequence": int(p["author_sequence"]), "s_pid": None, "s_choice": None, "s_conf": None}
        async with sem:
            if tot["usd"] >= MAX_USD: tot["capped"] = tot.get("capped", 0) + 1; return
            try:
                r = await client.messages.create(model=MODEL, max_tokens=3000, output_config={"effort": EFFORT, "format": {"type": "json_schema", "schema": SCHEMA}},
                                                 messages=[{"role": "user", "content": prompt}])
                tot["usd"] += r.usage.input_tokens * PRICE[0] / 1e6 + r.usage.output_tokens * PRICE[1] / 1e6
                if r.stop_reason == "refusal": rec["s_choice"] = "refusal"; tot["refusal"] += 1
                else:
                    j = json.loads(next(b.text for b in r.content if b.type == "text")); c = j.get("choice")
                    rec["s_conf"] = j.get("confidence")
                    if c and c.isdigit() and int(c) <= len(pids): rec["s_pid"] = int(pids[int(c) - 1]); rec["s_choice"] = "profile"
                    else: rec["s_choice"] = c if c in ("new_person", "unsure") else "unsure"
            except Exception as e: rec["s_choice"] = "error"; tot["err"] += 1
        fout.write(json.dumps(rec) + "\n"); tot["n"] += 1
        if tot["n"] % 2000 == 0: log(f"  sonnet {tot['n']} ({tot['n']/(time.time()-t1):.1f}/s, ${tot['usd']:.2f}, err {tot['err']}, refusal {tot['refusal']})")
    await asyncio.gather(*(one(p) for p in rows))
    log(f"sonnet done: {tot.get('capped', 0)} seats not sent (spend cap ${MAX_USD:.0f})")
    log(f"sonnet done: {tot['n']} calls in {time.time()-t1:.0f}s, ${tot['usd']:.2f} (${tot['usd']/max(tot['n'],1):.4f}/call), err {tot['err']}, refusal {tot['refusal']}")
th = threading.Thread(target=lambda: asyncio.run(judge())); th.start(); th.join(); fout.close()
os.makedirs(f"{HOME}/results", exist_ok=True); shutil.copy(res_path, f"{HOME}/results/sonnet_{D8}.jsonl")
if DRY: log("dry run: results on the Volume only, no MERGE"); log("NIGHTLY SONNET DONE", DATE); sys.exit(0)
sdf = spark.read.json(f"{HOME}/results/sonnet_{D8}.jsonl").withColumn("work_id", F.col("work_id").cast("bigint")).withColumn("author_sequence", F.col("author_sequence").cast("bigint")).withColumn("s_pid", F.col("s_pid").cast("bigint"))
sdf.createOrReplaceTempView("sonnet_tmp")
for col, typ in (("sonnet_pid", "BIGINT"), ("sonnet_choice", "STRING"), ("sonnet_conf", "STRING")):
    if col not in [f.name for f in spark.table(OUT).schema.fields]: spark.sql(f"ALTER TABLE {OUT} ADD COLUMNS ({col} {typ})")
spark.sql(f"""MERGE INTO {OUT} t USING sonnet_tmp s ON t.run_date = '{DATE}' AND t.work_id = s.work_id AND t.author_sequence = s.author_sequence
  WHEN MATCHED THEN UPDATE SET t.sonnet_pid = s.s_pid, t.sonnet_choice = s.s_choice, t.sonnet_conf = s.s_conf,
    t.rule = CASE WHEN s.s_pid IS NOT NULL AND s.s_conf = 'high' THEN 'sonnet' ELSE 'zone_unresolved' END,
    t.vec_author_id = CASE WHEN s.s_pid IS NOT NULL AND s.s_conf = 'high' THEN s.s_pid ELSE NULL END""")
log("final:", [(r["tier"], r["rule"], r["n"]) for r in spark.sql(f"SELECT tier, rule, COUNT(*) n FROM {OUT} WHERE run_date = '{DATE}' GROUP BY 1, 2 ORDER BY 1, 2").collect()])
log("NIGHTLY SONNET DONE", DATE)
