"""Callers for Jason's two Modal apps, used unchanged on an awards queue (job AwardTopics, jobs/award_topics.yaml).

- topics:   app openalex-topics-nightly    (notebooks/topics/nightly/modal_topics_nightly.py), q8b_2m on <= 8 H100s;
- keywords: app oxjob1322-keywords-nightly (notebooks/keywords/nightly/modal_keywords_nightly.py), student_qwen4b_v3fix on <= 10 B200s.
The request bodies, the bearer-token POST with retries, the call-id file that makes a retry poll the same call, and the spend
estimates are Jason's (TagTopicsModal / TagKeywordsModal); only the queue table, output folder and warehouse are ours.

`check_request` validates a request against the rules the deployed app enforces, read from the app source in this checkout
(TABLE_RX, OUT_RX, WH_RX, ALLOWED_MODELS), so a dry run shows a refusal before any token is read or any call is made.
"""
import json
import os
import re
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", "..", ".."))
APPS = {
    "topics": dict(source="notebooks/topics/nightly/modal_topics_nightly.py", model_tag="q8b_2m",
                   endpoint="https://wordslikethis--openalex-topics-nightly-api.modal.run", secret_key="topics1531_nightly_token",
                   max_containers=8, shard_works=25000),
    "keywords": dict(source="notebooks/keywords/nightly/modal_keywords_nightly.py", model="/vol/models/student_qwen4b_v3fix", quant="fp8",
                     endpoint="https://wordslikethis--oxjob1322-keywords-nightly-api.modal.run", secret_key="kw1322_nightly_token",
                     max_containers=10, shard_works=25000),
}
FORBIDDEN_WAREHOUSES = ("3996dc0a", "4d37bf76372a2901", "69a583", "ab72acf2")   # grants-hub rules: never ours to use


def estimate_usd(app, n, shards):
    """Jason's spend-guard formulas: TagTopicsModal ($5.5/M + $0.45 per H100 container start) and TagKeywordsModal ($15/M + $0.5 a
    shard, the guard; measured 2026-09-30: $7.49 for 613,905 works on 10 B200s incl. cold starts = $12.2/M)."""
    if app == "topics":
        return n / 1e6 * 5.5 + 0.45 * min(8, max(1, n // 25000))
    return n / 1e6 * 15 + 0.5 * min(shards, 10)


def app_rules(app):
    """The deployed app's request rules, read from its source in this checkout (no modal import)."""
    src = open(os.path.join(REPO, APPS[app]["source"])).read()

    def rx(name):
        m = re.search(rf'^{name} = re\.compile\(r"(.+?)"\)', src, re.M)
        return re.compile(m.group(1)) if m else None
    models = re.search(r"^ALLOWED_MODELS = \{(.+?)\}", src, re.M)
    return {"table": rx("TABLE_RX"), "out": rx("OUT_RX"), "warehouse": rx("WH_RX"),
            "models": set(re.findall(r'"([^"]+)"', models.group(1))) if models else None,
            "default_warehouse": re.search(r'^DEFAULT_WAREHOUSE = "([0-9a-f]+)"', src, re.M).group(1)}


def start_body(app, table_in, out_dir, build_id, warehouse, shards):
    if app == "topics":
        return {"table_in": table_in, "out_dir": out_dir, "build_id": build_id, "warehouse": warehouse}
    return {"table_in": table_in, "out_dir": out_dir, "shards": sorted(set(shards)), "model": APPS[app]["model"],
            "quant": APPS[app]["quant"], "warehouse": warehouse}


def check_request(app, body, approved_warehouse=""):
    """[] if the app would accept the body, else the reasons it would answer 400 (plus our own warehouse rule). approved_warehouse:
    a never-use warehouse OKed for this run by its exact id (Jason's or Rohan's direct OK); any other never-use id is still refused."""
    r, bad = app_rules(app), []
    if not r["table"].match(body["table_in"]):
        bad.append(f"table_in {body['table_in']!r} refused by the app's TABLE_RX {r['table'].pattern}")
    if not r["out"].match(body["out_dir"].rstrip("/")) or ".." in body["out_dir"]:
        bad.append(f"out_dir {body['out_dir']!r} refused by the app's OUT_RX {r['out'].pattern}")
    wh = body.get("warehouse") or ""
    if not r["warehouse"].match(wh):
        bad.append("warehouse empty or not a 16-hex id: the app answers 400 (and a body without the field would silently use its DEFAULT_WAREHOUSE, DEV-WH)")
    if any(wh.startswith(f) for f in FORBIDDEN_WAREHOUSES) and not (approved_warehouse and wh == approved_warehouse.strip()):
        bad.append(f"warehouse {wh} is on the grants-hub never-use list: needs Jason's or Rohan's explicit OK for this run")
    if app == "keywords" and r["models"] is not None and body["model"] not in r["models"]:
        bad.append(f"model {body['model']} not in ALLOWED_MODELS {sorted(r['models'])}")
    return bad


class ModalCall:
    """POST /start once per build, keep the call id in <out_dir>/_call_id.txt, poll /status until done (Jason's loop)."""

    def __init__(self, app, endpoint, token, fs, log=print):
        self.app, self.endpoint, self.token, self.fs, self.log = app, endpoint.rstrip("/"), token, fs, log

    def post(self, path, body, tries=5):
        for t in range(tries):
            try:
                req = urllib.request.Request(f"{self.endpoint}{path}", data=json.dumps(body).encode(), method="POST",
                                             headers={"authorization": f"Bearer {self.token}", "content-type": "application/json"})
                with urllib.request.urlopen(req, timeout=120) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as e:
                if e.code < 500 or t == tries - 1:
                    raise RuntimeError(f"{self.app} {path}: HTTP {e.code} {e.read()[:500]!r}")
            except (urllib.error.URLError, TimeoutError) as e:
                if t == tries - 1:
                    raise
                self.log(f"{self.app} {path}: {e!r}; retrying")
            time.sleep(15 * (t + 1))

    def run(self, body, max_minutes):
        out_dir = body["out_dir"].rstrip("/")
        call_file = f"{out_dir}/_call_id.txt"
        self.fs.mkdirs(out_dir)
        if "_call_id.txt" in {f.name for f in self.fs.ls(out_dir)}:
            call_id = self.fs.head(call_file).strip()
            self.log(f"{self.app}: resuming Modal call {call_id}")
        else:
            r = self.post("/start", body)
            call_id = r["call_id"]
            self.fs.put(call_file, call_id, overwrite=True)
            self.log(f"{self.app}: started Modal call {call_id} ({json.dumps({k: v for k, v in r.items() if k != 'call_id'})})")
        t0, last = time.time(), 0
        while True:
            s = self.post("/status", {"call_id": call_id})
            if s["state"] == "done":
                return s["result"]
            if s["state"] == "failed":
                self.fs.rm(call_file)   # a retry starts a fresh call instead of re-reading this failure
                raise RuntimeError(f"{self.app}: Modal call {call_id} failed: {s.get('error')}")
            if time.time() - t0 > max_minutes * 60:
                raise TimeoutError(f"{self.app}: Modal call {call_id} not done after {max_minutes} min (a retry resumes polling it)")
            if time.time() - last > 300:
                self.log(f"{self.app}: running ({(time.time() - t0) / 60:.0f} min)")
                last = time.time()
            time.sleep(30)
