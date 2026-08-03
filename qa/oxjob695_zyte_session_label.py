"""oxjob #695 — label blocked-host rows using the Zyte session warm-up pattern.

The pattern that beats Cloudflare (9/9 in testing): browser-render the article
landing page inside a Zyte session, then fetch the PDF URL in that SAME session.
Used here to generate ground-truth labels on hosts the normal harvester cannot
reach, so we can mine DOM rules for them. Resumes by anti-join on pdf_url.

Usage: oxjob695_zyte_session_label.py <sample.json> <out.jsonl> [limit]
"""
import base64
import json
import os
import sys
import time
import uuid
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv("/Users/caseymeyer/PycharmProjects/openalex-taxicab/.env")
KEY = os.getenv("ZYTE_API_KEY")
assert KEY, "ZYTE_API_KEY not found"
ENDPOINT = "https://api.zyte.com/v1/extract"

src, dst = sys.argv[1], sys.argv[2]
limit = int(sys.argv[3]) if len(sys.argv) > 3 else 10**9


def zyte(params):
    r = requests.post(ENDPOINT, auth=(KEY, ""), json=params, timeout=200)
    if r.status_code != 200:
        j = r.json() if "json" in r.headers.get("content-type", "") else {}
        return b"", f"zyte{r.status_code}:{j.get('title','?')}"
    j = r.json()
    body = base64.b64decode(j["httpResponseBody"]) if j.get("httpResponseBody") else b""
    return body, str(j.get("statusCode"))


def landing_for(pdf_url, native_id):
    """Article page for the PDF URL — the session warm-up target."""
    if "onlinelibrary.wiley.com" in pdf_url:
        return pdf_url.replace("/doi/pdfdirect/", "/doi/")
    if "tandfonline.com" in pdf_url:
        return pdf_url.split("/doi/pdf/")[0] + "/doi/" + pdf_url.split("/doi/pdf/")[1].split("?")[0]
    if native_id.startswith("https://doi.org/"):
        return native_id
    return pdf_url


done = set()
try:
    for line in open(dst):
        done.add(json.loads(line)["pdf_url"])
except FileNotFoundError:
    pass

items = [it for it in json.load(open(src)) if it["pdf_url"] not in done][:limit]
print(f"{len(items)} to label")
with open(dst, "a") as out:
    for i, it in enumerate(items, 1):
        sid = str(uuid.uuid4())
        zyte({"url": landing_for(it["pdf_url"], it["native_id"]),
              "browserHtml": True, "session": {"id": sid}})
        time.sleep(1)
        body, status = zyte({"url": it["pdf_url"], "httpResponseBody": True,
                             "httpResponseHeaders": True, "session": {"id": sid}})
        is_pdf = body[:4] == b"%PDF"
        rec = {**it, "zyte_status": status, "is_pdf": is_pdf, "bytes": len(body)}
        out.write(json.dumps(rec) + "\n")
        out.flush()
        print(f"[{i}/{len(items)}] {it['url_host']:<24} status={status:<14} PDF={is_pdf} {len(body):,}b", flush=True)
        time.sleep(1.5)
