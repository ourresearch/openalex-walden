"""IndexNow pings for openalex.org record pages (oxjob #1689).

IndexNow (https://www.indexnow.org/documentation) tells Bing, Yandex, Naver, Seznam, Yep and Amazon that
URLs were added, updated or deleted. One POST to https://api.indexnow.org/indexnow carries up to 10,000
URLs of one host; the participating engines share submissions with each other.

The key is public by design: the engines fetch https://openalex.org/<key>.txt and check that it contains
the key (a file at the root covers every URL on the host). The website serves that file from
openalex-gui public/<key>.txt.

Pure Python + urllib (no dependency). The nightly notebook (notebooks/sitemaps/IndexNowPing.py) and the
tests use it; `send=False` (the default everywhere) builds and logs every batch and sends nothing.
"""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Iterable, Sequence

ENDPOINT = "https://api.indexnow.org/indexnow"
HOST = "openalex.org"
KEY = "75eb1c787b7ae8460b0772366614b9ab"   # public; must equal the body of https://openalex.org/<KEY>.txt
MAX_URLS_PER_POST = 10_000
KEY_RE = re.compile(r"^[a-zA-Z0-9-]{8,128}$")

# What each response code means (IndexNow docs). 200/202 = accepted (202: key validation pending).
OK_CODES = {200, 202}
RETRY_CODES = {429, 500, 502, 503, 504}
FATAL_CODES = {400, 403, 422}   # bad format / key not valid / URLs not on this host: stop, fix, rerun


def key_location(host: str = HOST, key: str = KEY) -> str:
    return f"https://{host}/{key}.txt"


def payload(urls: Sequence[str], host: str = HOST, key: str = KEY) -> dict:
    if not KEY_RE.match(key):
        raise ValueError("IndexNow key must be 8-128 of [a-zA-Z0-9-]")
    if not 0 < len(urls) <= MAX_URLS_PER_POST:
        raise ValueError(f"1..{MAX_URLS_PER_POST} URLs per POST, got {len(urls)}")
    prefix = f"https://{host}/"
    bad = [u for u in urls if not u.startswith(prefix)]
    if bad:
        raise ValueError(f"{len(bad)} URLs not on {host}, e.g. {bad[0]}")
    return {"host": host, "key": key, "keyLocation": key_location(host, key), "urlList": list(urls)}


def batches(urls: Iterable[str], size: int = MAX_URLS_PER_POST) -> Iterable[list[str]]:
    """Dedups (keeping first occurrence, i.e. priority order) and splits into POST-sized lists."""
    seen, cur = set(), []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        cur.append(u)
        if len(cur) == size:
            yield cur
            cur = []
    if cur:
        yield cur


def http_post(url: str, body: dict, timeout: float = 60.0) -> int:
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json; charset=utf-8"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


@dataclass
class PingResult:
    sent_batches: int = 0
    sent_urls: int = 0
    skipped_urls: int = 0                 # over the cap, left for the next night / the sitemaps
    statuses: list = field(default_factory=list)
    stopped_reason: str = ""
    dry_run: bool = True


def ping(urls: Iterable[str], *, send: bool = False, max_urls: int = 1_000_000, pause_s: float = 1.0,
         max_retries: int = 4, poster: Callable[[str, dict], int] = http_post,
         log: Callable[[str], None] = print, sleep: Callable[[float], None] = time.sleep,
         endpoint: str = ENDPOINT, host: str = HOST, key: str = KEY) -> PingResult:
    """POST `urls` (priority order) in batches of 10,000, at most `max_urls` in total.

    send=False: build and validate every payload, log the plan, send nothing.
    429/5xx: back off (30 s, 60 s, 120 s, ...) and retry the same batch up to max_retries, then stop the
    run (IndexNow says too many requests "may be flagged as potential spam"). 400/403/422: stop at once.
    """
    res = PingResult(dry_run=not send)
    for b in batches(urls):
        if res.sent_urls + len(b) > max_urls:
            room = max_urls - res.sent_urls
            res.skipped_urls += len(b) - max(room, 0)
            b = b[:max(room, 0)]
            if not b:
                continue
        body = payload(b, host, key)
        if not send:
            res.sent_batches += 1
            res.sent_urls += len(b)
            res.statuses.append(None)
            if res.sent_batches == 1:
                log(f"[dry run] first batch: {len(b)} URLs, e.g. {b[0]}; keyLocation {body['keyLocation']}")
            continue
        attempt = 0
        while True:
            status = poster(endpoint, body)
            if status in OK_CODES:
                break
            if status in RETRY_CODES and attempt < max_retries:
                wait = 30 * (2 ** attempt)
                log(f"batch {res.sent_batches + 1}: HTTP {status}; retry in {wait}s")
                sleep(wait)
                attempt += 1
                continue
            res.statuses.append(status)
            res.stopped_reason = f"HTTP {status} on batch {res.sent_batches + 1}"
            log(f"stopping: {res.stopped_reason}")
            return res
        res.statuses.append(status)
        res.sent_batches += 1
        res.sent_urls += len(b)
        if res.sent_batches % 10 == 0:
            log(f"sent {res.sent_batches} batches, {res.sent_urls:,} URLs")
        sleep(pause_s)
    log(f"{'[dry run] would send' if not send else 'sent'} {res.sent_urls:,} URLs in {res.sent_batches} "
        f"batches; {res.skipped_urls:,} over the cap")
    return res
