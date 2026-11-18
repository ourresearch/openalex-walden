"""Read authorship facts from PMC open-data JATS (oxjob #1793).

Only the article's <front> is read: contributors, their affiliations, ORCIDs, corresponding flags and CRediT roles,
plus the article type and whether an abstract exists. Pure Python (stdlib), so it runs on the driver, in mapInPandas, and on
a laptop against saved XML. `fetch_front()` streams an object and stops at </front>, so the body is never downloaded
in full.

Rules (EXPLORE § 2-3 of #1793):
- A contributor is an author when contrib-type is "author" or missing, inside article-meta (never sub-articles).
- Corresponding = the contributor itself is flagged: corresp="yes" or an <xref ref-type="corresp">. An author-notes
  <corresp> line that flags no contributor is recorded (corresp_notes) but assigns nobody.
- Affiliations: an author's <xref ref-type="aff" rid=...> targets, or an <aff> nested in the contrib. When no author
  links to any aff, a single article-level aff applies to every author; several unlinked affs apply to nobody.
- CRediT roles (oxjob #2075): the <role> elements inside an author's <contrib>, kept only when they name one of the 14
  CRediT roles (by a credit.niso.org role URL, else by the term's text). Roles are taken as the article states them per
  author, never inferred: an "Author contributions" footnote that names people by initials is not read.
"""
import html.entities
import re
import time
import urllib.request
import xml.etree.ElementTree as ET

PARSER_VERSION = "pmc-jats-3"  # 2: affiliation text skips <institution-id>; named entities decoded. 3: CRediT roles (#2075)
BUCKET_HTTPS = "https://pmc-oa-opendata.s3.amazonaws.com"
_FRONT_END = b"</front>"
_MAX_FRONT_BYTES = 4 * 1024 * 1024
_NAMED_ENTITY = re.compile(rb"&(?!(?:amp|lt|gt|quot|apos|#[0-9]+|#x[0-9a-fA-F]+);)[A-Za-z][A-Za-z0-9]*;")
_DOCTYPE = re.compile(rb"<!DOCTYPE[^>\[]*(\[[^\]]*\])?\s*>", re.S)
_WS = re.compile(r"\s+")


def https_url(s3_url_or_key):
    """s3://pmc-oa-opendata/PMC1.1/PMC1.1.xml?md5=... -> https URL (md5 query dropped)."""
    key = s3_url_or_key.split("?", 1)[0]
    key = key.replace("s3://pmc-oa-opendata/", "")
    return f"{BUCKET_HTTPS}/{key.lstrip('/')}"


def _get(url, read, timeout=30, tries=4):
    """GET with retries and exponential backoff; 403/404 fail at once. `read(response)` returns the bytes kept."""
    last = None
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return read(r)
        except Exception as e:  # noqa: BLE001 - retried, then raised with context
            last = e
            if getattr(e, "code", None) in (403, 404):
                break
            time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"GET {url}: {last!r}")


def _read_until_front(r):
    buf = bytearray()
    while len(buf) < _MAX_FRONT_BYTES:
        chunk = r.read(32768)
        if not chunk:
            break
        buf += chunk
        if _FRONT_END in buf[-len(chunk) - 16:]:
            break
    return bytes(buf)


def http_get(url, timeout=30, tries=4):
    return _get(url, lambda r: r.read(), timeout, tries)


def fetch_front(url, timeout=30, tries=4):
    """Stream a JATS object until </front>; returns the bytes read (the whole object if it has no </front>)."""
    return _get(url, _read_until_front, timeout, tries)


def _strip_ns(root):
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
        for k in list(el.attrib):
            if "}" in k:
                el.attrib[k.split("}", 1)[1]] = el.attrib.pop(k)
    return root


def _front_root(raw):
    """Parse <article> ... </front> as a document (the body is cut off and the root closed by hand)."""
    raw = _DOCTYPE.sub(b"", raw, count=1)
    i = raw.find(_FRONT_END)
    if i >= 0:
        raw = raw[: i + len(_FRONT_END)] + b"</article>"
    try:
        return _strip_ns(ET.fromstring(raw))
    except ET.ParseError:
        return _strip_ns(ET.fromstring(_NAMED_ENTITY.sub(_decode_entity, raw)))


def _decode_entity(m):
    """HTML named entity (the DTD we drop declares them) -> its UTF-8 text, XML-escaped; unknown -> space."""
    ch = html.entities.html5.get(m.group(0)[1:].decode("ascii", "replace"))
    if ch is None:
        return b" "
    return ch.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").encode("utf-8")


def _text(el, skip=("label",)):
    if el is None:
        return ""
    parts = []

    def walk(e, top):
        if not top and e.tag in skip:
            if e.tail:
                parts.append(e.tail)
            return
        if e.text:
            parts.append(e.text)
        for c in e:
            walk(c, False)
        if not top and e.tail:
            parts.append(e.tail)

    walk(el, True)
    return _WS.sub(" ", "".join(parts)).strip(" ,;")


def _aff_text(aff):
    # Leading <sup>1</sup> markers act as labels too. <institution-id> holds GRID / ISNI / ROR / Ringgold ids, which
    # would otherwise be glued onto the name ("grid.412301.5Institut für ...").
    t = _text(aff, skip=("label", "sup", "fn", "institution-id"))
    return t[:1000] if len(t) > 3 else None


# The 14 CRediT roles (ANSI/NISO Z39.104-2022, https://credit.niso.org/), by the slug of their credit.niso.org URL.
# Crossref deposits use the same slugs (`role` with vocabulary "credit"), so both sources speak one vocabulary.
CREDIT_ROLES = (
    "conceptualization", "data-curation", "formal-analysis", "funding-acquisition", "investigation", "methodology",
    "project-administration", "resources", "software", "supervision", "validation", "visualization",
    "writing-original-draft", "writing-review-editing",
)
_CREDIT_URL = re.compile(r"credit\.niso\.org/contributor-roles/([a-z-]+)", re.I)
# A role's text, normalized by _credit_key, -> its slug: each slug's own words plus spellings seen in PMC JATS.
_CREDIT_TERMS = {s.replace("-", " "): s for s in CREDIT_ROLES} | {
    "conceptualisation": "conceptualization",
    "visualisation": "visualization",
    "writing original draft preparation": "writing-original-draft",
    "writing review and editing": "writing-review-editing",
}


def _credit_key(text):
    t = (text or "").lower().replace("&", " and ")
    t = re.sub(r"[^a-z]+", " ", t)
    return _WS.sub(" ", t).strip()


def credit_roles(contrib):
    """CRediT role slugs a <contrib> states in its <role> elements, first-seen order, no repeats (oxjob #2075).

    A <role> counts when its vocab-term-identifier or content-type is a credit.niso.org role URL, else when its text
    is a CRediT term; one <role> may list several terms separated by commas or semicolons. Anything else ("Editor",
    "Author", free text) is dropped."""
    out = []
    for r in contrib.iter("role"):
        slugs = []
        for attr in ("vocab-term-identifier", "content-type", "specific-use"):
            m = _CREDIT_URL.search(r.get(attr) or "")
            if m:
                slugs = [m.group(1).lower()]
                break
        if not slugs:
            terms = [r.get("vocab-term")] if r.get("vocab-term") else re.split(r"[,;]", _text(r, skip=()))
            slugs = [_CREDIT_TERMS.get(_credit_key(t)) for t in terms]
        for slug in slugs:
            if slug in CREDIT_ROLES and slug not in out:
                out.append(slug)
    return out


def parse_front(raw):
    """Authorship facts from a JATS document (bytes). Returns a dict; raises on unparseable XML."""
    root = _front_root(raw)
    meta = root.find("./front/article-meta")
    if meta is None:
        meta = root.find(".//article-meta")
    out = {"article_type": root.get("article-type"), "authors": [],
           "has_abstract": False, "corresp_notes": False, "n_affs": 0}
    if meta is None:
        return out

    affs_by_id, affs_all = {}, []
    for aff in meta.iter("aff"):
        t = _aff_text(aff)
        if not t:
            continue
        affs_all.append(t)
        if aff.get("id"):
            affs_by_id[aff.get("id")] = t
    out["n_affs"] = len(affs_all)
    out["has_abstract"] = any(
        len(_text(a)) > 100 for a in meta.findall("abstract")
        if a.get("abstract-type") not in ("graphical", "teaser", "toc", "author-highlights"))
    out["corresp_notes"] = meta.find("./author-notes/corresp") is not None

    authors = []
    for group in meta.findall("contrib-group"):
        for c in group.findall("contrib"):
            if c.get("contrib-type", "author") != "author":
                continue
            name_el = c.find("name")
            if name_el is None:
                name_el = c.find("name-alternatives/name")
            given = family = name = None
            if name_el is not None:
                family = _text(name_el.find("surname")) or None
                given = _text(name_el.find("given-names")) or None
                name = " ".join(x for x in (given, family) if x) or None
            elif c.find("string-name") is not None:
                name = _text(c.find("string-name")) or None
            elif c.find("collab") is not None:
                name = _text(c.find("collab")) or None
            if not name:
                continue
            orcid = None
            for cid in c.findall("contrib-id"):
                if (cid.get("contrib-id-type") or "").lower() == "orcid":
                    m = re.search(r"(\d{4}-\d{4}-\d{4}-\d{3}[\dXx])", _text(cid))
                    orcid = f"https://orcid.org/{m.group(1).upper()}" if m else None
            linked = []
            for x in c.findall(".//xref[@ref-type='aff']"):
                for rid in (x.get("rid") or "").split():
                    if rid in affs_by_id and affs_by_id[rid] not in linked:
                        linked.append(affs_by_id[rid])
            for a in c.findall(".//aff"):
                t = _aff_text(a)
                if t and t not in linked:
                    linked.append(t)
            corresp = c.get("corresp") == "yes" or c.find(".//xref[@ref-type='corresp']") is not None
            authors.append({"given": given, "family": family, "name": name, "orcid": orcid,
                            "affiliations": linked, "is_corresponding": corresp, "roles": credit_roles(c)})
    if authors and len(affs_all) == 1 and not any(a["affiliations"] for a in authors):
        for a in authors:
            a["affiliations"] = [affs_all[0]]
    out["authors"] = authors
    return out
