"""Shared text cleaning for work titles + abstracts (oxjob #807).

Three corruptions are repaired in one idempotent pass, applied at ingest so every
provenance is cleaned consistently and a full Silver reparse cleans the corpus at rest:

  1. HTML entities        `Universit&eacute;`  -> `Université`   (repos/JATS never decode)
  2. Mojibake             `UniversitÃ©`         -> `Université`   (double-encoded UTF-8; #801 core)
  3. HTML tags            `<i>E. coli</i>`      -> `E. coli`      (strip ALL tags; Jason 2026-08-18)

Order matters. Entity-decode and mojibake-repair are iterated together to a fixpoint
because either can reveal the other (an entity-encoded mojibake byte `&Atilde;&copy;`
decodes to `Ã©` which must then be repaired; a double-encoded `&amp;eacute;` needs two
decode rounds). Tags are stripped LAST, after all decoding, so an entity-encoded payload
(`&lt;script&gt;`) is first turned into a real tag and then removed -> the output is inert
(this is the XSS defense: strip-all-tags means no executable markup can survive).

The mojibake core (`repair_mojibake`) is ported verbatim from the `openalex.common.repair_mojibake`
UC UDF built by oxjob #801 (restricted-lead signature, strict whole-string round-trip, least-bad
codec among latin-1/cp1252/iso-8859-2/cp1250, fixpoint <= MAX_ROUNDS, never emits U+FFFD; 95.3%
repair / 0 FP on #801's 3K-string corpora). Keep this in sync with that UDF — the RLIKE gate and
the repair must agree across call sites (walden CLAUDE.md).

All functions are pure Python (no Spark) so they unit-test without a cluster, and idempotent so
`clean_abstract_text` can be applied twice on the abstract path without drift.
"""

import html
import re
import unicodedata

# Iterate decode+repair to a fixpoint; deeper than either corruption is observed
# nested (double-encoded entities ~2, mojibake ~5) so a clean input converges fast.
_MAX_CLEAN_ROUNDS = 6


# --------------------------------------------------------------------------- #
# Mojibake repair — ported verbatim from openalex.common.repair_mojibake (#801)
# --------------------------------------------------------------------------- #

_MAX_ROUNDS = 4  # corpus has strings mojibaked up to ~5 levels deep; each round strictly shrinks

# unified latin-1 + cp1252 reverse map: chars <= U+00FF map to their own codepoint
# (latin-1); cp1252's specials (> U+00FF) map to the 0x80-0x9F byte cp1252 decoded them from.
_CP1252_REV = {}
for _b in range(0x80, 0xA0):
    try:
        _CP1252_REV[bytes([_b]).decode("cp1252")] = _b
    except UnicodeDecodeError:
        pass  # 0x81 0x8D 0x8F 0x90 0x9D are undefined in cp1252

# Core leads: chars whose latin-1 byte is a UTF-8 lead byte for scripts that occur in real text.
_CORE_LEADS_2 = set("ÃÂÄÅÌÎÏÐÑ")  # Ì (CC) covers NFD combining-mark mojibake ("MuÌnster")
# Peripheral leads: may participate in repair but never trigger it alone (kills the
# Ö/Ü + typographic-NBSP legit-text corruption risk).
_PERIPHERAL_LEADS_2 = set("ÆÇÒÓÔÕÖØÙÚÛÜ")


def _is_cont(ch):
    """Char maps (via unified map) to a UTF-8 continuation byte 0x80-0xBF?"""
    cp = ord(ch)
    if 0x80 <= cp <= 0xBF:
        return True
    return _CP1252_REV.get(ch, 0) >= 0x80  # cp1252 specials are all 0x80-0x9F


def _detect(s):
    """True if s contains at least one CORE mojibake sequence (latin-1/cp1252 read)."""
    if not s:
        return False
    n = len(s)
    for i, ch in enumerate(s):
        if ch in _CORE_LEADS_2 and i + 1 < n and _is_cont(s[i + 1]):
            return True
        if ch == "â" and i + 2 < n and _is_cont(s[i + 1]) and _is_cont(s[i + 2]):
            return True
        cp = ord(ch)
        # 3-byte leads à-á ã-ï (skip â handled above) need two continuations, at least one
        # not NBSP: legit "Umeå\xa0\xa0University" is byte-valid UTF-8 for a rare CJK char.
        if (0xE0 <= cp <= 0xEF and cp != 0xE2 and i + 2 < n
                and _is_cont(s[i + 1]) and _is_cont(s[i + 2])
                and not (s[i + 1] == "\xa0" and s[i + 2] == "\xa0")):
            return True
        # 4-byte leads ð-ô need three continuations (same NBSP rule)
        if (0xF0 <= cp <= 0xF4 and i + 3 < n
                and _is_cont(s[i + 1]) and _is_cont(s[i + 2]) and _is_cont(s[i + 3])
                and not (s[i + 1] == s[i + 2] == s[i + 3] == "\xa0")):
            return True
    return False


def _fallback_signature(s, codec):
    """True if s carries a DISTINCTIVE lead+continuation pair under a Central-European
    single-byte codec (ISO-8859-2 / cp1250): UTF-8 lead bytes C3/C4/C5 decode to Ă/Ä/Ĺ there.
    'Distinctive' = the lead char maps to a byte != its own codepoint, so only that codec's
    read produces it — without this, latin2's É (0xC9, == latin-1) reopens the
    'UNIVERSITÉ\\xa0PARIS' trap the core detector closed."""
    if not s:
        return False

    def byte_of(ch):
        try:
            return ch.encode(codec)[0]
        except (UnicodeEncodeError, IndexError):
            return None

    n = len(s)
    for i, ch in enumerate(s):
        b = byte_of(ch)
        if b is not None and 0xC2 <= b <= 0xDF and b != ord(ch) and i + 1 < n:
            b2 = byte_of(s[i + 1])
            if b2 is not None and 0x80 <= b2 <= 0xBF:
                return True
    return False


def _to_bytes_unified(s):
    out = bytearray()
    for ch in s:
        cp = ord(ch)
        if cp <= 0xFF:
            out.append(cp)
        else:
            b = _CP1252_REV.get(ch)
            if b is None:
                return None
            out.append(b)
    return bytes(out)


def _validate(s, raw):
    if raw is None:
        return None
    try:
        fixed = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None
    if len(fixed) >= len(s) or "�" in fixed:
        return None
    return fixed


def _badness(s):
    """Count of chars implausible in repaired text (symbols, controls); picks among codec
    candidates — a wrong-codec 'repair' that still validates leaves stray symbols."""
    score = 0
    for ch in s:
        if ch in "\n\r\t":
            continue
        cat = unicodedata.category(ch)
        if cat.startswith("C"):
            score += 3
        elif cat.startswith("S"):
            score += 1
    return score


def _repair_once(s):
    """One round of repair, or None if not repairable. All candidate codecs are tried and the
    least-bad validated result wins (iso-8859-2/cp1250 signatures overlap; wrong codec can still
    yield valid-UTF-8 garbage, so first-match is unsafe)."""
    candidates = []
    if _detect(s):
        fixed = _validate(s, _to_bytes_unified(s))
        if fixed is not None:
            candidates.append(fixed)
    for codec in ("iso-8859-2", "cp1250"):
        if _fallback_signature(s, codec):
            try:
                raw = s.encode(codec, errors="strict")
            except UnicodeEncodeError:
                continue
            fixed = _validate(s, raw)
            if fixed is not None:
                candidates.append(fixed)
    if not candidates:
        return None
    return min(candidates, key=_badness)  # ties keep candidate order (min is stable)


def _fix_bare_c1(s):
    """Map surviving C1 controls (U+0080-U+009F) to their cp1252 chars."""
    if not any(0x80 <= ord(ch) <= 0x9F for ch in s):
        return s
    out = []
    for ch in s:
        if 0x80 <= ord(ch) <= 0x9F:
            try:
                ch = bytes([ord(ch)]).decode("cp1252")
            except UnicodeDecodeError:
                pass  # 0x81 0x8D 0x8F 0x90 0x9D undefined -> keep
        out.append(ch)
    return "".join(out)


# --------------------------------------------------------------------------- #
# Necessary-condition gate for repair_mojibake (oxjob #881 perf).
#
# Every path that can change a string requires a TRIGGER LEAD character:
#   - _detect: a core lead (_CORE_LEADS_2), "â", a 3-byte lead U+00E0-U+00EF (except â),
#     or a 4-byte lead U+00F0-U+00F4 -- all >= U+00A0;
#   - _fallback_signature: a char that encodes to 0xC2-0xDF under iso-8859-2/cp1250 with a
#     byte DIFFERENT from its own codepoint (the "distinctive" rule) -- e.g. latin2's C4/C5
#     reads;
#   - _fix_bare_c1 runs only after a repair happened.
# A string containing none of these characters cannot be modified, so skipping it is EXACT,
# not heuristic. The set is built from the same primitives the detectors use, so it cannot
# drift from them. Measured: the per-char Python detectors were ~50% of ALL enrichment UDF
# time corpus-wide; this gate removes them for the clean majority at C speed.
def _build_trigger_lead_re():
    chars = set(_CORE_LEADS_2) | {"\u00e2"}
    chars |= {chr(c) for c in range(0xE0, 0xF0) if c != 0xE2}
    chars |= {chr(c) for c in range(0xF0, 0xF5)}
    for codec in ("iso-8859-2", "cp1250"):
        for b in range(0xC2, 0xE0):
            try:
                ch = bytes([b]).decode(codec)
            except UnicodeDecodeError:
                continue
            if ord(ch) != b:
                chars.add(ch)
    return re.compile("[" + "".join(re.escape(c) for c in sorted(chars)) + "]")


_TRIGGER_LEAD_RE = _build_trigger_lead_re()


def repair_mojibake(s):
    """Repair mojibake in s; returns s unchanged if not confidently repairable."""
    if not s:
        return s
    # exact no-op fast path -- see _build_trigger_lead_re above
    if s.isascii() or not _TRIGGER_LEAD_RE.search(s):
        return s
    cur = s
    for _ in range(_MAX_ROUNDS):
        nxt = _repair_once(cur)
        if nxt is None:
            break
        cur = nxt
    if cur is not s:
        cur = _fix_bare_c1(cur)
    return cur


# --------------------------------------------------------------------------- #
# HTML entities + tags
# --------------------------------------------------------------------------- #

def decode_entities(s):
    """Decode HTML entities (named `&eacute;`, decimal `&#233;`, hex `&#xe9;`), iterating for
    double-encodings (`&amp;eacute;` -> `&eacute;` -> `é`). One `html.unescape` pass resolves
    every entity in the string; a second pass catches a layer that the first revealed."""
    if not s:
        return s
    for _ in range(_MAX_CLEAN_ROUNDS):
        nxt = html.unescape(s)
        if nxt == s:
            break
        s = nxt
    return s


# A tag is `<` immediately followed by a letter (open) or `/`+letter (close), up to the next `>`.
# Requiring NO space after `<` (real HTML has none) means math/inequalities like "p < 0.05 > 0"
# or "a < b > c" are NOT stripped — only real markup (`<i>`, `</i>`, `<br/>`, `<script ...>`,
# `<jats:p>`) is. The trailing pattern catches a tag truncated by a length cap ("... an <i").
_TAG_RE = re.compile(r"<(?:/[a-zA-Z]|[a-zA-Z])[^>]*>")
_TRAILING_OPEN_TAG_RE = re.compile(r"<(?:/[a-zA-Z]|[a-zA-Z])[^<>]*$")

# Pull the bare element name out of a tag: `</mml:msub>` -> `msub`, `<styled-content x="y">` ->
# `styled-content`. The namespace prefix is dropped because the corpus carries the *same* element
# under many prefixes — `mml:mi` / `m:mi` / bare `mi`, and `jats:p` / `jats1:p` / `ns4:p` / `p`.
_TAG_NAME_RE = re.compile(r"^</?([a-zA-Z][a-zA-Z0-9:._-]*)")

# Removing a tag has to decide what the markup MEANT, because the two wrong answers fail in
# opposite directions and both are unsearchable:
#
#   `CO<sub>2</sub>`                  -> "CO 2"  splits one token into two   (must NOT add a space)
#   `outcomes.<h3>SIGNIFICANCE</h3>`  -> "…SIGNIFICANCE…" welds two words    (MUST add a space)
#
# So: character-level markup closes up, structural markup becomes a space. Both lists are drawn
# from the actual corpus — a tag census over the 6.6M corrupted abstracts in
# `abstracts_backfill_corrupt_stage` (oxjob #807, 2026-08-18), which is why odd non-standard
# spellings like Elsevier's `<inf>` (229K occurrences, means subscript) are here.

# Character-level: text formatting, and the math LEAF/GROUPING elements. Removing these must
# close up — `<mi>x</mi><mo>+</mo>` is the single expression `x+`, not `x +`.
_INLINE_TAGS = frozenset("""
    b i em strong u s strike small big span a code tt sub sup inf sc
    italic bold underline overline monospace roman sans-serif serif styled-content fixed-case
    mi mo mn ms mtext mspace mrow mfrac msqrt mroot msub msup msubsup munder mover
    munderover mmultiscripts mfenced mstyle mpadded mphantom menclose mprescripts none
""".split())

# Structural: the tag IS a boundary. Removing these must leave a space.
#
# Note where the math wrappers sit. `math`, `semantics`, `inline-formula`, `tex-math` and
# `annotation` are NOT character-level — they delimit a formula from the surrounding prose, or
# one *representation* of a formula from another. MathML routinely carries the same expression
# twice, once as presentation markup and once as a TeX `annotation`, so treating the wrapper as
# inline welds the two copies into a single junk token (`x+y` + `x+y` -> `x+yx+y`). Most of the
# corpus already has whitespace between these elements, which hides the problem — but ~4% does
# not (1,027 of 25,624 annotation-bearing rows in the staged corpus). Structural is never worse
# in the spaced case and strictly better in the unspaced one.
_BLOCK_TAGS = frozenset("""
    p br div hr h1 h2 h3 h4 h5 h6 li ul ol dl dt dd
    table thead tbody tfoot tr td th caption blockquote pre
    section article header footer aside nav figure figcaption
    sec title abstract list list-item disp-formula disp-quote disp-quote-attrib
    fig graphic media label boxed-text table-wrap body front back
    ref ref-list statement verse-group speech def-list def term
    math semantics annotation annotation-xml
    inline-formula inline-graphic tex-math tex etx alternatives formula chem-struct
    mtable mtr mtd
    lsdexception
""".split())


def _tag_replacement(match):
    """A space, unless the tag is character-level markup. Unknown tags default to a space:
    splitting a token leaves two real, searchable words, whereas welding two words together
    creates a junk token that matches nothing and is invisible in QA."""
    name = _TAG_NAME_RE.match(match.group(0))
    if not name:
        return " "
    tag = name.group(1).lower().rsplit(":", 1)[-1]  # drop any namespace prefix
    return "" if tag in _INLINE_TAGS else " "


def strip_tags(s):
    """Remove all HTML/XML/JATS/MathML tags, leaving the inert text content.

    Character-level tags close up (`CO<sub>2</sub>` -> `CO2`, `S<span>ã</span>o` -> `São`);
    structural tags become a space (`a<br>b` -> `a b`, `<h3>METHODS</h3>` -> ` METHODS `), which
    is what the pre-#807 JATS handling did for every tag. Also removes a single unclosed tag left
    dangling at the end by an upstream title/abstract length truncation."""
    if not s:
        return s
    s = _TAG_RE.sub(_tag_replacement, s)
    s = _TRAILING_OPEN_TAG_RE.sub(" ", s)
    return s


# --------------------------------------------------------------------------- #
# Combined cleaners
# --------------------------------------------------------------------------- #

def clean_text(s):
    """Idempotent core: repair mojibake + decode entities (iterated together to a fixpoint,
    since each can reveal the other), then strip all tags LAST so any entity-encoded markup is
    revealed and removed -> output is inert. Does NOT collapse whitespace (callers do)."""
    if not s or not isinstance(s, str):
        return s
    prev = None
    for _ in range(_MAX_CLEAN_ROUNDS):
        s = decode_entities(s)
        s = repair_mojibake(s)
        if s == prev:
            break
        prev = s
    return strip_tags(s)


def clean_title(title):
    """Clean a display title: mojibake + entities + strip-all-tags, then collapse whitespace.
    Preserves case and accents (it's display text, not the matching key). None/non-str pass
    through unchanged. Applied AFTER merge_key derivation so the matching key is unaffected."""
    if not title or not isinstance(title, str):
        return title
    cleaned = " ".join(clean_text(title).split()).strip()
    return cleaned if cleaned else title
