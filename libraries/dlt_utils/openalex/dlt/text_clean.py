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


def repair_mojibake(s):
    """Repair mojibake in s; returns s unchanged if not confidently repairable."""
    if not s:
        return s
    # oxjob #881: pure-ASCII text cannot contain mojibake -- skip the per-character detectors
    # (single C-level check; ~50% of abstracts, ~68% of titles). The broader trigger-lead regex
    # gate from b81da17e stays OUT pending a repro of the at-scale worker crash it shipped with.
    if s.isascii():
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


# A tag-SHAPED span is `<` or `</`, an element name (optionally `prefix:`), then either `>` at once or
# whitespace / `/` and attribute text up to the next `>`, with no `<` inside. Requiring the name to
# end at whitespace, `/` or `>` already keeps "30<Rλ<130", "CK<RSA<RS" and "p < 0.05 > 0" out.
#
# Tag-shaped is NOT enough to be markup (oxjob #1928). After entities are decoded, a literal
# less-than before a letter is common in real text: "(<I00 days old)", "(<age 25 years)", "<LOD",
# "<VGPR", angle-quoted titles ("Film <The Throne>(2015)", "<Hygrophila phlomoides> Nees"). The
# old rule stripped every `<`+letter up to the next `>` (or the end of the text), and cut a median
# 17 words from ~24K Crossref abstracts alone. So a span is removed only when it IS markup:
# `_decide` below. The trailing pattern catches a tag truncated by a length cap ("... an <i").
_TAG_NAME_PATTERN = r"[A-Za-z][A-Za-z0-9._-]*(?::[A-Za-z_][A-Za-z0-9._-]*)?"
_TAG_SHAPED_RE = re.compile(r"<(/?)(" + _TAG_NAME_PATTERN + r")((?:[\s/][^<>]*)?)>")
# A length cap can also cut a tag inside its prefix: "... D T</mml:".
_TRAILING_TAG_SHAPED_RE = re.compile(r"<(/?)(" + _TAG_NAME_PATTERN + r":?)((?:[\s/][^<>]*)?)$")

# Well-formed attribute text: `name`, `name=value`, `name="v"`, `name='v'`, separated by
# whitespace or `/` (browsers read `<svg/onload=...>` that way). An unquoted value runs to the next
# whitespace, as in the HTML tokenizer; letting it stop at a `/` instead makes the pattern ambiguous
# and its backtracking exponential on "x=a/b=a/b=..." (one such string stalls a Spark task).
_ATTRS_RE = re.compile(
    r"""(?:[\s/]+[A-Za-z_:@][-A-Za-z0-9_:.@]*(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s"'<>`]+(?=\s|\Z)))?)*[\s/]*"""
)
# Anything that could run code if a consumer ever rendered the text as HTML: an event handler (the
# HTML ones by name, so prose like "one = 5" or "onset=" is not one) or a script URL. A tag-shaped span
# carrying one is always removed, whatever its name (#807 inertness).
_ACTIVE_ATTR_RE = re.compile(
    r"(?i)(?:^|[\s/\"'])on(?:abort|activate|afterprint|animation[a-z]*|auxclick|before[a-z]+|begin|blur|"
    r"cancel|canplay[a-z]*|change|click|close|contextmenu|copy|cuechange|cut|dblclick|drag[a-z]*|drop|"
    r"durationchange|emptied|end|ended|error|focus[a-z]*|formdata|fullscreen[a-z]*|hashchange|input|"
    r"invalid|key[a-z]+|load[a-z]*|message[a-z]*|mouse[a-z]+|offline|online|page[a-z]+|paste|pause|"
    r"play[a-z]*|pointer[a-z]+|popstate|progress|ratechange|repeat|reset|resize|scroll[a-z]*|search|"
    r"securitypolicyviolation|seek[a-z]+|select[a-z]*|show|slotchange|stalled|storage|submit|suspend|"
    r"timeupdate|toggle|touch[a-z]+|transition[a-z]+|unhandledrejection|unload|volumechange|waiting|"
    r"webkit[a-z]+|wheel|zoom)\s*=|javascript:|vbscript:|data:\s*text/html"
)
# What a browser would read as a tag: `<` + letter (or `</` + letter) up to the next `>`, `<` included.
# Text the rules above keep can still look like this ("(<I00 days old) ... p>0.05"); the last pass
# (`_disarm`) removes any such stretch that would arm the page (#807 inertness), and leaves the rest.
_BROWSER_TAG_START_RE = re.compile(r"</?([A-Za-z][^\s/>]*)")
# A tag cut off by a length cap, inside its attribute list: "<mml:mo st", "<span xml:lang", '<a href="ht'.
_PARTIAL_ATTRS_RE = re.compile(
    r"""(?:[\s/]+[A-Za-z_:@][-A-Za-z0-9_:.@]*(?:\s*=\s*(?:"[^"]*"?|'[^']*'?|[^\s"'<>`]*(?=\s|\Z)))?)*[\s/]*"""
)
_QUOTED_VALUE_RE = re.compile(r"=\s*[\"']")
# Some deposits arrive with every tag's brackets swapped: USP theses ">i<Pinus>/i<", IEEE
# "Rb>tex<$^+$>/tex<". A swapped CLOSING tag (`>/name<`) never occurs in prose, so where one is
# present, `>name<` / `>/name<` with that markup name are tags too.
_REVERSED_TAG_RE = re.compile(r">(/?)(" + _TAG_NAME_PATTERN + r")<")

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


def _local_name(name):
    """`mml:msub` -> `msub`: the corpus carries the same element under many prefixes (`mml:mi` /
    `m:mi` / `mi`, `jats:p` / `jats1:p` / `ns4:p` / `p`), so names are compared without one."""
    return name.lower().rsplit(":", 1)[-1]


def _tag_replacement(name):
    """A space, unless the tag is character-level markup. Unknown tags default to a space:
    splitting a token leaves two real, searchable words, whereas welding two words together
    creates a junk token that matches nothing and is invisible in QA."""
    return "" if _local_name(name) in _INLINE_TAGS else " "


# Element names that are markup (oxjob #1928): the #807 census lists above, plus every HTML element
# (current and obsolete), MathML (presentation and the common content elements), the SVG elements
# that are not English words, the JATS / BITS / NLM tag set, DocBook and TEI inline names seen in
# deposits, and odd spellings found in deposits (Wiley `scp`, Bloomsbury `nestend`).
_MARKUP_NAMES = _INLINE_TAGS | _BLOCK_TAGS | frozenset("""
    abbr acronym address applet area audio base basefont bdi bdo bgsound blink button canvas center
    cite col colgroup data datalist del details dfn dialog dir embed fieldset font form frame frameset
    head hgroup html iframe img input ins isindex kbd keygen legend link listing main map mark marquee
    menu menuitem meta meter multicol nextid nobr noembed noframes noscript object optgroup option
    output param picture plaintext progress q rb rp rt rtc ruby samp script search select slot source
    spacer style summary template textarea time track ul var video wbr xmp
    maction maligngroup malignmark merror mglyph mlabeledtr mlongdiv mscarries mscarry msgroup msline
    msrow mstack apply ci cn csymbol bvar lowlimit uplimit logbase
    svg tspan textpath defs clippath lineargradient radialgradient foreignobject polyline polygon
    abbrev-journal-title access-date ack addr-line aff aff-alternatives alt-text alt-title app-group
    article-categories article-id article-meta article-title article-version author-comment
    author-notes award-desc award-group award-id award-name block-alternatives chapter-title
    chem-struct-wrap citation-alternatives collab-alternatives compound-kwd compound-kwd-part
    compound-subject compound-subject-part conf-acronym conf-date conf-loc conf-name conf-num
    conf-sponsor conf-theme contrib contrib-group contrib-id copyright-holder copyright-statement
    copyright-year corresp custom-meta custom-meta-group data-title date-in-citation def-head def-item
    disp-formula-group element-citation elocation-id equation-count etal event-desc ext-link fig-count
    fig-group floats-group fn fn-group fpage front-stub funding-group funding-source funding-statement
    given-names glyph-data glyph-ref inline-media inline-supplementary-material institution-id
    institution-wrap issn-l issue-id issue-part issue-sponsor issue-title journal-id journal-meta
    journal-subtitle journal-title journal-title-group kwd kwd-group license-p long-desc lpage
    meta-name meta-value milestone-end milestone-start mixed-citation name-alternatives named-content
    nested-kwd nlm-citation object-id on-behalf-of open-access overline-end overline-start page-count
    page-range part-title person-group preformat principal-award-recipient principal-investigator
    private-char processing-meta pub-date pub-history pub-id publisher-loc publisher-name
    question-preamble question-wrap question-wrap-group ref-count related-article related-object
    resource-group resource-id resource-name resource-wrap restricted-by sec-meta self-uri series-text
    series-title sig-block std-organization string-conf string-date string-name sub-article subj-group
    supplementary-material support-description support-group support-source surname table-count
    table-wrap-foot table-wrap-group term-head textual-form time-stamp title-group trans-abstract
    trans-source trans-subtitle trans-title trans-title-group underline-end underline-start
    unstructured-kwd-group verse-line volume-id volume-issue-group volume-series word-count
    xref emph emphasis para subscript superscript ulink smallcaps sc-caps scp hi nestend doi
""".split())

# Markup names that are also everyday words, so an angle-quoted word in prose ("<Section>",
# "<Label>") is not mistaken for markup. These count as markup only in a text that already holds
# markup, or when closed later, self-closing, or carrying an attribute value.
_WORD_NAMES = frozenset("""
    abstract address alternatives annotation area article back body caption code def fig figure footer
    formula front header label legend list map media none picture ref section sec semantics source
    speech statement table term title track
""".split())
_MARKUP_ONLY_NAMES = _MARKUP_NAMES - _WORD_NAMES

# Namespace prefixes seen on tags in deposits. `prefix:local` with one of these is markup outright.
_MARKUP_PREFIXES = frozenset("""
    jats jats1 jats2 mml m math xlink xsi xs xml ali tei dc dcterms oasis ce ja sb ns ns0 ns1 ns2 ns3
    ns4 ns5 ns6 o w st1 v x xhtml html svg bits
""".split())

# Elements that run or reinterpret what follows them (scripts, plugins, raw-text parsing): removed
# even bare, so no parser trick can arm the text (#807 inertness).
_DANGEROUS_NAMES = frozenset("""
    script style iframe frame frameset object embed applet svg math noscript xmp plaintext noembed
    noframes template textarea title isindex keygen marquee img image
""".split())

_CONTEXT = object()  # `_decide` result for a word-named tag: markup only in a marked-up text


def _decide(m, closed_names):
    """Is one tag-shaped span markup? True (strip), False (text), or _CONTEXT (strip only when the
    text holds other markup)."""
    close, name, attrs = m.group(1), m.group(2), m.group(3)
    body = attrs.strip()
    if _ACTIVE_ATTR_RE.search(attrs):
        return True                      # inert output beats everything (#807)
    # Checked only where a rule reads it; the empty attribute text is always well formed.
    well_formed = body == "" or _ATTRS_RE.fullmatch(attrs) is not None
    if close:
        # `</name>`, `</p -->` (a commented-out tag): prose never closes an angle bracket. But IEEE's
        # "/spl" notation puts a less-than before a slash: "D(P/spl par/Q)</spl infin/. We ask ...>".
        return well_formed or body.strip("- ") == ""
    lname = name.lower()
    local = _local_name(name)
    if ":" in lname and (lname.split(":", 1)[0] in _MARKUP_PREFIXES or local in _MARKUP_NAMES):
        return True                      # `<jats:p>`, `<mml:mi>`, `<o:p>`
    if body == "/":
        return True                      # `<br/>`, `<lt/>`, `<lsdexception/>`
    if lname in closed_names or local in closed_names:
        return True                      # opened here, closed later: paired markup
    has_value = "=" in body
    if local in _DANGEROUS_NAMES:
        return well_formed or has_value
    if local in _MARKUP_ONLY_NAMES:
        # `<i>`, `<sub>`, `<span style="...">`; a malformed attribute list still counts when it
        # carries a quoted value (`<span style="font-size: 12pt;>`), but "<I (n=5) vs grade >" does not.
        return well_formed or _QUOTED_VALUE_RE.search(body) is not None
    if well_formed and has_value:
        return True                      # any name with an attribute value: `<foo bar="x">`
    if local in _WORD_NAMES and well_formed:
        return _CONTEXT                  # "<Section>" in prose stays; <sec> in a JATS abstract goes
    return False


def strip_tags(s):
    """Remove HTML/XML/JATS/MathML markup, leaving the inert text content.

    Character-level tags close up (`CO<sub>2</sub>` -> `CO2`, `S<span>ã</span>o` -> `São`);
    structural tags become a space (`a<br>b` -> `a b`, `<h3>METHODS</h3>` -> ` METHODS `), which
    is what the pre-#807 JATS handling did for every tag. Also removes a single unclosed tag left
    dangling at the end by an upstream title/abstract length truncation.

    A `<` that does not start markup is text and stays (oxjob #1928): "(<I00 days old)", "<LOD",
    "0<x<1", "Film <The Throne>(2015)". Nothing that could run as HTML survives: an event handler or
    script URL is always removed, and so is every script/style/iframe/svg/math element.

    Runs to a fixpoint: removing `<i>` from `&lt;<i>u</i>&gt;` leaves a new `<u>`, and the ES sync
    cleans the stored abstract a second time, so one pass must already be the final answer
    (oxjob #191.1: the searched abstract and the inverted index come from identical text)."""
    if not s or "<" not in s:
        return s
    for _ in range(_MAX_CLEAN_ROUNDS):
        nxt = _strip_tags_once(s)
        if nxt == s:
            break
        s = nxt
    return s


def _repair_reversed_tags(s):
    matches = list(_REVERSED_TAG_RE.finditer(s))
    names = {m.group(2).lower() for m in matches if m.group(1) and _local_name(m.group(2)) in _MARKUP_NAMES}
    if not names:
        return s
    return _REVERSED_TAG_RE.sub(
        lambda m: _tag_replacement(m.group(2)) if m.group(2).lower() in names else m.group(0), s)


def _strip_tags_once(s):
    spans = list(_TAG_SHAPED_RE.finditer(s))
    # A closing tag `</name>` is itself a span; prose never closes an angle bracket that way, so an
    # opening tag with the same name is markup.
    closed_names = {m.group(2).lower() for m in spans if m.group(1) and not m.group(3).strip()}
    decisions = [_decide(m, closed_names) for m in spans]
    # A text that holds definite markup is a marked-up text; there a word-named tag such as <title>
    # or <sec> is read as markup too.
    markup_context = True in decisions
    out = []
    last = 0
    for m, d in zip(spans, decisions):
        if d is True or (d is _CONTEXT and markup_context):
            out.append(s[last:m.start()])
            out.append(_tag_replacement(m.group(2)))
            last = m.end()
    if out:
        out.append(s[last:])
        s = "".join(out)
    # Swapped tags only after the real ones are gone: in "n<sup>+</sup>/p<sup>+</sup>" the stretch
    # ">/p<" is the end of one real tag and the start of the next, not a swapped </p>.
    if ">/" in s:
        s = _repair_reversed_tags(s)
    if "<" in s and ">" in s:
        s = _disarm(s)
    # A tag cut off by a length cap is malformed by nature, so this rule differs from `_decide` on
    # purpose: any closing tag or prefix counts, and the attribute list may stop mid-way.
    last_lt = s.rfind("<")
    t = _TRAILING_TAG_SHAPED_RE.match(s, last_lt) if last_lt >= 0 else None
    if t:
        close, name, attrs = t.group(1), t.group(2), t.group(3)
        local = _local_name(name)
        known = ":" in name or local in _MARKUP_ONLY_NAMES
        # "... an <i", "... of </jats:p", "... <mml:mo st" lost their `>` to a length cap; "... at <LOD"
        # and "... Δt <p for a buildup test" did not (a one-letter name needs an attribute value).
        if (close or _ACTIVE_ATTR_RE.search(attrs) or local in _DANGEROUS_NAMES
                or (known and (attrs.strip() == "" or "=" in attrs
                               or ((":" in name or len(local) > 1) and _PARTIAL_ATTRS_RE.fullmatch(attrs))))):
            s = s[:t.start()] + " "
    return s


def _disarm(s):
    """Last pass: a stretch a browser would read as a tag stays as text unless it could run code.
    Walks `<` to the next `>` and resumes after it, as the tokenizer does: linear, where a regex
    `<[^>]*>` re-scans to the end from every `<` when no `>` follows."""
    out, last, i = [], 0, s.find("<")
    while i != -1:
        m = _BROWSER_TAG_START_RE.match(s, i)
        if not m:
            i = s.find("<", i + 1)
            continue
        j = s.find(">", m.end())
        if j == -1:
            break
        if _ACTIVE_ATTR_RE.search(s, i, j + 1) or _local_name(m.group(1)) in _DANGEROUS_NAMES:
            out.append(s[last:i])
            out.append(" ")
            last = j + 1
        i = s.find("<", j + 1)
    if not out:
        return s
    out.append(s[last:])
    return "".join(out)


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
