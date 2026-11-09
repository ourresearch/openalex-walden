"""Award text for topics, keywords and SDGs (job AwardTopics, jobs/award_topics.yaml).

One place for: which awards have usable text, the CJK character rule, the text each scorer sees, and the text key that says
when an award must be scored again. Pure strings and Python: the notebooks, the tests and the local sample script import it.

Usable (an award is queued only if one holds):
- title >= 4 whitespace words, or description >= 20 whitespace words (the sizing rule);
- CJK (Chinese / Japanese) text is judged by characters, since it has no spaces between words: a title or description whose
  Han + Hiragana + Katakana characters are >= 30% of its non-space characters is CJK, and counts as usable with
  >= 8 CJK characters in the title or >= 40 in the description (about 2 characters per word, so the same 4 / 20 words).
  Measured 2026-11-08 on openalex.awards.awards_api (15,231,073 awards): 708,933 CJK titles, 1,965 of them under 8 characters
  (CJK-title 5th percentiles 11-17 characters per feed); the rule takes usable awards from 6,154,792 (words only) to 6,849,747.
Hangul is left to the word rule: Korean separates words with spaces.
Two reviewed exceptions, both data in ../text_rules.csv (no literals in code; edit the CSV, the tests read it too):
- placeholder: a title or description whose normalised text (lower case, spaces collapsed, trailing dots dropped) is a listed
  placeholder ("No description available", "n/a", NSERC's "No summary - Aucun sommaire", ...) or punctuation only is treated as
  empty: it neither qualifies the award nor is sent to a model as the abstract;
- person_feed: a feed whose titles are a person's or a fellowship's label ("Guggenheim Fellowship - <name> (<year>)"): its titles
  are treated as empty like a placeholder, so only an award whose own description qualifies is queued, and the models see that
  description alone (on the 10k sample, Humboldt's label titles pushed awards with research keywords to not-classifiable).

Texts (the models' training inputs, so awards look like the works they were trained on):
- topics (q8b_2m):  queue columns title / abstract / venue; the app builds "Title: / Abstract: [:4000] / Venue: (unknown)";
- keywords (v3fix): queue columns title / abstract / journal; the app builds "Title: / Abstract: [:6000] / Venue: (unknown)";
- SDGs (Qwen3 0.6B embedding + head v2): EmbedQwen3Incremental's TEXT_EXPR without a venue, cut at 2,000 characters.
The award's funder is not passed as a venue: the students saw journal names there, not funders (a choice to revisit with Jason).
"""

import csv
import os

RULES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "text_rules.csv")
CJK_CLASS = r"[^\\p{IsHan}\\p{IsHiragana}\\p{IsKatakana}]"   # Spark SQL string literal: strip everything that is not CJK
MIN_TITLE_WORDS, MIN_DESC_WORDS = 4, 20
MIN_TITLE_CJK, MIN_DESC_CJK, CJK_SHARE = 8, 40, 0.3
ABSTRACT_CUT = 6000      # stored in the queue; the topics app cuts again at 4,000, the keywords app at 6,000
EMBED_CUT = 2000         # = EmbedQwen3Incremental TEXT_EXPR
EMBED_ENDPOINT = "databricks-qwen3-embedding-0-6b"


def _words(col):
    """Whitespace words like Python's str.split(): (?U) makes Java's \\s Unicode (NBSP, ideographic space), as Python's is."""
    return (f"CASE WHEN length(regexp_replace(coalesce({col}, ''), '(?U)\\\\s', '')) > 0 "
            f"THEN size(split(regexp_replace({col}, '(?U)^\\\\s+|\\\\s+$', ''), '(?U)\\\\s+')) ELSE 0 END")


def text_columns(t="a"):
    """SELECT list fragment: per-award counts the usable rule reads (alias t = an awards_api-shaped relation)."""
    return f"""{_words(t + '.display_name')} AS tw,
  {_words(t + '.description')} AS dw,
  length(regexp_replace(coalesce({t}.display_name, ''), '{CJK_CLASS}', '')) AS tc,
  length(regexp_replace(coalesce({t}.description, ''), '{CJK_CLASS}', '')) AS dc,
  length(regexp_replace(coalesce({t}.display_name, ''), '(?U)\\\\s', '')) AS tl,
  length(regexp_replace(coalesce({t}.description, ''), '(?U)\\\\s', '')) AS dl"""


def load_rules(path=RULES_FILE):
    """{'placeholder': set of normalised strings, 'person_feed': set of provenances} from the reviewed CSV."""
    out = {"placeholder": set(), "person_feed": set()}
    for r in csv.DictReader(open(path, encoding="utf-8")):
        kind, value = r["kind"].strip(), r["value"].strip()
        if kind not in out or not value:
            raise ValueError(f"{path}: bad row {r}")
        out[kind].add(norm_text(value) if kind == "placeholder" else value)
    return out


# Python twins of the Java classes the SQL uses, so tests and local checks agree with Spark:
# (?U)\s = Unicode White_Space, which (unlike Python's \s) excludes the C0 separators U+001C-U+001F;
# \p{IsHan} / \p{IsHiragana} / \p{IsKatakana} = scripts, so the Common-script marks U+30FB and U+30FC are not CJK,
# halfwidth katakana is, and Han reaches the supplementary planes (Ext B-H).
WS = r"[^\S\x1c-\x1f]"
CJK_RX = (r"[\u3041-\u3096\u309d-\u309f\U0001b001-\U0001b11f"
          r"\u30a1-\u30fa\u30fd-\u30ff\u31f0-\u31ff\u32d0-\u32fe\u3300-\u3357\uff66-\uff6f\uff71-\uff9d"
          r"\u2e80-\u2e99\u2e9b-\u2ef3\u2f00-\u2fd5\u3005\u3007\u3021-\u3029\u3038-\u303b"
          r"\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufa6d\ufa70-\ufad9\U00020000-\U0002ffff\U00030000-\U0003134f]")


def norm_text(s):
    """Python twin of NORM: lower case, whitespace collapsed, trimmed, trailing dots / spaces dropped."""
    import re
    s = re.sub(WS + "+", " ", s or "")
    return re.sub(r"[ .。]+$", "", re.sub(r"^ +| +$", "", s)).lower()


def _lit(v):
    return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"


def norm_sql(col):
    return f"lower(trim(regexp_replace(regexp_replace(coalesce({col}, ''), '(?U)\\\\s+', ' '), '(?U)[\\\\s.。]+$', '')))"


def clean_sql(col, rules):
    """The column, or NULL when it is a placeholder or punctuation only. The punctuation test runs on the normalised text
    (whitespace already one ASCII space) WITHOUT (?U), so \\p{Punct} is ASCII punctuation = Python's string.punctuation."""
    ph = ", ".join(_lit(v) for v in sorted(rules["placeholder"]))
    return f"CASE WHEN {norm_sql(col)} IN ({ph}) OR {norm_sql(col)} RLIKE '^[\\\\p{{Punct}} –—…]*$' THEN NULL ELSE {col} END"


CJK_TITLE = f"(tc > 0 AND tc >= {CJK_SHARE} * tl)"
CJK_DESC = f"(dc > 0 AND dc >= {CJK_SHARE} * dl)"
USABLE = (f"(tw >= {MIN_TITLE_WORDS} OR dw >= {MIN_DESC_WORDS} OR ({CJK_TITLE} AND tc >= {MIN_TITLE_CJK}) "
          f"OR ({CJK_DESC} AND dc >= {MIN_DESC_CJK}))")   # on cleaned text: placeholders and person-feed titles are NULL

# The text an award was scored on; a changed key re-queues it (as text_key does for works, oxjob #2085).
TEXT_KEY = f"xxhash64(coalesce(trim(display_name), ''), coalesce(substr(description, 1, {ABSTRACT_CUT}), ''))"

# SDG embedding input: EmbedQwen3Incremental.TEXT_EXPR with title -> display_name, abstract -> description, no venue.
EMBED_TEXT = f"""SUBSTRING(CONCAT('Title: ', coalesce(trim(display_name), ''),
    CASE WHEN description IS NOT NULL AND length(trim(description)) > 0 THEN CONCAT('\\n\\nAbstract: ', description) ELSE '' END), 1, {EMBED_CUT})"""


def usable_awards_sql(awards="openalex.awards.awards_api", rules=None):
    """Every award with usable text, with the fields the queue needs; placeholders already NULL (they are never sent)."""
    rules = rules or load_rules()
    feeds = ", ".join(_lit(v) for v in sorted(rules["person_feed"])) or "NULL"
    return f"""SELECT id, provenance, display_name, description, {CJK_TITLE} AS cjk_title, {CJK_DESC} AS cjk_desc, text_key FROM (
  SELECT c.*, {text_columns('c')}, {TEXT_KEY.replace('display_name', 'c.display_name').replace('description', 'c.description')} AS text_key FROM (
    SELECT a.id, a.provenance, CASE WHEN a.provenance IN ({feeds}) THEN NULL ELSE {clean_sql('a.display_name', rules)} END AS display_name,
      {clean_sql('a.description', rules)} AS description
    FROM {awards} a) c) WHERE {USABLE}"""


def python_clean(s, rules=None):
    """Python twin of clean_sql: None for a listed placeholder or punctuation-only text, else the text unchanged."""
    import string
    rules = rules or load_rules()
    n = norm_text(s)
    return None if n in rules["placeholder"] or all(ch in set(string.punctuation) | set(" –—…") for ch in n) else s


def python_usable(title, description, provenance="", rules=None):
    """Python twin of USABLE incl. the CSV rules (tests, local checks). Whitespace split like Spark's split(trim(x), '\\s+')."""
    import re
    rules = rules or load_rules()
    title = None if provenance in rules["person_feed"] else python_clean(title, rules)
    description = python_clean(description, rules)
    cjk = re.compile(CJK_RX)

    def counts(s):
        s = s or ""
        words = len([w for w in re.split(WS + "+", s) if w])
        c = len(cjk.findall(s))
        nonspace = len(re.sub(WS, "", s))
        return words, c, nonspace
    tw, tc, tl = counts(title)
    dw, dc, dl = counts(description)
    cjk_t = tc > 0 and tc >= CJK_SHARE * tl
    cjk_d = dc > 0 and dc >= CJK_SHARE * dl
    return (tw >= MIN_TITLE_WORDS or dw >= MIN_DESC_WORDS or (cjk_t and tc >= MIN_TITLE_CJK) or (cjk_d and dc >= MIN_DESC_CJK))
