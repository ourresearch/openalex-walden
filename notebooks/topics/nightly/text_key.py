"""The text key the topics and keywords nightlies store on each scored work (oxjob #2085, normalised in oxjob #2515).

A work is scored again only when its key changes, so the key must change when the words the scorer reads change, and
stay the same when a cleanup only tidies them. #2085 hashed the raw title and abstract, and #2059's strip of a leading
"Abstract" heading (~5.4M works, night 2026-11-20) put millions of unchanged works ahead of real changes in a queue
capped at 1M a night.

The key hashes the scorer's input, normalised: the title and the abstract cut at the scorer's cut (topics 4,000,
keywords 6,000 characters) exactly as the scorer reads them, then markup tags and punctuation entities dropped, lower
case, letters, marks and digits only, whitespace collapsed, and a leading abstract heading ("Abstract", Wiley's
"A BSTRACT", "Summary", "Résumé", "摘要", ...) removed. A gained or lost abstract and any change to the words of the
title or of the abstract the scorer reads still change the key. Cutting before normalising keeps the regex work bounded
and makes the key a function of the raw key's input, so a row's raw key always maps to one normalised key.
(A heading strip on an abstract longer than the cut shifts a few characters into the window, so that work is
re-scored once.)

`raw_key_sql` is #2085's key, kept so rows stored before #2515 still count as current while their text is unchanged.
Both nightly queue notebooks import this module; keep it the one definition.
"""

# Headings a cleanup may strip from the start of an abstract, as they read after normalising (then a space or the end).
ABSTRACT_HEADINGS = ("abstract", "a bstract", "summary", "résumé", "resume", "resumen", "resumo", "zusammenfassung",
                     "riassunto", "samenvatting", "sammanfattning", "streszczenie", "аннотация", "реферат")
# CJK headings are followed directly by the text, with no space.
CJK_HEADINGS = ("摘要", "要旨", "초록")


def norm_sql(col):
    """SQL: the normalised text of column/expression `col` ('' for NULL)."""
    s = f"regexp_replace(regexp_replace(coalesce({col}, ''), '(?i)&lt;', '<'), '(?i)&gt;', '>')"     # escaped markup
    s = f"regexp_replace({s}, '</?[A-Za-z][^<>]{{0,200}}>', ' ')"                                    # markup tags
    s = f"regexp_replace({s}, '(?i)&(amp|quot|apos|nbsp|#160|#x0*a0);', ' ')"                       # punctuation entities
    s = f"regexp_replace(lower({s}), '[^\\\\p{{L}}\\\\p{{M}}\\\\p{{N}}]+', ' ')"                     # letters, marks, digits
    return f"trim({s})"


def abstract_norm_sql(col, cut):
    """SQL: the abstract as the scorer reads it (cut at `cut`), normalised, without a leading heading."""
    s = norm_sql(f"substr({col}, 1, {int(cut)})")
    s = f"regexp_replace({s}, '^(({'|'.join(ABSTRACT_HEADINGS)})( |$))+', '')"
    return f"regexp_replace({s}, '^({'|'.join(CJK_HEADINGS)}) ?', '')"


def text_key_sql(title, abstract, cut):
    """SQL: the key stored on a scored row; `abstract` is the uncut abstract column."""
    return f"xxhash64({norm_sql(title)}, {abstract_norm_sql(abstract, cut)})"


def raw_key_sql(title, abstract, cut):
    """SQL: #2085's key (raw title + abstract[:cut]); rows keyed before #2515 hold it."""
    return f"xxhash64(coalesce({title}, ''), coalesce(substr({abstract}, 1, {int(cut)}), ''))"
