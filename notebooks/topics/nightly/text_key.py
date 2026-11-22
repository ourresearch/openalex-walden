"""The text key the topics and keywords nightlies store on each scored work (oxjob #2085, normalised in oxjob #2515).

A work is scored again only when its key changes, so the key must change when the words change and stay the same when a
cleanup only tidies the text. #2085 hashed the raw title and abstract, and #2059's strip of a leading "Abstract" heading
(~5.4M works, night 2026-11-20) put millions of unchanged works ahead of real changes in a queue capped at 1M a night.
The key now hashes normalised text: markup tags and character entities dropped, lower case, letters and digits only,
whitespace collapsed, and a leading abstract heading ("Abstract", Wiley's "A BSTRACT", "Summary", "Résumé", ...) removed.
A gained or lost abstract and any change to the words of the title or abstract still change the key.

The abstract is cut after normalising, at the scorer's cut (topics 4,000, keywords 6,000 characters): the normalised
text is shorter, so the cut covers at least the characters the scorer reads.

`raw_key_sql` is #2085's key, kept so rows stored before #2515 still count as current while their text is unchanged.
Both nightly queue notebooks import this module; keep it the one definition.
"""

# Headings a cleanup may strip from the start of an abstract, as they read after normalising.
ABSTRACT_HEADINGS = ("abstract", "a bstract", "summary", "résumé", "resume", "resumen", "resumo", "zusammenfassung",
                     "riassunto", "samenvatting", "sammanfattning", "streszczenie", "аннотация", "реферат", "摘要", "要旨")


def norm_sql(col):
    """SQL: the normalised text of column/expression `col` ('' for NULL)."""
    s = f"coalesce({col}, '')"
    s = f"regexp_replace({s}, '</?[A-Za-z][^<>]{{0,200}}>', ' ')"                                    # markup tags
    s = f"regexp_replace({s}, '&(#[0-9]+|#[xX][0-9a-fA-F]+|[A-Za-z][A-Za-z0-9]{{1,7}});', ' ')"      # character entities
    s = f"regexp_replace(lower({s}), '[^\\\\p{{L}}\\\\p{{N}}]+', ' ')"                                # letters and digits only
    return f"trim({s})"


def abstract_norm_sql(col, cut):
    """SQL: the normalised abstract without a leading heading, cut to `cut` characters."""
    headings = "|".join(ABSTRACT_HEADINGS)
    return f"substr(regexp_replace({norm_sql(col)}, '^(({headings}) )+', ''), 1, {int(cut)})"


def text_key_sql(title, abstract, cut):
    """SQL: the key stored on a scored row; `abstract` is the full (uncut) abstract."""
    return f"xxhash64({norm_sql(title)}, {abstract_norm_sql(abstract, cut)})"


def raw_key_sql(title, abstract, cut):
    """SQL: #2085's key (raw title + abstract[:cut]); rows keyed before #2515 hold it."""
    return f"xxhash64(coalesce({title}, ''), coalesce(substr({abstract}, 1, {int(cut)}), ''))"
