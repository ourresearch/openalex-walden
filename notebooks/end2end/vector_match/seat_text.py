"""#1342: card text per ablation arm from a component row (dict from oxjob1342_comp). Numbers/short fields first so a 128-token cut
drops the title, never the name or affiliation. Arms (README design question 1):
  base   name | parsed institution(s) | subfield | venue | year | with 3 coauthors | title
  str    affiliation STRING as deposited instead of the parsed institution
  both   parsed institution + affiliation string
  novenue base without the venue
  co5    base with 5 coauthors
  prior  base + name-prior words (surname frequency word, concentration word)
  thin   name | year | title only (floor: what a seat says with no context)"""
ARMS = ["base", "str", "both", "novenue", "co5", "prior", "thin"]
def _j(xs, k, sep="; "): return sep.join(x for x in (xs or [])[:k] if x)
def card(r, arm="base"):
    name = r.get("raw_name") or ""
    inst = _j(r.get("inst_names"), 2); aff = _j(r.get("aff_strings"), 2, " / ")
    aff = aff[:160] if aff else ""
    if arm == "str": loc = aff or inst
    elif arm == "both": loc = "; ".join(x for x in (inst, aff) if x)
    else: loc = inst or aff
    parts = [name]
    if arm == "prior": parts += [f"surname {r.get('surname_word') or 'unknown'}", f"concentration {r.get('conc_word') or 'unknown'}"]
    if arm == "thin": parts += [str(r.get("publication_year") or ""), (r.get("title") or "")[:100]]
    else:
        parts += [loc, r.get("subfield") or "", "" if arm == "novenue" else (r.get("venue") or "")[:80], str(r.get("publication_year") or "")]
        co = _j(r.get("coauthors"), 5 if arm == "co5" else 3)
        parts += [f"with {co}" if co else "", (r.get("title") or "")[:100]]
    return " | ".join(p for p in parts if p)
if __name__ == "__main__":
    import json, sys
    for l in sys.stdin:
        r = json.loads(l)
        for a in ARMS: print(f"[{a}] {card(r, a)}")
        print()
