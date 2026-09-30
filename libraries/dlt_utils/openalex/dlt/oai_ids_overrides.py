"""Per-endpoint exceptions to the placeholder-host re-key (oxjob #1407). Data-derived, small, in code.

KEY_HOST_OVERRIDES: endpoint -> key host, replacing the one derived from its pmh_url.
    js.vnu.edu.vn serves seven journals at /index.php/<journal>/oai that are SEPARATE OJS installs:
    their article numbers overlap and name different articles (135 shared numbers, 0 matching
    titles, 2026-09-30), so the host alone would merge them. Every other host with several
    journal-scoped endpoints is one install (shared numbers carry the same title) or has no overlap.

KEEP_PLACEHOLDER_ENDPOINTS: the key keeps the placeholder inside the host part
    (oai:<host>/<placeholder>:<local>). Rule: every endpoint whose plain re-key would map two
    DIFFERENT stored keys onto one (it emits two placeholder hosts with the same local ids), so the
    cutover never merges records. For LA Referencia the pairs are different upstream articles
    (1,828 of 3,327); for the other eight they are the same article under an identifier that
    changed over time, which stays as two locations, as it is today (merge_key joins them on one
    work). Measured 2026-09-30 on repo_parsed + repo_items_backfill.
"""

KEY_HOST_OVERRIDES = {
    "c5cd470d65f13852e6e": "js.vnu.edu.vn/ees",
    "e7f1e5f627d37c0a037": "js.vnu.edu.vn/er",
    "fde81b4c711d8fd9359": "js.vnu.edu.vn/fs",
    "aaaa894f82b0c7ec519": "js.vnu.edu.vn/ls",
    "04bdd1887e730e42c88": "js.vnu.edu.vn/map",
    "277b255bbcc0d4edff0": "js.vnu.edu.vn/nst",
    "3ce2da931991ffc612b": "js.vnu.edu.vn/pam",
}

KEEP_PLACEHOLDER_ENDPOINTS = frozenset({
    "xfaytpa8kuwx8d2xs2dz",  # oai.lareferencia.info (3,327 pairs)
    "fe4fe812bff6bdcd493",   # rcaap.pt (809)
    "5443c9aaaa73c3c974d",   # journal.uinjkt.ac.id edusains (163)
    "32858e57d60bfa6c8f7",   # ojs.uac.edu.co install-wide (49)
    "9a95c7d6de00396667f",   # journal.uinjkt.ac.id SOSIO-FITK (10)
    "3cc2e72dc377e4c6a53",   # ojs.uac.edu.co encuentros (7)
    "385e5f248e0c95d5ae3",   # ojs.uac.edu.co dimension-empresarial (6)
    "f0d49affcb4b4beec29",   # ojs.uac.edu.co prospectiva (5)
    "a61932d3e4fa0e55d7d",   # journal.uinjkt.ac.id tarbiya (1)
})
