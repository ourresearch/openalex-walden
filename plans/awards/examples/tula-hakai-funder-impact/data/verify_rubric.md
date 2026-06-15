# Verification rubric — Tula Foundation / Hakai Institute

You decide whether each research output was genuinely supported by, produced at, or
affiliated with the **Tula Foundation** or its **Hakai Institute** — a British Columbia
philanthropic funder of MARINE & COASTAL science (founders Eric Peterson & Christina
Munck; field stations on Calvert Island and Quadra Island; historically the "Hakai
Network" or "Hakai Beach Institute").

Important disambiguations:
- Tula ALSO runs unrelated programs, notably **TulaSalud** (maternal/child health in
  Guatemala). Those are genuine Tula but NOT coastal.
- "Tula" is a common Russian place name. **Tula State University**, "Tula State
  Pedagogical University", "Tula Region", Tula oblast hospitals, etc. are NOT this
  funder — those are false positives.

For EACH record decide four fields:

- **label**: `include` if there is credible evidence of a real Tula/Hakai tie — a
  Hakai/Tula funder acknowledgement, a Hakai/Tula author affiliation (check
  `institutions` and `raw_affiliations`), work done at a Hakai field station / Calvert /
  Quadra, a Hakai-published dataset, or a clear text/methods mention. Use `exclude` for
  a false positive — "Tula" = a Russian university/region; "Calvert Island"/"Hakai"
  appearing only coincidentally with no real institute tie; or a work that clearly
  predates and has no connection to Tula/Hakai.
  - When `provenance` contains a structured tie (`funder_hakai`, `funder_tula`,
    `inst_hakai`, `inst_tula`) the work is almost always a genuine `include` — unless the
    affiliation strings clearly show a different (Russian) "Tula".
  - Be skeptical of records whose ONLY provenance is `ft_calvert` (fulltext "Calvert
    Island") or a bare `rawaff`/`ft` signal with no coastal/BC context.

- **scope** (only if include): `coastal` for marine / coastal / nearshore / ocean /
  freshwater / coastal-archaeology / coastal-earth-science / coastal-ecology research
  tied to the Hakai program; `other_tula` for genuine Tula work that is NOT coastal
  (TulaSalud health, Guatemala maternal/child health, or other unrelated domains).
  Use `na` if label = exclude.

- **confidence**: `high` | `medium` | `low`

- **reason**: one short clause (max ~12 words).
