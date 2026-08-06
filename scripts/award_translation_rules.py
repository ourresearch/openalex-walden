#!/usr/bin/env python3
"""#690 per-funder audit driver. Read-only. Generates + runs one SQL per funder.

Per funder we define (all applied to `norm`, the cleaned id):
  rkey  - registry-side match-key expression
  xkey  - crossref-side match-key expression
  gram  - boolean grammar expression (does the string look like this funder's ids)
Rates: matched (xkey in registry key set) / grammar_pass (gram, no match) / fail.
Computed on distinct-id basis and minted-row (volume) basis.
"""
import subprocess, sys, os, json

SQLQ = os.path.expanduser("~/openalex-onboarding-labs/sqlq")
OUT = os.path.dirname(os.path.abspath(__file__))

# norm: upper, trim, unicode dashes -> '-', unicode spaces -> ' ', collapse spaces
NORM = (r"regexp_replace(regexp_replace(regexp_replace(regexp_replace(UPPER(TRIM({c}))," r" '\\\\U2[0-9A-F]{{3}}', '-'),"
        r" '[\\u2010-\\u2015\\u2212\\uFE58\\uFE63\\uFF0D\\uF000-\\uF8FF]', '-'),"
        r" '[\\u00A0\\u1680\\u2000-\\u200B\\u202F\\u205F\\u3000]', ' '),"
        r" '  +', ' ')")

def ex(pattern, src="norm", grp=1):
    return f"NULLIF(regexp_extract({src}, '{pattern}', {grp}), '')"

FUNDERS = {
  # review pass-4 F3: (a) U-series joint-fund ids (U2243213) are REGISTRY-
  # ATTESTED (407 U\d{7} ids in the NSFC registry) — added to all three
  # fields; (b) spaced deposits ("51 708 106") die because NORM keeps single
  # spaces — xkey/gram get a strip-space arm (8-digit-exact runs only, so
  # stripping can't conjure an id out of shorter fragments).
  "NSFC": dict(fid=4320321001,
    rkey=ex(r"^(U?\\d{7,8})$"),
    xkey=f"COALESCE({ex(r'(?<![A-Z0-9])(U\\d{7})(?!\\d)')}, "
         f"{ex(r'(?<!\\d)(\\d{8})(?!\\d)')}, "
         f"""{ex(r'(?<!\\d)(\\d{8})(?!\\d)', src="regexp_replace(norm,' ','')")})""",
    gram=r"norm rlike '(?<!\\d)\\d{8}(?!\\d)' or norm rlike '(?<![A-Z0-9])U\\d{7}(?!\\d)'"
         r" or regexp_replace(norm,' ','') rlike '(?<!\\d)\\d{8}(?!\\d)'"
         r" or norm rlike '^8\\d{10}$'"),
  "NIH": dict(fid=4320332161,
    rkey=ex(r"([A-Z]{2}\\d{6})"),
    xkey=f"CASE WHEN {ex(r'([A-Z]{2}) ?-?(\\d{5,6})(?!\\d)')} IS NOT NULL THEN "
         f"CONCAT(regexp_extract(norm,'([A-Z]{{2}}) ?-?(\\\\d{{5,6}})(?!\\\\d)',1),"
         f" LPAD(regexp_extract(norm,'([A-Z]{{2}}) ?-?(\\\\d{{5,6}})(?!\\\\d)',2),6,'0')) END",
    gram=r"norm rlike '[A-Z]\\d{2} ?-?[A-Z]{2} ?-?\\d{5,6}' or norm rlike '^[A-Z]{2} ?-?\\d{5,6}'"),
  # 2026-08-03 audit: publishers regroup digits ("PHY17-48958" = PHY-1748958)
  # -> extra xkey arm rejoining the split groups; gram accepts the split form
  "NSF": dict(fid=4320306076,
    rkey=ex(r"^(\\d{7})$"),
    xkey=f"COALESCE({ex(r'(?<!\\d)(\\d{7})(?!\\d)')}, "
         r"CASE WHEN norm rlike '^[A-Z]{2,5}[ -]?\\d{2}[ -]\\d{5}$' THEN "
         r"CONCAT(regexp_extract(norm,'(\\d{2})[ -]\\d{5}$',1), regexp_extract(norm,'(\\d{5})$',1)) END)",
    gram=r"norm rlike '^([A-Z]{2,5}[ -]?)?\\d{7}$' or norm rlike '^[A-Z]{2,5}[ -]?\\d{2}[ -]\\d{5}$'"),
  # review pass-4 F5: spaced cores ("23 K22132") get a strip-space fallback arm
  "KAKEN": dict(fid=4320334764,
    rkey=ex(r"^(\\d{2}[A-Z]\\d{5}|\\d{8})$"),
    xkey=f"COALESCE({ex(r'^(?:KAKENHI|JP|NO\\.?|GRANT)?[ -]*(\\d{2}[A-Z]\\d{5}|\\d{8})$')}, "
         f"""{ex(r'^(?:KAKENHI|JP|NO\\.?|GRANT)?(\\d{2}[A-Z]\\d{5}|\\d{8})$', src="regexp_replace(norm,' ','')")})""",
    gram=r"norm rlike '^(KAKENHI|JP|NO\\.?|GRANT)?[ -]*(\\d{2}[A-Z]\\d{5}|\\d{8})$'"
         r" or regexp_replace(norm,' ','') rlike '^(KAKENHI|JP|NO\\.?|GRANT)?(\\d{2}[A-Z]\\d{5}|\\d{8})$'"),
  "DFG": dict(fid=4320320879,
    rkey=ex(r"^(\\d{9})$"),
    xkey=ex(r"(?<!\\d)(\\d{9})(?!\\d)"),
    gram=r"norm rlike '^(SFB|TRR|CRC|EXC|GRK|RTG|FOR|SPP|INST|NFDI|KFO|FZT) ?/?-?\\d+' "
         r"or norm rlike '^(DFG[ -])?[A-Z]{1,4} ?\\d{2,4}(/\\d+)?(-\\d+)?( .*)?$' "
         r"or norm rlike '(?<!\\d)\\d{9}(?!\\d)'"),
  "MOST_TW": dict(fid=4320322795,
    rkey="NULLIF(regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]',''),'')",
    xkey="NULLIF(regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]',''),'')",
    gram=r"regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]','')"
         r" rlike '^\\d{6,7}[A-Z]\\d{6}(MY\\d)?E?\\d?$'"
         # round-2 audit: hyphenated form with ALNUM institution codes (A49,
         # 182A, 002) and optional -MYn / letter suffix / trailing hyphen
         r" or norm rlike '^(MOST|NSC|NSTC)?[ -]*\\d{2,3}[ -]+\\d{4}[ -]+[A-Z0-9][ -]+[A-Z0-9]{3,4}[ -]+\\d{3}([ -]+MY\\d)?([ -]+[A-Z0-9]{1,3})?[ -]*$'"),
  "FAPESP": dict(fid=4320320997,
    rkey=f"CASE WHEN {ex(r'^(\\d{2})/(\\d{5})-(\\d)$')} IS NOT NULL THEN "
         f"CONCAT(regexp_extract(norm,'^(\\\\d{{2}})/(\\\\d{{5}})-(\\\\d)$',1),'/',"
         f"regexp_extract(norm,'^(\\\\d{{2}})/(\\\\d{{5}})-(\\\\d)$',2),'-',"
         f"regexp_extract(norm,'^(\\\\d{{2}})/(\\\\d{{5}})-(\\\\d)$',3)) END",
    xkey=f"CASE WHEN {ex(r'(?<!\\d)(\\d{2,4})/(\\d{4,5})-(\\d)(?!\\d)')} IS NOT NULL THEN "
         f"CONCAT(RIGHT(regexp_extract(norm,'(?<!\\\\d)(\\\\d{{2,4}})/(\\\\d{{4,5}})-(\\\\d)(?!\\\\d)',1),2),'/',"
         f"LPAD(regexp_extract(norm,'(?<!\\\\d)(\\\\d{{2,4}})/(\\\\d{{4,5}})-(\\\\d)(?!\\\\d)',2),5,'0'),'-',"
         f"regexp_extract(norm,'(?<!\\\\d)(\\\\d{{2,4}})/(\\\\d{{4,5}})-(\\\\d)(?!\\\\d)',3)) END",
    gram=r"norm rlike '(?<!\\d)\\d{2,4}/\\d{4,5}-\\d(?!\\d)'"),
  "FCT": dict(fid=4320334779,
    rkey="NULLIF(regexp_replace(norm,' ',''),'')",
    xkey="NULLIF(regexp_replace(norm,' ',''),'')",
    gram=r"norm rlike '^[A-Z0-9 ./-]+$' and (norm rlike '/' or norm rlike '^\\d{4}\\.\\d{5}\\.')"),
  "EC": dict(fid=4320320300,
    rkey=ex(r"^(\\d{6}|\\d{9})$"),
    xkey=f"COALESCE({ex(r'(?<!\\d)(101\\d{6})(?!\\d)')}, {ex(r'(?<!\\d)(\\d{6})(?!\\d)')})",
    gram=r"norm rlike '^(GA ?N?°? ?)?\\d{6}$' or norm rlike '^101\\d{6}$' "
         r"or norm rlike '-CT-\\d{4}-' or norm rlike '(FP[567]|H2020|HORIZON|MSCA|ERC|GA) ?N?°? ?-?\\d{6}'"),
  "NSERC": dict(fid=4320334593,
    # registry has BOTH old 'serial-year' (341949-2008) and new 'rgpin-2020-03053';
    # serial-int key spans both (year-collision risk noted in EXPLORE — guard should be year-aware)
    rkey="CASE WHEN norm rlike '^\\\\d{1,6}-\\\\d{4}$' THEN "
         "CONCAT(regexp_extract(norm,'-(\\\\d{4})$',1),'-',CAST(CAST(regexp_extract(norm,'^(\\\\d{1,6})-',1) AS BIGINT) AS STRING)) "
         "WHEN norm rlike '^[A-Z]+-\\\\d{4}-\\\\d{4,6}$' THEN "
         "CONCAT(regexp_extract(norm,'-(\\\\d{4})-',1),'-',CAST(CAST(regexp_extract(norm,'-(\\\\d{4,6})$',1) AS BIGINT) AS STRING)) END",
    xkey="CASE WHEN regexp_replace(norm,' ','') rlike '[A-Z]{3,7}/?-?\\\\d{4}-?\\\\d{4,6}$' THEN "
         "CONCAT(regexp_extract(regexp_replace(norm,' ',''),'(\\\\d{4})-?\\\\d{4,6}$',1),'-',CAST(CAST(regexp_extract(regexp_replace(norm,' ',''),'(\\\\d{4,6})$',1) AS BIGINT) AS STRING)) "
         "WHEN norm rlike '^\\\\d{5,6}[ -]\\\\d{4}$' THEN "
         "CONCAT(regexp_extract(norm,'(\\\\d{4})$',1),'-',CAST(CAST(regexp_extract(norm,'^(\\\\d{5,6})',1) AS BIGINT) AS STRING)) END",
    gram=r"norm rlike '^[A-Z]{3,7}[ /-]?\\d{4}[ -]?\\d{4,6}$' or "
         r"norm rlike '^[A-Z]{3,7}[ -]?\\d{4,6}([ -]{1,3}\\d{2,4})?$' or norm rlike '^\\d{5,6}([ -]?\\d{2,4})?$'"),
  "ANR": dict(fid=4320320883,
    rkey=f"CASE WHEN {ex(r'^ANR-(\\d{2})-([A-Z0-9]{2,6})-(\\d{4})')} IS NOT NULL THEN "
         f"CONCAT(regexp_extract(norm,'^ANR-(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',1),'-',"
         f"regexp_extract(norm,'^ANR-(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',2),'-',"
         f"regexp_extract(norm,'^ANR-(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',3)) END",
    xkey=f"CASE WHEN NULLIF(regexp_extract(regexp_replace(norm,' ',''),'(?:ANR-?)?(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',1),'') IS NOT NULL THEN "
         f"CONCAT(regexp_extract(regexp_replace(norm,' ',''),'(?:ANR-?)?(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',1),'-',"
         f"regexp_extract(regexp_replace(norm,' ',''),'(?:ANR-?)?(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',2),'-',"
         f"regexp_extract(regexp_replace(norm,' ',''),'(?:ANR-?)?(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',3)) END",
    # 2026-08-03 audit: publishers cite short serials (ANR-15-IDEX-02, ANR-10-
    # INBS-04) and underscore variants; widen gram to 1-4-digit serials + [-_]
    gram=r"regexp_replace(norm,' ','') rlike '(ANR[-_]?)?\\d{2}[-_]?[A-Z0-9]{2,6}[-_]\\d{1,4}'"),
  "SNSF": dict(fid=4320320924,
    rkey=f"CAST(CAST({ex(r'^(\\d{1,6})$')} AS BIGINT) AS STRING)",
    xkey=f"CAST(CAST(CASE WHEN norm rlike '^\\\\d{{12}}$' THEN SUBSTR(norm,7) "
         f"ELSE {ex(r'(\\d{5,6})$')} END AS BIGINT) AS STRING)",
    gram=r"norm rlike '^[0-9A-Z]{0,8}[_-]?\\d{4,6}$' or norm rlike '^\\d{12}$'"),
  "WELLCOME": dict(fid=4320311904,
    # registry re-derived 2026-08-03: source ships citable refs since the
    # 07-31 award-id-audit backfill (323416/Z/24/Z); 360G kept for residuals
    rkey=f"LPAD(COALESCE({ex(r'360G-WELLCOME-(\\d{5,6})')}, "
         f"{ex(r'^(\\d{5,6})(?:[/_ ][A-Z](?:[/_ ]\\d{2})?([/_ ][A-Z])?)?$')}),6,'0')",
    xkey=f"LPAD({ex(r'^(\\d{5,6})(?:[/_ ][A-Z](?:[/_ ]\\d{2})?([/_ ][A-Z])?)?$')},6,'0')",
    gram=r"norm rlike '^\\d{5,6}([/_ ][A-Z][/_ ]\\d{2}[/_ ][A-Z])?$'"),
  # review pass-4 F4: truncated council refs missing the /check-digit
  # ("EP/J016918") are registry-attested prefixes — KEPT as plausible via the
  # extra gram arm; completion/canonicalization is #171's job, not ours.
  "EPSRC": dict(fid=4320334627,
    rkey="NULLIF(regexp_replace(norm,' ',''),'')",
    xkey="NULLIF(regexp_replace(norm,' ',''),'')",
    gram=r"regexp_replace(norm,' ','') rlike '^EP/[A-Z0-9]{6,7}(/[0-9])?$' or norm rlike '^\\d{7}$'"),
  "NSTC_TW": dict(fid=2461203286,
    # 2026-08-03: Taiwan NSTC = MOST's 2022 rename (the "mystery funder" — in
    # mid.funder but absent from common.funders, curation gap flagged). Same
    # grammar as MOST_TW; registry = grb_nstc_projects.
    rkey="NULLIF(regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]',''),'')",
    xkey="NULLIF(regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]',''),'')",
    gram=r"regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]','')"
         r" rlike '^\\d{6,7}[A-Z]\\d{6}(MY\\d)?E?\\d?$'"
         # round-2 audit: hyphenated form with ALNUM institution codes (A49,
         # 182A, 002) and optional -MYn / letter suffix / trailing hyphen
         r" or norm rlike '^(MOST|NSC|NSTC)?[ -]*\\d{2,3}[ -]+\\d{4}[ -]+[A-Z0-9][ -]+[A-Z0-9]{3,4}[ -]+\\d{3}([ -]+MY\\d)?([ -]+[A-Z0-9]{1,3})?[ -]*$'"),
  "CIHR": dict(fid=4320334506,
    # derived 2026-08-03 (worklist #16): registry = NNNNNN_1 (application no. +
    # installment); deposits = program prefixes (MOP/PJT/FDN...), '950-' funding
    # reference wrapper, '#' decorations. Match key = application number as int.
    rkey="CAST(CAST(" + ex(r"^(\\d{4,6})_\\d+$") + " AS BIGINT) AS STRING)",
    xkey="CAST(CAST(NULLIF(regexp_extract(regexp_replace(regexp_replace(norm,'^[#]+ ?',''),"
         "'^(950[- ]|[A-Z]{2,4}[0-9]?[- ]?)',''),'^(\\\\d{4,6})([-_]\\\\d+)?$',1),'') AS BIGINT) AS STRING)",
    gram=r"norm rlike '^#? ?(950[- ])?([A-Z]{2,4}[0-9]?[- ]?)?\\d{4,6}([-_]\\d+)?$'"),
  "AHA": dict(fid=4320306230,
    rkey="NULLIF(regexp_replace(norm,' ',''),'')",
    xkey="NULLIF(regexp_replace(norm,' ',''),'')",
    gram=r"regexp_replace(norm,' ','') rlike '^\\d{2}[A-Z]{2,10}\\d{4,9}$' or norm rlike '^\\d{6,9}$'"),
  "CAPES": dict(fid=4320321091,
    # derived 2026-08-03 (worklist #19): registry = capes_cooperacao_internacional
    # (2,065 rows) but its ids are SYNTHETIC HASHES (capes-cooperation-<hex>) —
    # no citable reference exists, so registry matching is impossible until the
    # source gets the 07-31 citable-ref treatment (flagged for Kyle's rescrape
    # list). Keys target the real CAPES process number (8888d.dddddd/yyyy-dd)
    # so matching lights up automatically once the registry carries them.
    # Deposits: process numbers, BEX/PDSE/PNPD/PROEX/DS scholarship codes,
    # nnn/yyyy convenio refs, and the blanket "Finance Code: 001" (not an
    # award id -> garbage by design). Measured (crossref door): grammar 32.1%,
    # fail 67.9% incl. 2.8k FAPESP ids (-> S3) and Finance-Code junk.
    rkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),"
         r"'^((8888\\d|99999)\\.\\d{6}/\\d{4}-\\d{2})$', 1), '')",
    xkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),"
         r"'((8888\\d|99999)\\.\\d{6}/\\d{4}-\\d{2})', 1), '')",
    gram=r"(regexp_replace(norm,' ','') rlike '^(8888\\d|99999)\\.\\d{6}/\\d{4}-\\d{2}$'"
         r" or regexp_replace(norm,' ','') rlike '^(BEX|PDSE|PNPD|PROEX|DS|AUX)-?\\d{3,7}([-/.]\\d{1,4}){0,3}$'"
         r" or norm rlike '^\\d{1,4}/(19|20)\\d{2}$')"),
  "NCN": dict(fid=4320322511,
    # derived 2026-08-03 (worklist #20): registry = internal edition_CALL_serial
    # codes (17_OPUS_10000) with NO citable reference anywhere in the row —
    # the world cites the UMO registration number (2018/29/B/HS1/02676,
    # optionally UMO-/DEC- prefixed). projekty.ncn.gov.pl publishes the UMO
    # number, so NCN joins the rescrape/citable-ref list (like CAPES). Keys
    # target the UMO core so matching lights up post-rescrape. gram also
    # accepts inherited MNiSW 'N Ndddd dddddd' grants. NAWA (PPN/...), EU
    # POWER (POWR....), EUREKA (E!...) deposits = garbage here, correctly.
    # Measured (crossref door): grammar 75.2% / fail 24.8%.
    rkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),"
         r"'^(20\\d{2}/\\d{2}/[A-Z]{1,2}/[A-Z]{2,3}\\d{1,2}/\\d{5})$', 1), '')",
    xkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),"
         r"'(20\\d{2}/\\d{2}/[A-Z]{1,2}/[A-Z]{2,3}\\d{1,2}/\\d{5})', 1), '')",
    gram=r"(regexp_replace(norm,' ','') rlike '^(UMO-?|DEC-?)?20\\d{2}/\\d{2}/[A-Z]{1,2}/[A-Z]{2,3}\\d{1,2}/\\d{5}$'"
         r" or norm rlike '^N ?N[A-Z]?\\d{3} ?\\d{6}$')"),
  "DOE": dict(fid=4320306084,
    # derived 2026-08-03 (worklist #21): registry = usaspending (51,766 rows)
    # storing the DE-family scheme in MULTIPLE spellings (DESC0000033,
    # FG02-00ER14980, DEFG0200ER15031). Canonical key = strip spaces/hyphens,
    # strip leading DE only when followed by office-code shape (lookahead, so
    # NSF-style DEB-1234567 is untouched), then extract one of two chassis:
    # office+7-digit (SC0019115) or field-office FGdddd[A-Z]{1,2}ddddd
    # (FG0209ER46577). FOA references (SC-FOA-...) and bare numbers are
    # correctly garbage. Controls: crossref-door match 30.9% (vs 2.0%
    # generic); foreign-lettered probe 4,945 fires -> 264 registry hits,
    # 12/12 sampled = real DE- ids under NSF/NSFC/NIH/EC/NSERC (true
    # positives -> S3). DOE Office of Science (4320332359) deposits the same
    # scheme; S3 cross-detection to DOE covers it once this entry exists.
    rkey=ex(r"^((SC|EE|FE|AR|NE|NA|EM|OE|IA|PI|BI|CF|ET|SF|HS|DP|EW)\\d{7}"
            r"|(FG|FC|AC|AI|GO|PS|EV|ER|AA)\\d{4}[A-Z]{1,2}\\d{4,6})$",
            src="regexp_replace(regexp_replace(norm,'[ -]',''),'^DE(?=[A-Z]{2}[0-9])','')"),
    xkey=ex(r"((SC|EE|FE|AR|NE|NA|EM|OE|IA|PI|BI|CF|ET|SF|HS|DP|EW)\\d{7}"
            r"|(FG|FC|AC|AI|GO|PS|EV|ER|AA)\\d{4}[A-Z]{1,2}\\d{4,6})",
            src="regexp_replace(regexp_replace(norm,'[ -]',''),'^DE(?=[A-Z]{2}[0-9])','')"),
    gram=r"regexp_replace(regexp_replace(norm,'[ -]',''),'^DE(?=[A-Z]{2}[0-9])','')"
         r" rlike '^((SC|EE|FE|AR|NE|NA|EM|OE|IA|PI|BI|CF|ET|SF|HS|DP|EW)\\d{7}"
         r"|(FG|FC|AC|AI|GO|PS|EV|ER|AA)\\d{4}[A-Z]{1,2}\\d{4,6})$'"),
  "SHANDONG": dict(fid=4320324174,
    # derived 2026-08-03 (worklist #22): registry = ~100-row STUB of the OLD
    # format only (2014ZRE27001 = yyyyZR[A-E]xxxxx, 2014-15 vintage); deposits
    # are ~all the MODERN format ZRyyyy + program letters + serial
    # (ZR2016EEQ14, ZR2022QH134) which the registry doesn't carry -> grammar
    # recipe now, keys accept both formats so matching lights up if the
    # registry is ever extended (rescrape-list candidate, low priority given
    # stub size). Bare 8-digit NSFC ids under the province stay unclaimed by
    # policy (bare numbers are never cross-claimed).
    rkey=ex(r"^(ZR(19|20)\\d{2}[A-Z]{1,3}\\d{2,4}|\\d{4}ZR[A-Z][0-9A-Z]{5})$",
            src="regexp_replace(norm,'[ -]','')"),
    xkey=ex(r"(ZR(19|20)\\d{2}[A-Z]{1,3}\\d{2,4}|\\d{4}ZR[A-Z][0-9A-Z]{5})",
            src="regexp_replace(norm,'[ -]','')"),
    gram=r"regexp_replace(norm,'[ -]','') rlike"
         r" '^(ZR(19|20)\\d{2}[A-Z]{1,3}\\d{2,4}|\\d{4}ZR[A-Z][0-9A-Z]{5})$'"),
  "BMBF": dict(fid=4320321114,
    # derived 2026-08-03 (worklist #23): registry = Foerderkatalog FKZ in messy
    # encodings ('01IF20018N', 'NT+0062+/5', 'M000100+/A', 'U100100U/A');
    # deposits = clean FKZ ('01IS19078', '031L0251', '02NUK025B', '01GL1752A').
    # Key = strip [+ /-] and spaces, exact match (transform-based -> NOT
    # extractive, NO XGRAM: no single distinctive token). gram arms: FKZ
    # (2 digits + alnum body containing a letter) and letter-series
    # (M/G/U + 6 digits + optional letter suffix).
    rkey="NULLIF(regexp_replace(norm,'[+ /-]',''),'')",
    xkey="NULLIF(regexp_replace(norm,'[+ /-]',''),'')",
    gram=r"((regexp_replace(norm,'[+ /-]','') rlike '^\\d{2}[0-9A-Z]{5,10}$'"
         r" and regexp_replace(norm,'[+ /-]','') rlike '[A-Z]')"
         r" or regexp_replace(norm,'[+ /-]','') rlike '^[A-Z]\\d{6}[A-Z0-9]{0,2}$')"),
  "FWF": dict(fid=4320321181,
    # derived 2026-08-04 (overnight): registry = LETTER(S)+space+digits
    # (P 10186, PAT 1041324). Deposits: no-space, board suffixes (-N25/-B16),
    # FWF grant DOIs 10.55776/P37169 (audit-flagged misattribution class).
    # Control: foreign fires 7.8% but registry-hits 0.6% of fires -> NOT
    # extractive; XGRAM = DOI form only (bare P+5digit collides w/ JSPS).
    rkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),"
         r"'^([A-Z]{1,3}\\d{3,7})$', 1), '')",
    xkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),"
         r"'^(?:10\\.55776/|HTTPS?://(?:DX\\.)?DOI\\.ORG/10\\.55776/)?([A-Z]{1,3}\\d{3,7})(?:-[A-Z]\\d{1,3})?$', 1), '')",
    gram=r"regexp_replace(norm,' ','') rlike"
         r" '^(10\\.55776/|HTTPS?://(DX\\.)?DOI\\.ORG/10\\.55776/)?[A-Z]{1,3}\\d{3,7}(-[A-Z]\\d{1,3})?$'"),
  "ISCIII": dict(fid=4320334923,
    # derived 2026-08-04 (overnight): registry = canonical scheme codes
    # (PI13/00002, DTS14/00004, PI14CIII/00005, AC18/00002, COV20/00004).
    # Deposits cite PI20/01076 and hyphen variants (PI08-1389, audit-real).
    # Key = SCHEME+yy(+CIII)/serial LPAD5, separator-tolerant on deposits.
    rkey=r"CASE WHEN regexp_replace(norm,' ','') rlike '^[A-Z]{2,4}\\d{2}(CIII)?/\\d{5}$' THEN"
         r" CONCAT(regexp_extract(regexp_replace(norm,' ',''),'^([A-Z]{2,4}\\d{2}(CIII)?)/',1),'/',"
         r"regexp_extract(regexp_replace(norm,' ',''),'/(\\d{5})$',1)) END",
    xkey=r"CASE WHEN regexp_replace(norm,' ','') rlike '^[A-Z]{2,4}\\d{2}(CIII)?[/-]\\d{1,5}$' THEN"
         r" CONCAT(regexp_extract(regexp_replace(norm,' ',''),'^([A-Z]{2,4}\\d{2}(CIII)?)[/-]',1),'/',"
         r"LPAD(regexp_extract(regexp_replace(norm,' ',''),'[/-](\\d{1,5})$',1),5,'0')) END",
    gram=r"regexp_replace(norm,' ','') rlike '^[A-Z]{2,4}\\d{2}(CIII)?[/-]\\d{1,5}$'"),
  "AEI": dict(fid=4320335598,
    # derived 2026-08-04 (overnight): registry ids are INTERNAL row codes
    # (SB64030786, PR113143046) — NOT the citable PID2019-106337GB-I00 the
    # world cites -> grammar-first (rescrape-list candidate); keys target
    # the citable format so matching auto-lights post-rescrape.
    rkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),"
         r"'^((PID|PGC|RYC|RTI|CEX|TED|SEV|BES|FPU|FJC|IJC|CNS|EUR|EQC|PLEC|PDC)\\d{4}-\\d{5,6}[A-Z0-9-]{0,8})$', 1), '')",
    xkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),"
         r"'((PID|PGC|RYC|RTI|CEX|TED|SEV|BES|FPU|FJC|IJC|CNS|EUR|EQC|PLEC|PDC)\\d{4}-\\d{5,6})', 1), '')",
    gram=r"regexp_replace(norm,' ','') rlike"
         r" '^(PID|PGC|RYC|RTI|CEX|TED|SEV|BES|FPU|FJC|IJC|CNS|EUR|EQC|PLEC|PDC)\\d{4}-\\d{5,6}([A-Z0-9/-]{0,12})?$'"),
  "VR": dict(fid=4320322581,
    # derived 2026-08-04 (overnight): registry = swecris YYYY-NNNNN
    # (2004-00731, 23k keys). Deposits: same + VR/Dnr prefixes + 4-digit
    # serials (2011-2988 -> LPAD5). NO XGRAM: Formas/Forte/Vinnova share the
    # YYYY-NNNNN scheme (cross-claim would misattribute within the family).
    # MEASURED: 999/7,311 Formas registry ids collide byte-identically with
    # VR ids (13.7%) — same-funder merging is safe (join is per-funder) but
    # a sibling-misattributed deposit can merge onto the wrong Swedish
    # grant; sibling-ambiguity flag = documented follow-up.
    rkey=r"NULLIF(regexp_extract(norm,'^((19|20)\\d{2}-\\d{5})$',1),'')",
    xkey=r"CASE WHEN regexp_replace(norm,'^(VR|DNR|GRANT)[ .:#-]*','') rlike '^(19|20)\\d{2}[- ]\\d{4,5}$' THEN"
         r" CONCAT(regexp_extract(regexp_replace(norm,'^(VR|DNR|GRANT)[ .:#-]*',''),'^((19|20)\\d{2})',1),'-',"
         r"LPAD(regexp_extract(regexp_replace(norm,'^(VR|DNR|GRANT)[ .:#-]*',''),'[- ](\\d{4,5})$',1),5,'0')) END",
    gram=r"regexp_replace(norm,'^(VR|DNR|GRANT)[ .:#-]*','') rlike '^(19|20)\\d{2}[- ]\\d{4,5}$'"),
  "NASA": dict(fid=4320306101,
    # derived 2026-08-04 (overnight): registry = NAG/NAGW/NCC/NGT/NAS legacy
    # + 80NSSCyyKnnnn modern. Deposits add NNXyyAAnnG era + hyphen/space
    # variants (NAG5-12345). Key = strip [- ] exact match.
    rkey="NULLIF(regexp_replace(norm,'[ -]',''),'')",
    xkey="NULLIF(regexp_replace(norm,'[ -]',''),'')",
    gram=r"regexp_replace(norm,'[ -]','') rlike '^80NSSC\\d{2}[KM]\\d{4}$'"
         r" or regexp_replace(norm,'[ -]','') rlike '^NNX\\d{2}[A-Z]{2}\\d{2,3}[A-Z]?$'"
         r" or regexp_replace(norm,'[ -]','') rlike '^(NAG|NAGW|NCC|NGT|NAS|NNG|NNH|NNJ)\\d{0,2}[A-Z]?\\d{3,6}[A-Z]{0,3}$'"),
  "AMED": dict(fid=4320311405,
    # derived 2026-08-04 (overnight): registry = per-installment codes
    # yy+prog2+serial7+h+inst4 (15ek0109022h0002); deposits cite the 11-char
    # CORE with optional JP prefix (JP25EK0109811). Core key -> installments
    # form a family per core (existing newest-year election applies).
    rkey=r"NULLIF(regexp_extract(norm,'^(\\d{2}[A-Z]{2}\\d{7})H\\d{4}$',1),'')",
    xkey=r"NULLIF(regexp_extract(norm,'^(?:JP)?(\\d{2}[A-Z]{2}\\d{7})(?:H\\d{4})?$',1),'')",
    gram=r"norm rlike '^(JP)?\\d{2}[A-Z]{2}\\d{7}(H\\d{4})?$'"),
  "DHHS": dict(fid=4320306085,
    # derived 2026-08-03 (worklist #18): registry = hhs_taggs (964 ids), two
    # shapes: 5-alnum-starting-letter + 6 digits (CPIMP151089, TP1AH000086,
    # H79AE000058) and 90-series dd[A-Z][A-Z0-9]dddd(d) (90AX0005, 90C30001).
    # Deposits are ~all NIH ids filed under the parent dept (R01..., bare
    # IC+serial) + HHS contract PIIDs (75R60220C00011, HHSN...). gram EXCLUDES
    # the NIH activity-code shape so those flow to wrong-funder detection
    # (S3 -> #624 re-link) instead of dying as DHHS-plausible; registry-hit
    # precedes grammar, so TAGGS codes that look NIH-ish (H79AE000058) still
    # confirm. Contracts count as DHHS-plausible (75/HHSN are HHS PIID forms).
    # Controls: foreign-numeric probe = 0 by construction (rkey requires a
    # letter); foreign-lettered probe 19,902 shape-passes -> 0 registry hits.
    rkey="NULLIF(regexp_extract(regexp_replace(norm,'[ -]',''),"
         "'^([A-Z][A-Z0-9]{4}\\\\d{6}|\\\\d{2}[A-Z][A-Z0-9]\\\\d{4,5})$', 1), '')",
    xkey="NULLIF(regexp_extract(regexp_replace(norm,'[ -]',''),"
         "'([A-Z][A-Z0-9]{4}\\\\d{6}|(?<!\\\\d)\\\\d{2}[A-Z][A-Z0-9]\\\\d{4,5}(?!\\\\d))', 1), '')",
    gram=r"((regexp_replace(norm,'[ -]','') rlike '^([A-Z][A-Z0-9]{4}\\d{6}|\\d{2}[A-Z][A-Z0-9]\\d{4,5})$'"
         r" and not regexp_replace(norm,'[ -]','') rlike '^[A-Z]\\d{2}[A-Z]{2}\\d{5,6}$')"
         r" or regexp_replace(norm,'[ -]','') rlike '^(75[A-Z0-9]{9,13}|HHSN[A-Z0-9]{9,15})$')"),
  # --- staged 2026-08-05/06 worklist batch 2 (12 funders) ---
  "NHMRC": dict(fid=4320334705,
    # derived 2026-08-05 (overnight next-batch): registry = nhmrc prio 12,
    # bare numeric APP ID 6-7 digits, 2013+ only (pre-2013 6-digit deposits
    # like 209057/334047 have no registry row -> grammar_pass, refresh
    # candidate). Deposits are a tri-form of ONE number: bare (~58%),
    # APP-prefixed (~25%), GNT-prefixed (~8%), plus 'GNT 1234567'/'App'
    # spacing variants. Key = strip APP/GNT + spaces -> bare numeral.
    # Bare-numeral matching is funder-gated by construction (per-funder key);
    # foreign-numeric control run 2026-08-05 -> see res/control notes.
    # MRFF grants share the GNT series and deposit under NHMRC's funder DOI
    # (wrong-funder feed analog, DHHS->NIH class) — matching them to the
    # NHMRC registry is CORRECT here (registry rows carry MREA/MRFF org
    # in source; flag for #624-style split only if MRFF gets its own F-id).
    rkey=r"NULLIF(regexp_extract(norm,'^(\\d{6,7})$',1),'')",
    # arm1: APP/GNT token = strong chassis, extract ANYWHERE (wrappers like
    # 'EARLY CAREER FELLOWSHIP (APP1110230)', 'NHMRC-APP1009338'); arm2: bare
    # numeral only ANCHORED (never from prose), optional ID/NHMRC lead.
    xkey=r"COALESCE(NULLIF(regexp_extract(norm,'(?<![A-Z0-9])(?:APP|GNT) ?-?(\\d{6,7})(?!\\d)',1),''),"
         r" NULLIF(regexp_extract(regexp_replace(norm,' ',''),'^(?:ID|NHMRC)?[.:#-]*(\\d{6,7})$',1),''))",
    gram=r"(regexp_replace(norm,' ','') rlike '^(ID|NHMRC)?[.:#-]*(APP|GNT)?\\d{6,7}$'"
         r" or norm rlike '(?<![A-Z0-9])(APP|GNT) ?-?\\d{6,7}(?!\\d)')"),
  "GACR": dict(fid=4320321006,
    # derived 2026-08-05 (overnight next-batch): registry = isvavai_cep prio
    # 34, kod_projektu with leading 2-letter G-code (GA/GB/GC/GF/GJ/GM/GP/GX
    # = G + scheme letter): modern 'GA20-10205S', old panel-era
    # 'GA201/98/0853' / 'GBP405/12/G148'. Deposits DROP the G-code
    # ('22-18469S', 'P208/12/G016'; prefixed minority ~10% 'GB14-36681G');
    # U+2010 hyphens handled by NORM; 'GACR ' lead + embedded spaces seen.
    # Key = G-code-stripped core, suffix letter RETAINED (scheme-bearing,
    # part of the id). Old-era serial = [A-Z]?digits{3,4} (0591, P616, G148).
    # RVO:institutional-support ids excluded by grammar (not grants).
    # lead decorations seen at volume: EXPRO/GACR NO./GA ČR/CSF/GRANT chains;
    # 't' below = lead-stripped + space-stripped. Arms: (a) modern anchored,
    # (c) old-era slash anchored, (d) old-era DASH variant 'P210-11-1431' ->
    # slash form, (e) modern core rescued ANYWHERE (mangles '17-05409S/P301',
    # 'P504/19-16554S' — registry-gated so safe).
    rkey=r"COALESCE(NULLIF(regexp_extract(norm,'^G[A-Z](\\d{2}-\\d{5}[A-Z])$',1),''),"
         r" NULLIF(regexp_extract(norm,'^G[A-Z](P?\\d{3}/\\d{2}/[A-Z]?\\d{3,4})$',1),''))",
    xkey=r"COALESCE("
         r"NULLIF(regexp_extract(regexp_replace(regexp_replace(norm,'^((EXPRO|GA ?[CČ]R|GACR|CSF|GRANT|NO)[ .:#-]+)+',''),' ',''),"
         r"'^(?:G[A-Z])?(\\d{2}-\\d{5}[A-Z])$',1),''),"
         r" NULLIF(regexp_extract(regexp_replace(regexp_replace(norm,'^((EXPRO|GA ?[CČ]R|GACR|CSF|GRANT|NO)[ .:#-]+)+',''),' ',''),"
         r"'^(?:G[A-Z])?(P?\\d{3}/\\d{2}/[A-Z]?\\d{3,4})$',1),''),"
         r" CASE WHEN regexp_extract(regexp_replace(norm,' ',''),'^(?:G[A-Z])?(P?\\d{3})-(\\d{2})-([A-Z]?\\d{3,4})$',1) != ''"
         r" THEN CONCAT(regexp_extract(regexp_replace(norm,' ',''),'^(?:G[A-Z])?(P?\\d{3})-(\\d{2})-([A-Z]?\\d{3,4})$',1),'/',"
         r"regexp_extract(regexp_replace(norm,' ',''),'^(?:G[A-Z])?(P?\\d{3})-(\\d{2})-([A-Z]?\\d{3,4})$',2),'/',"
         r"regexp_extract(regexp_replace(norm,' ',''),'^(?:G[A-Z])?(P?\\d{3})-(\\d{2})-([A-Z]?\\d{3,4})$',3)) END,"
         r" NULLIF(regexp_extract(regexp_replace(norm,' ',''),'(?<![0-9A-Z-])(\\d{2}-\\d{5}[A-Z])(?![0-9A-Z])',1),''))",
    gram=r"(regexp_replace(regexp_replace(norm,'^((EXPRO|GA ?[CČ]R|GACR|CSF|GRANT|NO)[ .:#-]+)+',''),' ','')"
         r" rlike '^(G[A-Z])?\\d{2}-\\d{5}[A-Z]$'"
         r" or regexp_replace(regexp_replace(norm,'^((EXPRO|GA ?[CČ]R|GACR|CSF|GRANT|NO)[ .:#-]+)+',''),' ','')"
         r" rlike '^(G[A-Z])?P?\\d{3}([/-])\\d{2}\\2[A-Z]?\\d{3,4}$'"
         r" or regexp_replace(norm,' ','') rlike '(?<![0-9A-Z-])\\d{2}-\\d{5}[A-Z](?![0-9A-Z])')"),
  "HUNAN": dict(fid=4320322843,
    # derived 2026-08-05 (overnight next-batch): registry = hunan_nsf prio
    # 453, 项目编号 byte-identical to citations, but ONLY 2025-2026 rounds
    # (8,985 rows) -> most deposits (2013-2024) are grammar_pass by coverage,
    # not format: extension candidate (earlier-year roster .doc/.pdf dumps
    # exist per 2026-08-04 scout). Per-year digit-width trap: serial is
    # 4 digits pre-~2019 ('2018JJ2533'), 5 digits after ('2023JJ30388');
    # 2-digit-year era ('13JJ3001') normalized by prepending '20'.
    # gram also admits sibling Hunan S&T series JC/TP/RS seen in deposits
    # (registry is JJ-only, so they stay grammar_pass/kept, never matched).
    # 's' = NO./GRANT lead-stripped + space-stripped ('NO. 2023JJ60146',
    # class; '2019 J40401' typo-form NOT handled — accepted). Sibling Hunan S&T/Ed series in gram only:
    # JC/TP/RS/SK/NK/JK (registry is JJ-only). Hunan Education Dept shapes
    # (K1705018, 19B147) deliberately NOT in gram — different funder.
    rkey=r"NULLIF(regexp_extract(norm,'^((?:19|20)\\d{2}JJ\\d{4,5})$',1),'')",
    xkey=r"COALESCE(NULLIF(regexp_extract(regexp_replace(regexp_replace(norm,'^(NO|GRANT)[ .:#-]*',''),' ',''),'^((?:19|20)\\d{2}JJ\\d{4,5})$',1),''),"
         r" CASE WHEN regexp_replace(regexp_replace(norm,'^(NO|GRANT)[ .:#-]*',''),' ','') rlike '^\\d{2}JJ\\d{4,5}$'"
         r" THEN CONCAT('20',regexp_replace(regexp_replace(norm,'^(NO|GRANT)[ .:#-]*',''),' ','')) END)",
    gram=r"regexp_replace(regexp_replace(norm,'^(NO|GRANT)[ .:#-]*',''),' ','')"
         r" rlike '^((19|20)\\d{2}|\\d{2})(JJ|JC|TP|RS|SK|NK|JK)\\d{4,5}$'"),
  "ZHEJIANG": dict(fid=4320338464,
    # derived 2026-08-05 (overnight next-batch): registry = zhejiang_nsf prio
    # 403, 13,268 rows. Registry ids DO carry the leading L (LQ24H030002,
    # LZ22F010002, LKLY26H030006) — the 08-04 scout's 'registry omits the
    # leading L' claim is WRONG against prod raw (either the ingest already
    # prepends it or the scout misread the source); identity key, NO
    # transform. Shape = L + 0-3 scheme letters + 2-digit year + subject
    # letter + 6-digit serial. Known gap: 2018/2020/2021 rounds absent from
    # registry (dead attachments per tracker) -> those years grammar_pass.
    # Letters widened to {0,4} (LZJWY/LHDMD 4-letter series in registry).
    # LDT-series registry rows carry a trailing serial-echo artifact
    # ('LDT23F01011F01') -> keyless by design; flag as INGEST BUG candidate.
    # Deposit arm2 = dropped-L fallback (scout's claim holds on the DEPOSIT
    # side: 'Z19H100001' cites registry 'LZ19H100001'); prepend L, registry-
    # gated. Y-era pre-2015 ids + sibling provincial key-R&D \d{4}C\d{5}
    # in gram only (registry starts 2019).
    rkey=r"NULLIF(regexp_extract(norm,'^(L[A-Z]{0,4}\\d{2}[A-Z]\\d{6})$',1),'')",
    xkey=r"COALESCE(NULLIF(regexp_extract(regexp_replace(regexp_replace(norm,'^(NO|GRANT)[ .:#-]*',''),' ',''),'^(L[A-Z]{0,4}\\d{2}[A-Z]\\d{6})$',1),''),"
         r" CASE WHEN regexp_replace(regexp_replace(norm,'^(NO|GRANT)[ .:#-]*',''),' ','') rlike '^[A-Z]{1,4}\\d{2}[A-Z]\\d{6}$'"
         r" AND NOT regexp_replace(regexp_replace(norm,'^(NO|GRANT)[ .:#-]*',''),' ','') rlike '^L'"
         r" THEN CONCAT('L',regexp_replace(regexp_replace(norm,'^(NO|GRANT)[ .:#-]*',''),' ','')) END)",
    gram=r"(regexp_replace(regexp_replace(norm,'^(NO|GRANT)[ .:#-]*',''),' ','')"
         r" rlike '^L?[A-Z]{0,4}\\d{2}[A-Z]\\d{6}$'"
         r" or regexp_replace(norm,' ','') rlike '^Y\\d{7,9}$'"
         r" or regexp_replace(norm,' ','') rlike '^(19|20)\\d{2}C\\d{5}$')"),
  # ------- UK council family (staged 2026-08-06 overnight). All six GtR
  # councils share one design: registry = gateway_to_research prio 3
  # (legacy per-publication) + prio 30 (project awards); id core =
  # <CC>/<serial 6-7 alnum, X check-char era>/<1-2 digit part> + bare
  # 7-digit GtR studentships. KEYS ARE SEPARATOR-INSENSITIVE: strip
  # [ _/.-] on BOTH sides (deposits use dash-for-slash 'MR-L012936-1',
  # doubled slashes 'ST/V/001116/1', 'ST/G000/395/1', MC-vs-MC_ etc.).
  # xkey arm1 = council core extracted ANYWHERE (prose wrappers
  # 'CONSOLIDATED GRANT ST/N000609/1', 'MRC/DFID/NIHR MR/S023860/1');
  # arm2 = lead-token-stripped whole-string identity (registry-gated,
  # EPSRC precedent). -------
  "MRC": dict(fid=4320334626,
    # MC_UU/MC_PC intramural (dash/underscore variants unify under
    # strip-all); legacy G-series in gram. Wellcome ids misfiled here ->
    # correct fail (existing Wellcome XGRAM). Truncated bare refs
    # (K013351) NOT reconstructed — EPSRC-truncated policy (#171 feed).
    rkey=r"NULLIF(regexp_replace(norm,'[ _/.-]',''),'')",
    xkey=r"COALESCE("
         r"regexp_replace(NULLIF(regexp_extract(norm,'(?<![A-Z0-9])(MR/ ?[A-Z0-9]{6,7}(/[0-9]{1,2})?)(?![A-Z0-9])',1),''),'[ /]',''),"
         r" NULLIF(regexp_replace(regexp_replace(norm,'^(MRC|UKRI|GRANT|NO)[ .:#-]*',''),'[ _/.-]',''),''))",
    gram=r"(regexp_replace(regexp_replace(norm,'^(MRC|UKRI|GRANT|NO)[ .:#-]*',''),'[ _/.-]','')"
         r" rlike '^MR[A-Z0-9]{6,7}[0-9]{0,2}$'"
         r" or regexp_replace(norm,'[ _/.-]','') rlike '^MC(UU|PC|EX|U|G|W)[A-Z0-9]{4,12}$'"
         r" or regexp_replace(norm,'[ _/.-]','') rlike '^G[0-9]{6,7}$'"
         r" or norm rlike '^\\d{7}$'"
         r" or norm rlike '(?<![A-Z0-9])MR/ ?[A-Z0-9]{6,7}(/[0-9]{1,2})?(?![A-Z0-9])')"),
  "BBSRC": dict(fid=4320334629,
    # BBS/E/<inst>/<serial> institute programmes carry letters in the
    # serial (000I0320, 000PR9798).
    rkey=r"NULLIF(regexp_replace(norm,'[ _/.-]',''),'')",
    xkey=r"COALESCE("
         r"regexp_replace(NULLIF(regexp_extract(norm,'(?<![A-Z0-9])(BBS?/ ?[A-Z0-9/]{6,14}?(/[0-9]{1,2})?)(?![A-Z0-9])',1),''),'[ /]',''),"
         r" NULLIF(regexp_replace(regexp_replace(norm,'^(BBSRC|UKRI|GRANT|NO)[ .:#-]*',''),'[ _/.-]',''),''))",
    gram=r"(regexp_replace(regexp_replace(norm,'^(BBSRC|UKRI|GRANT|NO)[ .:#-]*',''),'[ _/.-]','')"
         r" rlike '^BB[A-Z0-9]{6,7}[0-9]{0,2}$'"
         r" or regexp_replace(norm,'[ _/.-]','') rlike '^BBS[A-Z]{1,3}[A-Z0-9]{7,9}$'"
         r" or norm rlike '^\\d{7}$'"
         r" or norm rlike '(?<![A-Z0-9])BBS?/ ?[A-Z0-9/]{6,14}(?![A-Z0-9])')"),
  "NERC": dict(fid=4320334631,
    rkey=r"NULLIF(regexp_replace(norm,'[ _/.-]',''),'')",
    xkey=r"COALESCE("
         r"regexp_replace(NULLIF(regexp_extract(norm,'(?<![A-Z0-9])(NE/ ?[A-Z0-9]{6,7}(/[0-9]{1,2})?)(?![A-Z0-9])',1),''),'[ /]',''),"
         r" NULLIF(regexp_replace(regexp_replace(norm,'^(NERC|UKRI|GRANT|NO)[ .:#-]*',''),'[ _/.-]',''),''))",
    gram=r"(regexp_replace(regexp_replace(norm,'^(NERC|UKRI|GRANT|NO)[ .:#-]*',''),'[ _/.-]','')"
         r" rlike '^NE[A-Z0-9]{6,7}[0-9]{0,2}$'"
         r" or norm rlike '^\\d{7}$'"
         r" or norm rlike '(?<![A-Z0-9])NE/ ?[A-Z0-9]{6,7}(/[0-9]{1,2})?(?![A-Z0-9])')"),
  "STFC": dict(fid=4320334632,
    # PP/ = legacy PPARC refs (present in GtR registry); GRIDPP-class
    # named programmes ride the identity arm.
    rkey=r"NULLIF(regexp_replace(norm,'[ _/.-]',''),'')",
    xkey=r"COALESCE("
         r"regexp_replace(NULLIF(regexp_extract(norm,'(?<![A-Z0-9])((ST|PP)/ ?[A-Z0-9/]{6,9}?(/[0-9]{1,2})?)(?![A-Z0-9])',1),''),'[ /]',''),"
         r" NULLIF(regexp_replace(regexp_replace(norm,'^(STFC|UKRI|GRANT|NO)[ .:#-]*',''),'[ _/.-]',''),''))",
    gram=r"(regexp_replace(regexp_replace(norm,'^(STFC|UKRI|GRANT|NO)[ .:#-]*',''),'[ _/.-]','')"
         r" rlike '^(ST|PP)[A-Z0-9]{6,7}[0-9]{0,2}$'"
         r" or norm rlike '^\\d{7}$'"
         r" or norm rlike '(?<![A-Z0-9])(ST|PP)/ ?[A-Z0-9/]{6,9}(/[0-9]{1,2})?(?![A-Z0-9])')"),
  "ESRC": dict(fid=4320334630,
    # RES-xxx-xx-xxxx / PTA-xxx-xxxx-xxxxx = pre-2011 ESRC scheme refs,
    # THE dominant deposit fail class; unify under strip-all (registry
    # rows carrying them match; the rest stay grammar_pass/kept).
    # UKRI\d{3,4} = cross-council FLF.
    rkey=r"NULLIF(regexp_replace(norm,'[ _/.-]',''),'')",
    xkey=r"COALESCE("
         r"regexp_replace(NULLIF(regexp_extract(norm,'(?<![A-Z0-9])(ES/ ?[A-Z0-9]{6,7}(/[0-9]{1,2})?)(?![A-Z0-9])',1),''),'[ /]',''),"
         r" NULLIF(regexp_replace(regexp_replace(norm,'^(ESRC|UKRI[ .:#-]|GRANT|NO)[ .:#-]*',''),'[ _/.-]',''),''))",
    gram=r"(regexp_replace(regexp_replace(norm,'^(ESRC|GRANT|NO)[ .:#-]*',''),'[ _/.-]','')"
         r" rlike '^ES[A-Z0-9]{6,7}[0-9]{0,2}$'"
         r" or regexp_replace(norm,'[ _/.-]','') rlike '^(RES|PTA)[0-9]{9,12}$'"
         r" or regexp_replace(norm,'[ _/.-]','') rlike '^UKRI[0-9]{3,4}$'"
         r" or norm rlike '^\\d{7}$'"
         r" or norm rlike '(?<![A-Z0-9])ES/ ?[A-Z0-9]{6,7}(/[0-9]{1,2})?(?![A-Z0-9])')"),
  "AHRC": dict(fid=4320334609,
    rkey=r"NULLIF(regexp_replace(norm,'[ _/.-]',''),'')",
    xkey=r"COALESCE("
         r"regexp_replace(NULLIF(regexp_extract(norm,'(?<![A-Z0-9])(AH/ ?[A-Z0-9]{6,7}(/[0-9]{1,2})?)(?![A-Z0-9])',1),''),'[ /]',''),"
         r" NULLIF(regexp_replace(regexp_replace(norm,'^(AHRC|UKRI[ .:#-]|GRANT|NO)[ .:#-]*',''),'[ _/.-]',''),''))",
    gram=r"(regexp_replace(regexp_replace(norm,'^(AHRC|GRANT|NO)[ .:#-]*',''),'[ _/.-]','')"
         r" rlike '^AH[A-Z0-9]{6,7}[0-9]{0,2}$'"
         r" or regexp_replace(norm,'[ _/.-]','') rlike '^UKRI[0-9]{3,4}$'"
         r" or norm rlike '^\\d{7}$'"
         r" or norm rlike '(?<![A-Z0-9])AH/ ?[A-Z0-9]{6,7}(/[0-9]{1,2})?(?![A-Z0-9])')"),
  "INNOVATE_UK": dict(fid=4320335087,
    # registry innovate_uk prio 28 (41,073): bare numerics 5-8 digit
    # (modern 10xxxxxx, legacy 5-6 digit) + KTP\d{6}. BARE-NUMERAL DENSE
    # funder — anchored-only arms, foreign control decides weak flag.
    # Funder MISSING from common.funders by name (NSTC-class dim gap;
    # fid recovered from raw prio-28 rows) — flag to Kyle/Casey.
    rkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),'^(\\d{5,8}|KTP\\d{6})$',1),'')",
    xkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),'^(?:PROJECT|GRANT|APP|NO)?[.:#-]*(\\d{5,8}|KTP\\d{6})$',1),'')",
    gram=r"regexp_replace(norm,' ','') rlike '^(PROJECT|GRANT|APP|NO)?[.:#-]*(\\d{5,8}|KTP\\d{6})$'"),
  "NIHR": dict(fid=4320319990,
    # registry nihr prio 13 (10,763, ODS-refresh 07-13), citable forms
    # stored verbatim: NIHR\d{6}, HTA slash-triplets (16/136/33),
    # hyphenated programme refs (PB-PG-1010-23263, RP-2017-08-ST2-006,
    # CL-2022-07-002), spaced legacy (CDRF 2009-40). Deposits add a
    # NIHR- lead (NIHR-SRF-2015-08-001; registry stores SRF-...) and
    # prose wraps -> n1 = global 'NIHR[- ]' strip (NIHR300437 has no
    # separator, unaffected); arm1 slash-triplet w/ optional programme
    # lead token (GHRU 16/136/54); arm2 year-anchored hyphen-ref
    # extracted anywhere; arm3 space-stripped identity (registry-gated).
    # MC_PC_* under NIHR = MRC ids, correct fail.
    rkey=r"NULLIF(regexp_replace(norm,' ',''),'')",
    xkey=r"COALESCE("
         r"NULLIF(regexp_extract(regexp_replace(regexp_replace(norm,'NIHR[- ]',''),' ',''),'^(?:[A-Z]{2,6})?(\\d{2}/\\d{2,4}/\\d{2,4})$',1),''),"
         r" NULLIF(regexp_extract(regexp_replace(norm,'NIHR[- ]',''),'(?<![A-Z0-9-])([A-Z]{1,5}(?:-[A-Z0-9]{1,4}){0,3}-(?:19|20)\\d{2}-[0-9]{2,6}(?:-[A-Z0-9]{1,6}){0,2})(?![A-Z0-9-])',1),''),"
         r" NULLIF(regexp_replace(regexp_replace(norm,'NIHR[- ]',''),' ',''),''))",
    gram=r"(regexp_replace(norm,' ','') rlike '^NIHR\\d{4,6}$'"
         r" or (regexp_replace(regexp_replace(norm,'NIHR[- ]',''),' ','') rlike '^([A-Z]{2,6})?\\d{2}/\\d{2,4}/\\d{2,4}$'"
         r" and not regexp_replace(regexp_replace(norm,'NIHR[- ]',''),' ','') rlike '^\\d{2}/(0[1-9]|1[0-2])/\\d{2,4}$')"
         r" or (regexp_replace(regexp_replace(norm,'NIHR[- ]',''),' ','') rlike '^[A-Z][A-Z0-9]{0,5}(-[A-Z0-9]{1,6}){1,5}$'"
         r" and not regexp_replace(norm,' ','') rlike '^(H2020|HORIZON|ORCID|DOI|ISBN)'"
         r" and norm rlike '\\d{4}')"
         r" or regexp_replace(norm,' ','') rlike '^[A-Z]{2,6}\\d{2,4}-\\d{2,4}$')"),
}

XREF = "('crossref_work_funders','crossref_work.grants','crossref_work')"

# ---- Salvage-chain constants (#690 guard; classification ported from
# ---- 690-audit/classify.py — keep in lockstep) ----
# Generic decorations the sharp deposited-side keys don't strip, applied to the
# normalized (upper/trim/unicode-cleaned) form. Leading words require >=1
# separator so prefixes of real tokens ("NOVO...") survive; a corrupted strip
# is harmless anyway because rescue still requires a registry hit.
DECOR_LEAD = (r"^((GRANT|GRANTS|AWARD|AWARDS|PROJECT|CONTRACT|AGREEMENT|APPLICATION|APP"
              r"|REFERENCE|REF|NUMBER|NUM|NO|N0|ID|CODE|FUNDREF|UNDER|JSPS|KAKENHI|MEXT"
              r"|OPUS|SONATA|PRELUDIUM|HARMONIA|MAESTRO|ETIUDA|GRIEG|NCN|PROBRAL|PROCESSO|PROCESS"
              r"|FKZ|PHD|POSTDOC|FELLOWSHIP|STUDENTSHIP"
              r"|AND|NSERC|CIHR|SNSF|SNF|CNPQ)"
              r"[ .:#°_-]+"
              r"|HTTPS?://KAKEN\\.NII\\.AC\\.JP/GRANT/KAKENHI-PROJECT-"
              r"|KAKENHI[ /]+"
              r"|[A-Z]{2,10} ?\\(+ ?"
              r"|GRANT ?\\(?NO\\.? ?"
              # review pass-4 F6: LONG words (>=5 chars, unambiguous) also strip
              # with NO separator ("award307834"); short words keep the
              # separator requirement so real-token prefixes survive
              r"|(GRANT|AWARD|PROJECT|CONTRACT|NUMBER|KAKENHI|REFERENCE|APPLICATION|PROCESSO|PROCESS)"
              r"|\\([A-Z0-9]{1,3}\\) ?"
              r"|[A-Z] ?# ?"
              r"|[#№(\\[/:.]+ ?)+")
DECOR_TRAIL = (r"([ .,;:)/\\]]+|[ -]*\\(.*\\)"
               # attributions: "to S.F.", "- C.G.M.", "for K. N.",
               # "awarded to A.B. and K.Z."
               r"|[ -]+((AWARDED )?TO|FOR) [A-Z][A-Z. ]{1,20}( AND [A-Z][A-Z. ]{1,10})?"
               r"|[ -]+[A-Z]\\.( ?[A-Z]\\.?){1,3}"
               r"|[-]{1,2}"                          # trailing lone dash(es): '360883-' 
               r")$")
# multi-id concat detection + split (mirrors classify.py CLS 'multi_id' arm)
MULTI_DETECT = ("((_n rlike '[,;&]') OR (_n rlike ' AND ') OR (_n rlike '[0-9A-Z]\\\\+[0-9A-Z]'))"
                " AND _n rlike '[0-9]{3}'")
MULTI_SPLIT = r"[,;&+]|\\bAND\\b"

# S3 candidate admission for letterless-but-structured ids (FAPESP chassis).
# MUST live here as a constant: review pass-4 F1 found the literal inlined in
# the generator f-string, where {2,4} was evaluated as a Python tuple and the
# emitted regex matched nothing (1,798 FAPESP-registry-verified ids would have
# been suppressed).
S3_NUMERIC_CHASSIS = r"(?<!\\d)\\d{2,4}/\\d{4,5}-\\d(?!\\d)"

# POSITIVE-JUNK CLASSES (recalibration round 1, 2026-08-03). DESIGN FLIP after
# the random-327 audit measured 64.8% of "failed to verify" suppressions as
# REAL grants in mangled dialects: suppression now requires POSITIVE
# classification as junk — an unclassifiable string defaults to KEEP. The
# space of real-id dialects is open-ended; the space of junk is enumerable
# (top-150 audit: 94.4% of junk links = exactly these classes). Patterns run
# on _n (normalized). Bare 7-digit (NSF-form) and 8-digit (NSFC-form) numbers
# are deliberately NOT junk (audit graded them [form]-real cross-deposits).
JUNK_POSITIVE = [
  r"^(HORIZON ?2020|HORIZON ?EUROPE|H2020|FP[4-7]|ERASMUS(\\+| ?PLUS)?|MSCA|COST( ACTION)?|PRELUDIUM ?\\d{0,2}|OPUS ?\\d{0,2}|SONATA( BIS)? ?\\d{0,2}|CAREER|EPSCOR|CREST|INSPIRE|SBIR|STTR|R&D|COVID(-?19)?|RESEARCH ?4 ?COVID.*|FRANCE ?2030|STI ?2030.*|EDCTP2?|PT ?2020|COMPETE ?2020?|NORTE ?2020|CENTRO ?2020|LISBOA ?2020|POCI|FEDER|NSFC|973( PROGRAM)?|863( PROGRAM)?|111( PROJECT)?|NIH|NSF|DFG|ANR|AHA|ERC|GACR|MOST|JSPS|KAKENHI|CNPQ|CAPES|FCT|N/?A)$",  # program/framework/funder NAMES
  r"^(19|20)\\d{2}[-–/ ]{1,3}(19|20)\\d{2}$",                            # year ranges (2007-2013)
  r"^(19|20)\\d{2}$",                                                    # bare year
  r"^10\\.13039/\\d{6,12}$",                                             # Crossref funder DOIs
  r"^[^0-9]*10\\.13039/[0-9]{6,12}[^0-9]*$",                                                         # funder-DOI anywhere (incl. URL forms)
  r"^(HTTPS?://|WWW\\.)(?!.*10\\.(58275|54499|35802|55776))",                        # URLs — EXCEPT grant-DOI registrars (AHA 10.58275, FCT 10.54499: they embed a real award id)
  r"^0000-000\\d-\\d{4}-[0-9X]{4}$",                                     # ORCIDs
  r"^0(?=.*[A-Z])[A-Z0-9]{6}[0-9]{2}$",                                  # ROR ids (00x0ma614): 0 + 6 alnum + 2-digit checksum; letter required (round-3: tail digits kill FKZ false-positives like 031L0260B)
  r"^(N/?A|NA|NONE|NIL|NOT APPLICABLE|UNKNOWN|TBD|PENDING|NULL|XXX+|[-.,;:/#*+ ]+)$",  # placeholders
  r"^\\(?(FINANCE|FINANCIAL)? ?CODE[ :]*0*1\\)?\\.?$|^0*1$",              # CAPES finance-code boilerplate (ANCHORED round-2: substring form swallowed ids embedding a real process number)
  r"^.{1,3}$",                                                           # <=3 chars
  r"^( ?[A-Z]{2,}){4,}$",                                                # prose: 4+ all-letter words
  r"^[0-9]{1,5}$",                                                       # bare integer <=5 digits (6-digit carved out round-2: SNSF project-number and H2020 GA space)
  r"^(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[A-Z]* (19|20)\\d{2}$|^\\d{1,2}[./]\\d{1,2}[./](19|20)?\\d{2}$",  # dates
  r"[-/_.]$|^[-/_.]",                                                    # truncation fragments (leading/trailing separator: 'DE-AC02-', '/Z/15/Z', '-0001')
  r"^(ANR|MOST|NSC|NSTC|RGPIN|MOP|PJT|UMO|DEC|FP[4-7]|H2020|GRANT|AWARD|PROJECT|NO|REF)[- _]?\\d{0,4}$",  # scheme prefix with no serial (ANR-10, MOST 104)
  r"^(19|20)\\d{2}[-–/](19|20)?\\d{1,2}$",                               # year-edition fragments (2014/, 2019/20)
]

# CHASSIS-ANYWHERE RESCUE (2026-08-04, non-DOE n=400 audit): nearly every
# remaining false-suppression is a COMPLETE id inside a wrapper (prose, DOI,
# punctuation, typo'd prefix). Per-class stripping is a losing race — so any
# string CONTAINING one of these structural id cores can never suppress.
# LETTERED/STRUCTURED cores only (never bare digit runs).
CHASSIS_ANYWHERE = [
  r"[0-9]{2,3}[- ][0-9]{4}[- ]?[A-Z][- ][A-Z0-9]{3,4}[- ]{1,3}[0-9]{2,3}",     # Taiwan MOST/NSTC core
  r"(RGPIN|RGPAS|RGPNS|DGECR|CRDPJ|RDCPJ|SAPIN|PGSD?[0-9]?)[ /=-]{1,3}[0-9]{5,6}([ -][0-9]{2,4})?",  # NSERC serial-first
  r"(RGPIN|RGPAS|RGPNS|DGECR|CRDPJ|SAPIN)[- ]?(19|20)[0-9]{2}[- ][0-9]{4,6}",  # NSERC modern year-first
  r"[0-9]{6}[A-Z]?_[0-9]{6}",                                            # SNSF instrument_serial
  r"(SFB|TRR?|CRC|EXC|GRK|RTG|FOR|SPP|NFDI|KFO)[ /]?[0-9]{2,4}",         # DFG programmes (incl. TR)
  r"[A-Z]{1,3}[ -]?[0-9]{1,4}/[0-9]{1,3}-[0-9]",                         # DFG signature X 123/4-5
  r"[0-9]{2}[A-Z][0-9]{5}(?![0-9])",                                     # KAKEN core yyLddddd
  r"[0-9]{5,6}/[A-Z]/[0-9]{2}(/[A-Z])?",                                    # Wellcome citable
  r"(GR|WT)[0-9]{6}(MA|MAJ|MF|AIA)?(?![0-9])",                                     # Wellcome legacy lettered
  r"(?<![0-9])(19|20)[0-9]{2}/[0-9]{4,5}-[0-9](?![0-9])",                # FAPESP full form
  r"(UIDB?|UIDP|PTDC|SFRH|CEEC(IND)?|POCI|ALT[0-9]{2}|LA/P)[/ -][A-Z0-9/. -]{3,24}[0-9]",  # FCT families
  r"(8888[0-9]|99999|23038)\\.[0-9]{6}/[0-9]{4}",                         # CAPES process core
  r"BEX[ :]{0,2}[0-9]{4,5}/[0-9]{2}",                                        # CAPES BEX
  r"20[0-9]{2}/[0-9]{2}/[A-Z]{1,2}/[A-Z]{2,3}[0-9]{1,2}/[0-9]{5}",       # NCN UMO
  r"(MOP|PJT|FDN|FRN|ROG|CPG|IAP|HHP|IRR|OV[0-9])[ #-]{1,3}[0-9]{5,6}",  # CIHR programs
  r"HHSN[0-9]{9,13}[A-Z]?",                                              # NIH/HHS contracts (incl. typo'd lengths)
  r"DE-?[A-Z]{2}[0-9]{2}-?[0-9]{2}[A-Z]{2}[0-9]{4,6}",                   # DOE DE-family
  r"ANR-?[0-9]{2}-[A-Z0-9]{2,6}-[0-9]{1,4}",                             # ANR
  r"[0-9]{2}[A-Z]{1,4}[0-9]{3,5}[A-Z](?![A-Z0-9])",                      # BMBF FKZ lettered-suffix
  r"(PID|PGC|RYC|RTI|CEX|TED|PCI|PDC|EQC|CNS|PLEC|SEV|EUR|MDM|IJC|FJC|JDC|RTC|RED)[- ]?[0-9]{4}[- .]{1,3}(?!(19|20)[0-9]{2}(?![0-9]))[0-9]{3,6}",                           # Spanish AEI
  r"(PI|DTS|AC|ICI|COV)[0-9]{2}(CIII)?[/-][0-9]{4,5}",                   # ISCIII
  r"EP/[A-Z][0-9]{5,6}[A-Z0-9]?(/[0-9])?",                               # EPSRC (incl. X-suffix)
  r"AHA[ -]?[0-9]{6,9}|[0-9]{2}(PRE|POST|SDG|GRNT|CDA|EIA|TPA|SFRN|IPA)[0-9]{6,8}",
  r"(?<![0-9])[0-9]{3}[- ](19|20)[0-9]{2}[- ][0-9]{4,5}(?![0-9])",  # VR/Formas dnr 3-seg
  r"(?<![0-9/])(?<![0-9])(19|20)[0-9]{2}-[0-9]{5}(?![0-9])",  # VR modern dnr year-5 (5 digits excludes year-range junk)
  r"PNRR[- ][A-Z]{1,4}[- ]?[A-Z0-9-]{0,10}20[0-9]{2}[- ][0-9]{6,9}",  # Italian PNRR project codes
  r"(?<![A-Z0-9])(ECS|PE|CN|IR)_?[0-9]{8}(?![0-9])",  # PNRR ecosystem/partenariato codes e.g. ECS00000036
  r"20[0-9]{2}ZD[0-9]{7}(?![0-9])",  # China STI2030 major project
  r"CUP[ :]{0,2}[A-Z][0-9A-Z]{10,14}",
  r"NNN[0-9]{2}[A-Z]{2}[0-9]{2}[A-Z](?![A-Z0-9])",  # NASA contracts (NNN06AA01C)
  r"FA[0-9]{4}-[0-9]{2}-[0-9]-[0-9]{4}",  # AFOSR
  r"JP ?[0-9]{2}[A-Z]{2}[0-9]{7}(?![0-9])",  # AMED canonical JP-form
  r"(?<![A-Z0-9/])PI[0-9]{6}(?![0-9])",  # ISCIII FIS compact (PI020499)
  r"(?<![0-9])(?<![0-9]-)(?<![0-9/])(19|20)[0-9]{2}-(?!(19|20)[0-9]{2}(?![0-9]))[0-9]{4}(?![0-9])",  # VR 4-digit dnr; 2nd part may not be a year
  r"[0-9]{2}-AIST[0-9]{2}-[0-9]-[0-9]{4}",  # NASA AIST awards
  r"80NSSC[0-9]{2}[A-Z][0-9]{4}",  # NASA NSSC awards
  r"[A-Z]{2,6}_[0-9]{1,2}-[0-9][- ]20[0-9]{2}-[0-9]{4}",  # Hungarian NKFIH (NVKP_16-1-2016-0017)
  r"N N[0-9]{3} [0-9]{4} [0-9]{2}(?![0-9])",  # Polish MNiSW legacy N-grants
  r"436 ?[A-Z]{3} ?[0-9]{2}/[0-9]{2}/[0-9]{2}",  # DFG bilateral 436-scheme
  r"RP[A-Z]{2}[.][0-9]{2}[.][0-9]{2}[.][0-9]{2}-[0-9]{2}-[0-9]{4}/[0-9]{2}",  # Polish ROP/ERDF project ids
  r"(?<![0-9/.])[0-9]{2,4}/[0-9]{5}[- ]?[0-9]?(?![0-9])",
  r"10[.]55776/[A-Z]{0,4}[0-9]{1,6}",  # FWF grant DOIs are the funder's own award ids
  r"20[0-9]{2}[MT][0-9]{6}(?![0-9])",  # China Postdoctoral Science Foundation
]

# FOREIGN-SCHEME KEEP-LIST (2026-08-03, shape census over the suppress pile):
# ids matching a KNOWN grant-id scheme of a funder OUTSIDE the configured 23
# must never suppress — they are real grants wrongly filed (the ~57k class the
# census surfaced: UKRI council refs, Spanish AEI, Italian PRIN/PNRR/CUP,
# Czech RVO/LM, Polish EU operational programmes, Chilean ANID, Chinese
# provincial year+letters+serial, DFG signature forms under non-DFG funders).
# Verdict 'foreign_scheme' -> kept + flagged (future re-link material once
# those funders get registries); grammar-only, no registry to verify against.
FOREIGN_SCHEMES = [
  r"^(MR|BB|EP|NE|ES|AH|ST|EY|G)[0-9]{0,2}/[A-Z0-9]{6,8}/[0-9]{1,2}$",  # UKRI councils
  r"^(PID|PGC|RYC|RTI|CEX|TED|SEV|BES|FPU|FJC|IJC|MAT|FIS|CTQ|SAF|BFU|AGL|ECO|DPI|TIN|FFI|HAR)[0-9]{4}-[0-9A-Z-]{3,}$",  # Spanish AEI/MICINN
  r"^(PRIN|PNRR|FIRB|FISR|PON|POR)[ :-]?[0-9A-Z]{2,}$",                  # Italian nationals
  r"^CUP[ :]?[A-Z][0-9][0-9A-Z]{8,13}$",                                 # Italian CUP codes
  r"^(RVO|MSM|LO|LM|LQ|GA|GX|GJ)[.:]? ?[0-9]{2,8}([./-][0-9A-Z]+)?$",   # Czech schemes
  r"^(POWR|POIR|POPC|POPW|RPMA)\\.[0-9.]{2,12}[/-][0-9A-Z-]{2,}$",       # Polish EU OPs
  r"^(ANID|FONDECYT|FONDAP|PIA|ACT|ICN)[ /-]?[0-9]{4,8}$",               # Chilean ANID family
  r"^2[0-9]{3}[A-Z]{2,8}[0-9]{3,8}$",                                    # CN provincial year+letters+serial
  r"^(INST )?[A-Z]{1,4}[- ]?[0-9]{2,4}/[0-9]{1,3}(-[0-9]{1,2})?( FUGG)?$",  # DFG signature under non-DFG funders (round-2: hyphen sep, INST, FUGG)
  r"^(?!0(?=.*[A-Z])[A-Z0-9]{6}[0-9]{2}$)[0-9]{2}(?=[A-Z0-9]*[A-Z])[A-Z0-9]{2,4}[0-9]{3,4}[A-Z]{0,3}$",      # BMBF FKZ chassis under any funder (01KR1304A, 031L0260B — program token may contain a digit)
  r"^[0-9]{2}(JJ|ZR|DZ|JC|SF|SK|YF)[0-9]{4,7}$",                         # CN provincial two-letter series (Hunan 06JJ50029, Shanghai ZR/DZ)
  r"^[A-Z]{2,5}-[0-9]{7}$",                                              # NSF division form under other funders (DGE-1650116)
  r"^[0-9]{6}[A-Z]?_[0-9]{6}(/[0-9])?$",                                 # SNSF instrument-coded (200020L_175755) under any funder
  r"^EFOP-[0-9]\\.[0-9]\\.[0-9]-[0-9]{2}-20[0-9]{2}-[0-9]{5}$",           # Hungarian EFOP
  r"^YXJL-20[0-9]{2}-[0-9]{4}-[0-9]{4}$",                                # Beijing Medical Award Foundation
  r"^[0-9]{2,3}-EPA-[A-Z0-9-]{5,12}$",                                   # Taiwan EPA commissioned projects
  r"^(HTTPS?://(DX\\.)?DOI\\.ORG/)?10\\.35802/[0-9]{5,6}$",              # Wellcome grant DOIs (10.35802/210622)
  r"(?<![0-9])[0-9]{6}/[0-9]{2,4}-[0-9](?![0-9])",                       # CNPq process numbers incl. legacy 2-digit years (303715/2011-1) — was lost in a revert
  r"^(ECS|IR|CN|PE|SOE)0{3,6}[0-9]{2,5}$",                               # Italian PNRR codes (ECS00000022 ecosystems, IR0000029 infrastructures, PE/CN partenariati)
  # --- contract-number schemes (top-150 link-weighted audit, 2026-08-03):
  # real research CONTRACTS (not grants) that no grant registry holds ---
  r"^(DE[- ]?)?A[CR][0-9]{2}[- ]{0,2}[0-9]{2}[- ]{0,2}[A-Z]{2,3} ?[0-9]{4,6}$",  # DOE M&O contracts (DE-AC02-05CH11231 + mangled variants incl. extra/double separators)
  r"^W[- ]?[0-9]{2,4}([- ]?[0-9]{1,3})?[- ]?ENG[- ]?[0-9]{2}$",          # DOE W-contracts (W-7405-ENG-48, W-31-109-Eng-38)
  r"^W81XWH[- ]?[0-9]{2}[- ]?[0-9][- ]?[0-9]{4}$",                       # DoD CDMRP (W81XWH-12-2-0012)
  r"^#? ?[0-9]{2}[A-Z0-9]{6,14}(CNA|NA)[0-9]{6}$",                       # NNSA lab contracts (89233218CNA000001)
  r"^#? ?HHSN[0-9]{9,12}[A-Z]?$",                                        # NIH/HHS contracts (HHSN261200800001C/E)
  r"^[A-Z]{3,8}[0-9]{0,2}[- ]?CT[- ]?[0-9]{2}[- ]?[0-9]{4}$",            # EU FP-era contracts (MAS3-CT98-0174, ERBIC20-CT98-0103)
]

# Funders whose deposited-side xkey is a TRUE EXTRACTOR (regexp_extract of a
# structured id; returns NULL when nothing id-shaped is present). Review
# pass-4 F2: when such an extractor FIRES but the registry misses (registry
# coverage gap), the id is evidence-bearing and must score >= 'plausible',
# never garbage/suppress — measured harm class: "OPUS 2019/35/B/ST10/04141"
# (NCN), "PROBRAL - 88887.283886/2018-00" (CAPES), 16/16 sampled real.
# Transform-based xkeys (MOST/NSTC/FCT/EPSRC/AHA strip-space) fire on ANY
# string and are excluded — membership here would neuter their suppression.
# DHHS is ALSO excluded (pass-4 follow-up): NIH activity codes syntactically
# fit the TAGGS shape ([A-Z][A-Z0-9]{4}\d{6}), so a fired TAGGS extraction is
# NOT evidence-of-DHHS — membership reclassified ~31k NIH-under-DHHS ids as
# DHHS-plausible and hid them from wrong-funder detection (#624 feed dropped
# 40,290 -> 9,071). Real DHHS ids stay protected by registry-hit, gram
# (NIH-shape-excluding), and the salvage chain.
EXTRACTIVE_FIDS = {
  4320321001,  # NSFC
  4320332161,  # NIH
  4320306076,  # NSF
  4320334764,  # KAKEN
  4320320879,  # DFG
  4320320997,  # FAPESP
  4320320300,  # EC
  4320334593,  # NSERC
  4320320883,  # ANR
  4320320924,  # SNSF
  4320311904,  # WELLCOME
  4320334506,  # CIHR
  4320321091,  # CAPES
  4320322511,  # NCN
  4320306084,  # DOE (chassis extractor; no cross-shape overlap — unlike DHHS)
  4320311405,  # AMED (JP-core extractor)
  4320324174,  # Shandong NSF (ZR chassis extractor)
  4320334705,  # NHMRC (APP/GNT chassis anywhere-extractor; batch 2)
  4320321006,  # GACR (modern-core anywhere-extractor; batch 2)
  4320334626,  # MRC (council-core anywhere-extractor; batch 2)
  4320334629,  # BBSRC (same)
  4320334631,  # NERC (same)
  4320334632,  # STFC (same)
  4320334630,  # ESRC (same)
  4320334609,  # AHRC (same)
  4320319990,  # NIHR (year-anchored hyphen-ref anywhere-extractor)
  # HUNAN/ZHEJIANG/INNOVATE_UK anchored-transform keys = NOT extractive
}

# STRONG cross-grammars for wrong-funder detection (S3) — measured lesson
# 2026-08-03: the per-funder `gram` fields are calibrated for ids ALREADY
# attributed to that funder; several (SNSF '^[0-9A-Z]{0,8}[_-]?\d{4,6}$',
# CIHR/NSERC any-letter-prefix arms) accept strings with no funder-specific
# token, and against dense numeric registries that yields tens of thousands
# of coincidental "detections" (first build: 23.7k NSFC ids "belonging to"
# SNSF). Claiming an id for ANOTHER funder therefore requires the funder's
# DISTINCTIVE token structure below. Funders with no distinctive lettered
# form (NSFC bare 8-digit, SNSF bare serials) cannot be cross-targets: absent
# here = excluded from S3. Philosophy + several patterns = classify.py OF_PAT.
XGRAM = {
  # NIH: activity-code form (classify.py NIH_PAT) + IC-restricted bare
  # institute+serial. The IC list is the closed set of NIH institute codes —
  # unrestricted [A-Z]{2} would accept any 2-letter prefix. 6-digit serials
  # are unanchored (tolerates '1'/'5' prefixes and '-01A1' suffixes:
  # "AR063759-05", "RO1 AR079224"); 5-digit serials stay anchored AND
  # exclude CA — EU COST Actions are exactly 'CA' + 5 digits.
  # arm 1 allows an ATTACHED application-type digit ("1R01AR059270-01",
  # "5U01MH105985" — digit is alphanumeric, so it defeats a plain boundary)
  # and both activity-code chassis: [A-Z]dd (R01, K99, T32) and [A-Z][A-Z]d
  # (KL2, UL1, UG1, DP2)
  4320332161: (r"norm rlike '(^|[^A-Z0-9])\\d?([A-Z]\\d{2}|[A-Z]{2}\\d)[ -]?[A-Z]{2}[ -]?\\d{5,6}([^0-9]|$)'"
               r" or norm rlike '(^|[^A-Z0-9])(AA|AG|AI|AR|AT|CA|DA|DC|DE|DK|EB|ES|EY|GM|HD|HG|HL|LM|MD|MH|NR|NS|OD|RR|TR|TW)[ -]?\\d{6}([^0-9]|$)'"
               r" or norm rlike '^(AA|AG|AI|AR|AT|DA|DC|DE|DK|EB|ES|EY|GM|HD|HG|HL|LM|MD|MH|NR|NS|OD|RR|TR|TW)[ -]?\\d{5}$'"),
  # DHHS/TAGGS: 5-alnum + 6-digit shape, minus the NIH activity-code shape
  # (which belongs to the NIH arm above); 90-series is too short/loose for
  # cross-funder claims
  4320306085: (r"(regexp_replace(norm,'[ -]','') rlike '^[A-Z][A-Z0-9]{4}\\d{6}$'"
               r" and not regexp_replace(norm,'[ -]','') rlike '^[A-Z]\\d{2}[A-Z]{2}\\d{5,6}$')"),
  # NSF: division-prefixed 7-digit, separator REQUIRED — concatenated forms
  # ("AC2019004", "AF0710020") are dominated by Asian program codes whose
  # trailing yy+serial digits coincidentally form valid NSF numbers (measured
  # in the NSFC->NSF flow of the second build); "ACI-1053575"/"ACI 1626217"
  # style survives. Concatenated true NSF cites ("DMS0819762") are a known
  # recall sacrifice.
  4320306076: r"norm rlike '^[A-Z]{2,5}[ -]\\d{7}$'",
  # KAKEN: JP/KAKENHI prefix, or the yy[A-Z]ddddd core (letter is structural)
  4320334764: (r"norm rlike '^(KAKENHI|JP)[ -]*(\\d{2}[A-Z]\\d{5}|\\d{8})$'"
               r" or norm rlike '^\\d{2}[A-Z]\\d{5}$'"),
  # DFG: programme-prefixed forms only
  4320320879: r"norm rlike '^(SFB|TRR|CRC|EXC|GRK|RTG|FOR|SPP|INST|NFDI|KFO|FZT) ?/?-?\\d+'",
  # MOST/NSTC Taiwan: distinctive ddddddd[A-Z]dddddd core, prefixed or not
  4320322795: (r"regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]','')"
               r" rlike '^\\d{6,7}[A-Z]\\d{6}(MY\\d)?E?\\d?$'"),
  2461203286: (r"regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]','')"
               r" rlike '^\\d{6,7}[A-Z]\\d{6}(MY\\d)?E?\\d?$'"),
  # FAPESP: NN/NNNNN-N shape is unique to it. NOTE: FAPESP ids are numeric-
  # with-structure (no letters) — the S3 candidate filter admits this chassis
  # explicitly alongside letter-bearing ids (2.8k FAPESP ids sat under CAPES).
  4320320997: r"norm rlike '(?<!\\d)\\d{2,4}/\\d{4,5}-\\d(?!\\d)'",
  # CAPES: process-number chassis (inert until the hash-id registry gets
  # citable refs — S3 requires a registry hit; future-proofing)
  4320321091: r"regexp_replace(norm,' ','') rlike '^(8888\\d|99999)\\.\\d{6}/\\d{4}-\\d{2}$'",
  # NCN: UMO chassis (inert until the internal-code registry gets citable
  # refs; slash-structured so the S3 letter filter admits it)
  4320322511: r"regexp_replace(norm,' ','') rlike '(UMO-?|DEC-?)?20\\d{2}/\\d{2}/[A-Z]{1,2}/[A-Z]{2,3}\\d{1,2}/\\d{5}'",
  # Shandong NSF: modern ZR-prefixed chassis is distinctive (inert until
  # the stub registry is extended)
  4320324174: r"regexp_replace(norm,'[ -]','') rlike 'ZR(19|20)\\d{2}[A-Z]{1,3}\\d{2,4}'",
  # AMED: JP-prefixed core is distinctive
  4320311405: r"norm rlike '^JP\\d{2}[A-Z]{2}\\d{7}$'",
  # NASA: modern + NNX-era forms are distinctive
  4320306101: (r"regexp_replace(norm,'[ -]','') rlike '^80NSSC\\d{2}[KM]\\d{4}$'"
               r" or regexp_replace(norm,'[ -]','') rlike '^NNX\\d{2}[A-Z]{2}\\d{2,3}[A-Z]$'"),
  # DOE: cross-funder claims require the literal DE prefix (self-identifying;
  # control: 12/12 sampled cross-hits were real DE- ids)
  4320306084: (r"regexp_replace(norm,'[ -]','') rlike '(?<![A-Z])DE(SC|EE|FE|AR|NE|NA|EM|OE|IA|PI|BI|CF|ET|SF|HS|DP|EW)\\d{7}'"
               r" or regexp_replace(norm,'[ -]','') rlike '(?<![A-Z])DE(FG|FC|AC|AI|GO|PS|EV|ER|AA)\\d{4}[A-Z]{1,2}\\d{4,6}'"),
  # FCT: slash-path grant refs (registry strings are long paths; exact-ish join)
  4320334779: r"norm rlike '^[A-Z0-9 ./-]+$' and norm rlike '[A-Z]' and norm rlike '/'",
  # EC: framework-token or CT-era forms only (bare 6/9-digit arms dropped)
  4320320300: (r"norm rlike '-CT-\\d{4}-'"
               r" or norm rlike '(FP[567]|H2020|HORIZON|MSCA|ERC|GA) ?N?°? ?-?\\d{6}'"),
  # NSERC: programme-code-prefixed modern forms only (classify.py used ^RGPIN)
  4320334593: r"norm rlike '^(RGPIN|RGPAS|RGPNS|DGECR|CRDPJ|SAPIN)[ -/]?\\d{4}[ -]?\\d{4,6}$'",
  # ANR: dd-TOKEN-dddd chassis (with or without ANR prefix)
  4320320883: r"regexp_replace(norm,' ','') rlike '(ANR-?)?\\d{2}-[A-Z0-9]{2,6}-\\d{4}'",
  # Wellcome: full nnnnnn/L/nn/L citable form only
  4320311904: r"norm rlike '^\\d{5,6}[/_ ][A-Z][/_ ]\\d{2}[/_ ][A-Z]$'",
  # EPSRC: EP/xxxxxxx/n council form only
  4320334627: r"regexp_replace(norm,' ','') rlike '^EP/[A-Z0-9]{6,7}/[0-9]$'",
  # CIHR: CIHR-specific program tokens only (NOT any 2-4 letter prefix)
  4320334506: r"norm rlike '^#? ?(950|MOP|PJT|FDN|FRN|CIHR)[- ]?\\d{4,6}([-_]\\d+)?$'",
  # AHA: yy + programme letters + serial
  4320306230: r"regexp_replace(norm,' ','') rlike '^\\d{2}[A-Z]{2,10}\\d{4,9}$'",
}

def summary_sql(name, f):
    norm_x = NORM.format(c="funder_award_id")
    return f"""
WITH xnorm AS (
  SELECT {norm_x} AS norm, COUNT(*) AS nrows
  FROM openalex.awards.openalex_awards_raw
  WHERE provenance IN {XREF} AND funder_id={f['fid']} AND funder_award_id IS NOT NULL
  GROUP BY 1
),
x AS (SELECT norm, nrows, {f['xkey']} AS xkey,
             CASE WHEN {f['gram']} THEN 1 ELSE 0 END AS g
      FROM xnorm),
rnorm AS (
  SELECT DISTINCT {norm_x} AS norm
  FROM openalex.awards.openalex_awards_raw
  WHERE priority>=3 AND funder_id={f['fid']} AND funder_award_id IS NOT NULL
),
r AS (SELECT DISTINCT {f['rkey']} AS rkey FROM rnorm WHERE {f['rkey']} IS NOT NULL)
SELECT '{name}' AS funder,
       (SELECT COUNT(*) FROM r) AS registry_keys,
       COUNT(*) AS xref_ids, SUM(nrows) AS xref_rows,
       SUM(m) AS matched, SUM((1-m)*g) AS grammar_pass, SUM((1-m)*(1-g)) AS fail,
       ROUND(100*SUM(m)/COUNT(*),1) AS match_pct,
       ROUND(100*SUM((1-m)*g)/COUNT(*),1) AS grammar_pct,
       ROUND(100*SUM((1-m)*(1-g))/COUNT(*),1) AS fail_pct,
       ROUND(100*SUM(m*nrows)/SUM(nrows),1) AS match_row_pct,
       ROUND(100*SUM((1-m)*(1-g)*nrows)/SUM(nrows),1) AS fail_row_pct
FROM (SELECT x.norm, x.g, x.nrows, CASE WHEN r.rkey IS NOT NULL THEN 1 ELSE 0 END AS m
      FROM x LEFT JOIN r ON x.xkey = r.rkey)
"""

if __name__ == "__main__":
    only = sys.argv[1:] or list(FUNDERS)
    for name in only:
        f = FUNDERS[name]
        sql = summary_sql(name, f)
        path = os.path.join(OUT, f"q_{name}.sql")
        open(path, "w").write(sql)
        p = subprocess.run([SQLQ, "-f", path], capture_output=True, text=True)
        out = p.stdout if p.returncode == 0 else "ERROR\n" + p.stderr + p.stdout
        print(f"=== {name}\n{out}")
        open(os.path.join(OUT, f"res_{name}.txt"), "w").write(out)


# Funder-scoped junk exemptions: shapes that are real ids AT THIS FUNDER but
# would be unsafe as global chassis (non-DOE r3 audit, 2026-08-04).
FUNDER_KEEPS = [
  (4320321181, r"^(?!H ?2020[.]?$)[A-Z]{1,3} ?[0-9]{1,5}[.]?$"),          # FWF short ids (T7, F45, Z49 — all resolvable at 10.55776/<id>)
  (4320321001, r"(?<![0-9A-Z])[WT][0-9]{7,10}(?![0-9])"),  # NSFC W-/T-series
  (4320321001, r"(?<![0-9])8[0-9]{10}(?![0-9])"),
  (4320334593, r"^(?!(19|20)[0-9]{2}[.]?$)[0-9]{4}[.]?$"),  # NSERC historic 4-digit serials (70% of native range; years excluded)
  (4320321181, r"^[1-3][0-9]{4}[.]?$"),  # FWF bare 5-digit project numbers (grader: 10000-39999 = unambiguous P-series, all resolve at 10.55776)    # NSFC 11-digit joint/major grants
]
