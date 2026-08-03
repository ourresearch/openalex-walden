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
NORM = (r"regexp_replace(regexp_replace(regexp_replace(UPPER(TRIM({c})),"
        r" '[\\u2010-\\u2015\\u2212\\uFE58\\uFE63\\uFF0D]', '-'),"
        r" '[\\u00A0\\u1680\\u2000-\\u200B\\u202F\\u205F\\u3000]', ' '),"
        r" '  +', ' ')")

def ex(pattern, src="norm", grp=1):
    return f"NULLIF(regexp_extract({src}, '{pattern}', {grp}), '')"

FUNDERS = {
  "NSFC": dict(fid=4320321001,
    rkey=ex(r"^(\\d{8})$"),
    xkey=ex(r"(?<!\\d)(\\d{8})(?!\\d)"),
    gram=r"norm rlike '(?<!\\d)\\d{8}(?!\\d)'"),
  "NIH": dict(fid=4320332161,
    rkey=ex(r"([A-Z]{2}\\d{6})"),
    xkey=f"CASE WHEN {ex(r'([A-Z]{2}) ?-?(\\d{5,6})(?!\\d)')} IS NOT NULL THEN "
         f"CONCAT(regexp_extract(norm,'([A-Z]{{2}}) ?-?(\\\\d{{5,6}})(?!\\\\d)',1),"
         f" LPAD(regexp_extract(norm,'([A-Z]{{2}}) ?-?(\\\\d{{5,6}})(?!\\\\d)',2),6,'0')) END",
    gram=r"norm rlike '[A-Z]\\d{2} ?-?[A-Z]{2} ?-?\\d{5,6}' or norm rlike '^[A-Z]{2} ?-?\\d{5,6}'"),
  "NSF": dict(fid=4320306076,
    rkey=ex(r"^(\\d{7})$"),
    xkey=ex(r"(?<!\\d)(\\d{7})(?!\\d)"),
    gram=r"norm rlike '^([A-Z]{2,5}[ -]?)?\\d{7}$'"),
  "KAKEN": dict(fid=4320334764,
    rkey=ex(r"^(\\d{2}[A-Z]\\d{5}|\\d{8})$"),
    xkey=ex(r"^(?:KAKENHI|JP|NO\\.?|GRANT)?[ -]*(\\d{2}[A-Z]\\d{5}|\\d{8})$"),
    gram=r"norm rlike '^(KAKENHI|JP|NO\\.?|GRANT)?[ -]*(\\d{2}[A-Z]\\d{5}|\\d{8})$'"),
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
         r" rlike '^\\d{6,7}[A-Z]\\d{6}(MY\\d)?E?\\d?$'"),
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
         r"norm rlike '^[A-Z]{3,7}[ -]?\\d{4,6}([ -]?\\d{2,4})?$' or norm rlike '^\\d{5,6}([ -]?\\d{2,4})?$'"),
  "ANR": dict(fid=4320320883,
    rkey=f"CASE WHEN {ex(r'^ANR-(\\d{2})-([A-Z0-9]{2,6})-(\\d{4})')} IS NOT NULL THEN "
         f"CONCAT(regexp_extract(norm,'^ANR-(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',1),'-',"
         f"regexp_extract(norm,'^ANR-(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',2),'-',"
         f"regexp_extract(norm,'^ANR-(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',3)) END",
    xkey=f"CASE WHEN NULLIF(regexp_extract(regexp_replace(norm,' ',''),'(?:ANR-?)?(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',1),'') IS NOT NULL THEN "
         f"CONCAT(regexp_extract(regexp_replace(norm,' ',''),'(?:ANR-?)?(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',1),'-',"
         f"regexp_extract(regexp_replace(norm,' ',''),'(?:ANR-?)?(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',2),'-',"
         f"regexp_extract(regexp_replace(norm,' ',''),'(?:ANR-?)?(\\\\d{{2}})-([A-Z0-9]{{2,6}})-(\\\\d{{4}})',3)) END",
    gram=r"regexp_replace(norm,' ','') rlike '(ANR-?)?\\d{2}-[A-Z0-9]{2,6}-\\d{4}'"),
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
  "EPSRC": dict(fid=4320334627,
    rkey="NULLIF(regexp_replace(norm,' ',''),'')",
    xkey="NULLIF(regexp_replace(norm,' ',''),'')",
    gram=r"regexp_replace(norm,' ','') rlike '^EP/[A-Z0-9]{6,7}/[0-9]$' or norm rlike '^\\d{7}$'"),
  "NSTC_TW": dict(fid=2461203286,
    # 2026-08-03: Taiwan NSTC = MOST's 2022 rename (the "mystery funder" — in
    # mid.funder but absent from common.funders, curation gap flagged). Same
    # grammar as MOST_TW; registry = grb_nstc_projects.
    rkey="NULLIF(regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]',''),'')",
    xkey="NULLIF(regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]',''),'')",
    gram=r"regexp_replace(regexp_replace(norm,'^(MOST|NSC|NSTC)[ -]*',''),'[ -]','')"
         r" rlike '^\\d{6,7}[A-Z]\\d{6}(MY\\d)?E?\\d?$'"),
  "CIHR": dict(fid=4320334506,
    # derived 2026-08-03 (worklist #16): registry = NNNNNN_1 (application no. +
    # installment); deposits = program prefixes (MOP/PJT/FDN...), '950-' funding
    # reference wrapper, '#' decorations. Match key = application number as int.
    rkey="CAST(CAST(" + ex(r"^(\\d{4,6})_\\d+$") + " AS BIGINT) AS STRING)",
    xkey="CAST(CAST(NULLIF(regexp_extract(regexp_replace(regexp_replace(norm,'^[#]+ ?',''),"
         "'^(950[- ]|[A-Z]{2,4}[- ]?)',''),'^(\\\\d{4,6})([-_]\\\\d+)?$',1),'') AS BIGINT) AS STRING)",
    gram=r"norm rlike '^#? ?(950[- ])?([A-Z]{2,4}[- ]?)?\\d{4,6}([-_]\\d+)?$'"),
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
         r"'^(8888\\d\\.\\d{6}/\\d{4}-\\d{2})$', 1), '')",
    xkey=r"NULLIF(regexp_extract(regexp_replace(norm,' ',''),"
         r"'(8888\\d\\.\\d{6}/\\d{4}-\\d{2})', 1), '')",
    gram=r"(regexp_replace(norm,' ','') rlike '^8888\\d\\.\\d{6}/\\d{4}-\\d{2}$'"
         r" or regexp_replace(norm,' ','') rlike '^(BEX|PDSE|PNPD|PROEX|DS|AUX)-?\\d{3,7}([-/.]\\d{1,4}){0,3}$'"
         r" or norm rlike '^\\d{1,4}/(19|20)\\d{2}$')"),
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
}

XREF = "('crossref_work_funders','crossref_work.grants','crossref_work')"

# ---- Salvage-chain constants (#690 guard; classification ported from
# ---- 690-audit/classify.py — keep in lockstep) ----
# Generic decorations the sharp deposited-side keys don't strip, applied to the
# normalized (upper/trim/unicode-cleaned) form. Leading words require >=1
# separator so prefixes of real tokens ("NOVO...") survive; a corrupted strip
# is harmless anyway because rescue still requires a registry hit.
DECOR_LEAD = (r"^((GRANT|GRANTS|AWARD|AWARDS|PROJECT|CONTRACT|AGREEMENT|APPLICATION|APP"
              r"|REFERENCE|REF|NUMBER|NUM|NO|N0|ID|CODE|FUNDREF|UNDER|JSPS|KAKENHI|MEXT)"
              r"[ .:#°-]+|[#№(\\[]+ ?)+")
DECOR_TRAIL = r"([ .,;:)\\]]+|[ -]*\\(.*\\))$"
# multi-id concat detection + split (mirrors classify.py CLS 'multi_id' arm)
MULTI_DETECT = ("((_n rlike '[,;&]') OR (_n rlike ' AND ') OR (_n rlike ' \\\\+ '))"
                " AND _n rlike '[0-9]{3}'")
MULTI_SPLIT = r"[,;&+]|\\bAND\\b"

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
  4320321091: r"regexp_replace(norm,' ','') rlike '^8888\\d\\.\\d{6}/\\d{4}-\\d{2}$'",
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
