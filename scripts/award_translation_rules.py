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
}

XREF = "('crossref_work_funders','crossref_work.grants','crossref_work')"

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
