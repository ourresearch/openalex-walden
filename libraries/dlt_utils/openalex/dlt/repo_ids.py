"""Identifier extraction for the repo ingest streams (Repo.py + RepoBackfill.py).

Extracted from both notebooks (oxjob #881): the two copies had drifted -- RepoBackfill's was
missing the pmid/pmcid patterns entirely -- which is the same disease the type vocabulary had.
One copy, imported by both. RepoBackfill therefore GAINS pmid/pmcid extraction on its next run.

Carries the oxjob #880 P3 fix: DOIs carried in the OAI identifier itself (native_id
"doi:10....") are now read, namespace doi, relationship self. See the inline comment for why
this is not the rule that was reverted on 2025-04-23.
"""

import re

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

id_struct_type = StructType([
    StructField("id", StringType(), True),
    StructField("namespace", StringType(), True),
    StructField("relationship", StringType(), True)
])

def extract_ids(identifiers, native_id):
   try:
       if identifiers is None:
           return []
       if not isinstance(identifiers, list):
           identifiers = [identifiers]
       if native_id is None:
           native_id = ""
           
       patterns = {
           'arxiv': (r"https?://arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5}|[a-z\-]+/\d+)", 1),
           'arxiv_native': (r"oai:arXiv\.org:([^/\s]+/\d+|\d+\.\d+)", 1),
           'doi': (r"\b10\.\d{4,9}/\S+\b", 0),
           'issn': (r"\b\d{4}-\d{3}[0-9X]\b", 0),
           'hal': (r"\bhal-\d+\b", 0),
           'handle': (r"https?://hdl\.handle\.net/([^/\s]+/[^/\s]+)", 1),
           'pmid': (r"/pubmed/(\d+)", 1),
           'pmcid': (r"/pmc/articles/(PMC\d+)", 1)
       }
       
       results = []
       arxiv_id_from_native = None
       
       # extract arxiv ID from native_id and normalize it
       try:
           if isinstance(native_id, str):
               match = re.search(patterns['arxiv_native'][0], native_id)
               if match:
                   arxiv_id_from_native = match.group(1)
       except Exception:
           pass
       
       # process each identifier
       for identifier in identifiers:
           if not identifier or not isinstance(identifier, str):
               continue
               
           try:
               for namespace, (pattern, group) in patterns.items():
                   match = re.search(pattern, identifier)
                   if match:
                       try:
                           relationship = None
                           
                           if namespace.startswith('arxiv'):
                               id_value = "arXiv:" + match.group(group)  # prepend arXiv:
                               
                               # check if this is an arxiv ID and compare with native_id
                               if arxiv_id_from_native:
                                   if id_value == f"arXiv:{arxiv_id_from_native}" or f"oai:arXiv.org:{match.group(group)}" == native_id:
                                       relationship = 'self'
                           else:
                               id_value = match.group(group)
                           
                           results.append({
                               "id": id_value,
                               "namespace": namespace.split('_')[0],
                               "relationship": relationship
                           })
                           break
                       except Exception:
                           continue
           except Exception:
               continue
       
       # add native_id
       if native_id:
           results.append({
               "id": native_id,
               "namespace": "pmh",
               "relationship": "self"
           })

       # oxjob #880 P3: some OAI endpoints put the record's DOI IN the OAI identifier itself --
       # native_id like "doi:10.57451/lhd.a.cxsmap7_nustar.101329.1" (Open MIND is 5.99M of the
       # 6.43M cases). Nothing read it, so merge_key.doi stayed NULL and 6M records fell to the
       # title anchor, minting shells instead of attaching to their DataCite works.
       #
       # This READS an explicit doi: scheme the repository itself supplied. It is NOT the rule
       # removed 2025-04-23 ("causing matching issues with arxiv due to doi") -- that one
       # FABRICATED 10.48550/arxiv.* DOIs from oai:arXiv.org ids. Do not revert this from memory
       # of that revert.
       #
       # Anchored on the doi: prefix, never the bare DOI pattern: these same records carry a
       # CITED-REFERENCE DOI in their URL slot, and the unanchored pattern would attach ~6M
       # records to the wrong work (oxjob #880 evidence/q40: all 6,432,183 doi:-prefixed
       # native_ids parse as valid DOIs, zero false positives).
       # Skipped when dc:identifier already yielded a DOI -- 444,188 records carry both, and the
       # trailing dedup keys on relationship so it alone would not collapse the pair.
       if not any(r.get("namespace") == "doi" for r in results):
           try:
               m = re.match(r"^doi:(10\.\d{4,9}/\S+)$", native_id)
               if m:
                   results.append({
                       "id": m.group(1),
                       "namespace": "doi",
                       "relationship": "self"
                   })
           except Exception:
               pass
       
       # deduplicate
       seen = set()
       unique_results = []
       for r in results:
           try:
               key = (r['id'], r['namespace'], r['relationship'])
               if key not in seen:
                   seen.add(key)
                   unique_results.append(r)
           except Exception:
               continue
       
       return unique_results
       
   except Exception as e:
       print(f"Error in extract_ids: {str(e)}")
       return []

# plain (non-pandas) UDF, as both notebooks had it. @TODO from Repo.py stands: convert to a
# pandas UDF -- repo_parsed is slow and this runs per row.
extract_ids_udf = F.udf(extract_ids, ArrayType(id_struct_type))
