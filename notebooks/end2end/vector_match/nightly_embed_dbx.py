"""#1348 production embed task (2026-09-25): one night's seats -> two 256-d int8 vectors in the store's schema, on a Databricks GPU node
(replaces the Modal step so the nightly lives in one pipeline; modal_nightly.py stays as the backup). Reads oxjob1348_daily_<d8> (cards
from nightly_daily_parquet.py), encodes card(r, "base") with seat-base-v2 (Volume oxjob1342) and stock multilingual-e5-base on every GPU,
projects with #1342's PCA bases (Volume oxjob1348/pca, copies of s3://openalex-sandbox/oxjob1342/pca), quantizes (s = max|x|/127), and writes
parquet to s3://openalex-sandbox/oxjob1342/seat_emb_daily/<d8>/ (columns work_id, author_sequence, author_id, ft_q, ft_s, st_q, st_s).
    argv: <home> --date 2026-09-25 [--out-prefix s3://openalex-sandbox/oxjob1342/seat_emb_daily] [--bs 256]"""
import os
os.environ.setdefault("USE_TF", "0"); os.environ.setdefault("TRANSFORMERS_NO_TF", "1"); os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
import sys, time, json, glob, shutil, numpy as np
HOME = sys.argv[1]
try: _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError: _HERE = HOME  # spark_python_task runs the file without __file__ (run 107981729893761)
sys.path.insert(0, HOME); sys.path.insert(0, _HERE)  # seat_text.py beside this file (walden) or on the Volume (tests)
def arg(k, d): return next((sys.argv[i + 1] for i, x in enumerate(sys.argv) if x == k), d)
DATE = arg("--date", None); D8 = DATE.replace("-", ""); OUT_PREFIX = arg("--out-prefix", "s3://openalex-sandbox/oxjob1342/seat_emb_daily"); BS = int(arg("--bs", 256))
A = "openalex.authors"; DAILY = arg("--cards-table", f"{A}.oxjob1348_daily_{D8}"); DAILY_T = arg("--daily-table", f"{A}.seat_embeddings_daily"); DAILY_T = None if DAILY_T == "none" else DAILY_T; DIM = 256; VERSION = "seat-base-v2+e5-base/pca256-int8/2026-09-24"
MODELS = {"base-v2": "/Volumes/openalex/authors/oxjob_tmp/oxjob1342/models/seat-base-v2", "stock": "intfloat/multilingual-e5-base"}
COLS = ["work_id", "author_sequence", "author_id", "raw_name", "aff_strings", "inst_names", "subfield", "venue", "publication_year", "coauthors", "title"]
def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)
def quant(X):
    s = (np.abs(X).max(1) / 127.0).clip(1e-8).astype(np.float32); return np.round(X / s[:, None]).astype(np.int8), s
def main():
    import torch, pyarrow as pa, pyarrow.parquet as pq
    from sentence_transformers import SentenceTransformer
    from seat_text import card
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate(); t0 = time.time(); local = f"/tmp/daily_{D8}"
    spark.table(DAILY).select(*COLS).write.mode("overwrite").parquet(f"file:{local}"); log(f"daily dumped ({time.time()-t0:.0f}s)")
    T = pa.concat_tables([pq.read_table(f) for f in sorted(glob.glob(f"{local}/*.parquet"))]); rows = T.to_pylist(); n = len(rows)
    texts = ["query: " + card(r, "base") for r in rows]; log(f"{n} cards")
    ngpu = torch.cuda.device_count(); result = {}
    for tag, path in MODELS.items():
        t1 = time.time(); m = SentenceTransformer(path, device="cuda"); m.max_seq_length = 128; m.half()
        if ngpu > 1:
            pool = m.start_multi_process_pool(target_devices=[f"cuda:{i}" for i in range(ngpu)]); E = m.encode_multi_process(texts, pool, batch_size=BS, normalize_embeddings=True); m.stop_multi_process_pool(pool)
        else: E = m.encode(texts, batch_size=BS, normalize_embeddings=True, show_progress_bar=False, convert_to_numpy=True)
        E = np.asarray(E, dtype=np.float32); z = np.load(f"{HOME}/pca/{tag}.npz"); Z = (E - z["mu"]) @ z["V"].T; Z /= np.linalg.norm(Z, axis=1, keepdims=True).clip(1e-6)
        result[tag] = quant(Z); del m, E, Z; torch.cuda.empty_cache(); log(f"{tag}: {n} seats on {ngpu} GPUs in {time.time()-t1:.0f}s ({n/(time.time()-t1):.0f}/s)")
    ftq, fts = result["base-v2"]; stq, sts = result["stock"]
    tbl = pa.table({"work_id": pa.array([r["work_id"] for r in rows], pa.int64()), "author_sequence": pa.array([r["author_sequence"] for r in rows], pa.int32()), "author_id": pa.array([r["author_id"] for r in rows], pa.int64()),
                    "ft_q": pa.array([q.tobytes() for q in ftq], pa.binary()), "ft_s": pa.array(fts, pa.float32()), "st_q": pa.array([q.tobytes() for q in stq], pa.binary()), "st_s": pa.array(sts, pa.float32())},
                   metadata={b"version": VERSION.encode(), b"source": DAILY.encode()})
    outl = f"/tmp/emb_{D8}"; shutil.rmtree(outl, ignore_errors=True); os.makedirs(outl); pq.write_table(tbl, f"{outl}/part-0.parquet", compression="zstd")
    df = spark.read.parquet(f"file:{outl}"); df.write.mode("overwrite").parquet(f"{OUT_PREFIX}/{D8}/"); log(f"written {OUT_PREFIX}/{D8}/ ({n} rows, {time.time()-t0:.0f}s total)")
    if DAILY_T:  # the table the pool reads (via seat_embeddings_live); re-runs replace the night
        from pyspark.sql import functions as F
        spark.sql(f"DELETE FROM {DAILY_T} WHERE run_date = DATE'{DATE}'")
        df.select(F.lit(DATE).cast("date").alias("run_date"), F.col("work_id").cast("bigint"), F.col("author_sequence").cast("int"), "ft_q", "ft_s", "st_q", "st_s").write.mode("append").saveAsTable(DAILY_T)
        log(f"appended to {DAILY_T}")
    log("EMBED DONE", DATE)
if __name__ == "__main__":  # REQUIRED for encode_multi_process (workers re-import this module)
    main()
