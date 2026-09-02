# Databricks notebook source
# Registers openalex.works.sdg_classifier from the Aurora mBERT SavedModel in
# /Volumes/openalex/works/models/sdg.
#
# v2: dynamic sequence length. The SavedModel's serving_default signature is fixed at
# [batch, 512], so v1 padded every row to 512 tokens although the median frontfill row is
# ~40 tokens. v2 reloads the Keras graph (tf_keras + transformers' TFBertMainLayer), wraps
# the bert -> pooler -> target_layer path in a tf.function with [None, None] inputs, and
# scores length-sorted sub-batches padded only to their own longest row. Padded positions are
# masked out of attention, so the pooler output is the same as at 512 up to float noise; the
# notebook proves that against the fixed signature before registering.
#
# register=auto registers only if parity passes; true/false force it.

dbutils.widgets.text("register", "auto")
dbutils.widgets.text("parity_rows", "1000")

# COMMAND ----------

import subprocess
import sys

import tensorflow as tf

tf_minor = ".".join(tf.__version__.split(".")[:2])
subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "-q",
     "transformers==4.50.2", f"tf-keras=={tf_minor}.*"]
)
dbutils.library.restartPython()

# COMMAND ----------

import os

os.environ["TF_USE_LEGACY_KERAS"] = "1"

import time

import mlflow
import numpy as np
import pandas as pd
import tensorflow as tf
import tf_keras
import transformers
from transformers import BertTokenizerFast, TFBertMainLayer

REGISTER = dbutils.widgets.get("register").strip().lower()
PARITY_ROWS = int(dbutils.widgets.get("parity_rows"))

VOLUME_SAVED_MODEL = "dbfs:/Volumes/openalex/works/models/sdg/saved_model"
VOLUME_TOKENIZER = "dbfs:/Volumes/openalex/works/models/sdg/tokenizer"
LOCAL_SAVED_MODEL = "/local_disk0/sdg/saved_model"
LOCAL_TOKENIZER = "/local_disk0/sdg/tokenizer"
UC_MODEL = "openalex.works.sdg_classifier"

mlflow.set_registry_uri("databricks-uc")
print("tensorflow", tf.__version__, "tf_keras", tf_keras.__version__,
      "transformers", transformers.__version__, "mlflow", mlflow.__version__)

# COMMAND ----------

dbutils.fs.cp(VOLUME_SAVED_MODEL, f"file:{LOCAL_SAVED_MODEL}", recurse=True)
dbutils.fs.cp(VOLUME_TOKENIZER, f"file:{LOCAL_TOKENIZER}", recurse=True)
print(sorted(os.listdir(LOCAL_SAVED_MODEL)), sorted(os.listdir(LOCAL_TOKENIZER)))

# COMMAND ----------


class SDGClassifier(mlflow.pyfunc.PythonModel):
    SCORE_THRESHOLD = 0.4
    TOP_K = 3
    MAX_LENGTH = 512
    BATCH_SIZE = 64
    PAD_TO_MULTIPLE = 8
    GOAL_NAMES = {
        1: "No poverty",
        2: "Zero hunger",
        3: "Good health and well-being",
        4: "Quality Education",
        5: "Gender equality",
        6: "Clean water and sanitation",
        7: "Affordable and clean energy",
        8: "Decent work and economic growth",
        9: "Industry, innovation and infrastructure",
        10: "Reduced inequalities",
        11: "Sustainable cities and communities",
        12: "Responsible consumption and production",
        13: "Climate action",
        14: "Life below water",
        15: "Life in Land",
        16: "Peace, Justice and strong institutions",
        17: "Partnerships for the goals",
    }

    def load_context(self, context):
        import os

        os.environ["TF_USE_LEGACY_KERAS"] = "1"
        import tensorflow as tf
        import tf_keras
        from transformers import BertTokenizerFast, TFBertMainLayer

        keras_model = tf_keras.models.load_model(
            context.artifacts["saved_model"],
            custom_objects={"TFBertMainLayer": TFBertMainLayer},
            compile=False,
        )
        bert = keras_model.get_layer("bert")
        head = keras_model.get_layer("target_layer")

        @tf.function(
            input_signature=[
                tf.TensorSpec([None, None], tf.int32),
                tf.TensorSpec([None, None], tf.int32),
            ]
        )
        def infer(input_ids, attention_mask):
            pooled = bert(
                input_ids=input_ids, attention_mask=attention_mask, training=False
            ).pooler_output
            return head(pooled)

        self.keras_model = keras_model
        self.infer = infer
        self.tokenizer = BertTokenizerFast.from_pretrained(context.artifacts["tokenizer"])
        self.pad_id = self.tokenizer.pad_token_id or 0

    @staticmethod
    def _texts(model_input):
        texts = [
            f"{(t or '').strip()}\n{(a or '').strip()}"
            for t, a in zip(model_input["title"], model_input["abstract"])
        ]
        return [t.strip().lower() if t and t.strip() else "" for t in texts]

    def _to_sdgs(self, scores):
        sdgs = [
            {
                "id": f"https://metadata.un.org/sdg/{i + 1}",
                "display_name": self.GOAL_NAMES[i + 1],
                "score": float(s),
            }
            for i, s in enumerate(scores)
        ]
        sdgs.sort(key=lambda x: x["score"], reverse=True)
        return [s for s in sdgs if s["score"] > self.SCORE_THRESHOLD][: self.TOP_K]

    def score_matrix(self, texts):
        """17 sigmoid scores per text, in input order; all-zero row for empty text."""
        import numpy as np
        import tensorflow as tf

        out = np.zeros((len(texts), len(self.GOAL_NAMES)), dtype=np.float32)
        idx = [i for i, t in enumerate(texts) if t]
        if not idx:
            return out
        enc = self.tokenizer(
            [texts[i] for i in idx],
            truncation=True,
            max_length=self.MAX_LENGTH,
            padding=False,
        )["input_ids"]
        lengths = [len(e) for e in enc]
        order = sorted(range(len(idx)), key=lambda k: lengths[k])
        for s in range(0, len(order), self.BATCH_SIZE):
            sub = order[s : s + self.BATCH_SIZE]
            longest = max(lengths[k] for k in sub)
            width = min(
                self.MAX_LENGTH,
                -(-longest // self.PAD_TO_MULTIPLE) * self.PAD_TO_MULTIPLE,
            )
            ids = np.full((len(sub), width), self.pad_id, dtype=np.int32)
            mask = np.zeros((len(sub), width), dtype=np.int32)
            for r, k in enumerate(sub):
                ids[r, : lengths[k]] = enc[k]
                mask[r, : lengths[k]] = 1
            scores = self.infer(tf.constant(ids), tf.constant(mask)).numpy()
            for r, k in enumerate(sub):
                out[idx[k]] = scores[r]
        return out

    def predict(self, context, model_input):
        import pandas as pd

        texts = self._texts(model_input)
        scores = self.score_matrix(texts)
        return pd.DataFrame(
            {"sdg": [self._to_sdgs(scores[i]) if t else [] for i, t in enumerate(texts)]}
        )


# COMMAND ----------


class _Ctx:
    artifacts = {"saved_model": LOCAL_SAVED_MODEL, "tokenizer": LOCAL_TOKENIZER}


t0 = time.time()
clf = SDGClassifier()
clf.load_context(_Ctx())
print(f"dynamic model loaded in {time.time() - t0:.1f}s")
clf.keras_model.summary()

# COMMAND ----------

# Parity vs the fixed-512 serving_default signature (what v1 served) on real frontfill rows.
fixed = tf.saved_model.load(LOCAL_SAVED_MODEL).signatures["serving_default"]


def fixed_scores(texts, batch=25):
    out = []
    for s in range(0, len(texts), batch):
        enc = clf.tokenizer(
            texts[s : s + batch], truncation=True, padding="max_length",
            max_length=512, return_tensors="tf",
        )
        out.append(
            fixed(input_ids=enc["input_ids"], attention_masks=enc["attention_mask"])
            ["target_layer"].numpy()
        )
    return np.concatenate(out)


sample = (
    spark.table("openalex.works.openalex_works_base")
    .select("id", "title", "abstract")
    .filter("id > 6600000000 AND title IS NOT NULL")
    .limit(PARITY_ROWS)
    .toPandas()
)
sample = sample.where(sample.notna(), None)
texts = clf._texts(sample)
tok_len = [len(e) for e in clf.tokenizer(texts, truncation=True, max_length=512)["input_ids"]]
print(f"parity rows: {len(sample):,}; token length p50 {int(np.median(tok_len))}, "
      f"p90 {int(np.percentile(tok_len, 90))}, max {max(tok_len)}")

t0 = time.time()
s_fixed = fixed_scores(texts)
t_fixed = time.time() - t0
t0 = time.time()
s_dyn = clf.score_matrix(texts)
t_dyn = time.time() - t0

max_diff = float(np.abs(s_fixed - s_dyn).max())
sets_fixed = [{s["id"] for s in clf._to_sdgs(r)} for r in s_fixed]
sets_dyn = [{s["id"] for s in clf._to_sdgs(r)} for r in s_dyn]
flips = sum(1 for a, b in zip(sets_fixed, sets_dyn) if a != b)
print(f"max |score diff| = {max_diff:.2e}; SDG-set flips = {flips}/{len(texts)}")
print(f"CPU rows/sec: fixed-512 {len(texts) / t_fixed:.1f} vs dynamic {len(texts) / t_dyn:.1f} "
      f"({t_fixed / t_dyn:.1f}x)")

parity_ok = max_diff < 1e-3 and flips <= len(texts) * 0.005
print("PARITY", "PASS" if parity_ok else "FAIL")

# COMMAND ----------

do_register = {"true": True, "false": False}.get(REGISTER, parity_ok)
if not do_register:
    dbutils.notebook.exit(f"not registered (register={REGISTER}, parity_ok={parity_ok})")

example = pd.DataFrame(
    {
        "title": ["Access to clean drinking water in rural communities"],
        "abstract": ["We assess sanitation infrastructure and waterborne disease outcomes."],
    }
)
signature = mlflow.models.infer_signature(
    example,
    pd.DataFrame(
        {"sdg": [[{"id": "https://metadata.un.org/sdg/6",
                   "display_name": "Clean water and sanitation",
                   "score": 0.9}]]}
    ),
)

with mlflow.start_run(run_name="sdg-classifier-v2-dynamic-length"):
    info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=SDGClassifier(),
        artifacts={"saved_model": LOCAL_SAVED_MODEL, "tokenizer": LOCAL_TOKENIZER},
        signature=signature,
        input_example=example,
        pip_requirements=[
            f"mlflow=={mlflow.__version__}",
            f"tensorflow=={tf.__version__}",
            f"tf-keras=={tf_keras.__version__}",
            f"transformers=={transformers.__version__}",
        ],
        registered_model_name=UC_MODEL,
    )
    mlflow.log_metrics({"parity_max_diff": max_diff, "parity_flips": flips,
                        "parity_rows": len(texts)})

print("registered version:", info.registered_model_version)

# COMMAND ----------

loaded = mlflow.pyfunc.load_model(info.model_uri)
print(loaded.predict(example))
print(loaded.predict(pd.DataFrame({"title": [None], "abstract": [None]})))
dbutils.notebook.exit(f"registered version: {info.registered_model_version}")
