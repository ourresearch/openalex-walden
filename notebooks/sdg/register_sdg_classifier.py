# Databricks notebook source
# MAGIC %pip install transformers==4.50.2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
import tensorflow as tf
import transformers

VOLUME_SAVED_MODEL = "dbfs:/Volumes/openalex/works/models/sdg/saved_model"
VOLUME_TOKENIZER = "dbfs:/Volumes/openalex/works/models/sdg/tokenizer"
LOCAL_SAVED_MODEL = "/local_disk0/sdg/saved_model"
LOCAL_TOKENIZER = "/local_disk0/sdg/tokenizer"
UC_MODEL = "openalex.works.sdg_classifier"

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

dbutils.fs.cp(VOLUME_SAVED_MODEL, f"file:{LOCAL_SAVED_MODEL}", recurse=True)
dbutils.fs.cp(VOLUME_TOKENIZER, f"file:{LOCAL_TOKENIZER}", recurse=True)

import os

print(sorted(os.listdir(LOCAL_SAVED_MODEL)), sorted(os.listdir(LOCAL_TOKENIZER)))

# COMMAND ----------


class SDGClassifier(mlflow.pyfunc.PythonModel):
    SCORE_THRESHOLD = 0.4
    TOP_K = 3
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
        import tensorflow as tf
        from transformers import BertTokenizerFast

        self.model = tf.saved_model.load(context.artifacts["saved_model"])
        self.predict_fn = self.model.signatures["serving_default"]
        self.tokenizer = BertTokenizerFast.from_pretrained(context.artifacts["tokenizer"])

    def predict(self, context, model_input):
        texts = [
            f"{(t or '').strip()}\n{(a or '').strip()}"
            for t, a in zip(model_input["title"], model_input["abstract"])
        ]
        valid = [t.strip().lower() if t and t.strip() else "" for t in texts]
        if not any(valid):
            return pd.DataFrame({"sdg": [[] for _ in texts]})

        enc = self.tokenizer(
            valid,
            truncation=True,
            padding="max_length",
            max_length=512,
            return_tensors="tf",
        )
        out = self.predict_fn(
            input_ids=enc["input_ids"], attention_masks=enc["attention_mask"]
        )
        logits_batch = out["target_layer"].numpy()

        results = []
        for logits in logits_batch:
            sdgs = [
                {
                    "id": f"https://metadata.un.org/sdg/{i + 1}",
                    "display_name": self.GOAL_NAMES[i + 1],
                    "score": float(score),
                }
                for i, score in enumerate(logits)
            ]
            sdgs.sort(key=lambda x: x["score"], reverse=True)
            results.append([s for s in sdgs if s["score"] > self.SCORE_THRESHOLD][: self.TOP_K])
        return pd.DataFrame({"sdg": results})


# COMMAND ----------

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

with mlflow.start_run(run_name="sdg-classifier"):
    info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=SDGClassifier(),
        artifacts={"saved_model": LOCAL_SAVED_MODEL, "tokenizer": LOCAL_TOKENIZER},
        signature=signature,
        input_example=example,
        pip_requirements=[
            f"mlflow=={mlflow.__version__}",
            f"tensorflow=={tf.__version__}",
            f"transformers=={transformers.__version__}",
        ],
        registered_model_name=UC_MODEL,
    )

print("registered version:", info.registered_model_version)

# COMMAND ----------

loaded = mlflow.pyfunc.load_model(info.model_uri)
print(loaded.predict(example))
print(loaded.predict(pd.DataFrame({"title": [None], "abstract": [None]})))
