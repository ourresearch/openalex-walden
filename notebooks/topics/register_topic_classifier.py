# Databricks notebook source
# MAGIC %pip install transformers==4.50.2 accelerate==1.5.2
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import shutil

import mlflow
import pandas as pd
import torch

VOLUME_DIR = "/Volumes/openalex/works/models/topic-classification-title-abstract"
MODEL_DIR = "/local_disk0/topic_classifier_model"
UC_MODEL = "openalex.works.topic_classifier"
TOP_K = 3
BATCH_SIZE = 25

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

os.makedirs(MODEL_DIR, exist_ok=True)
for name in os.listdir(VOLUME_DIR):
    shutil.copyfile(os.path.join(VOLUME_DIR, name), os.path.join(MODEL_DIR, name))
print(sorted(os.listdir(MODEL_DIR)))

# COMMAND ----------


class TopicClassifier(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        import torch
        from transformers import pipeline

        self.pipe = pipeline(
            task="text-classification",
            model=context.artifacts["model_dir"],
            device=0 if torch.cuda.is_available() else -1,
            top_k=TOP_K,
            truncation=True,
            max_length=512,
            batch_size=BATCH_SIZE,
        )

    def predict(self, context, model_input):
        outputs = self.pipe(model_input["text"].tolist())
        topics = [
            [
                {"topic_id": 10000 + int(t["label"].split(":")[0]), "score": float(t["score"])}
                for t in row
            ]
            for row in outputs
        ]
        return pd.DataFrame({"topics": topics})


# COMMAND ----------

example = pd.DataFrame(
    {
        "text": [
            "[CLS]<TITLE> Deep learning for protein structure prediction "
            "<ABSTRACT> We present a neural approach to folding. [SEP]"
        ]
    }
)

signature = mlflow.models.infer_signature(
    example,
    pd.DataFrame({"topics": [[{"topic_id": 10001, "score": 0.9}]]}),
)

with mlflow.start_run(run_name="topic-classifier"):
    info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=TopicClassifier(),
        artifacts={"model_dir": MODEL_DIR},
        signature=signature,
        input_example=example,
        pip_requirements=[
            f"mlflow=={mlflow.__version__}",
            f"torch=={torch.__version__.split('+')[0]}",
            "transformers==4.50.2",
            "accelerate==1.5.2",
        ],
        registered_model_name=UC_MODEL,
    )

print("registered version:", info.registered_model_version)

# COMMAND ----------

loaded = mlflow.pyfunc.load_model(info.model_uri)
print(loaded.predict(example))
