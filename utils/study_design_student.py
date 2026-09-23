"""Study-design student model (oxjob #1335): the encoder that reproduces Jev's study-design outputs.

Kept apart from utils/study_design.py so that module stays requests-only. torch / transformers are imported inside
the functions, so importing this module is free on a CPU driver. Training / evaluation code and the certification
tables live in oxjobs #1335 scratch (student_train.py, score_student.py, cascade.py); this file is inference only
and must produce the same outputs as scratch/student_infer.py for the same weights.
"""
from __future__ import annotations

import json
import os
from typing import Any

CHOICES = ["rct", "nonrandomized_trial", "secondary_analysis", "observational", "case_report", "systematic_review",
           "meta_analysis", "narrative_review", "editorial_letter", "protocol", "guideline", "other_primary_research", "unknown"]
NOULS = ["is_rct", "human_subjects"]
ABSTRACT_CHARS = 6000


def text_of(w: dict) -> str:
    """Same fields Jev sees (title, venue, abstract[:6000]); one string for the encoder. == oxjobs #1335 common.text_of."""
    parts = [w.get("title") or ""]
    if w.get("venue"):
        parts.append(w["venue"])
    if w.get("abstract"):
        parts.append(w["abstract"][:ABSTRACT_CHARS])
    return "\n".join(parts)


def build_model(base: str):
    import torch
    import torch.nn as nn
    from transformers import AutoModel

    class Student(nn.Module):
        def __init__(self, base, dropout=0.1):
            super().__init__()
            self.enc = AutoModel.from_pretrained(base, torch_dtype=torch.float32)
            d = self.enc.config.hidden_size
            self.drop = nn.Dropout(dropout)
            self.choice = nn.Linear(d, len(CHOICES))
            self.noul = nn.Linear(d, len(NOULS))

        def forward(self, ids, mask):
            h = self.enc(input_ids=ids, attention_mask=mask).last_hidden_state
            m = mask.unsqueeze(-1).to(h.dtype)
            pooled = (h * m).sum(1) / m.sum(1).clamp_min(1.0)
            return self.choice(self.drop(pooled)), self.noul(self.drop(pooled))

    return Student(base)


class StudentModel:
    """Loads <model_dir>/{student.json, model.pt, tokenizer files}; predict(works) -> list of (probs, is_rct, human)."""

    def __init__(self, model_dir: str, device: str | None = None, batch_size: int = 128):
        import torch
        from transformers import AutoTokenizer

        meta = json.load(open(os.path.join(model_dir, "student.json")))
        assert meta["choices"] == CHOICES and meta["nouls"] == NOULS, "student.json disagrees with this module"
        self.maxlen = int(meta["maxlen"])
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.bs = batch_size
        self.model = build_model(meta["base"])
        self.model.load_state_dict(torch.load(os.path.join(model_dir, "model.pt"), map_location="cpu"))
        self.model.to(self.device).eval()
        self.tok = AutoTokenizer.from_pretrained(model_dir)
        self.meta = meta

    def predict(self, works: list[dict]) -> list[tuple[dict, float, float]]:
        import torch

        out: list[Any] = [None] * len(works)
        order = sorted(range(len(works)), key=lambda i: len(text_of(works[i])))
        with torch.no_grad():
            for s in range(0, len(order), self.bs):
                idx = order[s:s + self.bs]
                enc = self.tok([text_of(works[i]) for i in idx], padding=True, truncation=True, max_length=self.maxlen,
                               return_tensors="pt")
                with torch.autocast(device_type="cuda", dtype=torch.bfloat16, enabled=self.device == "cuda"):
                    lc, ln = self.model(enc["input_ids"].to(self.device), enc["attention_mask"].to(self.device))
                pc = torch.softmax(lc.float(), -1).cpu().numpy()
                pn = torch.sigmoid(ln.float()).cpu().numpy()
                for j, i in enumerate(idx):
                    out[i] = ({c: round(float(p), 4) for c, p in zip(CHOICES, pc[j])}, round(float(pn[j][0]), 4),
                              round(float(pn[j][1]), 4))
        return out
