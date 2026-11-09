"""The works SDG head (oxjob #1300) for awards: the same weights file and the same math as notebooks/sdg/sdg_jev_head_score.py.

HEAD_FILE is read from notebooks/sdg/ (the file the works job loads), never copied: a new works head is picked up by a full
award re-score, and `model_version` in award_sdgs says which head scored a row. score = sigmoid(W.x + b) -> per-goal isotonic
(np.interp on iso_x / iso_y, clipped) -> round 4; a goal is served when score >= its threshold (0.4 on every goal in v2), highest
score first, ties in goal order, as {id, display_name, score} (the shape of works' sustainable_development_goals).
"""
import json
import os

import numpy as np

HEAD_FILE = "sdg_jev_head_v2.json"   # = sdg_jev_head_score.HEAD_FILE
HERE = os.path.dirname(os.path.abspath(__file__))
HEAD_DIR = os.path.abspath(os.path.join(HERE, "..", "..", "..", "sdg"))
HEAD_VOLUME_DIR = "/Volumes/openalex/works/models/sdg_jev"   # = the works notebook's fallback copy


def load(path=None):
    path = path or os.path.join(HEAD_DIR, HEAD_FILE)
    if not os.path.exists(path):
        path = os.path.join(HEAD_VOLUME_DIR, HEAD_FILE)
    h = json.load(open(path))
    W = np.asarray(h["W"], dtype=np.float32)
    B = np.asarray(h["b"], dtype=np.float32)
    assert W.shape == (17, h["input_dim"]), W.shape
    assert [g["id"] for g in h["goals"]] == [f"https://metadata.un.org/sdg/{k}" for k in range(1, 18)]
    return {"version": h["model_version"], "W": W, "B": B, "input_dim": h["input_dim"],
            "iso_x": [np.asarray(g["iso_x"], dtype=np.float32) for g in h["goals"]],
            "iso_y": [np.asarray(g["iso_y"], dtype=np.float32) for g in h["goals"]],
            "thr": [float(g["threshold"]) for g in h["goals"]], "ids": [g["id"] for g in h["goals"]],
            "names": [g["display_name"] for g in h["goals"]], "path": path}


def probs(head, X):
    """(n, 1024) embeddings -> (n, 17) calibrated scores, rounded to 4 places (= sdg_probs in the works notebook)."""
    X = np.asarray(X, dtype=np.float32)
    raw = 1.0 / (1.0 + np.exp(-(X @ head["W"].T + head["B"])))
    out = np.empty_like(raw)
    for j in range(17):
        out[:, j] = np.interp(raw[:, j], head["iso_x"][j], head["iso_y"][j])
    return np.round(out, 4)


def sdgs(head, p):
    """One row of 17 scores -> the served list (= SDGS_EXPR in the works notebook)."""
    hits = [(j, float(p[j])) for j in range(17) if p[j] >= head["thr"][j]]
    hits.sort(key=lambda x: (-x[1], x[0]))
    return [{"id": head["ids"][j], "display_name": head["names"][j], "score": s} for j, s in hits]


# Spark SQL twin of sdgs() for the notebook (probs / ids / names / thr are columns there).
SDGS_EXPR = """
transform(
  array_sort(
    transform(filter(sequence(0, 16), i -> probs[i] >= thr[i]),
              i -> named_struct('g', i, 'id', ids[i], 'display_name', names[i], 'score', probs[i])),
    (l, r) -> CASE WHEN l.score > r.score THEN -1 WHEN l.score < r.score THEN 1
                   WHEN l.g < r.g THEN -1 WHEN l.g > r.g THEN 1 ELSE 0 END),
  s -> named_struct('id', s.id, 'display_name', s.display_name, 'score', s.score))
"""
