"""The frozen chooser as plain JSON + numpy, so the walden nightly doesn't need the scikit-learn / numpy versions
that pickled it (#1363 froze it with scikit-learn 1.9.1, numpy 2.5; Databricks runtimes ship older ones).

    export (desk, where the pickle loads):  json.dump(to_json(pickle.load(open("chooser_jev.pkl", "rb"))), f)
    load (anywhere):                        d = load_decider("chooser_jev.json", features)   # a decider.Decider

A binary HistGradientBoostingClassifier predicts sigmoid(baseline + sum of tree leaf values); a numerical split sends
x <= threshold left and NaN to the side recorded at fit time. Categorical splits are refused on export.
"""
import json

import numpy as np

from .decider import Decider


def to_json(frozen):
    """frozen = the dict #1363 chooser.py --save pickles ({"clf": HistGradientBoostingClassifier, "t", "no_p", ...})."""
    clf = frozen["clf"]
    assert list(clf.classes_) == [False, True] or list(clf.classes_) == [0, 1], clf.classes_
    trees = []
    for (pred,) in clf._predictors:  # one tree per iteration for binary classification
        n = pred.nodes
        if n["is_categorical"].any():
            raise ValueError("categorical split: not supported by this exporter")
        trees.append({k: n[f].tolist() for k, f in (("value", "value"), ("feature", "feature_idx"),
                                                   ("threshold", "num_threshold"), ("missing_left", "missing_go_to_left"),
                                                   ("left", "left"), ("right", "right"), ("is_leaf", "is_leaf"))})
    meta = {k: v for k, v in frozen.items() if k != "clf"}
    meta["pool"] = [list(p) for p in meta.get("pool", [])]
    return {"format": "hgb-binary-v1", "baseline": float(np.ravel(clf._baseline_prediction)[0]), "trees": trees,
            "n_features": int(clf.n_features_in_), **meta}


class GBT:
    def __init__(self, d):
        assert d["format"] == "hgb-binary-v1", d.get("format")
        self.baseline = d["baseline"]
        self.n_features = d["n_features"]
        self.trees = [{k: np.asarray(v) for k, v in t.items()} for t in d["trees"]]

    def raw(self, X):
        X = np.asarray(X, dtype=float)
        assert X.ndim == 2 and X.shape[1] == self.n_features, X.shape
        out = np.full(X.shape[0], self.baseline)
        rows = np.arange(X.shape[0])
        for t in self.trees:
            node = np.zeros(X.shape[0], dtype=np.int64)
            active = ~t["is_leaf"][node].astype(bool)
            while active.any():
                r, nd = rows[active], node[active]
                x = X[r, t["feature"][nd]]
                go_left = np.where(np.isnan(x), t["missing_left"][nd].astype(bool), x <= t["threshold"][nd])
                node[active] = np.where(go_left, t["left"][nd], t["right"][nd])
                active = ~t["is_leaf"][node].astype(bool)
            out += t["value"][node]
        return out

    def predict_proba(self, X):
        p = 1.0 / (1.0 + np.exp(-self.raw(X)))
        return np.column_stack([1.0 - p, p])


def load_decider(path, features):
    """A decider.Decider backed by the JSON export (same decide() as the pickled one)."""
    d = json.load(open(path))
    dec = Decider.__new__(Decider)
    dec.m = {"clf": GBT(d), "t": d["t"], "no_p": d["no_p"], "decider": d.get("decider"), "features": d.get("features")}
    dec.F = features
    return dec
