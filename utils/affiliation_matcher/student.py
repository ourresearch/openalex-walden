# Vendored from oxjobs #1363 work/student_xenc.py (sha256 75908651…, FROZEN_DECIDER.md) for the walden nightly
# (oxjob #1386): the cross-encoder student and its predict(), unchanged. Only additions: load() and p_for().
# Contract: p = student on (s[:1500], Index().card(i)), longest_first truncation at maxlen 192, bf16 autocast on
# CUDA, sigmoid; the chooser was trained on p rounded to 4 decimals (#1363 infer writes round(p, 4)).
import json
import os
import time

os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
import torch  # noqa: E402
import torch.nn as nn  # noqa: E402
from transformers import AutoModel, AutoTokenizer  # noqa: E402


class XEnc(nn.Module):
    def __init__(self, base):
        super().__init__()
        self.enc = AutoModel.from_pretrained(base, torch_dtype=torch.float32, add_pooling_layer=False)  # unused pooler breaks DDP
        self.drop = nn.Dropout(0.1)
        self.head = nn.Linear(self.enc.config.hidden_size, 1)

    def forward(self, ids, mask):
        h = self.enc(input_ids=ids, attention_mask=mask).last_hidden_state
        m = mask.unsqueeze(-1).to(h.dtype)
        return self.head(self.drop((h * m).sum(1) / m.sum(1).clamp_min(1.0))).squeeze(-1)


def enc(tok, batch, maxlen):
    return tok([r["s"] for r in batch], [r["c"] for r in batch], padding=True, truncation="longest_first",
               max_length=maxlen, return_tensors="pt")


class _Batches(torch.utils.data.Dataset):
    """Length-sorted batches, tokenized in DataLoader workers (the GPU waits on the tokenizer otherwise)."""

    def __init__(self, tok, R, order, bs, maxlen):
        self.tok, self.R, self.maxlen = tok, R, maxlen
        self.b = [order[s:s + bs] for s in range(0, len(order), bs)]

    def __len__(self):
        return len(self.b)

    def __getitem__(self, j):
        return self.b[j], enc(self.tok, [self.R[i] for i in self.b[j]], self.maxlen)


@torch.no_grad()
def predict(m, tok, R, dev, maxlen, bs=512, log=None, workers=0):
    """Same batches and math with or without workers (workers only move tokenization off the main thread)."""
    m.eval()
    order = sorted(range(len(R)), key=lambda i: len(R[i]["s"]) + len(R[i]["c"]))
    out, t0 = [0.0] * len(R), time.time()
    D = _Batches(tok, R, order, bs, maxlen)
    it = torch.utils.data.DataLoader(D, batch_size=None, num_workers=workers, prefetch_factor=8 if workers else None,
                                     collate_fn=lambda x: x) if workers else (D[j] for j in range(len(D)))
    for n, (idx, e) in enumerate(it):
        s = n * bs
        with torch.autocast(device_type="cuda", dtype=torch.bfloat16, enabled=dev.type == "cuda"):
            z = m(e["input_ids"].to(dev), e["attention_mask"].to(dev))
        for j, p in zip(idx, torch.sigmoid(z.float()).cpu().tolist()):
            out[j] = p
        if log and (s // bs) % 200 == 0:
            log(f"  infer {s + len(idx):,}/{len(R):,} {time.time() - t0:.0f}s")
    return out


def load(model_dir, base_dir=None):
    """(model, tokenizer, device, maxlen) from a #1363 student dir (model.pt, student.json, tokenizer files).
    base_dir = the pinned base encoder (#1363 decider/v1/base_multilingual-e5-base), so no Hugging Face Hub call;
    model.pt overwrites every weight anyway, the base only supplies the architecture."""
    dev = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    meta = json.load(open(f"{model_dir}/student.json"))
    m = XEnc(base_dir if base_dir and os.path.isdir(base_dir) else meta["base"])
    m.load_state_dict(torch.load(f"{model_dir}/model.pt", map_location="cpu"))
    m.to(dev)
    tok = AutoTokenizer.from_pretrained(model_dir)
    return m, tok, dev, meta["maxlen"]


def p_for(student, ix, pairs, workers=8, log=None):
    """pairs: [(key, string, inst_id)] -> {(key, inst_id): p rounded to 4 decimals}."""
    m, tok, dev, maxlen = student
    R = [{"s": s[:1500], "c": ix.card(i)} for _, s, i in pairs]
    P = predict(m, tok, R, dev, maxlen, log=log, workers=workers)
    return {(k, i): round(p, 4) for (k, _, i), p in zip(pairs, P)}
