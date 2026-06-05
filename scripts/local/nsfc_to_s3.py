#!/usr/bin/env python3
"""
NSFC (National Natural Science Foundation of China) to S3 Data Pipeline
========================================================================

Harvests NSFC completed-project (结题) grant records from the official NSFC
big-data portal and uploads to S3 for Databricks.

Data source: https://kd.nsfc.cn  (国家自然科学基金大数据知识管理服务门户)
             POST /api/baseQuery/completionQueryResultsData  (anonymous, no login/captcha)
Output:      s3://openalex-ingest/awards/nsfc/nsfc_projects.parquet

⚠️  GEO-CONSTRAINT (important for admin/refresh):
    kd.nsfc.cn is GFW/geo-restricted and is UNREACHABLE from US/EU infrastructure.
    This scraper MUST be run from a **China or Hong Kong IP** (e.g. an in-region
    VPN exit). It cannot be run from OpenAlex US infra. The harvested parquet is
    therefore produced by the contractor in-region and handed off for upload;
    admin only runs Step 2/3 (upload + Databricks notebook), not the harvest.

Method notes (reverse-engineered 2026-06-03):
  - Responses are DES-ECB encrypted (key "IFROMC86", PKCS7, base64). See decrypt().
  - Query gate: fuzzyKeyword (>=2 chars, TITLE search) is required; pageSize is
    server-clamped to 10; deep-offset ceiling = pageNum 100 (=1000 records/query).
    So broad terms are sliced by conclusionYear x subject-dept code (A-H) to stay
    under 1000, then paginated. Coverage is title-substring-sweep bounded (high,
    not provably 100%); the achieved unique-grant count is reported honestly.
  - Each list row carries the full record (no per-project detail fetch needed).

Output columns (mapped for CreateNSFCAwards.ipynb):
    project_id        -> internal NSFC id (dedup key)
    funder_award_id   -> ratify_no (批准号, the real grant number, cited in papers)
    display_name      -> title (项目名称)
    funder_scheme     -> project_type (面上项目 / 青年科学基金项目 / 重点项目 / ...)
    institution       -> dependUnit (依托单位)
    pi_name / given_name / family_name -> personInCharge (项目负责人), split
    amount            -> DOUBLE, CNY (source is 万元 / 10k RMB -> *10000)
    start_year, start_date
    conclusion_year
    description       -> keywords (关键词, ';'-joined) [no abstract on the list API]
    subject_code, subject_dept (申请代码; dept = code[0], A..H)

Usage:
    python nsfc_to_s3.py                 # full harvest + parquet + upload (needs CN/HK IP)
    python nsfc_to_s3.py --skip-harvest  # reuse rows.jsonl, just build parquet
    python nsfc_to_s3.py --skip-upload   # local only
    python nsfc_to_s3.py --limit 50      # smoke (cap harvested terms)
"""
import argparse, base64, json, os, re, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

import pandas as pd
from Crypto.Cipher import DES                      # pip install pycryptodome
from Crypto.Util.Padding import unpad

# ---- config ----
FUNDER_ID   = 4320321001                            # OpenAlex F4320321001
FUNDER_NAME = "National Natural Science Foundation of China"
FUNDER_ROR  = "https://ror.org/01h0zpd94"
FUNDER_DOI  = "10.13039/501100001809"
PROVENANCE  = "nsfc_kd"
S3_BUCKET   = "openalex-ingest"
S3_KEY      = "awards/nsfc/nsfc_projects.parquet"

URL = "https://kd.nsfc.cn/api/baseQuery/completionQueryResultsData"
HDR = {"Content-Type":"application/json","Origin":"https://kd.nsfc.cn",
       "Referer":"https://kd.nsfc.cn/","User-Agent":"Mozilla/5.0"}
DES_KEY = b"IFROMC86"
DELAY = 0.32
YEARS = [str(y) for y in range(1986, 2027)]
CODES = list("ABCDEFGH")

ULTRA = ("研究 机制 基于 分析 系统 方法 影响 作用 关系 特征 技术 设计 调控 性能 结构 模型 应用 及其 制备 合成 "
         "理论 实验 数值 优化 评估 预测 检测 诊断 治疗 功能 调节 表征 演化 形成 响应 识别 算法 仿真 控制 性质").split()
DOMAIN = ("肿瘤 癌症 细胞 基因 蛋白 神经 免疫 病毒 心脏 血管 糖尿 干细胞 信号 受体 代谢 炎症 凋亡 突变 表达 "
          "材料 纳米 催化 半导体 石墨烯 薄膜 合金 晶体 聚合物 陶瓷 复合材料 光电 电池 传感 激光 量子 超导 磁性 "
          "网络 图像 数据 机器学习 通信 信号处理 芯片 软件 气候 海洋 大气 地震 遥感 生态 环境 污染 土壤 水文 "
          "矿物 地质 地层 构造 沉积 季风 流域 植被 物种 群落 力学 流体 振动 燃烧 传热 能源 风能 太阳能 制造 机器人 "
          "数学 方程 几何 概率 统计 拓扑 代数 函数 微分 算子 流形 矩阵 收敛 随机 稳定性 谱 经济 管理 金融 决策 风险 "
          "供应链 创新 政策 区域 城市 企业 市场 绩效 植物 动物 微生物 生物多样 农业 作物 育种 昆虫 反应 分子 光谱 "
          "电化学 高分子 有机 无机 粒子 引力 光学 等离子 凝聚态 天体 宇宙").split()
SEED = ULTRA + DOMAIN

# compound Chinese surnames (>=2 chars) for name splitting
COMPOUND = {"欧阳","太史","端木","上官","司马","东方","独孤","南宫","万俟","闻人","夏侯","诸葛","尉迟","公羊",
            "赫连","澹台","皇甫","宗政","濮阳","公冶","太叔","申屠","公孙","慕容","仲孙","钟离","长孙","宇文",
            "司徒","鲜于","司空","闾丘","子车","亓官","司寇","巫马","公西","颛孙","壤驷","公良","漆雕","乐正",
            "宰父","谷梁","拓跋","夹谷","轩辕","令狐","段干","百里","呼延","东郭","南门","羊舌","微生","公户",
            "公玉","公仪","梁丘","公仲","公上","公门","公山","公坚","左丘","公伯","西门","南荣","东里","东宫"}

# ---- harvest (needs CN/HK IP) ----
import urllib.request
def _post(body, tries=6):
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    for a in range(tries):
        try:
            raw = urllib.request.urlopen(urllib.request.Request(URL, data=data, headers=HDR), timeout=45).read()
            s = raw.decode("ascii","replace").strip()
            if s.startswith("{"):
                return {"_err": json.loads(s).get("message","")}
            return json.loads(unpad(DES.new(DES_KEY, DES.MODE_ECB).decrypt(base64.b64decode(s)), 8).decode("utf-8","replace"))
        except Exception as e:
            if a == tries-1: return {"_exc": str(e)}
            time.sleep(min(60, 2**a))
    return {"_exc":"unreachable"}

def _body(term, pn=0, **ex):
    b = {"code":"","fuzzyKeyword":term,"complete":True,"isFuzzySearch":True,"conclusionYear":"",
         "dependUnit":"","keywords":"","pageNum":pn,"pageSize":10,"personInCharge":"","projectName":"",
         "projectType":"","subPType":"","psPType":"","ratifyNo":"","ratifyYear":""}
    b.update(ex); return b

def _total(term, **ex):
    r = _post(_body(term, 0, **ex))
    return -1 if ("_err" in r or "_exc" in r) else (r.get("data") or {}).get("itotalRecords", 0)

def harvest(rows_path: Path, limit=None):
    seen = set()
    if rows_path.exists():
        for l in open(rows_path, encoding="utf-8"):
            try: seen.add(json.loads(l)[0])
            except Exception: pass
    queue = list(SEED); done = set(); f = open(rows_path, "a", encoding="utf-8")
    def save(rows):
        n = 0
        for r in rows:
            if r[0] in seen: continue
            seen.add(r[0]); n += 1; f.write(json.dumps(r, ensure_ascii=False)+"\n")
            if len(r) > 8 and r[8]:
                for kw in re.split(r"[;；,，、]", r[8]):
                    kw = kw.strip()
                    if 2 <= len(kw) <= 8 and kw not in done and kw not in queue:
                        queue.append(kw)
        f.flush(); return n
    def paginate(term, **ex):
        t = _total(term, **ex)
        if t <= 0: return
        for pn in range(min(100, (t+9)//10)):
            rows = (_post(_body(term, pn, **ex)).get("data") or {}).get("resultsData") or []
            if not rows: break
            save(rows); time.sleep(DELAY)
    i = 0
    while queue and (limit is None or len(done) < limit):
        term = queue.pop(0)
        if term in done: continue
        done.add(term); t = _total(term)
        if t <= 0: pass
        elif t <= 1000: paginate(term)
        else:
            for y in YEARS:
                ty = _total(term, conclusionYear=y)
                if ty <= 0: continue
                if ty <= 1000: paginate(term, conclusionYear=y)
                else:
                    for c in CODES: paginate(term, conclusionYear=y, code=c)
        i += 1
        if i % 5 == 0: print(f"  [{i}] terms done={len(done)} queue={len(queue)} uniq_rows={len(seen)}", flush=True)
    f.close(); print(f"  harvest done: {len(seen):,} unique rows across {len(done)} terms")

# ---- name split ----
def split_name(name):
    name = (name or "").strip()
    if not name: return (None, None)
    if re.search(r"[A-Za-z]", name):                 # foreign/latinized -> split on space
        parts = name.split()
        return (" ".join(parts[:-1]) or None, parts[-1]) if len(parts) > 1 else (None, name)
    if len(name) >= 3 and name[:2] in COMPOUND:      # compound surname
        return (name[2:], name[:2])
    if len(name) >= 2:                               # single-char surname (common case)
        return (name[1:], name[:1])
    return (None, name)

# ---- transform rows.jsonl -> parquet ----
def process(rows_path: Path, out_dir: Path):
    print(f"\n{'='*60}\nStep 2: Processing -> parquet\n{'='*60}")
    seen = {}; n_raw = 0
    for l in open(rows_path, encoding="utf-8"):
        try: r = json.loads(l)
        except Exception: continue
        n_raw += 1
        seen[r[0]] = r                                # dedup by internal id, keep last
    print(f"  read {n_raw:,} rows -> {len(seen):,} unique by project_id")
    recs = []
    for rid, r in seen.items():
        def g(i): return (r[i] if len(r) > i else None)
        pi = (g(5) or "").strip()
        given, family = split_name(pi)
        try: amt = float(g(6)) * 10000.0             # 万元 -> CNY
        except Exception: amt = None
        yr = g(7); yr = int(yr) if str(yr).isdigit() else None
        cyr = g(15); cyr = int(cyr) if str(cyr).isdigit() else None
        code = (g(14) or "").strip()
        recs.append({
            "project_id": rid,
            "funder_award_id": (g(2) or "").strip() or None,
            "display_name": (g(1) or "").strip() or None,
            "funder_scheme": (g(3) or "").strip() or None,
            "institution": (g(4) or "").strip() or None,
            "pi_name": pi or None,
            "given_name": given, "family_name": family,
            "amount": amt if (amt is not None and amt > 0) else None,   # §6.7: 0/neg -> NULL
            "currency": "CNY" if (amt is not None and amt > 0) else None,
            "start_year": yr,
            "start_date": f"{yr}-01-01" if yr else None,
            "conclusion_year": cyr,
            "description": (g(8) or "").strip() or None,                 # keywords
            "subject_code": code or None,
            "subject_dept": code[0] if code else None,
            "person_id": g(11), "org_id": g(12),
            "country_code": "CN",
        })
    df = pd.DataFrame(recs)
    # future-year cap
    yr_now = datetime.now(timezone.utc).year
    df.loc[df["start_year"] > yr_now, ["start_year","start_date"]] = pd.NA
    # all object cols -> pandas string (spark-safe)
    for c in df.columns:
        if df[c].dtype == object: df[c] = df[c].astype("string")
    df["provenance"] = PROVENANCE
    df["ingested_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "nsfc_projects.parquet"
    # shrink guard
    if out.exists():
        prev = len(pd.read_parquet(out))
        if len(df) < prev * 0.95:
            print(f"  [WARN] shrink: {len(df):,} < prev {prev:,}; aborting overwrite"); sys.exit(1)
    df.to_parquet(out, index=False)
    print(f"  saved {out.name}: {len(df):,} rows, {out.stat().st_size/1e6:.0f} MB")
    print(f"\n  Coverage ({len(df):,} grants):")
    for c in ["funder_award_id","display_name","institution","family_name","amount","start_year","subject_code","description"]:
        nn = df[c].notna().sum(); print(f"    {c:18s} {nn:,} ({100*nn/len(df):.1f}%)")
    print(f"  dept dist: {df['subject_dept'].value_counts().to_dict()}")
    print(f"  year span: {int(df['start_year'].min())}-{int(df['start_year'].max())}")
    return out

def upload_to_s3(path: Path):
    print(f"\n{'='*60}\nStep 3: Upload to S3\n{'='*60}")
    import shutil
    aws = shutil.which("aws")
    uri = f"s3://{S3_BUCKET}/{S3_KEY}"
    if not aws:
        print(f"  [ERROR] aws CLI not found. Manual: aws s3 cp {path} {uri}"); return False
    try:
        subprocess.run([aws,"s3","cp",str(path),uri], check=True, capture_output=True, text=True)
        print(f"  [OK] {path.name} -> {uri}"); return True
    except subprocess.CalledProcessError as e:
        print(f"  [ERROR] {e.stderr}"); return False

def main():
    ap = argparse.ArgumentParser(description="NSFC kd.nsfc.cn -> S3 (requires CN/HK IP)")
    ap.add_argument("--output-dir", type=Path, default=Path("./nsfc_data"))
    ap.add_argument("--skip-harvest", action="store_true", help="reuse existing rows.jsonl")
    ap.add_argument("--skip-upload", action="store_true")
    ap.add_argument("--limit", type=int, default=None, help="cap number of harvested terms (smoke)")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows_path = args.output_dir / "rows.jsonl"
    print("="*60 + f"\nNSFC -> S3   (funder F{FUNDER_ID}, provenance {PROVENANCE})\n" + "="*60)
    if not args.skip_harvest:
        print(f"\nStep 1: Harvest (REQUIRES China/Hong Kong IP — geo-blocked elsewhere)")
        harvest(rows_path, limit=args.limit)
    parquet = process(rows_path, args.output_dir)
    if not args.skip_upload:
        upload_to_s3(parquet)
    print(f"\nDone. Next: run notebooks/awards/CreateNSFCAwards.ipynb in Databricks")

if __name__ == "__main__":
    main()
