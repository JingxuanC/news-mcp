#!/usr/bin/env python3
"""持久化层测试：schema 兼容 + 幂等 + 跨调用去重 + 边界。"""
import os, sys, tempfile, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
os.environ.pop("NEWS_MCP_DB", None)

from core import RawArticle, GoldEvent, SilverArticle
from storage import NewsStore, DDL

P=F=0
def ck(n,c,e=""):
    global P,F
    if c: P+=1; print(f"  ✓ {n}")
    else: F+=1; print(f"  ✗ {n} {e}")

tmp = tempfile.mkdtemp()
db = os.path.join(tmp, "t.db")

print("── 建库与 schema ──")
st = NewsStore(db)
import sqlite3
with sqlite3.connect(db) as c:
    tabs = {r[0] for r in c.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
ck("四张主表齐备", {"news_articles_raw","news_articles","market_events",
                    "pipeline_runs"} <= tabs, str(tabs))
ck("额外有 simhash 表（athena 用 Redis，这里落表）", "news_simhash" in tabs)
# 各表列数按 athena 原始 DDL（pipeline_runs 本来就只有 6 列）
EXPECT_MIN = {"news_articles_raw": 10, "news_articles": 12,
              "market_events": 9, "pipeline_runs": 6}
for t, mn in EXPECT_MIN.items():
    with sqlite3.connect(db) as c:
        cols = [r[1] for r in c.execute(f"PRAGMA table_info({t})").fetchall()]
    ck(f"{t} 列数 >= {mn}（athena 同构）", len(cols) >= mn, f"{len(cols)} 列")

print("\n── Bronze 幂等（对齐 athena 的 external_id 语义）──")
arts = [RawArticle(source="s", channel="c", title="标题1", content="正文",
                   external_id="id-1", pub_time="1789395731"),
        RawArticle(source="s", channel="c", title="标题2", content="正文",
                   external_id="id-2", pub_time="1789395731")]
r1 = st.insert_raw(arts)
ck("首次插入 2 条", r1["inserted"] == 2 and r1["duplicate"] == 0, str(r1))
r2 = st.insert_raw(arts)
ck("重复插入 0 条、判重 2 条", r2["inserted"] == 0 and r2["duplicate"] == 2, str(r2))
ck("重复时复用已有 id（不是新建）", r2["ids"] == r1["ids"], f"{r2['ids']} vs {r1['ids']}")
ck("无 external_id 的不参与判重（对齐 WHERE 条件）",
    NewsStore(db).insert_raw([RawArticle(source="s", title="x", external_id="")])["inserted"] == 1)

print("\n── CSV 转换（库里是字符串，与 athena 一致）──")
st.insert_events([GoldEvent(event_type="INDUSTRY", title="t", description="d",
                            symbols=["sz000858","sh600519"], sectors=["AI算力"],
                            confidence=0.3, sentiment=0.5, pub_time="2026-09-09")])
with sqlite3.connect(db) as c:
    row = c.execute("SELECT symbols, sectors, event_time FROM market_events").fetchone()
ck("symbols 存成 CSV", row[0] == "sz000858,sh600519", str(row[0]))
ck("sectors 存成 CSV", row[1] == "AI算力", str(row[1]))
ck("event_time 存成 RFC3339（对齐 Go 的 Format(time.RFC3339)）",
   row[2].startswith("2026-09-09T"), str(row[2]))

print("\n── SimHash 持久化（sqlite signed 64 位边界）──")
big = (1 << 64) - 1                      # unsigned 最大值，必须不炸
st.record_simhashes([big, 12345])
loaded = st.load_simhashes()
ck("unsigned 64 位指纹不触发 OverflowError", len(loaded) == 2, str(loaded))
ck("mask 后落在 signed 范围内", all(v < (1 << 63) for v in loaded), str(loaded))
ck("重复写入不新增（INSERT OR IGNORE）", st.record_simhashes([12345]) == 0)
ck("清理过期指纹（retention=0 时全清）",
   NewsStore(db).load_simhashes(retention_sec=0) == [])

print("\n── pipeline_runs 记账 ──")
rid = st.record_run(bronze=10, silver=8, gold=2, duration_ms=33, new_causal_links=0)
with sqlite3.connect(db) as c:
    r = c.execute("SELECT bronze_count,silver_count,gold_events,duration_ms "
                  "FROM pipeline_runs WHERE id=?", (rid,)).fetchone()
ck("记录可按 id 取回且数值正确", r == (10, 8, 2, 33), str(r))

print("\n── stats ──")
s = st.stats()
ck("stats 含全部计数", {"bronze","silver","gold","runs","simhash_fingerprints",
                        "bronze_unique"} <= set(s), str(sorted(s)))
ck("bronze_unique 不等于行数时能反映重复",
   s["bronze_unique"] <= s["bronze"], str(s))

print(f"\n{'='*46}\n通过 {P} / 失败 {F}")
sys.exit(1 if F else 0)
