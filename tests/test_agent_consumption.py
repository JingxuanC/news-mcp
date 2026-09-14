#!/usr/bin/env python3
"""第三方智能体消费路径测试：采集→落库→按过滤条件读回。
证明"别的智能体不读代码也能用"：只走 MCP 协议 + schema。
"""
import json, os, pathlib, subprocess, sys, tempfile, time, urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
PORT = int(os.environ.get("TEST_PORT", "50071"))
BASE = f"http://127.0.0.1:{PORT}"
DB = os.path.join(tempfile.mkdtemp(), "consume.db")

env = dict(os.environ, NEWS_MCP_DB=DB)
srv = subprocess.Popen([sys.executable, "server.py", "--port", str(PORT)],
                       cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
P = F = 0
def ck(n, c, e=""):
    global P, F
    if c: P += 1; print(f"  ✓ {n}")
    else: F += 1; print(f"  ✗ {n} {e}")

def rpc(method, params=None, rid=1):
    req = urllib.request.Request(BASE + "/mcp",
        data=json.dumps({"jsonrpc": "2.0", "id": rid, "method": method,
                         "params": params or {}}).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())

def call(tool, args, rid=9):
    resp = rpc("tools/call", {"name": tool, "arguments": args}, rid)
    return json.loads(resp["result"]["content"][0]["text"])

try:
    for _ in range(30):
        try:
            with urllib.request.urlopen(BASE + "/health", timeout=2): break
        except Exception: time.sleep(0.3)

    print("── 发现（无需读代码）──")
    init = rpc("initialize", {"protocolVersion": "2024-11-05", "capabilities": {},
                              "clientInfo": {"name": "third-party", "version": "1"}})
    ck("initialize 自报身份", init["result"]["serverInfo"]["name"] == "news-mcp")
    tools = rpc("tools/list")["result"]["tools"]
    names = {t["name"] for t in tools}
    ck("工具可枚举", len(tools) >= 8, str(sorted(names)))
    ck("读侧工具存在（智能体主入口）", {"get_events", "get_articles"} <= names)
    ck("每个工具有非空 description（智能体靠它选工具）",
       all(t.get("description") for t in tools))
    ck("每个工具有 inputSchema", all("inputSchema" in t for t in tools))

    print("\n── 写入（可选，离线数据即可，不依赖外网）──")
    arts = [
      {"source":"t","channel":"c","title":"某晶圆厂扩产，sz000858 受益","content":"半导体扩产，业绩增长","external_id":"a1","pub_time":"1789395731"},
      {"source":"t","channel":"c","title":"央行宣布降息，LPR下调","content":"货币政策宽松","external_id":"a2","pub_time":"1789395731"},
      {"source":"t","channel":"c","title":"某公司被制裁，出口管制","content":"贸易摩擦","external_id":"a3","pub_time":"1789395731"},
    ]
    r = call("run_pipeline", {"raw_articles": arts, "persist": True})
    ck("落库成功", r["persist"]["gold"]["inserted"] >= 1, str(r.get("persist")))
    ck("环境变量 NEWS_MCP_DB 生效（写到指定库）", os.path.exists(DB))

    print("\n── 消费（智能体真实用法）──")
    ev = call("get_events", {"limit": 10})
    ck("get_events 能读到刚写入的事件", ev["count"] >= 1, str(ev)[:150])
    ck("事件字段完整（供智能体直接推理）",
       all(k in ev["events"][0] for k in
           ["id","event_type","title","symbols","sectors","sentiment","event_time"]),
       str(sorted(ev["events"][0])))

    corp = call("get_events", {"event_type": "CORPORATE"})
    ck("按事件类型过滤", all(e["event_type"] == "CORPORATE" for e in corp["events"]),
       str([e["event_type"] for e in corp["events"]]))

    sem = call("get_events", {"event_type": "INDUSTRY"})
    ck("INDUSTRY 事件带板块标签",
       any("AI算力" in e["sectors"] or "芯片半导体" in e["sectors"] for e in sem["events"])
       or sem["count"] == 0, str([e["sectors"] for e in sem["events"]]))

    art = call("get_articles", {"limit": 10})
    ck("get_articles 读 Silver 层", art["count"] >= 1, str(art)[:150])

    print("\n── 边界：智能体读空库不能崩 ──")
    empty_db = os.path.join(tempfile.mkdtemp(), "empty.db")
    e = call("get_events", {"db_path": empty_db})
    ck("空库返回 0 条而非报错", e.get("count") == 0 and "events" in e, str(e)[:150])
    st = call("db_stats", {"db_path": empty_db})
    ck("空库 stats 正常", st.get("bronze") == 0, str(st)[:150])

    print("\n── 幂等：重复采集不膨胀 ──")
    before = call("db_stats", {})["gold"]
    call("run_pipeline", {"raw_articles": arts, "persist": True})
    after = call("db_stats", {})["gold"]
    ck("重复写入 gold 行数不变", before == after, f"{before} → {after}")
finally:
    srv.terminate()
    try: srv.wait(timeout=5)
    except Exception: srv.kill()

print(f"\n{'='*46}\n通过 {P} / 失败 {F}")
sys.exit(1 if F else 0)
