#!/usr/bin/env python3
"""服务层 e2e：起服务 → MCP 握手 → 调工具。零外部依赖。

用法: python3 tests/test_mcp_e2e.py
"""
import json, os, pathlib, subprocess, sys, time, urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
PORT = int(os.environ.get("TEST_PORT", "50069"))
BASE = f"http://127.0.0.1:{PORT}"

srv = subprocess.Popen([sys.executable, "server.py", "--port", str(PORT)],
                       cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
P = F = 0
def ck(name, cond, extra=""):
    global P, F
    if cond: P += 1; print(f"  ✓ {name}")
    else:    F += 1; print(f"  ✗ {name} {extra}")

def post(path, payload):
    req = urllib.request.Request(BASE + path, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode())

try:
    for _ in range(30):                      # 等服务起来
        try:
            with urllib.request.urlopen(BASE + "/health", timeout=2) as r:
                if r.status == 200: break
        except Exception: time.sleep(0.3)

    print("── HTTP 端点 ──")
    with urllib.request.urlopen(BASE + "/health", timeout=5) as r:
        h = json.loads(r.read().decode())
    ck("/health ok 且报告工具数", h.get("status") == "ok" and h.get("tools", 0) >= 5, str(h))
    with urllib.request.urlopen(BASE + "/tools", timeout=5) as r:
        tl = json.loads(r.read().decode())["tools"]
    ck("/tools 返回全部工具", len(tl) >= 5)
    ck("每个工具有 inputSchema", all("inputSchema" in t for t in tl))

    print("\n── MCP 协议 ──")
    init = post("/mcp", {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                         "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                                    "clientInfo": {"name": "t", "version": "1"}}})
    ck("initialize 返回 serverInfo", init["result"]["serverInfo"]["name"] == "news-mcp")
    lst = post("/mcp", {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    ck("tools/list 有工具", len(lst["result"]["tools"]) >= 5)

    call = post("/mcp", {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                         "params": {"name": "gold_classify", "arguments": {"articles": [
                             {"title": "央行降息，LPR下调", "content": "货币政策宽松"}]}}})
    t = json.loads(call["result"]["content"][0]["text"])
    ck("gold_classify 端到端 → MACRO_POLICY",
       t["count"] == 1 and t["events"][0]["event_type"] == "MACRO_POLICY", str(t))

    src = post("/mcp", {"jsonrpc": "2.0", "id": 7, "method": "tools/call",
                        "params": {"name": "list_sources", "arguments": {}}})
    ck("list_sources 返回 6 个源",
       len(json.loads(src["result"]["content"][0]["text"])["sources"]) == 6)

    dd = post("/mcp", {"jsonrpc": "2.0", "id": 8, "method": "tools/call",
                       "params": {"name": "silver_dedup", "arguments": {"articles": [
                           {"title": "同一条新闻", "content": "内容"}, {"title": "同一条新闻", "content": "内容"}]}}})
    t2 = json.loads(dd["result"]["content"][0]["text"])
    ck("silver_dedup 去掉完全重复（2 进 1 出）", t2["count"] == 1, str(t2))

    print("\n── 健壮性 ──")
    bad = post("/mcp", {"jsonrpc": "2.0", "id": 4, "method": "tools/call",
                        "params": {"name": "不存在的工具", "arguments": {}}})
    ck("未知工具返回 error 而非崩溃", "error" in bad)
    e = post("/mcp", {"jsonrpc": "2.0", "id": 5, "method": "tools/call",
                      "params": {"name": "gold_classify", "arguments": {"articles": []}}})
    ck("空入参返回结构化 error", "error" in json.loads(e["result"]["content"][0]["text"]))
    nf = post("/mcp", {"jsonrpc": "2.0", "id": 6, "method": "tools/call",
                       "params": {"name": "gold_classify", "arguments": {"articles": [
                           {"title": "半导体设备招标，sz000858 受益", "content": "国产化提速"}]}}})
    g = json.loads(nf["result"]["content"][0]["text"])
    ck("中文关键词与代码提取正常（无编码问题）",
       g["count"] == 1 and "sz000858" in g["events"][0]["symbols"], str(g))
finally:
    srv.terminate()
    try: srv.wait(timeout=5)
    except Exception: srv.kill()

print(f"\n{'='*46}\n通过 {P} / 失败 {F}")
sys.exit(1 if F else 0)
