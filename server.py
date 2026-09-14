#!/usr/bin/env python3
"""news-mcp — A股因果链推演的独立 MCP 服务。

把 Athena `internal/causal`（1505 行 Go + 217 行反驳）里**因果推断之外**的
那部分抽成独立服务：事件↔标的匹配、CAR 计算、方向/置信度定级、估计窗清洗、
端到端链路构建。

与原实现的边界（重要）：
  * 本服务**不抓行情、不写库**。K 线与基准由调用方传入 —— 这样它不绑定任何
    数据源，任何 MCP 客户端都能用，回归测试也能离线复现。
  * 事件研究的统计推断（市场模型 OLS / t 检验 / BSTS）不在本服务内，
    已有 causal-mcp 的 event_study / causal_impact 覆盖，不重复造。

用法:
    python3 server.py --port 50059

端点:
    GET  /health   健康检查
    GET  /tools    工具列表（JSON Schema）
    POST /mcp      MCP JSON-RPC（initialize / tools/list / tools/call）
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tools import HANDLERS, TOOLS  # noqa: F401 — 导入即注册全部工具

SERVER_NAME = "news-mcp"
VERSION = "1.0.0"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(SERVER_NAME)


def _tool_schemas() -> list[dict]:
    return [t.to_dict() for t in TOOLS.values()]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *a):        # 收敛访问日志
        logger.debug("%s - %s", self.address_string(), fmt % a)

    # ── 响应工具 ──
    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode() or "{}")
        except json.JSONDecodeError:
            return {}

    # ── GET ──
    def do_GET(self):                                    # noqa: N802
        path = self.path.split("?")[0]
        if path == "/health":
            self._send(200, {"status": "ok", "server": SERVER_NAME,
                             "version": VERSION, "tools": len(TOOLS),
                             "time": int(time.time())})
        elif path in ("/tools", "/mcp/tools"):
            self._send(200, {"tools": _tool_schemas()})
        else:
            self._send(404, {"error": f"unknown path: {path}"})

    # ── POST /mcp ──
    def do_POST(self):                                   # noqa: N802
        path = self.path.split("?")[0]
        if path not in ("/mcp", "/mcp/causal-chain", "/"):
            self._send(404, {"error": f"unknown path: {path}"})
            return

        req = self._read_json()
        method = req.get("method")
        mid = req.get("id")

        if method == "initialize":
            self._send(200, {"jsonrpc": "2.0", "id": mid, "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": VERSION},
            }})
        elif method in ("notifications/initialized", "notifications/cancelled"):
            self._send(200, {})                          # 通知无响应体
        elif method == "tools/list":
            self._send(200, {"jsonrpc": "2.0", "id": mid,
                             "result": {"tools": _tool_schemas()}})
        elif method == "tools/call":
            params = req.get("params") or {}
            name = params.get("name")
            args = params.get("arguments") or {}
            fn = HANDLERS.get(name)
            if fn is None:
                self._send(200, {"jsonrpc": "2.0", "id": mid, "error": {
                    "code": -32601, "message": f"Unknown tool: {name}"}})
                return
            if not isinstance(args, dict):
                self._send(200, {"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text",
                                 "text": json.dumps({"error": "arguments 必须是对象"},
                                                    ensure_ascii=False)}],
                    "isError": True}})
                return
            t0 = time.time()
            try:
                text = fn(args)
                err = False
            except Exception as e:                       # 工具异常不能打死服务
                logger.exception("tool %s failed", name)
                text = json.dumps({"error": f"{type(e).__name__}: {e}"},
                                  ensure_ascii=False)
                err = True
            logger.info("tool=%s ok=%s dur_ms=%d", name, not err,
                        int((time.time() - t0) * 1000))
            self._send(200, {"jsonrpc": "2.0", "id": mid, "result": {
                "content": [{"type": "text", "text": text}], "isError": err}})
        else:
            self._send(200, {"jsonrpc": "2.0", "id": mid, "error": {
                "code": -32601, "message": f"Unknown method: {method}"}})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=50059)
    ap.add_argument("--host", default="127.0.0.1")
    a = ap.parse_args()
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    logger.info("%s v%s listening on http://%s:%d  (%d tools)",
                SERVER_NAME, VERSION, a.host, a.port, len(TOOLS))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        logger.info("shutting down")
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
