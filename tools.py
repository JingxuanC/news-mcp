"""news-mcp 工具层 —— 把 Bronze/Silver/Gold 三段暴露成 MCP 工具。

设计：每一段都是独立工具（可单独跑、单独测），
`run_pipeline` 把它们串起来，对应 athena 的 runPipelineTick 前三段。
"""

from __future__ import annotations

import json
import time
from typing import Any, Optional

from core import (RawArticle, classify_by_keywords, gold_process, sim_hash,
                  silver_process, SIMHASH_RADIUS)
from sources import SOURCE_REGISTRY, fetch_by_name
from storage import NewsStore, default_db_path


class ToolDef:
    def __init__(self, name: str, description: str, input_schema: dict):
        self.name = name
        self.description = description
        self.inputSchema = input_schema

    def to_dict(self) -> dict:
        return {"name": self.name, "description": self.description,
                "inputSchema": self.inputSchema}


TOOLS: dict[str, ToolDef] = {}
HANDLERS: dict[str, Any] = {}
EXTRA_SCHEMAS: list[dict] = []


def tool(name: str, description: str, properties: dict,
         required: Optional[list] = None):
    def deco(fn):
        TOOLS[name] = ToolDef(name, description, {
            "type": "object", "properties": properties, "required": required or [],
        })
        HANDLERS[name] = fn
        return fn
    return deco


def _ok(p: Any) -> str:
    return json.dumps(p, ensure_ascii=False)


def _err(m: str) -> str:
    return json.dumps({"error": m}, ensure_ascii=False)


def _raws(items: Any) -> list[RawArticle]:
    out: list[RawArticle] = []
    for a in items or []:
        if not isinstance(a, dict):
            continue
        out.append(RawArticle(
            source=a.get("source", ""), channel=a.get("channel", ""),
            title=a.get("title", ""), content=a.get("content", ""),
            url=a.get("url", ""), pub_time=a.get("pub_time", ""),
            external_id=a.get("external_id", ""),
        ))
    return out


def _store_of(args: dict):
    """按参数决定是否落库。

    规则：persist=True 时用 db_path（缺省 NEWS_MCP_DB 或服务目录 news.db）；
    未显式要求持久化 → 返回 None，保持纯内存行为（不改变原有语义）。
    """
    if not args.get("persist"):
        return None
    return NewsStore(path=args.get("db_path") or None,
                     dedup=bool(args.get("dedup", True)))


def _raw_dict(a: RawArticle) -> dict:
    return {"source": a.source, "channel": a.channel, "title": a.title,
            "content": a.content[:500], "url": a.url,
            "pub_time": a.pub_time, "external_id": a.external_id}


def _iso_date(v: Any) -> str:
    """把源时间戳归一成 YYYY-MM-DD，供下游因果服务对齐 K 线日期。"""
    if v is None or v == "":
        return ""
    sv = str(v).strip()
    if sv.isdigit():
        try:
            import datetime as _dt
            ts = float(sv)
            if ts > 1e11:
                ts /= 1000.0
            return _dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        except Exception:
            return ""
    return sv[:10]


def _event_dict(e) -> dict:
    """GoldEvent 的对外契约（两个工具必须一致，否则联动会缺字段）。"""
    return {
        "event_type": e.event_type,
        "title": e.title,
        "description": e.description,
        "symbols": e.symbols,          # list；causal-chain-mcp 已能收 list 或 CSV
        "sectors": e.sectors,
        "confidence": e.confidence,
        "sentiment": round(e.sentiment, 4),
        "pub_time": e.pub_time,
        "event_time": _iso_date(e.pub_time),   # 下游对齐 K 线直接用这个
    }


# ── 1. 源清单 ────────────────────────────────────────────────────────

@tool("list_sources",
      "列出可用的 Bronze 采集源（对齐 Athena internal/data 的 6 个 Source）。",
      {})
def list_sources(args: dict) -> str:
    return _ok({"sources": [{"name": k, "description": v}
                            for k, v in SOURCE_REGISTRY.items()]})


# ── 2. Bronze：采集 ──────────────────────────────────────────────────

@tool("collect_raw",
      "Bronze 采集（对齐 Athena BronzeCollector）：从指定源抓原始文章。"
      "抓取失败不报错、返回已拿到的部分（与 Go 的容错行为一致）。",
      {
          "sources": {"type": "array", "items": {"type": "string"},
                      "description": "源名列表，见 list_sources"},
          "limit": {"type": "integer", "description": "每源条数上限，默认 20"},
          "codes": {"type": "array", "items": {"type": "string"},
                    "description": "仅 notice 源需要：股票代码列表"},
          "date_from": {"type": "string", "description": "仅 lhb/hot_rank 需要：起始日 YYYY-MM-DD"},
          "date_to": {"type": "string", "description": "仅 lhb/hot_rank 需要：结束日"},
          "persist": {"type": "boolean", "description": "是否写入 SQLite（Bronze 层），默认 false"},
          "db_path": {"type": "string", "description": "库路径；缺省 = NEWS_MCP_DB 或服务目录 news.db"},
          "dedup": {"type": "boolean", "description": "按 (source,external_id) 幂等去重，默认 true"},
      },
      required=["sources"])
def collect_raw(args: dict) -> str:
    names = args.get("sources") or []
    if not isinstance(names, list) or not names:
        return _err("sources 不能为空")
    arts: list[RawArticle] = []
    per_source: dict[str, int] = {}
    errors: dict[str, str] = {}
    for n in names:
        try:
            got = fetch_by_name(str(n), limit=args.get("limit"),
                                codes=args.get("codes"),
                                date_from=args.get("date_from"),
                                date_to=args.get("date_to"))
            per_source[str(n)] = len(got)
            arts.extend(got)
        except KeyError:
            errors[str(n)] = "未知源"
        except Exception as e:
            errors[str(n)] = f"{type(e).__name__}: {e}"
    out = {"articles": [_raw_dict(a) for a in arts],
           "count": len(arts), "per_source": per_source, "errors": errors}
    st = _store_of(args)
    if st is not None:
        out["persist"] = st.insert_raw(arts)
        out["db"] = st.path
    return _ok(out)


# ── 3. Silver：去重 ──────────────────────────────────────────────────

@tool("silver_dedup",
      "Silver 去重（对齐 Athena SilverPipeline.Process）：SimHash + 汉明距离 <= 3 判近重复。"
      "传入 seen_hashes 可做跨请求去重；返回更新后的 seen_hashes 供持久化。",
      {
          "articles": {"type": "array", "description": "原始文章 [{title,content,source,...}]"},
          "seen_hashes": {"type": "array", "items": {"type": "integer"},
                          "description": "已知指纹（上次调用返回的），可空"},
          "radius": {"type": "integer", "description": f"汉明距离阈值，默认 {SIMHASH_RADIUS}"},
          "persist": {"type": "boolean", "description": "是否写入 SQLite（Silver 层），默认 false"},
          "db_path": {"type": "string", "description": "库路径"},
      },
      required=["articles"])
def silver_dedup(args: dict) -> str:
    arts = _raws(args.get("articles"))
    if not arts:
        return _err("articles 为空")
    seen = [int(x) & ((1 << 64) - 1) for x in (args.get("seen_hashes") or [])]
    radius = int(args.get("radius", SIMHASH_RADIUS))
    out, new_seen, stats = silver_process(arts, seen, radius)
    res = {"articles": [_raw_dict(s.raw) for s in out],
           "count": len(out), "stats": stats, "seen_hashes": new_seen}
    st = _store_of(args)
    if st is not None:
        res["persist"] = st.insert_silver([(sa, None) for sa in out])
        res["db"] = st.path
    return _ok(res)


# ── 4. Gold：分类 ────────────────────────────────────────────────────

@tool("gold_classify",
      "Gold 分类（对齐 Athena RuleClassifier.ClassifyArticles / classifyByKeywords）："
      "关键词→事件类型（CORPORATE/MACRO_POLICY/TRADE/GEOPOLITICAL/INDUSTRY/MARKET）、"
      "板块、标的、情感分。判不出类型的文章被丢弃（与 Go 一致），置信度固定 0.3。",
      {
          "articles": {"type": "array",
                       "description": "文章 [{title,content}]；可含 source/pub_time"},
          "persist": {"type": "boolean", "description": "是否写入 SQLite（Gold 层），默认 false"},
          "db_path": {"type": "string", "description": "库路径"},
      },
      required=["articles"])
def gold_classify(args: dict) -> str:
    arts = _raws(args.get("articles"))
    if not arts:
        return _err("articles 为空")
    # 直接走 Silver→Gold（不重复去重，去重是 silver_dedup 的职责）
    silver = []
    for a in arts:
        from core import SilverArticle
        silver.append(SilverArticle(raw=a))
    events, stats = gold_process(silver)
    res = {"events": [_event_dict(e) for e in events],
           "count": len(events), "stats": stats}
    st = _store_of(args)
    if st is not None:
        res["persist"] = st.insert_events(events)
        res["db"] = st.path
    return _ok(res)


# ── 5. 端到端：Bronze → Silver → Gold ────────────────────────────────

@tool("run_pipeline",
      "端到端跑 Bronze→Silver→Gold（对齐 Athena runPipelineTick 的前三段，"
      "但不写库、不触发因果发现）。可指定源，或直接喂 raw_articles 跳过采集。",
      {
          "sources": {"type": "array", "items": {"type": "string"},
                      "description": "要采集的源；与 raw_articles 二选一"},
          "raw_articles": {"type": "array", "description": "直接给的原始文章（跳过 Bronze）"},
          "seen_hashes": {"type": "array", "items": {"type": "integer"},
                          "description": "跨请求去重指纹，可空"},
          "limit": {"type": "integer", "description": "每源条数，默认 20"},
          "codes": {"type": "array", "items": {"type": "string"}, "description": "notice 源用"},
          "date_from": {"type": "string"}, "date_to": {"type": "string"},
          "persist": {"type": "boolean",
                      "description": "是否落库（Bronze+Silver+Gold 三段 + pipeline_runs），默认 false"},
          "db_path": {"type": "string", "description": "库路径；缺省 = NEWS_MCP_DB 或服务目录 news.db"},
          "dedup": {"type": "boolean", "description": "Bronze 幂等去重，默认 true"},
      })
def run_pipeline(args: dict) -> str:
    stage: dict[str, Any] = {}
    arts = _raws(args.get("raw_articles"))
    if arts:
        stage["bronze"] = {"mode": "provided", "count": len(arts)}
    else:
        names = args.get("sources") or ["wallstcn"]
        errors: dict[str, str] = {}
        per: dict[str, int] = {}
        for n in names:
            try:
                got = fetch_by_name(str(n), limit=args.get("limit"),
                                    codes=args.get("codes"),
                                    date_from=args.get("date_from"),
                                    date_to=args.get("date_to"))
                per[str(n)] = len(got)
                arts.extend(got)
            except Exception as e:
                errors[str(e) if isinstance(e, KeyError) else type(e).__name__] = str(e)
        stage["bronze"] = {"mode": "collected", "per_source": per,
                           "count": len(arts), "errors": errors}
    if not arts:
        return _ok({"stage": stage, "events": [], "count": 0,
                    "note": "无文章可处理（采集全部失败或无输入）"})

    seen = [int(x) & ((1 << 64) - 1) for x in (args.get("seen_hashes") or [])]

    # 落库模式下：把库里保留窗口内的指纹一并载入 —— 这样跨 tick 的近重复
    # 才真的会被判掉（athena 靠 Redis SimHashBackend 做同一件事）。
    st = _store_of(args)
    carry = st.load_simhashes() if st is not None else []
    seen = list({*seen, *carry})

    t0 = time.time()
    silver, new_seen, sstats = silver_process(arts, seen)
    stage["silver"] = sstats
    events, gstats = gold_process(silver)
    stage["gold"] = gstats

    res = {
        "stage": stage,
        "events": [_event_dict(e) for e in events],
        "count": len(events),
        "seen_hashes": new_seen,
    }

    # 落库（可选）：三段都写 + 记一次 pipeline_runs，对齐 athena 的记账方式
    if st is not None:
        # 回写本轮新增的指纹（在 seen 之前的那一批是库里已有的）
        added = [h & ((1 << 63) - 1) for h in new_seen
                 if (h & ((1 << 63) - 1) not in set(carry))]
        st.record_simhashes(added)
        raw_res = st.insert_raw(arts)
        # Silver 行需要 raw_id 关联：用插入顺序对应（dedup 命中时复用已有 id）
        items = []
        ids = raw_res.get("ids") or []
        for i, sa in enumerate(silver):
            items.append((sa, ids[i] if i < len(ids) else None))
        sil_res = st.insert_silver(items)
        ev_res = st.insert_events(events)
        run_id = st.record_run(raw_res["inserted"] + raw_res["duplicate"],
                               sil_res["inserted"], ev_res["inserted"],
                               int((time.time() - t0) * 1000))
        res["persist"] = {"bronze": raw_res, "silver": sil_res,
                          "gold": ev_res, "pipeline_run_id": run_id}
        res["db"] = st.path
    return _ok(res)


# ── 6. 库状态 ────────────────────────────────────────────────────────

@tool("db_stats",
      "查看落库情况：Bronze/Silver/Gold 三表行数、唯一性、pipeline_runs 次数。"
      "对齐 Athena 的 pipeline_runs 记账口径。",
      {
          "db_path": {"type": "string", "description": "库路径；缺省 = NEWS_MCP_DB 或服务目录 news.db"},
      })
def db_stats(args: dict) -> str:
    try:
        st = NewsStore(path=args.get("db_path") or None)
        return _ok(st.stats())
    except Exception as e:
        return _err(f"打开库失败: {type(e).__name__}: {e}")


# ── 7-8. 读侧（其他智能体消费已落库数据，不触发采集）──────────────────

@tool("get_events",
      "读已落库的 Gold 事件（market_events），**不触发采集**。"
      "其他智能体消费新闻事件的主入口：可按类型/标的/板块/起始日过滤。",
      {
          "limit": {"type": "integer", "description": "返回条数，默认 20"},
          "event_type": {"type": "string",
                         "description": "CORPORATE/MACRO_POLICY/TRADE/GEOPOLITICAL/INDUSTRY/MARKET"},
          "symbol": {"type": "string", "description": "按标的过滤，如 sz000858"},
          "sector": {"type": "string", "description": "按板块过滤，如 AI算力"},
          "since": {"type": "string", "description": "起始日期 YYYY-MM-DD"},
          "db_path": {"type": "string", "description": "库路径；缺省 = NEWS_MCP_DB 或服务目录 news.db"},
      })
def get_events(args: dict) -> str:
    try:
        st = NewsStore(path=args.get("db_path") or None)
        rows = st.query_events(limit=int(args.get("limit") or 20),
                               event_type=args.get("event_type") or "",
                               symbol=args.get("symbol") or "",
                               sector=args.get("sector") or "",
                               since=args.get("since") or "")
        return _ok({"events": rows, "count": len(rows), "db": st.path})
    except Exception as e:
        return _err(f"查询失败: {type(e).__name__}: {e}")


@tool("get_articles",
      "读已落库的 Silver 文章（news_articles），**不触发采集**。"
      "用于回溯带情感分的原始新闻。",
      {
          "limit": {"type": "integer", "description": "返回条数，默认 20"},
          "symbol": {"type": "string", "description": "按标的过滤"},
          "sector": {"type": "string", "description": "按板块过滤"},
          "since": {"type": "string", "description": "起始日期 YYYY-MM-DD"},
          "db_path": {"type": "string", "description": "库路径"},
      })
def get_articles(args: dict) -> str:
    try:
        st = NewsStore(path=args.get("db_path") or None)
        rows = st.query_articles(limit=int(args.get("limit") or 20),
                                 symbol=args.get("symbol") or "",
                                 sector=args.get("sector") or "",
                                 since=args.get("since") or "")
        return _ok({"articles": rows, "count": len(rows), "db": st.path})
    except Exception as e:
        return _err(f"查询失败: {type(e).__name__}: {e}")
