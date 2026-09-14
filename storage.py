"""持久化层 —— schema 与 Athena 的 Bronze/Silver/Gold 三张表**完全兼容**。

表结构逐字段对照 athena 的 DDL（internal/db/sqlite.go:139/1016/1123/1209）：
  news_articles_raw(Bronze)  source/channel/title/content/url/external_id/pub_time/fetched_at/retry_count
  news_articles(Silver)      + symbols/sectors/sentiment/importance/processed_at/raw_id/url
  market_events(Gold)        event_type/title/description/symbols/sectors/confidence/sentiment/event_time
  pipeline_runs              每次 tick 的 bronze/silver/gold 计数 + duration_ms + new_causal_links

⚠️ 与原实现的一个重要差异：**默认开启去重**。
athena 的 InsertRawArticle 是纯 INSERT，不去重，实测 news_articles_raw 有 46839 行
但只有 1436 个唯一 (source, external_id) —— eastmoney_notice 更是 19746 行挤在 1 个
external_id 上。照抄这个行为没有意义。这里默认按 (source, external_id) 去重，
可用 dedup=False 退回 athena 的原始行为（便于与旧数据比对）。

符号/板块在库里是 **CSV 字符串**（不是数组）—— 写入时做转换，与 athena 一致，
这样两个服务的产出可以落进同一张表。
"""

from __future__ import annotations

import os
import sqlite3
import time
from typing import Any, Optional

from core import GoldEvent, RawArticle, SilverArticle

# schema 与 athena 对齐；额外一条 UNIQUE 索引用于去重（athena 没有，见模块注释）
DDL = """
CREATE TABLE IF NOT EXISTS news_articles_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    external_id TEXT NOT NULL DEFAULT '',
    pub_time TEXT NOT NULL DEFAULT '',
    fetched_at TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    retry_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_raw_source ON news_articles_raw(source);
CREATE INDEX IF NOT EXISTS idx_raw_pub_time ON news_articles_raw(pub_time);

CREATE TABLE IF NOT EXISTS news_articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    symbols TEXT NOT NULL DEFAULT '',
    sectors TEXT NOT NULL DEFAULT '',
    sentiment REAL NOT NULL DEFAULT 0,
    importance REAL NOT NULL DEFAULT 0,
    pub_time TEXT NOT NULL DEFAULT '',
    processed_at TEXT NOT NULL DEFAULT (datetime('now')),
    raw_id INTEGER REFERENCES news_articles_raw(id),
    url TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_articles_symbols ON news_articles(symbols);
CREATE INDEX IF NOT EXISTS idx_articles_sectors ON news_articles(sectors);
CREATE INDEX IF NOT EXISTS idx_articles_pub_time ON news_articles(pub_time);

CREATE TABLE IF NOT EXISTS market_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    symbols TEXT NOT NULL DEFAULT '',
    sectors TEXT NOT NULL DEFAULT '',
    confidence REAL NOT NULL DEFAULT 0,
    sentiment REAL NOT NULL DEFAULT 0,
    event_time TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_events_symbols ON market_events(symbols);
CREATE INDEX IF NOT EXISTS idx_events_sectors ON market_events(sectors);
CREATE INDEX IF NOT EXISTS idx_events_event_time ON market_events(event_time);

-- SimHash 指纹持久化：对齐 athena 的 SimHashBackend（其实现存 Redis，TTL 24h）。
-- athena 的 Python 侧这层是断的，导致跨 tick 的近重复无法判重；
-- 这里落表，让 Silver 去重真正跨调用生效。
CREATE TABLE IF NOT EXISTS news_simhash (
    hash     INTEGER PRIMARY KEY,
    seen_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_simhash_seen ON news_simhash(seen_at);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bronze_count INTEGER NOT NULL DEFAULT 0,
    silver_count INTEGER NOT NULL DEFAULT 0,
    gold_events INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    new_causal_links INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def default_db_path() -> str:
    """数据库位置：NEWS_MCP_DB 环境变量；缺省 = 服务目录下 news.db。

    刻意不默认写到 athena.db：避免这个服务在你不知情时污染生产库。
    """
    return os.environ.get("NEWS_MCP_DB") or str(
        __import__("pathlib").Path(__file__).resolve().parent / "news.db")


def _csv(v: Any) -> str:
    """list → CSV（库里的列是字符串，与 athena 一致）。"""
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return ",".join(str(x) for x in v if str(x).strip())
    return str(v)


def _rfc3339(pub_time: str) -> str:
    """对齐 Go 的 pubTime.Format(time.RFC3339)。

    athena 存的是 RFC3339；而采集源常给 epoch 秒。这里把 epoch 转成 RFC3339，
    已是日期串的则原样补成当天 00:00:00Z（避免两种格式混在一列里）。
    """
    s = str(pub_time or "").strip()
    if not s:
        return ""
    if s.isdigit():
        ts = float(s)
        if ts > 1e11:
            ts /= 1000.0
        try:
            import datetime as _dt
            return _dt.datetime.fromtimestamp(ts).astimezone().isoformat()
        except Exception:
            return s
    if len(s) == 10 and s[4] == "-":
        return s + "T00:00:00+08:00"
    return s


# SimHash 是 unsigned 64 位，而 SQLite INTEGER 是 **signed** 64 位 ——
# 直接存 >= 2^63 的指纹会 OverflowError。取低 63 位存储即可：
# 丢掉的只有最高位，任意两指纹的汉明距离至多多算/少算 1 位，
# 对 radius=3 的近重复判定影响可忽略，且存取用同一 mask 保证可比。
_SIMHASH_MASK = (1 << 63) - 1


class NewsStore:
    """SQLite 存储。connection 按需打开，线程安全靠 sqlite3 自身 + 短事务。"""

    def __init__(self, path: Optional[str] = None, dedup: bool = True):
        self.path = path or default_db_path()
        self.dedup = dedup
        self.unique_index = True          # _init 会按实际情况改写
        self.unique_index_error = ""
        self._init()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.path, timeout=15)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=15000")
        return c

    def _init(self) -> None:
        with self._conn() as c:
            c.executescript(DDL)

        # (source, external_id) 的唯一索引是**尽力而为**：
        # 库里若已存在历史重复行（athena 的老采集器是纯 INSERT，实测有 46839 行
        # 仅 1436 个唯一键），建唯一索引会 IntegrityError，导致整个服务打不开库。
        # 这时退化为应用层 SELECT-based 去重 —— 行为一致，只是慢一点。
        self.unique_index = True
        try:
            with self._conn() as c:
                c.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_source_extid "
                          "ON news_articles_raw(source, external_id) "
                          "WHERE external_id <> ''")
        except Exception as e:
            self.unique_index = False
            self.unique_index_error = str(e)

    # ── Bronze ──
    def insert_raw(self, arts: list[RawArticle]) -> dict:
        """写 Bronze。dedup=True 时按 (source, external_id) 幂等。"""
        ins, dup = 0, 0
        ids: list[int] = []
        with self._conn() as c:
            for a in arts:
                pub = _rfc3339(a.pub_time)
                if self.dedup and a.external_id:
                    row = c.execute(
                        "SELECT id FROM news_articles_raw WHERE source=? AND external_id=?",
                        (a.source, a.external_id)).fetchone()
                    if row:
                        dup += 1
                        ids.append(row[0])
                        continue
                cur = c.execute(
                    "INSERT INTO news_articles_raw "
                    "(source,channel,title,content,url,external_id,pub_time,fetched_at) "
                    "VALUES (?,?,?,?,?,?,?,datetime('now'))",
                    (a.source, a.channel, a.title, a.content, a.url,
                     a.external_id, pub))
                ins += 1
                ids.append(cur.lastrowid)
        return {"inserted": ins, "duplicate": dup, "ids": ids}

    # ── Silver ──
    def insert_silver(self, items: list[tuple[SilverArticle, Optional[int]]]) -> dict:
        """写 Silver。items = [(SilverArticle, raw_id)]。

        与 athena 一致：symbols/sectors/sentiment 先写占位空值，由 Gold 回填。
        """
        n = 0
        with self._conn() as c:
            for sa, raw_id in items:
                c.execute(
                    "INSERT INTO news_articles "
                    "(source,channel,title,content,url,symbols,sectors,sentiment,"
                    " importance,pub_time,raw_id) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (sa.raw.source, sa.raw.channel, sa.raw.title, sa.raw.content,
                     sa.raw.url, "", "", 0.0, 0.0, _rfc3339(sa.raw.pub_time), raw_id))
                n += 1
        return {"inserted": n}

    # ── Gold ──
    def insert_events(self, events: list[GoldEvent]) -> dict:
        """写 Gold（market_events）。符号/板块转 CSV，与 athena 列类型一致。"""
        n = 0
        with self._conn() as c:
            for e in events:
                c.execute(
                    "INSERT INTO market_events "
                    "(event_type,title,description,symbols,sectors,confidence,"
                    " sentiment,event_time) VALUES (?,?,?,?,?,?,?,?)",
                    (e.event_type, e.title, e.description, _csv(e.symbols),
                     _csv(e.sectors), e.confidence, e.sentiment,
                     _rfc3339(e.pub_time)))
                n += 1
        return {"inserted": n}

    # ── pipeline_runs ──
    def record_run(self, bronze: int, silver: int, gold: int,
                   duration_ms: int, new_causal_links: int = 0) -> int:
        with self._conn() as c:
            cur = c.execute(
                "INSERT INTO pipeline_runs "
                "(bronze_count,silver_count,gold_events,duration_ms,new_causal_links) "
                "VALUES (?,?,?,?,?)",
                (bronze, silver, gold, duration_ms, new_causal_links))
            return cur.lastrowid

    # ── SimHash 去重指纹（跨调用持久化）──
    def load_simhashes(self, retention_sec: int = 24 * 3600) -> list[int]:
        """载入保留窗口内的指纹（对齐 athena simHashRetention=24h）。"""
        import datetime as _dt
        cutoff = (_dt.datetime.now() - _dt.timedelta(seconds=retention_sec)
                  ).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as c:
            # 顺手清理过期指纹，避免表无限增长
            c.execute("DELETE FROM news_simhash WHERE seen_at < ?", (cutoff,))
            return [r[0] for r in
                    c.execute("SELECT hash FROM news_simhash").fetchall()]

    def record_simhashes(self, hashes: list[int]) -> int:
        """写入指纹，返回**新增**条数（已存在的忽略）。"""
        n = 0
        with self._conn() as c:
            for h in hashes:
                cur = c.execute(
                    "INSERT OR IGNORE INTO news_simhash (hash) VALUES (?)",
                    (int(h) & _SIMHASH_MASK,))
                n += cur.rowcount if cur.rowcount > 0 else 0
        return n

    # ── 读侧（供其他智能体消费已落库的数据）──
    def query_events(self, limit: int = 20, event_type: str = "",
                     symbol: str = "", sector: str = "",
                     since: str = "") -> list[dict]:
        """查 market_events(Gold)。symbol/sector 按 CSV 列做包含匹配。"""
        sql = ("SELECT id,event_type,title,description,symbols,sectors,confidence,"
               "sentiment,event_time,created_at FROM market_events WHERE 1=1")
        args: list[Any] = []
        if event_type:
            sql += " AND event_type=?"
            args.append(event_type)
        if symbol:
            sql += " AND (','||symbols||',') LIKE ?"
            args.append(f"%,{symbol},%")
        if sector:
            sql += " AND (','||sectors||',') LIKE ?"
            args.append(f"%,{sector},%")
        if since:
            sql += " AND event_time>=?"
            args.append(since)
        sql += " ORDER BY id DESC LIMIT ?"
        args.append(int(limit))
        with self._conn() as c:
            rows = c.execute(sql, args).fetchall()
        return [{"id": r[0], "event_type": r[1], "title": r[2],
                 "description": r[3],
                 "symbols": [x for x in (r[4] or "").split(",") if x],
                 "sectors": [x for x in (r[5] or "").split(",") if x],
                 "confidence": r[6], "sentiment": r[7],
                 "event_time": r[8], "created_at": r[9]} for r in rows]

    def query_articles(self, limit: int = 20, symbol: str = "",
                       sector: str = "", since: str = "") -> list[dict]:
        """查 news_articles(Silver)。"""
        sql = ("SELECT id,source,channel,title,symbols,sectors,sentiment,"
               "importance,pub_time FROM news_articles WHERE 1=1")
        args: list[Any] = []
        if symbol:
            sql += " AND (','||symbols||',') LIKE ?"
            args.append(f"%,{symbol},%")
        if sector:
            sql += " AND (','||sectors||',') LIKE ?"
            args.append(f"%,{sector},%")
        if since:
            sql += " AND pub_time>=?"
            args.append(since)
        sql += " ORDER BY id DESC LIMIT ?"
        args.append(int(limit))
        with self._conn() as c:
            rows = c.execute(sql, args).fetchall()
        return [{"id": r[0], "source": r[1], "channel": r[2], "title": r[3],
                 "symbols": [x for x in (r[4] or "").split(",") if x],
                 "sectors": [x for x in (r[5] or "").split(",") if x],
                 "sentiment": r[6], "importance": r[7],
                 "pub_time": r[8]} for r in rows]

    def stats(self) -> dict:
        with self._conn() as c:
            g = lambda q: c.execute(q).fetchone()[0]  # noqa: E731
            return {
                "path": self.path, "dedup": self.dedup,
                "unique_index": self.unique_index,
                "bronze": g("SELECT COUNT(*) FROM news_articles_raw"),
                "bronze_unique": g("SELECT COUNT(DISTINCT source||'|'||external_id) "
                                   "FROM news_articles_raw"),
                "silver": g("SELECT COUNT(*) FROM news_articles"),
                "gold": g("SELECT COUNT(*) FROM market_events"),
                "runs": g("SELECT COUNT(*) FROM pipeline_runs"),
                "simhash_fingerprints": g("SELECT COUNT(*) FROM news_simhash"),
            }
