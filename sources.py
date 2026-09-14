"""Bronze 采集源 —— 对齐 Athena internal/data 的 6 个 Source 实现。

端点从 athena 代码里逐条核对后搬移（不是猜的）：
  WallStCN   internal/data/bronze.go:95   api-one.wallstcn.com  /apiv1/content/lives
  SinaFinance internal/data/bronze.go:204 feed.mix.sina.com.cn  /api/roll/get
  CLSFlash   internal/data/cls_flash.go:47  www.cls.cn/api/sw
  LHB        internal/data/lhb.go:42        datacenter-web.eastmoney.com RPT_DAILYBILLBOARD_DETAILSNEW
  HotRank    internal/data/hot_rank.go:40   datacenter-web.eastmoney.com RPT_DAILYBILLBOARD_DETAILSNEW（按净买额排序）
  Notice     internal/data/notices.go:58    np-anotice-stock.eastmoney.com /api/security/ann

设计：每个源是独立函数，接受 fetcher 注入（便于测试与代理替换）。
只做 HTTP + 解析，不落库 —— 落库由调用方决定。
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any, Callable, Optional

from core import RawArticle

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 causal-mcp/1.0"
TIMEOUT = 15


def _http_json(url: str, timeout: int = TIMEOUT) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


# ── WallStreetCN（华尔街见闻快讯）──────────────────────────────────────

WALLSTCN_CHANNELS = ["global", "a-stock", "tech", "forex", "commodity"]


def fetch_wallstcn(channels: Optional[list[str]] = None,
                   limit: int = 20,
                   fetcher: Callable[[str], Any] = _http_json) -> list[RawArticle]:
    """对齐 Go WallStCNSource。channel 默认取 5 个（与 athena 的采集面一致）。"""
    out: list[RawArticle] = []
    for ch in (channels or WALLSTCN_CHANNELS):
        url = (f"https://api-one.wallstcn.com/apiv1/content/lives"
               f"?channel={ch}&limit={limit}")
        try:
            data = fetcher(url)
        except Exception:
            continue
        for item in (data.get("data", {}).get("items") or []):
            out.append(RawArticle(
                source="wallstcn",
                channel=ch,
                title=(item.get("title") or item.get("content_text") or "")[:200],
                content=item.get("content_text") or item.get("content") or "",
                url=item.get("uri") or "",
                pub_time=str(item.get("display_time") or ""),
                external_id=str(item.get("id") or ""),
            ))
    return out


# ── Sina Finance（新浪财经滚动）────────────────────────────────────────

def fetch_sina(num: int = 30,
               fetcher: Callable[[str], Any] = _http_json) -> list[RawArticle]:
    """对齐 Go SinaFinanceSource（pageid=153, lid=2509）。"""
    url = f"https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2509&num={num}"
    try:
        data = fetcher(url)
    except Exception:
        return []
    out: list[RawArticle] = []
    for item in (((data.get("result") or {}).get("data")) or []):
        out.append(RawArticle(
            source="sina",
            channel="finance",
            title=item.get("title") or "",
            content=item.get("intro") or item.get("title") or "",
            url=item.get("url") or "",
            pub_time=str(item.get("ctime") or ""),
            external_id=str(item.get("oid") or item.get("docid") or ""),
        ))
    return out


# ── 财联社快讯 ────────────────────────────────────────────────────────

def fetch_cls(limit: int = 30,
              fetcher: Callable[[str], Any] = _http_json) -> list[RawArticle]:
    """对齐 Go CLSFlashSource（app=CailianpressWeb）。"""
    url = "https://www.cls.cn/api/sw?app=CailianpressWeb&os=web&sv=8.4.6"
    try:
        data = fetcher(url)
    except Exception:
        return []
    items = ((data.get("data") or {}).get("roll_data")) or []
    out: list[RawArticle] = []
    for it in items[:limit]:
        cid = it.get("id")
        out.append(RawArticle(
            source="cls",
            channel="flash",
            title=(it.get("title") or it.get("brief") or "")[:200],
            content=it.get("content") or it.get("brief") or "",
            url=f"https://www.cls.cn/detail/{cid}" if cid else "",
            pub_time=str(it.get("ctime") or ""),
            external_id=str(cid or ""),
        ))
    return out


# ── 龙虎榜 / 热门榜（东财 datacenter）─────────────────────────────────

_EM_LHB_COLS = ("TRADE_DATE,SECURITY_CODE,SECURITY_NAME_ABBR,EXPLANATION,"
                "CHANGE_RATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,"
                "BILLBOARD_SELL_AMT,TURNOVERRATE,CLOSE_PRICE")
_EM_RANK_COLS = ("SECURITY_CODE,SECURITY_NAME_ABBR,CHANGE_RATE,BILLBOARD_NET_AMT")


def _em_datacenter(cols: str, date_from: str, date_to: str, sort_col: str,
                   page_size: int, fetcher: Callable[[str], Any]) -> list[RawArticle]:
    url = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
           "?reportName=RPT_DAILYBILLBOARD_DETAILSNEW"
           f"&pageSize={page_size}&sortColumns={sort_col}&sortTypes=-1"
           f"&columns={cols}&source=WEB&client=WEB"
           f"&filter=(TRADE_DATE>='{date_from}')(TRADE_DATE<='{date_to}')")
    try:
        data = fetcher(url)
    except Exception:
        return []
    out: list[RawArticle] = []
    for row in (((data.get("result") or {}).get("data")) or []):
        code = str(row.get("SECURITY_CODE") or "")
        name = str(row.get("SECURITY_NAME_ABBR") or "")
        net = row.get("BILLBOARD_NET_AMT")
        out.append(RawArticle(
            source="eastmoney_lhb",
            channel="lhb",
            title=f"{name}({code}) 龙虎榜 净买额 {net}",
            content=json.dumps(row, ensure_ascii=False),
            url="https://data.eastmoney.com/stock/lhb.html",
            pub_time=str(row.get("TRADE_DATE") or ""),
            external_id=f"{code}-{row.get('TRADE_DATE')}",
        ))
    return out


def fetch_lhb(date_from: str, date_to: str, page_size: int = 50,
              fetcher: Callable[[str], Any] = _http_json) -> list[RawArticle]:
    """对齐 Go LHBSource。"""
    return _em_datacenter(_EM_LHB_COLS, date_from, date_to, "TRADE_DATE",
                          page_size, fetcher)


def fetch_hot_rank(date_from: str, date_to: str, page_size: int = 50,
                   fetcher: Callable[[str], Any] = _http_json) -> list[RawArticle]:
    """对齐 Go HotRankSource（同接口，按净买额排序）。"""
    arts = _em_datacenter(_EM_RANK_COLS, date_from, date_to, "BILLBOARD_NET_AMT",
                          page_size, fetcher)
    for a in arts:
        a.source = "eastmoney_hot_rank"
        a.channel = "hot_rank"
    return arts


# ── 公告（东财 anotice）───────────────────────────────────────────────

def fetch_notices(codes: list[str], page_size: int = 3,
                  fetcher: Callable[[str], Any] = _http_json) -> list[RawArticle]:
    """对齐 Go NoticeSource（stock_list 用裸 6 位码）。"""
    out: list[RawArticle] = []
    for code in codes:
        bare = code.removeprefix("sh").removeprefix("sz")
        url = ("https://np-anotice-stock.eastmoney.com/api/security/ann"
               f"?page_size={page_size}&page_index=1&stock_list={bare}")
        try:
            data = fetcher(url)
        except Exception:
            continue
        for it in (((data.get("data") or {}).get("list")) or []):
            out.append(RawArticle(
                source="eastmoney_notice",
                channel="announcement",
                title=it.get("title") or it.get("art_title") or "",
                content=it.get("digest") or it.get("title") or "",
                url=(f"https://np-anotice-stock.eastmoney.com/api/security/ann/detail"
                     f"?artCode={it.get('art_code')}"),
                pub_time=str(it.get("notice_date") or ""),
                external_id=str(it.get("art_code") or ""),
            ))
    return out


# ── 源注册表 ──────────────────────────────────────────────────────────

SOURCE_REGISTRY = {
    "wallstcn": "华尔街见闻快讯（5 channel）",
    "sina": "新浪财经滚动",
    "cls": "财联社快讯",
    "lhb": "东方财富龙虎榜",
    "hot_rank": "东方财富热门榜（按净买额）",
    "notice": "东方财富公告（需 codes）",
}


def fetch_by_name(name: str, **kw) -> list[RawArticle]:
    """按名字调源；未知名字抛 KeyError（由 MCP 层转成结构化 error）。"""
    if name == "wallstcn":
        return fetch_wallstcn(channels=kw.get("channels"), limit=int(kw.get("limit") or 20))
    if name == "sina":
        return fetch_sina(num=int(kw.get("limit") or 30))
    if name == "cls":
        return fetch_cls(limit=int(kw.get("limit") or 30))
    if name == "lhb":
        return fetch_lhb(kw.get("date_from") or "", kw.get("date_to") or "",
                         int(kw.get("page_size") or 50))
    if name == "hot_rank":
        return fetch_hot_rank(kw.get("date_from") or "", kw.get("date_to") or "",
                              int(kw.get("page_size") or 50))
    if name == "notice":
        return fetch_notices(kw.get("codes") or [], int(kw.get("page_size") or 3))
    raise KeyError(name)
