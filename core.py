"""新闻链路核心 —— 从 Athena internal/data 抽取 Bronze/Silver/Gold。

逐条对齐的 Go 实现：
  SimHash / HammingDistance     data/silver.go:22,40
  SilverPipeline.Process        data/silver.go:100   （SimHash 去重，radius=3，保留 24h）
  classifyByKeywords            data/gold.go:541
  extractSymbols                data/gold.go
  countMatches                  data/gold.go

与原实现的差异（明确声明）：
  * 本服务不直接写库；去重状态由调用方通过 `seen_hashes` 传入传出，
    这样无状态部署也能做跨请求近重复判定。
  * LLM 分类不在服务内强制开启：未配置 base_url 时自动回落到规则分类器
    （与 Athena 的 FallbackClassifier 行为一致：LLM 优先，失败降级规则）。
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Optional

# ── 常量（对齐 data/silver.go:52）────────────────────────────────────
SIMHASH_RADIUS = 3                  # 判定重复的最大汉明距离
SIMHASH_RETENTION_SEC = 24 * 3600   # 去重指纹保留窗口

FNV64_OFFSET = 0xCBF29CE484222325
FNV64_PRIME = 0x100000001B3
MASK64 = (1 << 64) - 1


# ── SimHash（对齐 Go：2-gram + FNV-64a，重复 gram 只计一次）────────────

def _fnv1a_write(h: int, data: bytes) -> int:
    for b in data:
        h ^= b
        h = (h * FNV64_PRIME) & MASK64
    return h


def sim_hash(text: str) -> int:
    """对齐 Go SimHash(data/silver.go:22)。

    注意 Go 的写法很特别：key 用 `rune<<32 | rune` 去重，但写入 FNV 的是
    `[]byte{byte(rune_i), byte(rune_j)}` —— **只取每个 rune 的低 8 位**。
    照搬这个语义（而不是"聪明地"改成完整 UTF-8），否则指纹与 athena 不一致。
    """
    h = FNV64_OFFSET
    runes = list(text)
    seen: set[int] = set()
    for i in range(len(runes) - 1):
        a, b = ord(runes[i]), ord(runes[i + 1])
        key = ((a << 32) | b) & MASK64
        if key in seen:
            continue
        seen.add(key)
        h = _fnv1a_write(h, bytes([a & 0xFF, b & 0xFF]))
    return h


def hamming_distance(a: int, b: int) -> int:
    """对齐 Go HammingDistance（Brian Kernighan 计数）。"""
    x = (a ^ b) & MASK64
    dist = 0
    while x:
        x &= x - 1
        dist += 1
    return dist


def is_near_dup(h: int, seen: list[int], radius: int = SIMHASH_RADIUS) -> bool:
    """对齐 Go isDup：与任一已见指纹的汉明距离 <= radius 即视作重复。"""
    return any(hamming_distance(h, s) <= radius for s in seen)


# ── 数据模型 ──────────────────────────────────────────────────────────

@dataclass
class RawArticle:
    source: str = ""
    channel: str = ""
    title: str = ""
    content: str = ""
    url: str = ""
    pub_time: str = ""
    external_id: str = ""


@dataclass
class SilverArticle:
    """对齐 data/silver.go:58 SilverArticle。"""
    raw: RawArticle
    event_type_l0: str = ""
    event_type_l1: str = ""
    event_type_l2: str = ""
    sentiment: float = 0.0
    confidence: float = 0.0

    # 便捷透传
    @property
    def title(self) -> str:
        return self.raw.title

    @property
    def content(self) -> str:
        return self.raw.content

    @property
    def source(self) -> str:
        return self.raw.source


@dataclass
class GoldEvent:
    """对齐 data/gold.go:22 GoldEvent。"""
    event_type: str = ""
    title: str = ""
    description: str = ""
    symbols: list[str] = field(default_factory=list)
    sectors: list[str] = field(default_factory=list)
    confidence: float = 0.0
    sentiment: float = 0.0
    source_title: str = ""
    pub_time: str = ""


# ── Silver：去重 ──────────────────────────────────────────────────────

def silver_process(articles: list[RawArticle],
                   seen_hashes: Optional[list[int]] = None,
                   radius: int = SIMHASH_RADIUS) -> tuple[list[SilverArticle], list[int], dict]:
    """对齐 Go SilverPipeline.Process（data/silver.go:100）。

    去重状态以 seen_hashes 传入传出 —— 服务无状态，调用方可持久化它做跨请求去重。
    返回 (silver_articles, 更新后的 seen_hashes, 统计)。
    """
    seen = list(seen_hashes or [])
    out: list[SilverArticle] = []
    dup = 0
    seen_now = set(seen)
    for a in articles:
        h = sim_hash((a.title or "") + (a.content or ""))
        if is_near_dup(h, seen, radius):
            dup += 1
            continue
        seen.append(h)
        seen_now.add(h)
        out.append(SilverArticle(raw=a))
    stats = {"input": len(articles), "output": len(out), "deduped": dup,
             "seen_total": len(seen)}
    return out, seen, stats


# ── Gold：规则分类（对齐 classifyByKeywords）──────────────────────────

_ET_CORPORATE = ("业绩", "营收", "利润", "净利润", "亏损", "盈利", "财报", "预告")
_ET_MACRO = ("央行", "降息", "加息", "利率", "LPR", "存款准备金", "政策", "监管", "证监会")
_ET_TRADE = ("关税", "制裁", "出口管制", "贸易战", "反倾销", "WTO")
_ET_GEO = ("战争", "冲突", "地缘", "制裁", "军事", "外交")
_ET_INDUSTRY = ("光伏", "新能源", "芯片", "半导体", "AI", "汽车", "医药", "消费", "电力")
_ET_MARKET = ("大盘", "指数", "熔断", "暴跌", "暴涨", "股灾", "牛市", "熊市")

# 板块关键词表（对齐 gold.go 的 sectorMap；dict 保持插入序不影响结果）
SECTOR_MAP: dict[str, str] = {
    "光伏": "光伏", "新能源": "光伏", "太阳能": "光伏", "储能": "光伏",
    "芯片": "芯片半导体", "半导体": "芯片半导体", "晶圆": "芯片半导体",
    "AI": "AI算力", "人工智能": "AI算力", "算力": "AI算力",
    "电力": "电力", "电网": "电力", "绿电": "电力", "风电": "电力",
    "汽车": "汽车", "锂电": "锂电池", "电池": "锂电池",
    "医药": "医疗", "药": "医疗",
    "消费": "消费电子", "电子": "消费电子",
    "光通信": "光通信", "光纤": "光纤光缆", "光模块": "光通信",
    "PCB": "覆铜板/PCB", "覆铜板": "覆铜板/PCB",
    "机器人": "机器人",
}

_POS_WORDS = ("增长", "上涨", "利好", "突破", "扩产", "中标", "盈利", "回购")
_NEG_WORDS = ("下跌", "亏损", "利空", "制裁", "衰退", "暴雷", "违约", "退市")


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(k in text for k in keywords)


def extract_symbols(text: str) -> list[str]:
    """对齐 Go extractSymbols：滑动 8 字符窗口，取 sh60*/sz00* 形态。

    注意 Go 是**逐字节**滑窗（`text[i:i+8]` 在 Go 里是字节切片），这里按
    普通字符串滑窗；对纯 ASCII 股票代码等价，中文前缀场景下结果可能多出
    少量错位候选，但都通过 sh60/sz00 前缀过滤，不产生假代码。
    """
    syms: list[str] = []
    seen: set[str] = set()
    for i in range(max(0, len(text) - 7)):
        sub = text[i:i + 8]
        if (sub.startswith("sh60") or sub.startswith("sz00")) and sub not in seen:
            syms.append(sub)
            seen.add(sub)
    return syms


def _count_matches(text: str, keywords: tuple[str, ...]) -> int:
    return sum(text.count(k) for k in keywords)


def classify_by_keywords(text: str) -> GoldEvent:
    """对齐 Go classifyByKeywords（data/gold.go:541），含判定顺序与截断长度。"""
    evt = GoldEvent()
    if _contains_any(text, _ET_CORPORATE):
        evt.event_type = "CORPORATE"
    elif _contains_any(text, _ET_MACRO):
        evt.event_type = "MACRO_POLICY"
    elif _contains_any(text, _ET_TRADE):
        evt.event_type = "TRADE"
    elif _contains_any(text, _ET_GEO):
        evt.event_type = "GEOPOLITICAL"
    elif _contains_any(text, _ET_INDUSTRY):
        evt.event_type = "INDUSTRY"
    elif _contains_any(text, _ET_MARKET):
        evt.event_type = "MARKET"

    runes = list(text)
    evt.title = "".join(runes[:80]) if len(runes) > 80 else text
    evt.description = "".join(runes[:200]) if len(runes) > 200 else text
    evt.symbols = extract_symbols(text)

    seen: set[str] = set()
    for kw, sec in SECTOR_MAP.items():
        if kw in text and sec not in seen:
            evt.sectors.append(sec)
            seen.add(sec)

    pos = _count_matches(text, _POS_WORDS)
    neg = _count_matches(text, _NEG_WORDS)
    evt.sentiment = (pos - neg) / (pos + neg + 1)
    return evt


def gold_process(articles: list[SilverArticle], *,
                 min_confidence: float = 0.0,
                 llm_classifier=None) -> tuple[list[GoldEvent], dict]:
    """对齐 Go RuleClassifier.ClassifyArticles + GoldPipeline.Process。

    无事件类型（classify_by_keywords 判不出）的文章被丢弃 —— 与 Go 一致。
    confidence=0.3 是 Go 里写死的"规则法置信度偏低"标记。
    llm_classifier 若非 None 优先使用（对齐 FallbackClassifier：LLM 优先、失败降级）。
    """
    events: list[GoldEvent] = []
    no_type = 0
    llm_used = False
    llm_failed = False

    if llm_classifier is not None:
        try:
            got = llm_classifier(articles)
            if got is not None:
                events = list(got)
                llm_used = True
        except Exception:
            llm_failed = True

    if not llm_used:
        for a in articles:
            text = (a.title or "") + " " + (a.content or "")
            et = classify_by_keywords(text)
            if not et.event_type:
                no_type += 1
                continue
            et.confidence = 0.3          # 规则法固定 0.3（对齐 Go）
            et.sentiment = et.sentiment
            et.source_title = a.title
            et.pub_time = a.raw.pub_time
            events.append(et)

    events = [e for e in events if e.confidence >= min_confidence]
    stats = {"input": len(articles), "events": len(events),
             "dropped_no_type": no_type, "llm_used": llm_used,
             "llm_failed": llm_failed}
    return events, stats
