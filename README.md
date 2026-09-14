# news-mcp

A股**新闻链路**的独立 MCP 服务 —— 从 [Athena](https://github.com/JingxuanC/Athena)
的 `internal/data`（Bronze→Silver→Gold，约 2100 行 Go）抽取。

## 部署定位（重要）

**本服务独立运行，不回灌 athena.db。** 数据落在自己的 `news.db`：

```
news-mcp (news.db)  ──JSON──▶  causal-chain-mcp  （由调用方/编排层搬运）
```

选择独立而非回灌的原因：
- athena.db 的历史 Bronze 有大量重复（46839 行仅 1436 个唯一键），
  建唯一索引会失败，只能退化为应用层去重
- SQLite 单写者：两个进程同时写一个库容易 `database is locked`
- 不需要动 athena 的任何读取路径

> 若将来要回灌，`db_path` 传 athena.db 即可 —— schema 完全兼容，
> 实测对已有库零改动（`storage.py` 的唯一索引已改为"尽力而为"）。

调度：**当前无内置定时**，按需手动调用工具。

## 抽的是什么

```
Bronze  6 个采集源 → RawArticle
        wallstcn / sina / cls / lhb / hot_rank / notice
Silver  SimHash 去重（radius=3，32 位 2-gram + FNV-64a，保留 24h）→ SilverArticle
Gold    规则分类（关键词→事件类型/板块/标的/情感），可选 LLM 优先
        → GoldEvent
```

## 工具（8 个）

| 工具 | 对齐 Athena 的 |
|---|---|
| `list_sources` | `internal/data` 的 6 个 Source |
| `collect_raw` | `BronzeCollector`（`bronze.go:43`） |
| `silver_dedup` | `SilverPipeline.Process`（`silver.go:100`） |
| `gold_classify` | `RuleClassifier.ClassifyArticles`（`gold.go:526`）/ `classifyByKeywords`（`gold.go:541`） |
| `run_pipeline` | `runPipelineTick` 的前三段（`engine/pipeline.go:101`） |
| `db_stats` | `pipeline_runs` 记账口径 | |
| **`get_events`** | **读侧：不触发采集** | 智能体消费 Gold 事件的主入口 |
| **`get_articles`** | **读侧：不触发采集** | 回溯 Silver 层带情感分新闻 |

## 持久化（可选，默认关闭）

`persist=true` 时落库，schema 与 athena **完全兼容**（4 张表字段逐个对照过）：

| 表 | 层 | 说明 |
|---|---|---|
| `news_articles_raw` | Bronze | + `retry_count`；`pub_time` 存 RFC3339 |
| `news_articles` | Silver | 含 `raw_id` 外键 / `symbols`/`sectors` 存 CSV |
| `market_events` | Gold | 含 `event_time` / `created_at` |
| `pipeline_runs` | 记账 | bronze/silver/gold 计数 + `duration_ms` |
| `news_simhash` | **新增** | SimHash 指纹持久化（athena 用 Redis，这里落表） |

库路径：`db_path` 参数 → 否则 `NEWS_MCP_DB` → 否则服务目录 `news.db`。
**刻意不默认写 `athena.db`**，避免这个服务污染生产库。

### 与 athena 的两处有意偏差

1. **Bronze 默认幂等去重**。athena 的 `InsertRawArticle` 是纯 INSERT 不去重——实测
   `news_articles_raw` 46839 行里只有 **1436 个唯一 `(source, external_id)`**，
   `eastmoney_notice` 更是 19746 行挤在 1 个 id 上。这里按 `(source, external_id)`
   去重（`dedup=false` 可退回原行为）。

2. **SimHash 指纹落表**。athena 靠 Redis `SimHashBackend` 做跨 tick 判重，而其
   Python 侧那层是断的 → 同一条新闻每轮都会重新写进 Silver。这里落表并自动载入
   保留窗口（24h）内的指纹，**跨调用判重真的生效**：实测第 2 次跑
   `bronze+0 / silver+0`，库不再膨胀。

> ⚠️ SimHash 是 unsigned 64 位，SQLite INTEGER 是 **signed** 64 位，直接存会
> `OverflowError`。存储时 mask 到低 63 位（对 radius=3 的判重等价）。

## 与 athena 的边界

- **不直接写库**：去重状态以 `seen_hashes` 传入传出，服务可无状态部署，
  调用方自行持久化（对齐 athena 用 Redis 持久化 simhash 的设计意图）。
- **LLM 分类不强制**：未传 `llm_base_url` 时自动走规则分类器，
  与 athena 的 `FallbackClassifier`（LLM 优先、失败降级规则）行为一致。
- 规则法置信度固定 **0.3**（athena 里写死的"规则法偏低"标记，未改）。

## 供其他智能体调用

标准 MCP over HTTP，**任何 MCP 客户端都能直接接**（Claude Desktop / Kimi Code /
Cursor / 自研 Agent / DSH）：

```yaml
news:
  url: http://127.0.0.1:50059/mcp
```

```bash
python3 server.py --port 50059
curl http://127.0.0.1:50059/health
```

智能体的典型用法（**先读，需要新鲜数据再采**）：

| 意图 | 调用 |
|---|---|
| 看有哪些板块相关的事件 | `get_events {"sector":"AI算力"}` |
| 看某类事件（政策/公司/贸易） | `get_events {"event_type":"MACRO_POLICY"}` |
| 看某标的的新闻 | `get_events {"symbol":"sz000858"}` / `get_articles` |
| 数据太旧、要拉新的 | `run_pipeline {"sources":[...],"persist":true}` |
| 查库状态 | `db_stats` |

工具描述里写明了各自的 `required` 参数，**智能体不需要读源码就能选对工具**
（`tests/test_agent_consumption.py` 锁定了这条路径）。

### 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `NEWS_MCP_DB` | 服务目录 `news.db` | 落库位置；智能体也可用 `db_path` 参数覆盖 |
| `PORT` | 50059 | 监听端口 |

## 用法

```bash
python3 server.py --port 50059
make serve   # 或直接跑上面的命令
```

## 测试

```bash
python3 tests/test_core.py               # 39 项：SimHash/去重/分类/情感
python3 tests/test_storage.py            # 20 项：schema 兼容/幂等/signed 边界
python3 tests/test_mcp_e2e.py            # 11 项：MCP 协议端到端
python3 tests/test_agent_consumption.py  # 15 项：第三方智能体消费路径
```

## ⚠️ 移植时注意到的语义（已照搬，未"修正"）

`SimHash` 的 Go 实现有个不直观的地方：指纹去重用的是完整 rune，但**写进 FNV
的只有每个 rune 的低 8 位**（`[]byte{byte(runes[i]), byte(runes[i+1])}`）。
中文 rune 的低 8 位会撞，所以这个指纹对中文文本的区分度弱于常规 SimHash。
本服务照搬该语义以保证与 athena 指纹一致；若要提升去重质量需两边同时改。
