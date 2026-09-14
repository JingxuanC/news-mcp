"""news-mcp 核心逻辑测试 —— 每条断言对应 Athena Go 实现的具体行为。"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from core import (RawArticle, sim_hash, hamming_distance, is_near_dup,
                  silver_process, gold_process, classify_by_keywords,
                  extract_symbols, SIMHASH_RADIUS, MASK64)

P=F=0
def ck(n,c,e=""):
    global P,F
    if c: P+=1; print(f"  ✓ {n}")
    else: F+=1; print(f"  ✗ {n} {e}")

print("── SimHash / Hamming ──")
h1 = sim_hash("央行宣布降息")
ck("指纹是 64 位无符号", 0 <= h1 <= MASK64)
ck("同文本指纹稳定", sim_hash("央行宣布降息") == h1)
ck("不同文本指纹不同", sim_hash("央行宣布加息") != h1)
ck("空串不崩", isinstance(sim_hash(""), int))
ck("单字符不崩（无 2-gram）", isinstance(sim_hash("啊"), int))
ck("汉明距离自身=0", hamming_distance(h1,h1)==0)
ck("汉明距离对称", hamming_distance(h1,7)==hamming_distance(7,h1))
ck("汉明距离 全0 vs 全1 = 64", hamming_distance(0, MASK64)==64)
ck("半径 3 判定自重复", is_near_dup(h1,[h1],3))
ck("半径 3 不误判明显不同文本", not is_near_dup(sim_hash("完全不同的另一条新闻内容"),[h1],3))

print("\n── Silver 去重 ──")
arts=[RawArticle(source="s",title="光伏组件涨价",content="光伏组件涨价，硅料价格上行。"),
      RawArticle(source="s",title="光伏组件涨价",content="光伏组件涨价，硅料价格上行。"),  # 完全重复
      RawArticle(source="s",title="半导体设备招标",content="某晶圆厂发布招标公告。")]
out,seen,st = silver_process(arts)
ck("完全重复被剔除（3 进 2 出）", len(out)==2, f"{st}")
ck("统计字段完整", set(st)=={"input","output","deduped","seen_total"}, str(st))
ck("输出指纹数=输出条数", st["seen_total"]==len(seen)==len(out))
out2,seen2,_ = silver_process([RawArticle(title="另一条无关新闻")], seen)
ck("跨请求去重生效（传入 seen 后新增）", len(seen2)==len(seen)+1)
ck("radius=0 时不同长文本不误判",
   not is_near_dup(sim_hash("光伏组件涨价消息"),[sim_hash("半导体设备招标公告")],0))
# ⚠ 算法特性（照搬 Go，非 bug 修复）：少于 2 个字符的文本没有任何 2-gram，
# SimHash 直接返回 FNV 初始值 —— 所有单字符文本指纹相同，会被判为重复。
ck("单字符文本指纹碰撞（Go 同样行为）", sim_hash("甲") == sim_hash("乙") == sim_hash(""))

print("\n── Gold 事件类型（判定顺序与 Go 一致）──")
ck("业绩→CORPORATE", classify_by_keywords("公司发布业绩预告，净利润增长").event_type=="CORPORATE")
ck("降息→MACRO_POLICY", classify_by_keywords("央行宣布降息，LPR 下调").event_type=="MACRO_POLICY")
ck("关税→TRADE", classify_by_keywords("美方加征关税，贸易战升级").event_type=="TRADE")
ck("军事→GEOPOLITICAL", classify_by_keywords("地区冲突升级，军事演习").event_type=="GEOPOLITICAL")
ck("半导体→INDUSTRY", classify_by_keywords("半导体设备国产化提速").event_type=="INDUSTRY")
ck("大盘→MARKET", classify_by_keywords("大盘指数暴跌，熔断").event_type=="MARKET")
ck("优先级：业绩 > 降息（Go 里 CORPORATE 在前）",
   classify_by_keywords("业绩预增，同时央行降息").event_type=="CORPORATE")
ck("无关键词→空类型", classify_by_keywords("今天天气不错").event_type=="")

print("\n── Gold 字段抽取 ──")
e = classify_by_keywords("新能源汽车销量增长，锂电扩产，龙头 sz000858 与 sh600519 受益")
ck("板块命中新能源车相关", any(s in e.sectors for s in ["光伏","锂电池","汽车"]), str(e.sectors))
ck("板块去重", len(e.sectors)==len(set(e.sectors)))
ck("提取 sz00 形态代码", "sz000858" in e.symbols, str(e.symbols))
ck("提取 sh60 形态代码", "sh600519" in e.symbols, str(e.symbols))
ck("标题截断 80 字符", len(e.title)<=80)
ck("描述截断 200 字符", len(e.description)<=200)
ck("情感为正（含增长/扩产）", e.sentiment>0, str(e.sentiment))
neg = classify_by_keywords("公司亏损扩大，股价下跌，遭制裁")
ck("情感为负", neg.sentiment<0, str(neg.sentiment))
ck("情感范围 (-1,1)", -1 < e.sentiment < 1)

print("\n── extractSymbols 边界 ──")
ck("不误取不足 8 字符", extract_symbols("sh60051")==[])
ck("正确取 8 字符", "sh600519" in extract_symbols("xx sh600519 yy"))
ck("去重", len(extract_symbols("sh600519 sh600519"))==1)

print("\n── Gold 过滤（判不出类型的丢弃 + 置信度 0.3）──")
silver=[type("S",(),{"title":"半导体扩产","content":"","raw":RawArticle()})(),
        type("S",(),{"title":"今天天气不错","content":"","raw":RawArticle()})()]
evs,st = gold_process(silver)
ck("判不出类型的被丢弃", len(evs)==1 and st["dropped_no_type"]==1, str(st))
ck("规则法置信度固定 0.3", evs[0].confidence==0.3)
ck("未用 LLM 时 llm_used=False", st["llm_used"] is False)

print(f"\n{'='*46}\n通过 {P} / 失败 {F}")
sys.exit(1 if F else 0)
