---
name: ask-q2-moat
description: '七看八问·八问 Q2：竞争优势与护城河。Use when: 八问Q2, 竞争优势, 护城河, 品牌壁垒, 技术壁垒, 成本优势, 毛利率, MD&A, fin_mainbz. 强制 evidence harness，单独可执行、可实测。'
argument-hint: '输入股票代码；可选分析日期、输出目录。默认运行本 skill 自带 scripts/q02_moat.py。'
user-invocable: true
---

# Q2 独立 Skill —— 竞争优势

本 skill 只负责八问的 Q2：**竞争优势**（护城河）。它与其它 `ask-qN` 是 `2min-company-analysis/` 目录树下的同级子目录技能；运行时依赖 `2min-company-analysis/seven-look-eight-question/scripts/` 下的共享 harness，因此必须在保留完整目录树的前提下在仓库根目录执行。

## 原文问句

> **Q2：企业核心竞争力是什么？是品牌、技术还是成本优势？**
>
> **关键点**：识别"护城河"是否可持续，如贵州茅台的品牌壁垒。

## 适用场景

- 单独实测 Q2 的证据采集与评分逻辑
- 调整 MD&A 抓取策略或 `fin_mainbz` 毛利结构指标口径
- SQL 复核主营构成的多年趋势
- 单独排查 Q2 输出是否异常

## 合格证据定义（Agent 必须对齐）

- 年报 MD&A / 主营毛利结构（`fin_mainbz`）/ 专利与商标登记证据
- 必须能回答"对手为什么复制不了"
- 至少 1 条 `primary/db` 支撑，方可 rating

## 反面信号（命中则降分并记 cross_validation_flags）

- 毛利率连续 3 年下降
- 所谓护城河仅来自行政许可 / 关联交易 / 单一大客户

## 输入参数

- `--ts-code`（必填）、`--as-of-date`（可选）、`--output-dir`（可选）、`--duckdb-path`（可选）

## 执行方式

```bash
# 在仓库根目录执行
python 2min-company-analysis/ask-q2-moat/scripts/q02_moat.py \
    --ts-code 000002.SZ --output-dir /tmp/ask_q2
```

## 核心证据数据源

| 类型 | 来源 |
| --- | --- |
| 外部·MCP | `fetch_reports(annual, fetch_content=True)` → MD&A 正文 |
| 结构化·DB | `fin_mainbz`（主营业务分部收入/毛利） |

## 输出要求

- 默认产出 `q02_moat.json` / `q02_moat.md`；JSON 顶层固定包含 `ts_code`、`as_of_date`、`question_id`、`question_title`、`answer`
- `answer` 文本先复述关键点再给结论
- 若 `look-01` 已判定"毛利率连续 3 年下滑" → 本问必须把 rating 下调并在 `cross_validation_flags` 留痕

## 何时暂停并讨论

- 用户希望纳入专利/商标数据库（当前未接入）
- MD&A 年报抓取失败但用户要求 rating≥4
