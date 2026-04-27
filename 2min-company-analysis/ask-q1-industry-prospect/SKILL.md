---
name: ask-q1-industry-prospect
description: '七看八问·八问 Q1：行业前景与市场规模。Use when: 八问Q1, 行业前景, 行业周期, 市场规模, 产业政策, 申万L1L2L3, 光伏政策. 强制 evidence harness，单独可执行、可实测。'
argument-hint: '输入股票代码；可选分析日期、输出目录。默认运行本 skill 自带 scripts/q01_industry.py。'
user-invocable: true
---

# Q1 独立 Skill —— 行业前景

本 skill 只负责八问的 Q1：**行业前景**。它与其它 `ask-qN` 是 `2min-company-analysis/` 目录树下的同级子目录技能；运行时依赖 `2min-company-analysis/seven-look-eight-question/scripts/` 下的共享 harness（`eight_questions_domain.py` 等），因此必须在保留完整目录树的前提下在仓库根目录执行。

## 原文问句

> **Q1：行业是否处于上升周期？市场规模有多大？**
>
> **关键点**：避免"逆风而行"，如分析光伏行业需关注政策与技术迭代。

## 适用场景

- 单独实测 Q1 的证据采集与评分逻辑
- 调整 Q1 的 evidence harness（行业分类、研报/政策权重、阈值）
- SQL 复核 `idx_sw_l3_peers` 申万分类映射
- 单独排查 Q1 输出是否异常

## 合格证据定义（Agent 必须对齐）

- 行业协会/统计局/监管政策原文 + 近 3 年市场规模数据
- 必须标注数据截止时点
- 至少 1 条 `primary/regulatory/db` + 1 条 `industry_report` 才允许 rating

## 反面信号（命中则降分并记 cross_validation_flags）

- 行业 CAGR 连续 2 年 <0
- 政策明确限制扩张（如光伏产能、房地产"三道红线"）

## 输入参数

- `--ts-code`（必填）：A 股 Tushare 代码
- `--as-of-date`（可选）：ISO 日期，默认今天
- `--output-dir`（可选）：默认 `logs/look-08/<ts_code>/<YYYYmmdd-HHMM>`
- `--duckdb-path`（可选）：默认 `data/ashare.duckdb`

## 执行方式

```bash
# 在仓库根目录执行
python 2min-company-analysis/ask-q1-industry-prospect/scripts/q01_industry.py \
    --ts-code 000002.SZ --output-dir /tmp/ask_q1
```

## 核心证据数据源

| 类型 | 来源 |
| --- | --- |
| 外部·MCP | `fetch_industry_report_list`（预测·券商观点）、`fetch_industry_policy_list`（gov.cn） |
| 结构化·DB | `idx_sw_l3_peers`（定位申万 L1/L2/L3） |

## 评级规则

> 实际评分算法在 `scripts/q01_industry.py`；下表是**规则语义层**的描述，与代码一致。

### 信号与门槛

- **情绪净值**（`_score_sentiment`）：仅对 `REGULATORY` + `INDUSTRY_REPORT` 两类证据的 `title + excerpt` 做关键词计数。
  - 正向关键词：`支持 / 鼓励 / 扶持 / 利好 / 高景气 / 持续增长 / 龙头 / 升级 / 加快发展`
  - 负向关键词：`限制 / 去产能 / 替代 / 萎缩 / 衰退 / 过剩 / 下行 / 淘汰 / 严控`
  - 记 `sentiment_net = pos_hits - neg_hits`
- **政策密度**：`collect_industry_policies` 返回的 `REGULATORY` 证据条数 `policy_count`。
- **研报密度**：`collect_industry_reports` 返回的 `INDUSTRY_REPORT` 证据条数 `report_count`。

### 评级映射

| rating | 触发条件（对齐 `q01_industry.answer` 实现）                                             |
|:------:|:-----------------------------------------------------------------------------------------|
| 5      | `net ≥ +6` **且** `policy_cnt ≥ 2`                                                       |
| 4      | `net ≥ +3`（未达 5 级）                                                                   |
| 3      | `-2 ≤ net ≤ +2`（基线）                                                                   |
| 2      | `net ≤ -3`（未达 1 级）                                                                   |
| 1      | `net ≤ -6` **且** `policy_cnt ≥ 2`                                                       |

额外门槛（不满足则跳过评级）：

- **有 DB 事实**（`idx_sw_l3_peers` 成功映射申万 L2）
- **有研报观点**（`report_cnt ≥ 1`）
- `has_factual & !has_view` → `status = partial`
- 其它 → `status = insufficient-evidence`

> 所有关键词清单/阈值变更都必须同步更新本节；反向：本节变更必须同步 `_score_sentiment` / `answer` 的评级逻辑。

## 输出要求

- 默认产出 `q01_industry.json` / `q01_industry.md`；JSON 顶层固定包含 `ts_code`、`as_of_date`、`question_id`、`question_title`、`answer`
- `status ∈ {ready, partial, insufficient-evidence}`
- `status=ready` → 至少 1 条合格 Evidence；answer 文本先复述关键点再给结论
- `industry_report` 证据在 Markdown 中自动打 `[预测·券商观点]`

## 何时暂停并讨论

- 希望引入本 SKILL 未列的行业数据源（如 iFinD/Wind）
- 证据命中反面信号但用户要求强行 rating≥4
- 非 A 股标的或行业分类缺失（`idx_sw_l3_peers` 返回空）
