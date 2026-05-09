---
name: ask-q1-industry-prospect
description: '七看八问·八问 Q1：行业前景与市场规模。Use when: 八问Q1, 行业前景, 行业周期, 市场规模, 产业政策, 申万分类。调用 company-analysis-mcp 工具自动评级。'
argument-hint: '输入股票代码（如 000002.SZ）。'
user-invocable: true
---

# 八问 Q1：行业前景

## 原文问句

> **Q1：行业是否处于上升周期？市场规模有多大？**
>
> **关键点**：避免"逆风而行"，如分析光伏行业需关注政策与技术迭代。

## 调用方式

调用 MCP 工具 `ask_q1_industry_prospect`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| question_id | 1 |
| question_title | "行业前景" |
| rating | 1-5 评级 |
| status | ready / partial / insufficient-evidence |
| answer | 分析文本 |
| evidence[] | 证据列表（source_type, excerpt, source_url） |
| rating_signals | 评级触发信号 |

## 评级规则

### 信号定义

- **情绪净值**：对 REGULATORY + INDUSTRY_REPORT 证据做关键词计数
  - 正向：支持/鼓励/扶持/利好/高景气/持续增长/龙头/升级/加快发展
  - 负向：限制/去产能/替代/萎缩/衰退/过剩/下行/淘汰/严控
  - `sentiment_net = pos_hits - neg_hits`
- **政策密度**：REGULATORY 证据条数 `policy_count`
- **研报密度**：INDUSTRY_REPORT 证据条数 `report_count`

### 评级映射

| rating | 触发条件 |
|:------:|:---------|
| 5 | net >= +6 且 policy_cnt >= 2 |
| 4 | net >= +3（未达5级） |
| 3 | -2 <= net <= +2（基线） |
| 2 | net <= -3（未达1级） |
| 1 | net <= -6 且 policy_cnt >= 2 |

### 前提门槛

- 需有 DB 事实（idx_sw_l3_peers 成功映射申万 L2）
- 需有研报观点（report_cnt >= 1）
- has_factual & !has_view → status = partial
- 其它 → status = insufficient-evidence

## 合格证据定义

- 行业协会/统计局/监管政策原文 + 近 3 年市场规模数据
- 至少 1 条 primary/regulatory/db + 1 条 industry_report 才允许 rating

## 反面信号

- 行业 CAGR 连续 2 年 < 0
- 政策明确限制扩张（如光伏产能、房地产"三道红线"）

## 输出要求

向用户呈现：
- rating 及理由
- 关键证据摘要（标注来源类型和权重）
- 行业分类定位（申万 L1/L2/L3）
- 证据缺口提示

## 交叉校验

- 与 look-03（增长趋势）对比：行业景气但公司增长停滞需警惕

## 何时暂停

- 希望引入本 SKILL 未列的行业数据源（如 iFinD/Wind）
- 证据命中反面信号但用户要求 rating >= 4
- 非 A 股标的或行业分类缺失
