---
name: ask-q7-risk-factors
description: '七看八问·八问 Q7：风险因素与应对机制。Use when: 八问Q7, 风险因素, 行政处罚, 诉讼, 股权质押, 供应链风险, 技术替代, stk_pledge_stat, stk_st_daily, 政策风险. 强制 evidence harness，单独可执行、可实测。'
argument-hint: '输入股票代码；可选分析日期、输出目录。默认运行本 skill 自带 scripts/q07_risk.py。'
user-invocable: true
---

# Q7 独立 Skill —— 风险因素

本 skill 只负责八问的 Q7：**风险因素**。它与其它 `ask-qN` 是 `2min-company-analysis/` 目录树下的同级子目录技能；运行时依赖 `2min-company-analysis/seven-look-eight-question/scripts/` 下的共享 harness，因此必须在保留完整目录树的前提下在仓库根目录执行。

## 原文问句

> **Q7：行业政策、技术替代、供应链等风险如何识别与应对？**
>
> **关键点**：关注企业风险应对机制，而非仅看风险披露。

## 适用场景

- 单独实测 Q7 的处罚 / 诉讼 / 质押证据
- 调整处罚金额 vs 当年净利润的比例阈值
- 排查年报"风险因素"段落的应对措施完整性

## 合格证据定义

- 监管处罚 / 诉讼 / `stk_pledge_stat` / `stk_st_daily` + 年报"风险因素"段落对应的**应对措施**
- 仅列风险不给应对 → `status=partial`

## 反面信号

- 被行政处罚且金额 > 当年净利润 10%
- 关键原料单一供应（无备用供应商披露）
- 实控人股权质押率 >80%
- 进入 ST / *ST

## 输入参数

- `--ts-code`（必填）、`--as-of-date`（可选）、`--output-dir`（可选）、`--duckdb-path`（可选）

## 执行方式

```bash
# 在仓库根目录执行
python 2min-company-analysis/ask-q7-risk-factors/scripts/q07_risk.py \
    --ts-code 000002.SZ --output-dir /tmp/ask_q7
```

## 核心证据数据源

| 类型 | 来源 |
| --- | --- |
| 外部·MCP | `fetch_announcement_list`（诉讼/处罚）、`fetch_penalty_list` |
| 结构化·DB | `stk_pledge_stat`、`stk_st_daily` |

## 输出要求

- 每一项风险必须配对"应对措施"或显式标注"未披露"
- 命中任一反面信号 → rating 强制 ≤2，`cross_validation_flags.risk = "red"`

## 质押阈值口径说明

- 本 skill 的质押评分采用**前瞻风控阈值**：`pledge_ratio > 30%` 记重度风险（+2），`>10%` 记预警（+1）。
- 说明：监管场景常见“高比例质押”公开口径是 `>50%`，但该口径偏事后识别；Q7 用 `30%` 作为提前预警线，目标是尽早暴露潜在流动性风险。
- 输出中的 `rating_signals` 会显式回写触发条款，避免下游误解为监管定义阈值。

## 何时暂停并讨论

- 行业重大政策变动（如"双碳""反垄断"）尚未入库
- 境外诉讼 / SEC 调查超出本 skill 数据源范围
