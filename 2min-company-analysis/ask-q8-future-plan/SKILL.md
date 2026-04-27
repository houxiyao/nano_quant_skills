---
name: ask-q8-future-plan
description: '七看八问·八问 Q8：未来规划与战略兑现率。Use when: 八问Q8, 未来规划, 战略, 业绩预告兑现率, IR调研, fin_forecast, fin_express, 规划执行闭环. 强制 evidence harness，单独可执行、可实测。'
argument-hint: '输入股票代码；可选分析日期、输出目录。默认运行本 skill 自带 scripts/q08_future.py。'
user-invocable: true
---

# Q8 独立 Skill —— 未来规划

本 skill 只负责八问的 Q8：**未来规划**。它与其它 `ask-qN` 是 `2min-company-analysis/` 目录树下的同级子目录技能；运行时依赖 `2min-company-analysis/seven-look-eight-question/scripts/` 下的共享 harness，因此必须在保留完整目录树的前提下在仓库根目录执行。

## 原文问句

> **Q8：企业战略目标是否清晰？执行路径是否可行？**
>
> **关键点**：验证"规划-执行-结果"闭环，避免"画饼"。

## 适用场景

- 单独实测 Q8 的预告兑现率计算
- 调整 IR 调研纪要抓取窗口
- 排查"战略连年变更"的样本

## 合格证据定义

- `fin_forecast` / `fin_express` 兑现率 + IR 调研纪要 + 年报"未来展望"
- **必须对比往年承诺 vs 实际兑现**（不是单看当期预告）

## 反面信号

- 过去 3 年业绩预告偏差率 >30%（预告上限 vs 实际净利润）
- 战略表述连年变更且无兑现
- IR 调研纪要长期缺失（≥2 年无公开纪要）

## 输入参数

- `--ts-code`（必填）、`--as-of-date`（可选）、`--output-dir`（可选）、`--duckdb-path`（可选）

## 执行方式

```bash
# 在仓库根目录执行
python 2min-company-analysis/ask-q8-future-plan/scripts/q08_future.py \
    --ts-code 000002.SZ --output-dir /tmp/ask_q8
```

## 核心证据数据源

| 类型 | 来源 |
| --- | --- |
| 外部·MCP | `fetch_ir_meeting_list`、`fetch_reports(annual)`（future_strategy 段落） |
| 结构化·DB | `fin_forecast`（`p_change_min/max`、`net_profit_min/max`、`summary`）、`fin_express` |

## 真实列名约束

- `fin_forecast` 有：`p_change_min`、`p_change_max`、`net_profit_min`、`net_profit_max`、`summary`
- 兑现率 = 实际 `n_income_attr_p` / `(net_profit_min + net_profit_max) / 2`

## 输出要求

- answer 必须显式给出"过去 3 年兑现率"表格
- 若 IR 纪要缺失 → `status=partial` 且 `missing_inputs` 中列出

## 何时暂停并讨论

- 战略涉及重大并购 / 分拆上市（需要单独走专项评估）
- 业绩预告口径变更（如并表范围调整），需人工判定可比性
