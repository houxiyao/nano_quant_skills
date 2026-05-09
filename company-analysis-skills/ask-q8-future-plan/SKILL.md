---
name: ask-q8-future-plan
description: '七看八问·八问 Q8：未来规划与战略兑现率。Use when: 八问Q8, 未来规划, 战略, 业绩预告兑现率, IR调研, 规划执行闭环。调用 company-analysis-mcp 工具自动评级。'
argument-hint: '输入股票代码（如 000002.SZ）。'
user-invocable: true
---

# 八问 Q8：未来规划

## 原文问句

> **Q8：企业战略目标是否清晰？执行路径是否可行？**
>
> **关键点**：验证"规划-执行-结果"闭环，避免"画饼"。

## 调用方式

调用 MCP 工具 `ask_q8_future_plan`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| question_id | 8 |
| question_title | "未来规划" |
| rating | 1-5 评级 |
| status | ready / partial / insufficient-evidence |
| evidence[] | fin_forecast、fin_express 等 |

## 合格证据定义

- fin_forecast / fin_express 兑现率 + IR 调研纪要 + 年报"未来展望"
- **必须对比往年承诺 vs 实际兑现**（不是单看当期预告）

## 兑现率计算

- 兑现率 = 实际 n_income_attr_p / ((net_profit_min + net_profit_max) / 2)

## 反面信号

- 过去 3 年业绩预告偏差率 > 30%（预告上限 vs 实际净利润）
- 战略表述连年变更且无兑现
- IR 调研纪要长期缺失（>= 2 年无公开纪要）

## 输出要求

向用户呈现：
- 过去 3 年兑现率表格
- 最新业绩预告/快报摘要
- 战略执行评估
- IR 纪要缺失 → status=partial

## 交叉校验

- look-03（增长趋势）：CAGR 与预告方向是否一致

## 何时暂停

- 战略涉及重大并购/分拆上市（需专项评估）
- 业绩预告口径变更（并表范围调整），需人工判定可比性
