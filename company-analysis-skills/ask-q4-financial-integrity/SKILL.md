---
name: ask-q4-financial-integrity
description: '七看八问·八问 Q4：财务真实性与会计质量。Use when: 八问Q4, 财务真实性, 会计洞穴, 净现比, 审计意见, 问询函, 立案, ST更名。调用 company-analysis-mcp 工具自动评级。'
argument-hint: '输入股票代码（如 000002.SZ）。'
user-invocable: true
---

# 八问 Q4：财务真实性

## 原文问句

> **Q4：财务数据是否与业务描述一致？**
>
> **关键点**：警惕"会计洞穴"现象，即报表数字与实际业务脱节。

## 调用方式

调用 MCP 工具 `ask_q4_financial_integrity`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| question_id | 4 |
| question_title | "财务真实性" |
| rating | 1-5 评级 |
| status | ready / partial / insufficient-evidence |
| evidence[] | 净现比、更名记录等 |
| cross_validation_flags | financial_integrity 标记 |

## 反面信号

- 净现比连续 2 年 < 0.5（tushare 通用口径 ocf_to_profit）
- 被立案/问询/曾更名至 ST
- 非标审计意见（保留/无法表示/否定）→ rating 强制 <= 2

## 口径差异说明

- Q4 使用 tushare 通用口径 `ocf_to_profit`（分母含少数股东损益），红旗阈值 < 0.3
- look-01 使用归母口径 `n_cashflow_act / n_income_attr_p`，阈值 < 0.5
- 二者同时触发时 → `cross_validation_flags.financial_integrity = "reinforced"`

## 输出要求

向用户呈现：
- 净现比趋势
- 异常信号（审计/问询/立案/更名）
- 与 look-01 交叉校验结论

## 交叉校验

- **look-01**：net_profit_cash_ratio < 0.5 且 Q4 rating <= 2 → financial_integrity = "reinforced"

## 何时暂停

- 需要引入 IPO 以来全部问询函
- 业务复杂如会计处理变更，需人工核对附注
