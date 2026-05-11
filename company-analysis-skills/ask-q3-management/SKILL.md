---
name: ask-q3-management
description: '七看八问·八问 Q3：管理团队与股权结构。Use when: 八问Q3, 管理团队, 高管变动, 股权结构, 持股集中度, 实控人质押。调用 company-analysis-mcp 工具自动评级。'
argument-hint: '输入股票代码（如 000002.SZ）。'
user-invocable: true
---

# 八问 Q3：管理团队

## 原文问句

> **Q3：管理层行业经验、战略眼光与执行能力如何？**
>
> **关键点**：关注团队稳定性与股权结构，避免"一言堂"风险。

## 调用方式

调用 MCP 工具 `ask_q3_management`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| question_id | 3 |
| question_title | "管理团队" |
| rating | 1-5 评级 |
| status | ready / partial / insufficient-evidence |
| evidence[] | stk_managers、fin_top10_holders 等数据 |
| cross_validation_flags | 治理红旗 |

## 合格证据定义

- stk_managers 任期 + fin_top10_holders 集中度 + 高管变动公告
- 至少覆盖董事长/总经理/CFO 三要职的当前在任信息

## 反面信号

- 12 个月内董/总/CFO 同时变动
- 实控人质押率 > 50%
- 前十大股东累计持股 < 20%（控制权真空）

命中反面信号 → `cross_validation_flags.governance = "red"` 并强制降分

## 输出要求

向用户呈现：
- 当前管理层名单及任期
- 股权集中度分析
- 高管稳定性评估
- 红旗提示（若有）

## 交叉校验

- ask-q7（风险因素）：质押率数据共享

## 何时暂停

- 用户要求加入独董/监事会数据
- 实控人为境外/基金结构，股权穿透超出范围
