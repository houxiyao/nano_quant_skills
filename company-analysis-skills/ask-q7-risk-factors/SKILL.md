---
name: ask-q7-risk-factors
description: '七看八问·八问 Q7：风险因素与应对机制。Use when: 八问Q7, 风险因素, 行政处罚, 诉讼, 股权质押, ST, 供应链风险, 政策风险。调用 company-analysis-mcp 工具自动评级。'
argument-hint: '输入股票代码（如 000002.SZ）。'
user-invocable: true
---

# 八问 Q7：风险因素

## 原文问句

> **Q7：行业政策、技术替代、供应链等风险如何识别与应对？**
>
> **关键点**：关注企业风险应对机制，而非仅看风险披露。

## 调用方式

调用 MCP 工具 `ask_q7_risk_factors`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| question_id | 7 |
| question_title | "风险因素" |
| rating | 1-5 评级 |
| status | ready / partial / insufficient-evidence |
| evidence[] | 质押/ST/处罚等数据 |
| cross_validation_flags | risk 标记 |

## 反面信号

- 被行政处罚且金额 > 当年净利润 10%
- 关键原料单一供应（无备用供应商披露）
- 实控人股权质押率 > 80%
- 进入 ST / *ST

命中任一反面信号 → rating 强制 <= 2，`cross_validation_flags.risk = "red"`

## 质押阈值口径

- `pledge_ratio > 30%` 记重度风险（+2 风险分）
- `pledge_ratio > 10%` 记预警（+1 风险分）
- 此为前瞻风控阈值，非监管定义的 50% 高比例口径

## 输出要求

向用户呈现：
- 每项风险配对"应对措施"或标注"未披露"
- 质押率及历史趋势
- ST 记录
- 综合风险评估

## 交叉校验

- ask-q3（管理团队）：质押率数据共享
- look-05（资产负债）：负债风险叠加

## 何时暂停

- 行业重大政策变动尚未入库
- 境外诉讼/SEC 调查超出数据源范围
