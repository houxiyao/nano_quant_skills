---
name: look-05-balance-sheet-health
description: '七看八问·第5看：资产负债健康度。Use when: 五看资产负债健康度, 经营现金流覆盖, 有息负债, 偿债能力, 资产负债率, 隐性负债, 杠杆趋势。调用 company-analysis-mcp 工具。'
argument-hint: '输入股票代码（如 000002.SZ），可选分析日期和回看年数。'
user-invocable: true
---

# 第5看：资产负债健康度

## 职责

评估企业现金流覆盖能力、有息负债水平、偿债能力趋势，并对隐性负债（担保/表外融资）取证。

## 调用方式

调用 MCP 工具 `look_05_balance_sheet_health`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |
| as_of_date | string | | 分析截止日 YYYY-MM-DD，默认今天 |
| lookback_years | int | | 回看年数，默认 3 |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| status | `ready` / `not-applicable` / `partial` / `human-in-loop-required` |
| summary | 杠杆趋势、CapEx 覆盖度、偿债能力摘要 |
| rows | 逐年负债/现金流/偿债指标 |
| hidden_liability_status | 隐性负债取证状态 |

## 分析框架

### 第一层：结构化指标（自动）

1. CapEx 覆盖度：ocf_minus_capex = 经营现金流 - 购建固定资产支付现金；>=0 标记 ocf_covers_capex=true
2. 有息负债水平：interestdebt、短期借款+长期借款+应付债券+租赁负债
3. 偿债能力：current_ratio、quick_ratio、cash_ratio、debt_to_assets、ebit_to_interest
4. 现金流偿债：ocf_to_debt、ocf_to_shortdebt、ocf_to_interestdebt
5. 杠杆趋势：assets_to_eqt 逐年变化 → rising/declining/stable/insufficient-data

### 第二层：隐性负债（human-in-loop）

若未提供年报附注，返回 `hidden_liability_status: human-in-loop-required`。

关键词：对外担保、或有事项、表外融资、售后回租、应收账款转让、明股实债。

## 关键指标与红旗信号

- **红旗**：杠杆趋势持续 rising 且 debt_to_assets > 70%
- **红旗**：经营现金流连续为负且 CapEx 无覆盖
- **红旗**：ebit_to_interest < 1.5（利息覆盖不足）
- 隐性负债未取证时降低报告置信度

## 输出要求

向用户呈现：
- 最近 N 年现金流覆盖度表
- 有息负债与偿债能力趋势
- 杠杆变化方向
- 隐性负债提示（需补充年报附注）

## 与其他维度的关联

- look-01（利润质量）：现金流数据共享
- look-07（ROE）：权益乘数与杠杆关联
- ask-q7（风险因素）：质押率与负债风险叠加

## 何时暂停

- 用户希望对金融类公司套用本规则
- 用户希望设定偿债能力自动评分阈值
- 用户希望从 PDF 直接做 OCR 提取隐性负债信息
