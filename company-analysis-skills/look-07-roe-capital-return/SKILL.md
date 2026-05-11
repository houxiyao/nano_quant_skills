---
name: look-07-roe-capital-return
description: '七看八问·第7看：ROE与资本回报。Use when: 七看收益率与资本回报, ROE, ROA, 杜邦分析, DuPont三要素, 销售净利润率, 资产周转率, 权益乘数, 高ROE来源。调用 company-analysis-mcp 工具。'
argument-hint: '输入股票代码（如 000002.SZ），可选分析日期和回看年数（默认5年）。'
user-invocable: true
---

# 第7看：ROE与资本回报

## 职责

用杜邦三要素拆解 ROE = 销售净利润率(NPM) x 资产周转率(AT) x 权益乘数(EM)，判断 ROE 质量（盈利驱动 vs 杠杆驱动 vs 周转驱动），并与行业标杆对比。

## 调用方式

调用 MCP 工具 `look_07_roe_capital_return`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |
| as_of_date | string | | 分析截止日 YYYY-MM-DD，默认今天 |
| lookback_years | int | | 回看年数，默认 5 |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| status | `ready` / `not-applicable` / `no-data` |
| summary | ROE最新值、驱动类型、趋势 |
| dupont_rows | 逐年杜邦分解数据 |
| indicator_rows | ROE/ROA/负债率交叉验证 |
| benchmark | 标杆公司同指标 |
| comparison | 逐项对比表 |

## 分析框架

### 杜邦三要素

- NPM = 归母净利润 / 营业收入
- AT = 营业收入 / 平均总资产
- EM = 平均总资产 / 平均归母净资产

### ROE 驱动类型判断

| 驱动类型 | 特征 |
|---|---|
| profitability-driven | NPM > 10% 且 EM < 4.0 |
| leverage-driven | EM > 5.0 且 NPM < 8% |
| turnover-driven | AT > 1.0 且 NPM < 8% |
| mixed | 多因素均衡 |
| negative-roe | 亏损状态 |

### 标杆对比

同一申万 L3 行业中总市值最大的非自身公司，计算相同杜邦分解进行对比。

## 关键指标与红旗信号

- ROE 趋势（improving/deteriorating/stable）
- **红旗**：ROE 由 leverage-driven 驱动且 EM > 5.0
- **红旗**：ROE 连续下降 3 年以上
- **红旗**：negative-roe 状态
- 与标杆的差距方向

## 输出要求

向用户呈现：
- 杜邦分解逐年表
- ROE 驱动类型分析
- 趋势判断
- 标杆对比结论

## 与其他维度的关联

- look-05（资产负债）：权益乘数与杠杆直接关联
- look-06（效率）：资产周转率是共享指标
- look-01（利润质量）：净利率关联

## 何时暂停

- 用户希望对金融类公司做杜邦分析（需专门框架）
- 用户希望加入 ROIC 等补充指标
