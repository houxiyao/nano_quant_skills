---
name: look-03-growth-trend
description: '七看八问·第3看：增长率趋势。Use when: 三看增长率趋势, 营业收入CAGR, 归母净利润CAGR, 增长质量, 内生增长, 并购增长。调用 company-analysis-mcp 工具。'
argument-hint: '输入股票代码（如 000002.SZ），可选分析日期和回看年数。'
user-invocable: true
---

# 第3看：增长率趋势

## 职责

计算营收与净利润的复合增长率（CAGR），并用代理信号区分增长质量是"内生增长"还是"并购驱动"。

## 调用方式

调用 MCP 工具 `look_03_growth_trend`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |
| as_of_date | string | | 分析截止日 YYYY-MM-DD，默认今天 |
| lookback_years | int | | 回看年数，默认 3 |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| status | `ready` / `not-applicable` / `no-data` |
| summary | CAGR 值、增长模式判断 |
| rows | 逐年营收/利润/并购代理数据 |

## 分析框架

1. CAGR 只在起点和终点都为正数时计算；任一端非正则返回 null 并保留原因
2. 并购代理信号：商誉增长(goodwill_change)、取得子公司支付现金净额(n_disp_subs_oth_biz)
3. 未出现并购信号 → likely-endogenous；出现 → acquisition-assisted-or-mixed
4. 金融类公司不适用

## 关键指标与红旗信号

- 营业收入 CAGR（正/负/不可计算）
- 归母净利润 CAGR
- 并购代理信号出现次数
- 增长模式判断（内生/并购辅助/混合）
- **红旗**：利润 CAGR 远高于营收 CAGR 但有大额并购

## 输出要求

向用户呈现：
- CAGR 计算结果及不可计算原因
- 逐年增长数据表
- 并购信号分析
- 增长质量判断

## 与其他维度的关联

- look-01（利润质量）：营收/利润增速数据共享
- look-05（资产负债）：并购导致商誉增长与负债关联
- ask-q6（业务模式）：增长模式辅助判断第二曲线

## 何时暂停

- 用户希望把"并购增长"升级为强结论
- 用户希望加入非结构化信息（并购公告、管理层讨论）
- CAGR 在亏损转盈利场景下需要特殊算法
