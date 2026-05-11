---
name: look-06-efficiency
description: '七看八问·第6看：投入产出效率。Use when: 六看投入产出效率, 营运资金效率, 固定资产效率, 人均投入产出, 行业标杆对比, 周转率。调用 company-analysis-mcp 工具。'
argument-hint: '输入股票代码（如 000002.SZ），可选分析日期和回看年数。'
user-invocable: true
---

# 第6看：投入产出效率

## 职责

评估企业每一元收入需要多少营运资金、多少固定资产，以及人力成本产出效率，并与行业标杆对比。

## 调用方式

调用 MCP 工具 `look_06_input_output_efficiency`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |
| as_of_date | string | | 分析截止日 YYYY-MM-DD，默认今天 |
| lookback_years | int | | 回看年数，默认 3 |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| status | `ready` / `not-applicable` / `partial` / `no-data` |
| summary | WC/revenue、fix_assets/revenue、人力产出比、趋势 |
| efficiency_rows | 逐年效率指标 |
| benchmark | 标杆公司同指标 |
| comparison | 逐项对比表 |

## 分析框架

1. 营运资金/收入 = (应收+存货+预付-应付-预收-合同负债) / 营业收入
2. 固定资产/收入 = fix_assets / revenue
3. 人力成本产出 = revenue / c_paid_to_for_empl（非"人均"，是单位人力成本产出）
4. 辅助周转指标：ar_turn、fa_turn、assets_turn、ca_turn
5. 标杆选取：同一申万 L3 行业中总市值最大的非自身公司
6. 金融类公司不适用

**重要**：真实人均指标需要员工总数（数据库无此字段），per_capita_status 可能为 human-in-loop-required。

## 关键指标与红旗信号

- WC/revenue 趋势（improving/deteriorating/stable）
- **红旗**：WC/revenue 连续恶化且显著高于标杆
- **红旗**：应收账款周转率持续下降
- 与标杆差距分析

## 输出要求

向用户呈现：
- 效率指标逐年表
- 标杆对比表
- 趋势判断
- 人均口径缺失提示（若需员工数）

## 与其他维度的关联

- look-04（业务分布）：同行标杆池共享
- look-07（ROE）：资产周转率是杜邦分解要素

## 何时暂停

- 用户要求真实人均但未提供员工总数
- 标杆公司行业分类不合适需手动指定
