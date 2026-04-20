---
name: look-05-balance-sheet-health
description: '七看八问规则5：五看资产负债健康度。Use when: 规则5, 五看资产负债健康度, 经营现金流覆盖, 有息负债, 偿债能力, 资产负债率, 隐性负债, 表外融资, 现金流质量。适合单独迭代、单独排查、单独实测。'
argument-hint: '输入股票代码、分析日期、回看年数，可选提供年报附注文本包用于隐性负债取证。'
user-invocable: true
---

# 规则5独立 Skill

本 skill 只负责七看八问的第 5 条规则：五看资产负债健康度。

目标是评估企业现金流状况、有息负债水平和偿债能力，并对隐性负债（未披露担保、表外融资等）做取证。

## 适用场景

- 单独执行规则5
- 判断企业经营现金流能否覆盖投资和融资需求
- 分析有息负债水平、负债结构和偿债能力趋势
- 从年报附注中提取隐性负债（对外担保、表外融资等）证据
- 对规则5做 SQL 复核、样本实测、code review

## 输入参数

- 股票代码
- 分析日期，可选，默认今天
- 回看年数，可选，默认最近 3 年
- 可选年报附注文本包 `--report-bundle`，JSON 文件，供脚本从附注中提取隐性负债证据

## 当前口径

### 第一层：结构化指标（自动计算）

1. 只取合并报表，即 report_type='1'。
2. 只取年报，即 end_date 月份为 12、日期为 31。
3. 仅使用分析日之前已经可见的数据。
4. 现金流覆盖度：经营现金流 vs 投资现金流、筹资现金流。
5. 有息负债水平：interestdebt、st_borr + lt_borr + bond_payable + lease_liab。
6. 偿债能力指标：current_ratio、quick_ratio、cash_ratio、debt_to_assets、debt_to_eqt、ebit_to_interest。
7. 现金流质量：ocf_to_debt、ocf_to_shortdebt、ocf_to_interestdebt。
8. 杠杆趋势：assets_to_eqt 逐年变化。
9. 金融类公司不适用；如果 comp_type 属于银行、保险、证券，直接返回 not-applicable。

### 第二层：隐性负债取证（human-in-loop）

1. 当前数据库不直接提供年报附注中的对外担保、或有事项、表外融资等非结构化信息。
2. 若未提供 `--report-bundle`，脚本在结构化分析之后附加 `hidden_liability_status: human-in-loop-required`。
3. 若提供了年报文本包，脚本用关键词匹配提取相关证据片段。

## 年报附注文本包格式

与 look-04 相同，`--report-bundle` 接收 JSON 文件：

```json
[
  {
    "ts_code": "000002.SZ",
    "name": "万科A",
    "year": 2025,
    "text": "年报附注全文文本"
  }
]
```

## 核心证据

### 结构化

- 经营现金流：n_cashflow_act, free_cashflow
- 投资现金流：n_cashflow_inv_act
- 筹资现金流：n_cash_flows_fnc_act
- 有息负债：interestdebt, st_borr, lt_borr, bond_payable, lease_liab
- 偿债能力：current_ratio, quick_ratio, cash_ratio, debt_to_assets, debt_to_eqt, ebit_to_interest
- 现金流偿债：ocf_to_debt, ocf_to_shortdebt, ocf_to_interestdebt
- 杠杆：assets_to_eqt
- 货币资金：money_cap
- 净负债：netdebt

### 隐性负债关键词

- 对外担保、担保余额、担保总额、被担保方
- 或有事项、或有负债、潜在义务
- 表外安排、表外融资
- 售后回租、融资租赁
- 应收账款转让、保理、出表
- 明股实债、有限合伙、SPV

## 执行实现

- 脚本入口：./scripts/look_05_balance_sheet_health.py
- 公司类型前检：./scripts/common.py

## 输出要求

- 最近 N 年的现金流覆盖度证据表
- 最近 N 年的有息负债与偿债能力证据表
- 杠杆趋势与变化方向
- 隐性负债证据（若提供了年报文本）
- 明确列出任何缺失输入，并触发 human-in-loop
- 如不适用，则明确给出原因

## 何时暂停并讨论

- 用户希望对金融类公司也套用本规则
- 用户希望设定偿债能力的自动评分阈值
- 用户希望从 PDF 或图片直接做 OCR 抽取隐性负债信息
