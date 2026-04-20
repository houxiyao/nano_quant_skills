---
name: look-06-input-output-efficiency
description: '七看八问规则6：六看投入产出效率。Use when: 规则6, 六看投入产出效率, 营运资金效率, 固定资产效率, 人均投入产出, 行业标杆对比, 资源利用效率。适合单独迭代、单独排查、单独实测。'
argument-hint: '输入股票代码、分析日期、回看年数。自动通过申万三级行业找到市值最大的标杆企业进行对比。'
user-invocable: true
---

# 规则6独立 Skill

本 skill 只负责七看八问的第 6 条规则：六看投入产出效率。

目标是评估企业每一元收入需要多少营运资金、多少固定资产，以及人均投入产出效率，并与行业标杆对比。

## 适用场景

- 单独执行规则6
- 评估营运资金周转效率（一元收入需要多少营运资金）
- 评估固定资产利用效率（一元收入需要多少固定资产）
- 评估人均投入产出（人均营业收入、人均利润）
- 与申万三级行业内市值最大的标杆企业对比
- 对规则6做 SQL 复核、样本实测、code review

## 输入参数

- 股票代码
- 分析日期，可选，默认今天
- 回看年数，可选，默认最近 3 年
- `--db-path`，可选，DuckDB 路径

## 当前口径

### 核心指标

1. **一元收入需要营运资金(WC)**
   - WC = (应收账款 + 存货 + 预付款项 - 应付账款 - 预收款项 - 合同负债) / 营业收入
   - 字段：`accounts_receiv + inventories + prepayment - acct_payable - adv_receipts - contract_liab`
   - 预收款项 `adv_receipts` 和合同负债 `contract_liab` 取 `COALESCE(contract_liab, 0) + COALESCE(adv_receipts, 0)`（2020年新收入准则后 `adv_receipts` 逐步并入 `contract_liab`）

2. **一元收入需要固定资产**
   - = `fix_assets / revenue`

3. **人均投入产出**
   - 数据库无员工人数字段，使用现金流量表 `c_paid_to_for_empl`（支付给职工及为职工支付的现金）作为人力成本代理
   - 人力成本产出比 = `revenue / c_paid_to_for_empl`
   - 人力成本利润比 = `n_income_attr_p / c_paid_to_for_empl`

4. **辅助周转指标（来自 fin_indicator）**
   - `ar_turn`：应收账款周转率
   - `fa_turn`：固定资产周转率
   - `assets_turn`：总资产周转率
   - `ca_turn`：流动资产周转率
   - 注意：`inv_turn`（存货周转率）在当前数据库中全部为 NULL，不输出

### 行业标杆对比

1. 通过 `idx_sw_l3_peers` 找到目标股票的申万三级行业同业列表
2. 取同业中最近交易日 `total_mv`（总市值）最大的非自身公司作为标杆
3. 标杆查询来源：`stk_factor_pro.total_mv`
4. 对标杆公司计算完全相同的指标，进行逐项对比

### 通用规则

1. 只取合并报表，即 `report_type='1'`
2. 只取年报，即 end_date 月份为 12、日期为 31
3. 仅使用分析日之前已经可见的数据
4. 金融类公司不适用；如果 comp_type 属于银行、保险、证券，直接返回 `not-applicable`

## 核心数据来源

| 指标 | 来源表 | 关键字段 |
|---|---|---|
| 营运资金组件 | fin_balance | accounts_receiv, inventories, prepayment, acct_payable, adv_receipts, contract_liab |
| 固定资产 | fin_balance | fix_assets |
| 营业收入 | fin_income | revenue |
| 归母净利润 | fin_income | n_income_attr_p |
| 人力成本代理 | fin_cashflow | c_paid_to_for_empl |
| 周转指标 | fin_indicator | ar_turn, fa_turn, assets_turn, ca_turn |
| 行业同业 | idx_sw_l3_peers | anchor_ts_code, peer_ts_code, l3_name |
| 总市值 | stk_factor_pro | total_mv |

## 输出

### JSON 模式

```json
{
  "rule_id": "look-06",
  "status": "ready | not-applicable | no-data",
  "stock": "000002.SZ",
  "company_profile": { ... },
  "summary": {
    "years_returned": 4,
    "wc_per_revenue_latest": 0.83,
    "fix_assets_per_revenue_latest": 0.08,
    "revenue_per_labor_cost_latest": 3.2,
    "wc_trend": "improving | deteriorating | stable | insufficient-data"
  },
  "efficiency_rows": [ ... ],
  "benchmark": {
    "ts_code": "001979.SZ",
    "name": "招商蛇口",
    "total_mv": 1234567.0,
    "efficiency_rows": [ ... ]
  },
  "comparison": [ ... ]
}
```

### Markdown 模式

包含 Summary、Efficiency Metrics 表格、Benchmark Comparison 表格、Turnover Indicators 表格。
