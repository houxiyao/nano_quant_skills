---
name: look-07-roe-capital-return
description: '七看八问规则7：七看收益率与资本回报。Use when: 规则7, 七看收益率与资本回报, ROE, ROA, 杜邦分析, DuPont三要素拆解, 销售净利润率, 资产周转率, 权益乘数, 高ROE来源判断。适合单独迭代、单独排查、单独实测。'
argument-hint: '输入股票代码、分析日期、回看年数。自动通过杜邦三要素拆解ROE，并与行业标杆对比。'
user-invocable: true
---

# 规则7独立 Skill

本 skill 只负责七看八问的第 7 条规则：七看收益率与资本回报。

核心是用杜邦三要素拆解 ROE = 销售净利润率 × 资产周转率 × 权益乘数，分析各环节对整体回报的贡献，并识别高 ROE 是来自真实盈利能力还是过度杠杆。

## 适用场景

- 单独执行规则7
- 杜邦分析（DuPont decomposition）
- 判断 ROE 质量：盈利驱动 vs 杠杆驱动 vs 周转驱动
- ROE/ROA 趋势分析
- 与申万三级行业标杆企业对比
- 对规则7做 SQL 复核、样本实测、code review

## 输入参数

- 股票代码
- 分析日期，可选，默认今天
- 回看年数，可选，默认 5 年
- `--db-path`，可选，DuckDB 路径

## 当前口径

### 杜邦三要素

$$\text{ROE} = \underbrace{\frac{\text{归母净利润}}{\text{营业收入}}}_{\text{销售净利润率 (NPM)}} \times \underbrace{\frac{\text{营业收入}}{\text{平均总资产}}}_{\text{资产周转率 (AT)}} \times \underbrace{\frac{\text{平均总资产}}{\text{平均归母净资产}}}_{\text{权益乘数 (EM)}}$$

1. **销售净利润率 (Net Profit Margin, NPM)**
   - = `n_income_attr_p / revenue`
   - 交叉验证：`fin_indicator.netprofit_margin`

2. **资产周转率 (Asset Turnover, AT)**
   - = `revenue / avg_total_assets`
   - 平均总资产 = (期初total_assets + 期末total_assets) / 2
   - 交叉验证：`fin_indicator.assets_turn`

3. **权益乘数 (Equity Multiplier, EM)**
   - = `avg_total_assets / avg_parent_equity`
   - 平均归母净资产 = (期初 + 期末 total_hldr_eqy_exc_min_int) / 2
   - 等价于 1 / (1 - 资产负债率)（近似）

### ROE 质量判断

根据三要素的相对大小和变动趋势，判断 ROE 驱动类型：

| 驱动类型 | 特征 |
|---|---|
| **盈利驱动 (profitability-driven)** | NPM 高或改善，EM 适中 |
| **杠杆驱动 (leverage-driven)** | EM 高且 NPM 偏低 |
| **周转驱动 (turnover-driven)** | AT 高且持续改善 |
| **混合型 (mixed)** | 多因素均衡 |
| **负ROE** | 亏损状态，不适用分类 |

### 辅助指标

- `roe`：加权平均净资产收益率（fin_indicator，用于核对）
- `roe_dt`：扣非后 ROE
- `roa`：总资产报酬率
- `debt_to_assets`：资产负债率

### 行业标杆对比

1. 通过 `idx_sw_l3_peers` 找到申万三级行业同业列表
2. 取最近交易日 `total_mv` 最大的非自身公司作为标杆
3. 对标杆计算相同的杜邦分解，进行逐项对比

### 通用规则

1. 只取合并报表，即 `report_type='1'`
2. 只取年报，即 end_date 月份为 12、日期为 31
3. 仅使用分析日之前已经可见的数据
4. 金融类公司不适用（银行/保险/证券的杠杆结构与一般工商业差异过大）

## 核心数据来源

| 指标 | 来源表 | 关键字段 |
|---|---|---|
| 归母净利润 | fin_income | n_income_attr_p |
| 营业收入 | fin_income | revenue |
| 总资产 | fin_balance | total_assets |
| 归母净资产 | fin_balance | total_hldr_eqy_exc_min_int |
| ROE/ROA/周转 | fin_indicator | roe, roe_dt, roa, netprofit_margin, assets_turn, debt_to_assets |
| 行业同业 | idx_sw_l3_peers | anchor_ts_code, peer_ts_code, l3_name |
| 总市值 | stk_factor_pro | total_mv |

## 输出

### JSON 模式

```json
{
  "rule_id": "look-07",
  "status": "ready | not-applicable | no-data",
  "stock": "000002.SZ",
  "company_profile": { ... },
  "summary": {
    "years_returned": 5,
    "roe_latest": -55.42,
    "roe_driver": "negative-roe",
    "roe_trend": "deteriorating",
    "npm_latest": -0.3933,
    "at_latest": 0.2024,
    "em_latest": 8.73
  },
  "dupont_rows": [ ... ],
  "indicator_rows": [ ... ],
  "benchmark": { ... },
  "comparison": [ ... ]
}
```

### Markdown 模式

包含 Summary、DuPont Decomposition 表格、Indicator Cross-Check 表格、ROE Driver Analysis、Benchmark Comparison 表格。
