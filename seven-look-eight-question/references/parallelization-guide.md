# Parallelization Guide

“七看八问”有 15 条规则，但不应该当成一条串行长链。更合理的方式是按数据依赖拆成并行批次，再在最后做汇总。

## 推荐的并行原则

1. 同一批次里的规则尽量共享数据域，减少重复查询。
2. 彼此独立的批次可以并行执行。
3. 汇总结论放在所有批次完成之后。
4. 如果某条规则依赖用户尚未定义的阈值，不要阻塞其它规则。

## 建议的 4 个并行 lane

### Lane A: basic-profile

适合依赖以下数据的规则：

- `stk_info`
- `stk_name_history`
- `stk_st_daily`
- `stk_ah_comparison`

适合回答的问题类型：公司身份、行业属性、上市年限、风险标签、A/H 溢价。

### Lane B: finance-core

适合依赖以下数据的规则：

- `fin_income`
- `fin_balance`
- `fin_cashflow`
- `fin_indicator`

适合回答的问题类型：盈利能力、成长质量、资产负债结构、现金流质量、资本回报。

### Lane C: event-shareholder

适合依赖以下数据的规则：

- `fin_express`
- `fin_forecast`
- `fin_top10_holders`
- `fin_top10_float_holders`

适合回答的问题类型：业绩前瞻、公告信号、股东结构变化、筹码稳定性。

### Lane D: market-validation

适合依赖以下数据的规则：

- `stk_factor_pro`
- `stk_moneyflow`
- `stk_moneyflow_ths`
- `stk_margin`
- `stk_cyq_perf`
- `stk_cyq_chips`
- `idx_*`

适合回答的问题类型：价格趋势、估值位置、资金确认、筹码结构、相对指数强弱。

## 汇总层应该做什么

- 对 15 条规则的结论做矩阵化展示
- 标出每条规则的证据强度和数据缺口
- 避免把多个弱证据叠加成一个过强结论
- 把“无法判断”和“结论偏弱”明确区分

## 注册表中的并行字段

建议在 `assets/rule_registry.json` 里维护 `parallel_lane`：

- `basic-profile`
- `finance-core`
- `event-shareholder`
- `market-validation`
- `unassigned`

在规则定义不完整时，先用 `unassigned`，不要猜。