---
name: look-03-growth-trend
description: '七看八问规则3：三看增长率趋势。Use when: 规则3, 三看增长率趋势, 营业收入CAGR, 归母净利润CAGR, 增长质量, 内生增长, 并购增长。适合单独迭代、单独排查、单独实测。'
argument-hint: '输入股票代码、分析日期、回看年数，或说明要修改规则3的 CAGR 与增长质量口径。'
user-invocable: true
---

# 规则3独立 Skill

本 skill 只负责七看八问的第 3 条规则：三看增长率趋势。

目标是稳定地产出营业收入 CAGR、归母净利润 CAGR 与增长质量的结构化证据，并用代理信号区分“更偏内生增长”还是“存在并购驱动迹象”。

## 适用场景

- 单独执行规则3
- 调整 CAGR 口径或增长质量判断逻辑
- 对规则3做 SQL 复核、样本实测、code review
- 排查增长来自经营改善还是并购扩张

## 输入参数

- 股票代码
- 分析日期，可选，默认今天
- 回看年数，可选，默认最近 3 年

## 当前口径

1. 只取合并报表，即 report_type='1'。
2. 只取年报，即 end_date 月份为 12、日期为 31。
3. 仅使用分析日之前已经可见的数据，使用 COALESCE(f_ann_date, ann_date, end_date) 控制可见性。
4. 营业收入 CAGR 使用 revenue 计算；同时展示 total_revenue 作为辅助字段。
5. 归母净利润 CAGR 使用 n_income_attr_p 计算。
6. CAGR 只在起点和终点都为正数时计算；若任一端非正，则返回 null 并保留原因说明。
7. “内生增长 vs 并购增长”不做强判定，使用两类代理信号：商誉增长 goodwill_change，以及取得子公司及其他营业单位支付的现金净额 n_disp_subs_oth_biz。
8. 若窗口内未出现并购代理信号，则标记为 likely-endogenous；若出现并购代理信号，则标记为 acquisition-assisted-or-mixed。
9. 金融类公司不适用；如果 comp_type 属于银行、保险、证券，直接返回 not-applicable。

## 核心证据

- 增长核心：revenue, total_revenue, n_income_attr_p
- 质量辅助：revenue_yoy_calc, net_profit_yoy_calc
- 并购代理：goodwill, goodwill_change, goodwill_to_assets, n_disp_subs_oth_biz, acquisition_cash_to_revenue
- 模式判断：acquisition_signal, growth_mode_signal

## 执行实现

- 脚本入口：./scripts/look_03_growth_trend.py
- 公司类型前检：./scripts/common.py

## 输出要求

- 最近 N 年的增长证据表
- 营业收入 CAGR
- 归母净利润 CAGR
- CAGR 不可计算原因
- 并购代理信号次数
- 增长模式信号与理由
- 如不适用，则明确给出原因

## 何时暂停并讨论

- 用户希望把“并购增长”升级为强结论而不是代理判断
- 用户希望加入分部收入、并购公告、管理层讨论等非结构化信息
- 用户要求对 CAGR 在亏损转盈利场景下定义特殊算法