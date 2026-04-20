---
name: look-01-profit-quality
description: '七看八问规则1：一看盈收与利润质量。Use when: 规则1, 一看盈收与利润质量, 盈收质量, 利润质量, 扣非利润, 毛利率, 净利率, 经营现金流。适合单独迭代、单独排查、单独实测。'
argument-hint: '输入股票代码、分析日期、回看年数，或说明要修改规则1的指标口径。'
user-invocable: true
---

# 规则1独立 Skill

本 skill 只负责七看八问的第 1 条规则：一看盈收与利润质量。

目标不是一次性给出主观结论，而是稳定地产出规则1所需的结构化证据，便于单独排查、单独迭代、单独实测。

## 适用场景

- 单独执行规则1
- 调整规则1的数据口径或阈值
- 对规则1做 SQL 复核、样本实测、code review
- 排查规则1输出是否异常

## 输入参数

- 股票代码
- 分析日期，可选，默认今天
- 回看年数，可选，默认最近 3 年

## 当前口径

1. 只取合并报表，即 report_type='1'。
2. 只取年报，即 end_date 月份为 12、日期为 31。
3. 仅使用分析日之前已经可见的数据，使用 COALESCE(f_ann_date, ann_date, end_date) 控制可见性。
4. 营收同时展示 revenue 与 total_revenue。
5. fin_indicator 按最新 COALESCE(ann_date_key, ann_date, end_date) 与 ann_date 去重后再参与分析。
6. 净利率字段优先使用 netprofit_margin，如覆盖率更差则切换为 profit_to_gr；行级缺失时允许 fallback。
7. 金融类公司不适用；如果 comp_type 属于银行、保险、证券，直接返回 not-applicable。

## 核心证据

- 营收：revenue, total_revenue
- 利润：profit_dedt, grossprofit_margin, netprofit_margin, profit_to_gr
- 现金流：n_cashflow_act
- 增长字段：tr_yoy, or_yoy, dt_netprofit_yoy, ocf_yoy

## 执行实现

- 脚本入口：./scripts/look_01_profit_quality.py
- 公司类型前检：./scripts/common.py

## 输出要求

- 数据质量摘要
- 最近 N 年的逐年证据表
- 缺失值统计
- 扣非利润为正的年份数
- 经营现金流为正的年份数
- 如不适用，则明确给出原因

## 何时暂停并讨论

- 用户希望把“证据输出”升级为“自动判定优劣”，但阈值尚未定义
- 用户要求加入 docs 当前不支持的数据项
- 用户希望对金融企业也套用本规则