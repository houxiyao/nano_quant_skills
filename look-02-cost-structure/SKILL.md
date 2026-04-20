---
name: look-02-cost-structure
description: '七看八问规则2：二看费用成本结构。Use when: 规则2, 二看费用成本结构, 四大费用率, 管理费用率, 研发费用率, 销售费用率, 财务费用率, 费用与业务匹配度。适合单独迭代、单独排查、单独实测。'
argument-hint: '输入股票代码、分析日期、回看年数，或说明要修改规则2的费用率口径与匹配逻辑。'
user-invocable: true
---

# 规则2独立 Skill

本 skill 只负责七看八问的第 2 条规则：二看费用成本结构。

目标是稳定地产出四大费用率与业务增长匹配度的结构化证据，便于单独排查、单独迭代、单独实测。

## 适用场景

- 单独执行规则2
- 调整费用率、费用增长与营收增长的匹配口径
- 对规则2做 SQL 复核、样本实测、code review
- 排查费用率异常波动是否源于数据口径或真实经营变化

## 输入参数

- 股票代码
- 分析日期，可选，默认今天
- 回看年数，可选，默认最近 3 年

## 当前口径

1. 只取合并报表，即 report_type='1'。
2. 只取年报，即 end_date 月份为 12、日期为 31。
3. 仅使用分析日之前已经可见的数据，使用 COALESCE(f_ann_date, ann_date, end_date) 控制可见性。
4. 销售、管理、财务费用率优先使用 fin_indicator 中现成字段：saleexp_to_gr、adminexp_of_gr、finaexp_of_gr。
5. 研发费用率使用 rd_exp / total_revenue * 100 派生。
6. 费用与业务匹配度暂按方向一致性取证，不额外引入“高增长”的人工阈值。
7. 重点警示信号为：销售费用增长为正，但营收同比不增长。
8. 金融类公司不适用；如果 comp_type 属于银行、保险、证券，直接返回 not-applicable。

## 核心证据

- 费用原值：sell_exp, admin_exp, fin_exp, rd_exp
- 费用率：saleexp_to_gr, adminexp_of_gr, finaexp_of_gr, rdexp_to_gr
- 业务增长：tr_yoy, or_yoy
- 匹配信号：sales_growth_mismatch, rd_growth_mismatch

## 执行实现

- 脚本入口：./scripts/look_02_cost_structure.py
- 公司类型前检：./scripts/common.py

## 输出要求

- 最近 N 年的四大费用率证据表
- 四大费用原值同比变化
- 费用率年度变动
- 销售费用与营收增长不匹配次数
- 研发费用与营收增长不匹配次数
- 研发费用缺失情况
- 如不适用，则明确给出原因

## 何时暂停并讨论

- 用户希望加入“高增长”明确阈值并据此自动评分
- 用户要求按行业差异设置不同费用率阈值
- 用户希望把期间费用率异常定义为固定数值阈值