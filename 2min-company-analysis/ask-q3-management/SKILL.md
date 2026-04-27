---
name: ask-q3-management
description: '七看八问·八问 Q3：管理团队与股权结构。Use when: 八问Q3, 管理团队, 高管变动, 股权结构, 持股集中度, 实控人质押, stk_managers, fin_top10_holders. 强制 evidence harness，单独可执行、可实测。'
argument-hint: '输入股票代码；可选分析日期、输出目录。默认运行本 skill 自带 scripts/q03_management.py。'
user-invocable: true
---

# Q3 独立 Skill —— 管理团队

本 skill 只负责八问的 Q3：**管理团队**。它与其它 `ask-qN` 是 `2min-company-analysis/` 目录树下的同级子目录技能；运行时依赖 `2min-company-analysis/seven-look-eight-question/scripts/` 下的共享 harness，因此必须在保留完整目录树的前提下在仓库根目录执行。

## 原文问句

> **Q3：管理层行业经验、战略眼光与执行能力如何？**
>
> **关键点**：关注团队稳定性与股权结构，避免"一言堂"风险。

## 适用场景

- 单独实测 Q3 的证据采集与评分逻辑
- 调整 `stk_managers` 任期窗口 / `fin_top10_holders` 集中度阈值
- 排查高管变动公告抓取是否及时

## 合格证据定义（Agent 必须对齐）

- `stk_managers` 任期 + `fin_top10_holders` 股东集中度 + 高管变动公告
- 至少覆盖董事长 / 总经理 / CFO 三要职的当前在任信息

## 反面信号

- 12 个月内董/总/CFO 同时变动
- 实控人质押率 >50%
- 前十大股东累计持股 <20%（控制权真空）

## 输入参数

- `--ts-code`（必填）、`--as-of-date`（可选）、`--output-dir`（可选）、`--duckdb-path`（可选）

## 执行方式

```bash
# 在仓库根目录执行
python 2min-company-analysis/ask-q3-management/scripts/q03_management.py \
    --ts-code 000002.SZ --output-dir /tmp/ask_q3
```

## 核心证据数据源

| 类型 | 来源 |
| --- | --- |
| 外部·MCP | `fetch_announcement_list`（高管变动公告） |
| 结构化·DB | `stk_managers`、`stk_company`、`stk_rewards`、`fin_top10_holders` |

## 真实列名约束（避免字段名错）

- `stk_managers.lev`（职位级别）、`edu`、`begin_date`、`end_date`
- 禁止假设 `title` / `position_level` 等不存在的字段

## 输出要求

- 默认产出 `q03_management.json` / `q03_management.md`；JSON 顶层固定包含 `ts_code`、`as_of_date`、`question_id`、`question_title`、`answer`
- 若命中反面信号 → `cross_validation_flags.governance = "red"` 并强制降分

## 何时暂停并讨论

- 用户要求加入独董 / 监事会数据（当前未纳入评分）
- 实控人为境外/基金结构，股权穿透超出本 skill 范围
