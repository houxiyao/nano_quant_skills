---
name: ask-q4-financial-integrity
description: '七看八问·八问 Q4：财务真实性与会计质量。Use when: 八问Q4, 财务真实性, 会计洞穴, 净现比, 审计意见, 问询函, 立案, ST更名, fin_indicator, stk_name_history. 强制 evidence harness，单独可执行、可实测。'
argument-hint: '输入股票代码；可选分析日期、输出目录。默认运行本 skill 自带 scripts/q04_integrity.py。'
user-invocable: true
---

# Q4 独立 Skill —— 财务真实性

本 skill 只负责八问的 Q4：**财务真实性**。它与其它 `ask-qN` 是 `2min-company-analysis/` 目录树下的同级子目录技能；运行时依赖 `2min-company-analysis/seven-look-eight-question/scripts/` 下的共享 harness，因此必须在保留完整目录树的前提下在仓库根目录执行。

## 原文问句

> **Q4：财务数据是否与业务描述一致？**
>
> **关键点**：警惕"会计洞穴"现象，即报表数字与实际业务脱节。

## 适用场景

- 单独实测 Q4 证据采集（审计/问询/立案/更名 四大异常信号）
- 与 `look-01 净现比` 做交叉校验
- 排查 Q4 输出是否漏查某类处罚公告

## 合格证据定义

- 年报 / 问询函 + `fin_indicator` 净现比 + `stk_name_history`
- **需与七看 look-01 交叉验证**：look-01 净现比 <0.5 且 Q4 rating ≤2 → `cross_validation_flags.financial_integrity = "reinforced"`

## 反面信号

- 净现比连续 2 年 <0.5
- 被立案 / 问询 / 曾更名至 ST
- 非标审计意见（保留 / 无法表示 / 否定）

## 输入参数

- `--ts-code`（必填）、`--as-of-date`（可选）、`--output-dir`（可选）、`--duckdb-path`（可选）

## 执行方式

```bash
# 在仓库根目录执行
python 2min-company-analysis/ask-q4-financial-integrity/scripts/q04_integrity.py \
    --ts-code 000002.SZ --output-dir /tmp/ask_q4
```

## 核心证据数据源

| 类型 | 来源 |
| --- | --- |
| 外部·MCP | `fetch_announcement_list`（audit / 问询 / 立案 / 更名） |
| 结构化·DB | `fin_indicator`（`ocf_to_profit`、`salescash_to_or`）、`stk_name_history` |

## 真实列名约束

- `fin_indicator.ocf_to_profit` = 净现比（经营现金流 / 净利润，tushare 通用口径，**分母含少数股东损益**）
- `fin_indicator.salescash_to_or` = 销售现金流 / 营业收入
- 禁止使用 `net_cash_ratio` 等不存在的字段

### 口径差异（与 look-01）

本 skill 的净现比阈值使用的是 **tushare 通用口径 `ocf_to_profit`**，而七看 `look-01-profit-quality` 使用**归母口径 `n_cashflow_act / n_income_attr_p`**。两者存在系统性差异，因此：

- Q4 红旗阈值设在 `< 0.3`（更宽松），仅作方向性信号；
- look-01 阈值保持 `< 0.5`（归母口径，指向"利润含金量不足"）；
- 当二者同时触发时，`eight_questions_orchestrator.cross_validate` 会把 `financial_integrity` 标记为 `reinforced`。
- 若需精确的归母净现比，请以 look-01 中间 JSON `net_profit_cash_ratio_avg` 为准。

## 输出要求

- 默认产出 `q04_integrity.json` / `q04_integrity.md`；若需 `cross_validation_flags`，由 `seven-look-eight-question/scripts/eight_questions_orchestrator.py` 或总编排器在汇总阶段补出
- 非标审计意见触发 → rating 强制 ≤2

## 何时暂停并讨论

- 需要引入 IPO 以来的全部问询函（当前仅抓最近 N 条）
- 业务复杂如会计处理变更，需要人工核对附注
