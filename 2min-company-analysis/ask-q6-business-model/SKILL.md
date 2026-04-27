---
name: ask-q6-business-model
description: '七看八问·八问 Q6：业务模式与第二曲线。Use when: 八问Q6, 业务模式, 商业模式, 第二曲线, 单一客户依赖, 单一产品依赖, 业务分部, fin_mainbz多期对比. 强制 evidence harness，单独可执行、可实测。'
argument-hint: '输入股票代码；可选分析日期、输出目录。默认运行本 skill 自带 scripts/q06_business_model.py。'
user-invocable: true
---

# Q6 独立 Skill —— 业务模式

本 skill 只负责八问的 Q6：**业务模式**。它与其它 `ask-qN` 是 `2min-company-analysis/` 目录树下的同级子目录技能；运行时依赖 `2min-company-analysis/seven-look-eight-question/scripts/` 下的共享 harness，因此必须在保留完整目录树的前提下在仓库根目录执行。

## 原文问句

> **Q6：商业模式是否依赖单一客户或产品？**
>
> **关键点**：评估"第二曲线"发展情况，避免增长瓶颈。

## 适用场景

- 单独实测 Q6 的业务分部证据
- 调整"第二曲线"判定阈值（新业务营收占比 / 增速）
- 排查新业务披露是否抓漏

## 合格证据定义

- 年报业务分部 + 多年 `fin_mainbz` 变化 + 新业务披露（在研 / 试产 / 新签合同）
- 必须给出至少 3 年的主营构成对比

## 反面信号

- 单一产品营收占比 >80% 且连续 3 年未孵化新曲线
- 主营分部描述连年微调但营收结构不变（疑似"换马甲"）

## 输入参数

- `--ts-code`（必填）、`--as-of-date`（可选）、`--output-dir`（可选）、`--duckdb-path`（可选）

## 执行方式

```bash
# 在仓库根目录执行
python 2min-company-analysis/ask-q6-business-model/scripts/q06_business_model.py \
    --ts-code 000002.SZ --output-dir /tmp/ask_q6
```

## 核心证据数据源

| 类型 | 来源 |
| --- | --- |
| 外部·MCP | `fetch_reports(annual)`（business_overview / 分部披露） |
| 结构化·DB | `fin_mainbz` 多期对比 |

## 输出要求

- `evidence` 必须包含至少 3 期 `fin_mainbz` 快照
- answer 中显式列出"第二曲线占比"或说明"尚未出现"

## 何时暂停并讨论

- 用户希望把并购标的的独立业务视作"第二曲线"（需单独判定）
- `fin_mainbz` 缺失超过 2 期 → `status=partial`
