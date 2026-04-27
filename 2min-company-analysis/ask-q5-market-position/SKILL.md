---
name: ask-q5-market-position
description: '七看八问·八问 Q5：市场地位与客户集中度。Use when: 八问Q5, 市场地位, 市占率, 前五大客户, 同行对比, 龙头, 伪龙头, fin_mainbz, idx_sw_l3_peers. 强制 evidence harness，单独可执行、可实测。'
argument-hint: '输入股票代码；可选分析日期、输出目录。默认运行本 skill 自带 scripts/q05_position.py。'
user-invocable: true
---

# Q5 独立 Skill —— 市场地位

本 skill 只负责八问的 Q5：**市场地位**。它与其它 `ask-qN` 是 `2min-company-analysis/` 目录树下的同级子目录技能；运行时依赖 `2min-company-analysis/seven-look-eight-question/scripts/` 下的共享 harness，因此必须在保留完整目录树的前提下在仓库根目录执行。

## 原文问句

> **Q5：企业在行业中的排名与市场份额变化如何？**
>
> **关键点**：区分"龙头"与"伪龙头"，如通过客户集中度验证。

## 适用场景

- 单独实测 Q5 的排名 / 客户集中度证据
- 调整 `idx_sw_l3_peers` 同行规模对比口径
- 排查"自称龙头"但数据不支撑的样本

## 合格证据定义

- 年报"前五大客户/供应商"段落 + `fin_mainbz` 主业收入 + `idx_sw_l3_peers` 同行规模对比
- 必须同时给出"自家规模"和"同行 TopN 规模"

## 反面信号

- 前五大客户占比 >50% 且无签约锁定
- 自称龙头但三级行业规模排名 ≥5
- 市占率连续 2 年下降

## 输入参数

- `--ts-code`（必填）、`--as-of-date`（可选）、`--output-dir`（可选）、`--duckdb-path`（可选）

## 执行方式

```bash
# 在仓库根目录执行
python 2min-company-analysis/ask-q5-market-position/scripts/q05_position.py \
    --ts-code 000002.SZ --output-dir /tmp/ask_q5
```

## 核心证据数据源

| 类型 | 来源 |
| --- | --- |
| 外部·MCP | `fetch_reports(annual)`（前五大客户段落） |
| 结构化·DB | `fin_mainbz`、`idx_sw_l3_peers` |

## 真实 View 约束

- `idx_sw_l3_peers` 是 VIEW，返回列：`anchor_ts_code, l1/l2/l3_name, peer_group_size, peer_ts_code, peer_name, peer_is_self`

## 输出要求

- answer 必须同时呈现"自家规模"与"同行 TopN 规模"两个数
- 若年报段落缺失 → `status=partial`，不得自行编造市占率

## 何时暂停并讨论

- 行业分类无 L3 同行（`peer_group_size < 3`）→ 需用户确认是否升级到 L2 对比
