---
name: ask-q2-moat
description: '七看八问·八问 Q2：竞争优势与护城河。Use when: 八问Q2, 竞争优势, 护城河, 品牌壁垒, 技术壁垒, 成本优势, 毛利率, 主营结构。调用 company-analysis-mcp 工具自动评级。'
argument-hint: '输入股票代码（如 000002.SZ）。'
user-invocable: true
---

# 八问 Q2：竞争优势

## 原文问句

> **Q2：企业核心竞争力是什么？是品牌、技术还是成本优势？**
>
> **关键点**：识别"护城河"是否可持续，如贵州茅台的品牌壁垒。

## 调用方式

调用 MCP 工具 `ask_q2_moat`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| question_id | 2 |
| question_title | "竞争优势" |
| rating | 1-5 评级 |
| status | ready / partial / insufficient-evidence |
| answer | 分析文本 |
| evidence[] | 证据列表 |
| cross_validation_flags | 交叉校验标记 |

## 合格证据定义

- 年报 MD&A / 主营毛利结构（fin_mainbz）/ 专利与商标登记
- 必须能回答"对手为什么复制不了"
- 至少 1 条 primary/db 支撑，方可 rating

## 反面信号

- **毛利率连续 3 年下降**（与 look-01 交叉校验）
- 所谓护城河仅来自行政许可/关联交易/单一大客户

## 输出要求

向用户呈现：
- rating 及护城河类型判断
- 主营毛利结构证据
- 若 look-01 判定毛利率连续下滑 → 必须说明并降分

## 交叉校验

- look-01 `grossprofit_margin_declining_3y=true` → 本问 rating 下调，记 cross_validation_flags

## 何时暂停

- 用户希望纳入专利/商标数据库（当前未接入）
- MD&A 年报抓取失败但用户要求 rating >= 4
