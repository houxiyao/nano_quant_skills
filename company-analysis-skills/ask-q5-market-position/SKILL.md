---
name: ask-q5-market-position
description: '七看八问·八问 Q5：市场地位与客户集中度。Use when: 八问Q5, 市场地位, 市占率, 前五大客户, 同行对比, 龙头, 伪龙头。调用 company-analysis-mcp 工具自动评级。'
argument-hint: '输入股票代码（如 000002.SZ）。'
user-invocable: true
---

# 八问 Q5：市场地位

## 原文问句

> **Q5：企业在行业中的排名与市场份额变化如何？**
>
> **关键点**：区分"龙头"与"伪龙头"，如通过客户集中度验证。

## 调用方式

调用 MCP 工具 `ask_q5_market_position`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| question_id | 5 |
| question_title | "市场地位" |
| rating | 1-5 评级 |
| status | ready / partial / insufficient-evidence |
| evidence[] | 同行规模对比、主营数据 |

## 合格证据定义

- 年报"前五大客户/供应商"段落 + fin_mainbz + idx_sw_l3_peers 同行规模对比
- 必须同时给出"自家规模"和"同行 TopN 规模"

## 反面信号

- 前五大客户占比 > 50% 且无签约锁定
- 自称龙头但三级行业规模排名 >= 5
- 市占率连续 2 年下降

## 输出要求

向用户呈现：
- 行业排名与同行对比
- 客户集中度（若有数据）
- rating 及判断依据
- 若年报段落缺失 → status=partial，不得编造市占率

## 交叉校验

- look-04（业务分布）：同行池数据共享
- ask-q6（业务模式）：客户集中度共享

## 何时暂停

- 行业分类无 L3 同行（peer_group_size < 3）→ 需确认是否升级到 L2 对比
