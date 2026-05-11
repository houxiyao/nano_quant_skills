---
name: ask-q6-business-model
description: '七看八问·八问 Q6：业务模式与第二曲线。Use when: 八问Q6, 业务模式, 商业模式, 第二曲线, 单一产品依赖, 业务分部, 主营构成多期对比。调用 company-analysis-mcp 工具自动评级。'
argument-hint: '输入股票代码（如 000002.SZ）。'
user-invocable: true
---

# 八问 Q6：业务模式

## 原文问句

> **Q6：商业模式是否依赖单一客户或产品？**
>
> **关键点**：评估"第二曲线"发展情况，避免增长瓶颈。

## 调用方式

调用 MCP 工具 `ask_q6_business_model`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| question_id | 6 |
| question_title | "业务模式" |
| rating | 1-5 评级 |
| status | ready / partial / insufficient-evidence |
| evidence[] | fin_mainbz 多期数据 |

## 合格证据定义

- 年报业务分部 + 多年 fin_mainbz 变化 + 新业务披露
- 必须给出至少 3 年的主营构成对比

## 反面信号

- 单一产品营收占比 > 80% 且连续 3 年未孵化新曲线
- 主营分部描述连年微调但营收结构不变（疑似"换马甲"）

## 输出要求

向用户呈现：
- 主营构成多年对比
- "第二曲线"占比或说明"尚未出现"
- rating 及理由
- fin_mainbz 缺失超过 2 期 → status=partial

## 交叉校验

- look-03（增长趋势）：增长模式（内生/并购）辅助判断
- look-04（业务分布）：业务结构共享
- ask-q5（市场地位）：客户集中度关联

## 何时暂停

- 用户希望把并购标的独立业务视作"第二曲线"
- fin_mainbz 缺失超过 2 期
