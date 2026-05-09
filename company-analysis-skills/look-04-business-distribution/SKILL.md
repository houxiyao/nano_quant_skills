---
name: look-04-business-distribution
description: '七看八问·第4看：业务构成与市场分布。Use when: 四看业务构成与市场分布, 主营收入占比, 海外销售占比, 区域风险, 单一客户依赖, 同行对比, 申万三级行业。调用 company-analysis-mcp 工具。'
argument-hint: '输入股票代码（如 000002.SZ），可选分析日期和回看年数。'
user-invocable: true
---

# 第4看：业务构成与市场分布

## 职责

识别企业是否过度依赖单一业务或市场，并用申万三级行业同行池做横向对比。

## 调用方式

调用 MCP 工具 `look_04_business_market_distribution`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |
| as_of_date | string | | 分析截止日 YYYY-MM-DD，默认今天 |
| lookback_years | int | | 回看年数，默认 3 |

## 返回结构解读

| 字段 | 说明 |
|------|------|
| status | `ready` / `not-applicable` / `partial` / `human-in-loop-required` |
| summary | 业务集中度、同行信息 |
| rows | 主营分部数据 |
| human_in_loop_requests | 缺失数据请求（若需年报文本） |

## 分析框架

1. 结构化数据可直接提供：stk_info 画像 + idx_sw_l3_peers 同行池
2. 分产品收入/分区域收入/海外销售占比/客户集中度来自年报附注，需 human-in-loop
3. 同行池使用 idx_sw_l3_peers 标准视图
4. 金融类公司不适用

## 关键指标与红旗信号

- 申万三级行业分类与同行池
- 主营业务集中度（前 N 大业务占比）
- **红旗**：单一业务营收占比 > 80%
- **红旗**：海外销售占比 > 50% 且集中在单一地区
- human-in-loop 状态（是否需要年报文本补充）

## 输出要求

向用户呈现：
- 公司画像与行业定位
- 主营分部构成（若有数据）
- 同行池对比
- 缺失数据提示（若 status=human-in-loop-required）

## 与其他维度的关联

- look-06（效率）：同行标杆选取共享
- ask-q5（市场地位）：同行排名数据共享
- ask-q6（业务模式）：业务结构判断互补

## 何时暂停

- 用户希望从 PDF/图片直接做 OCR 抽取
- 用户希望把"单一市场依赖"升级为严格量化结论
- 用户希望使用自定义公司池而非申万 L3
