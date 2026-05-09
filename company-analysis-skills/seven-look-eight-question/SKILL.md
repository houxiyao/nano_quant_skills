---
name: seven-look-eight-question
description: '七看八问综合编排。Use when: 一键执行七看八问, 高效财务分析框架, 财务快审, A股基本面分析, 综合质量评分, 股票全面分析。调用 company-analysis-mcp 编排工具。'
argument-hint: '输入股票代码。可选分析日期、回看年数。可选是否同时执行八问。'
user-invocable: true
---

# 七看八问综合编排

## 职责

一键编排执行全部七看（定量财务分析）和/或八问（定性证据分析），产出统一的质量评分报告。

单个 look 或单个问答的独立分析，应直接调用对应 sibling skill。

## 调用方式

### 七看编排

调用 MCP 工具 `orchestrate_seven_looks`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |
| as_of_date | string | | 分析截止日 YYYY-MM-DD，默认今天 |
| lookback_years | int | | 统一回看年数，默认各维度自己的默认值 |
| report_bundle_json_04 | string | | look-04 年报全文 JSON（可选） |
| report_bundle_json_05 | string | | look-05 年报附注 JSON（可选） |
| employee_bundle_json_06 | string | | look-06 员工数 JSON（可选） |
| max_workers | int | | 并行线程数，默认 4 |

### 八问编排

调用 MCP 工具 `orchestrate_eight_questions`：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| ts_code | string | Y | A股代码，如 "000002.SZ" |
| question_ids | string | | 逗号分隔问题编号如 "1,2,4"，空=全部 |
| max_workers | int | | 并行线程数，默认 4 |

### 推荐执行策略

1. 先调用 `orchestrate_seven_looks` → 获取定量结果与质量评分
2. 根据需要调用 `orchestrate_eight_questions` → 获取定性评估
3. 综合两者给出最终判断

## 七看返回结构

```json
{
  "framework": "七看财务质量综合评估",
  "stock": "000002.SZ",
  "as_of_date": "2026-05-08",
  "quality_score": {"score": 75, "grade": "B", "deductions": [...]},
  "red_flags": [...],
  "commentary": {...},
  "human_in_loop_requests": [...],
  "recommendations": [...],
  "results": {"look-01": {...}, ...},
  "raw_results": {"look-01": {...}, ...}
}
```

### 字段职责

| 字段 | 说明 |
|------|------|
| results | 标准化汇总：每个 look 仅 rule_id/title/status/summary |
| raw_results | 原始透传：完整明细，用于审计追溯 |
| quality_score | 综合质量评分 |
| red_flags | 所有红旗预警 |
| recommendations | 最多 3 条行动建议 |

## 质量评分规则

- 起始 100 分
- 每个 critical 红旗：-15 分
- 每个 warning 红旗：-5 分
- 评级：A (>=80) / B (60-79) / C (40-59) / D (<40)

| 等级 | 含义 |
|------|------|
| A | 财务质量良好 |
| B | 财务质量一般，存在部分隐患 |
| C | 财务质量较差，多项红旗预警 |
| D | 财务质量极差，建议高度警惕 |

## 八问返回结构

```json
{
  "ts_code": "000002.SZ",
  "generated_at": "2026-05-08T14:30:00",
  "summary": {
    "question_count": 8,
    "status_counts": {"ready": 5, "partial": 2, "insufficient-evidence": 1},
    "avg_rating": 3.75,
    "avg_weighted_rating": 3.62,
    "human_in_loop_requests": [...],
    "critical_gaps": [...]
  },
  "answers": [...]
}
```

### 证据权重

| source_type | weight | 说明 |
|---|---|---|
| primary | 1.0 | 年报、法定披露 |
| regulatory | 1.0 | 监管处罚、问询函 |
| db | 1.0 | ClickHouse 结构化指标 |
| industry_report | 0.6 | 券商研报（预测） |
| ir_meeting | 0.5 | IR 调研 |
| news | 0.4 | 新闻舆情 |

`avg_weighted_rating` = 各问 rating x avg_evidence_weight 的平均值，同时反映结论高低与证据质量。

## Human-in-loop 工作流

look-04/05 依赖年报文本。若首次未提供：
1. 输出标记 status=partial，列出需补充信息
2. 用户准备好 JSON 文本包后，通过 report_bundle_json_04/05 参数传入
3. 重新调用获取完整分析

文本包格式：
```json
[{"ts_code": "000002.SZ", "name": "万科A", "year": 2025, "text": "年报全文"}]
```

## 综合分析指南

1. 先看 quality_score 等级判断整体水位
2. 逐条审视 red_flags，区分 critical 和 warning
3. 结合八问 avg_rating 判断定性面
4. 关注 human_in_loop_requests 和 critical_gaps 了解报告置信度
5. 参考 recommendations 给出下一步建议

## 输出要求

向用户呈现：
- 综合质量评分（等级 + 分数）
- 红旗预警清单（按严重程度排序）
- 七看各维度一句话摘要
- 八问平均评级（若执行）
- 行动建议（最多 3 条）
- 数据缺口提示

## 使用边界

1. 金融类公司（银行/保险/证券）相关 look 返回 not-applicable
2. 八问不改动七看评分体系，作为独立扩展
3. 若 orchestration_status = "blocked"，表示预检失败，需先补数据
