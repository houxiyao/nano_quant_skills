---
name: seven-look-eight-question
description: '快速量化执行“高效财务分析框架”的七看八问。Use when: 七看八问, 高效财务分析框架, 财务分析, A股基本面分析, 股票分析, ROE, ROA, 现金流, 负债, 增长, 估值, 股东结构。一键执行七看，并可选接入八问总入口。'
argument-hint: '输入股票代码、分析日期，以及是否需要附带执行八问。'
user-invocable: true
---

# 七看八问总入口

本 skill 只保留**总入口编排**职责：

1. 通过 `./scripts/seven_looks_orchestrator.py` 一键执行 look-01 ~ look-07。
2. 通过 `--include-eight-questions` 可选接入 `./scripts/eight_questions_orchestrator.py`，批量调度 ask-q1 ~ ask-q8。
3. 输出统一的 JSON / Markdown 最终报告；七看评分体系保持独立，八问结果以扩展字段并入。

单个 look 或单个问答的独立执行，应该直接调用对应 sibling skill，而不是走这里。

## 当前可执行入口

### 执行命令

```bash
# 全自动模式（look-04/05 将标记为需人工补充年报）
python .github/skills/seven-look-eight-question/scripts/seven_looks_orchestrator.py \
    --stock 000002.SZ --as-of-date 2025-04-30

# 提供年报文本后完整执行
python .github/skills/seven-look-eight-question/scripts/seven_looks_orchestrator.py \
    --stock 000002.SZ --as-of-date 2025-04-30 \
    --report-bundle-04 /path/to/reports.json \
    --report-bundle-05 /path/to/notes.json \
    --final-output /path/to/final-report.json

# 在七看总报告中并入八问摘要（evidence harness 自动取证）
python .github/skills/seven-look-eight-question/scripts/seven_looks_orchestrator.py \
    --stock 000002.SZ --as-of-date 2025-04-30 \
    --include-eight-questions \
    --format json \
    --final-output /path/to/final-report.json
```

### 参数说明

| 参数 | 必填 | 说明 |
|------|------|------|
| --stock | ✅ | 股票代码，如 000002.SZ |
| --as-of-date | | 分析日期 YYYY-MM-DD，默认今天 |
| --lookback-years | | 统一回看年数（不设则各维度用自己默认值） |
| --db-path | | DuckDB 路径，默认 data/ashare.duckdb |
| --report-bundle-04 | | look-04 年报全文文本包 JSON 路径 |
| --report-bundle-05 | | look-05 年报附注文本包 JSON 路径 |
| --output-dir | | 中间文件输出目录，默认临时目录 |
| --final-output | | 最终综合报告落盘路径，作为唯一权威 artifact |
| --include-eight-questions | | 调用本 skill 下的 `eight_questions_orchestrator.py`，把八问摘要合并进最终输出 |
| --eight-questions-bundle | | **已废弃**。旧版 bundle 入口会显式报错，防止静默走错链路 |
| --format | | markdown（默认）或 json |
| --db-table-map | | 逻辑表名→物理表名映射（JSON 字符串或 JSON 文件路径），用于跨机器异构表名适配 |

### 执行流程

1. **Phase 0（门禁）**: 先做 DuckDB 数据完整性预检（逻辑数据域检查 + 关键证券数据可用）。门禁会按 `--db-table-map` 先做显式映射，再按字段能力自动识别兼容关系（表/视图均可）。若缺表、映射冲突或核心数据不足，立即停止后续编排并输出 `human-in-loop` 精准补数清单。
2. **Phase 1（自动）**: 依次运行 look-01, 02, 03, 06, 07（纯数据库查询，无需外部输入）
3. **Phase 2（半自动）**: 运行 look-04, 05（若未提供 --report-bundle 则标记 human-in-loop）
4. **Phase 2.5（可选）**: 若传入 `--include-eight-questions`，调用本 skill 下的 `scripts/eight_questions_orchestrator.py`，并发调度 ask-q1 ~ ask-q8 的本地脚本，从 `eight_questions.json` 回读完整 payload，并补充 `cross_validation_flags`
5. **Phase 3（汇总）**: 合并 7 份中间 JSON → 红旗预警 + 质量评分；八问不改动七看的评分体系
6. **Phase 4（评语）**: 附加量化评语 + 最多 3 条行动建议

### 输出内容

- **7 份中间 JSON 文件**: 保存在 --output-dir 指定目录，每个文件对应一个 look 维度
- **可选八问中间文件**: 开启 `--include-eight-questions` 后，会在 `--output-dir/look-08/` 下额外写出 `eight_questions.json`
- **综合报告**: 包含质量评分（A/B/C/D）、红旗预警表、七看概览、human-in-loop 清单、行动建议和量化评语
- **最多 3 条建议**: 根据分析结果自动推荐下一步操作（补充年报、深挖风险、估值分析等）
- **最终权威文件**: 传入 --final-output 后，脚本会把最终 JSON 或 Markdown 直接落盘，避免下游代理再手工拼接 final artifact

### 输出契约

`seven_looks_orchestrator.py` 支持两种最终输出格式：

- **markdown**: 面向人工阅读。报告顺序为综合评分、红旗预警、七看概览、待人工补充信息、行动建议、量化评语，最后追加“分项原始分析透传”章节，把每个 look 的原始 JSON 输出附在总结后面，便于核对汇总是否有遗漏或失真。
- **json**: 面向程序消费。默认顶层稳定包含 `framework`、`stock`、`as_of_date`、`lookback_years`、`quality_score`、`red_flags`、`commentary`、`human_in_loop_requests`、`recommendations`、`results`、`look_results`、`raw_results`、`intermediate_files`；启用 `--include-eight-questions` 时，会额外追加 `eight_questions` 与 `critical_gaps`。

JSON 中与分项结果相关的 3 个字段职责如下：

| 字段 | 职责边界 | 是否稳定 | 使用建议 |
|------|----------|----------|----------|
| `results` | **标准化汇总视图**。每个 look 仅保留 `rule_id`、`title`、`status`、`summary` 四类最小信息，用于统一渲染概览、评语、下游摘要消费。 | 顶层结构稳定；`summary` 的键取决于对应 look 的公开摘要契约。 | 新接入方如只关心统一摘要、状态和评分，优先读取这里。 |
| `look_results` | **兼容别名**。当前内容与 `results` 完全一致，仅用于兼容历史消费者。 | 兼容保留，语义等同于 `results`。 | 新代码不要单独依赖它的差异语义；如果没有历史包袱，直接用 `results`。 |
| `raw_results` | **原始透传视图**。每个 look 保留其原始脚本输出，包含明细表、原始分析段落、证据计数、状态字段等，不做摘要裁剪。 | 只保证“按 look 原样透传”的原则；各 look 的内部字段可能随单项脚本演进。 | 需要审计、追溯、复核汇总结论，或读取分项细节时，使用这里。 |

补充约定：

- `results` / `look_results` 的 `summary` 如果上游脚本返回 `null`，统一折算为 `{}`，避免下游处理空对象时出现崩溃。
- `results` / `look_results` 不承载明细表、原始分析正文或大块证据数据；这些内容只放在 `raw_results`。
- `raw_results` 的存在不替代 `--output-dir` 下的 7 份中间 JSON 文件。中间文件仍是单个 look 的落盘产物，`raw_results` 是最终汇总输出中的内联透传副本。
- `eight_questions` 是**独立扩展字段**，不并入 `results` / `look_results`，这样可保证原有七看消费者在不开新开关时完全无感。
- `critical_gaps` 仅在启用八问时出现，承载八问总入口汇总出的关键证据缺口；这类缺口不直接影响七看评分，但应视为降低报告置信度的阻塞信息。

当 Phase 0 预检失败时，输出契约如下：

- 顶层新增 `orchestration_status = "blocked"`
- 顶层新增 `block_reason = "duckdb-preflight-failed"`
- 顶层新增 `preflight`（包含 `missing_tables`、`insufficient_data`、`human_in_loop_requests`、`resolved_relations`、`alias_shim_required`）
- `results` / `look_results` / `raw_results` 为空对象，不继续执行 look-01..08
- `human_in_loop_requests` 提供可直接执行的补数指令（按表、按证券精确定位）

八问 summary 中的 `avg_weighted_rating` 语义说明：

- 单问 `weighted_rating = rating × avg_evidence_weight`。
- 其中 `avg_evidence_weight` 是该问所有证据的权重平均值，用于表达“结论强度/可信度”。
- `avg_weighted_rating` 是所有可计算 `weighted_rating` 的问答项平均值，不替代 `avg_rating`；两者应结合解读：
    - `avg_rating` 更偏结论高低。
    - `avg_weighted_rating` 同时反映结论高低与证据质量。

`SOURCE_WEIGHTS`（来自 `eight_questions_domain.py`）如下：

| source_type | weight | 说明 |
| --- | --- | --- |
| primary | 1.0 | 年报、定期报告、法定披露 |
| regulatory | 1.0 | 监管处罚、问询函、立案 |
| db | 1.0 | 本地 DuckDB 结构化指标 |
| industry_report | 0.6 | 券商研报（预测） |
| ir_meeting | 0.5 | IR 调研/公司口径 |
| news | 0.4 | 新闻舆情 |

最小示意：

```json
{
    "results": {
        "look-05": {
            "rule_id": "look-05",
            "title": "资产负债健康度",
            "status": "partial",
            "summary": {
                "leverage_trend": "rising",
                "hidden_liability_status": "human-in-loop-required"
            }
        }
    },
    "look_results": {
        "look-05": {
            "rule_id": "look-05",
            "title": "资产负债健康度",
            "status": "partial",
            "summary": {
                "leverage_trend": "rising",
                "hidden_liability_status": "human-in-loop-required"
            }
        }
    },
    "raw_results": {
        "look-05": {
            "status": "partial",
            "summary": {
                "leverage_trend": "rising",
                "hidden_liability_status": "human-in-loop-required"
            },
            "debt_solvency_rows": [
                {
                    "debt_to_assets": 72.5
                }
            ],
            "hidden_liability_analysis": {
                "status": "human-in-loop-required"
            }
        }
    }
}
```

### Human-in-loop 工作流

look-04（业务构成）和 look-05（资产负债健康度）依赖年报全文/附注文本。如果首次运行未提供，脚本会：
1. 在输出中列出需要人工补充的具体信息
2. 生成 "补充年报文本" 建议
3. 用户准备好 JSON 文本包后，重新运行并通过 --report-bundle-04/05 传入

年报文本包格式：
```json
[{"ts_code": "000002.SZ", "name": "万科A", "year": 2025, "text": "年报全文文本"}]
```

补充约束：

- 顶层既可以是数组，也可以是 `{"reports": [...]}`。
- `reports` 只允许出现在顶层；每个 entry 必须是扁平对象，不允许再嵌套 `reports`。
- 每个 entry 至少需要 `ts_code` 和 `year`，否则脚本会直接报错而不是静默降级。

### 质量评分规则

- 起始 100 分，每个严重红旗（critical）扣 15 分，每个警示（warning）扣 5 分
- A (≥80): 财务质量良好
- B (60-79): 财务质量一般，存在部分隐患
- C (40-59): 财务质量较差，多项红旗预警
- D (<40): 财务质量极差，建议高度警惕

## 使用边界

1. 该 skill 只负责总入口编排与最终汇总，不承担拆分路线图、开发流程或项目管理说明。
2. 结构化数据边界仍以 `./references/data-coverage.md` 为准；若超出当前数据覆盖，不应擅自补设口径。
3. 八问证据优先级与外部来源模板以 `./references/evidence-playbook.md` 为准。
4. 如目标公司 `comp_type` 属于银行、保险、证券，相关 look 脚本会返回不适用或要求改走金融类分析路径。

## 保留资源

- `./assets/rule_registry.json`
- `./references/data-coverage.md`
- `./references/evidence-playbook.md`
- `./scripts/seven_looks_orchestrator.py`
- `./scripts/eight_questions_orchestrator.py`