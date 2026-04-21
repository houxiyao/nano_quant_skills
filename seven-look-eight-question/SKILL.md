---
name: seven-look-eight-question
description: '快速量化执行“高效财务分析框架”的七看八问。Use when: 七看八问, 高效财务分析框架, 财务分析, A股基本面分析, 股票分析, 15条分析规则, ROE, ROA, 现金流, 负债, 增长, 估值, 股东结构。支持规则拆分、并行执行、实测、code review、Python 脚本固化。'
argument-hint: '输入股票代码、分析日期、要执行的规则编号，或说明要新建哪一条规则。'
user-invocable: true
---

# 七看八问工作流

本 skill 承担两个职责：

1. **七看统一入口**：通过 `./scripts/run_seven_looks.py` 一键执行 look-01 ~ look-07 七个独立分析维度，汇总红旗预警、质量评分和行动建议。
2. **总工作流/规划视角**：管理七看八问的 15 条规则拆分、进度、编排边界。

## 一键七看执行（推荐入口）

### 执行命令

```bash
# 全自动模式（look-04/05 将标记为需人工补充年报）
python .github/skills/seven-look-eight-question/scripts/run_seven_looks.py \
    --stock 000002.SZ --as-of-date 2025-04-30

# 提供年报文本后完整执行
python .github/skills/seven-look-eight-question/scripts/run_seven_looks.py \
    --stock 000002.SZ --as-of-date 2025-04-30 \
    --report-bundle-04 /path/to/reports.json \
    --report-bundle-05 /path/to/notes.json
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
| --format | | markdown（默认）或 json |

### 执行流程

1. **Phase 1（自动）**: 依次运行 look-01, 02, 03, 06, 07（纯数据库查询，无需外部输入）
2. **Phase 2（半自动）**: 运行 look-04, 05（若未提供 --report-bundle 则标记 human-in-loop）
3. **Phase 3（汇总）**: 合并 7 份中间 JSON → 红旗预警 + 质量评分
4. **Phase 4（评语）**: 附加量化评语 + 最多 3 条行动建议

### 输出内容

- **7 份中间 JSON 文件**: 保存在 --output-dir 指定目录，每个文件对应一个 look 维度
- **综合报告**: 包含质量评分（A/B/C/D）、红旗预警表、七看概览、human-in-loop 清单、行动建议和量化评语
- **最多 3 条建议**: 根据分析结果自动推荐下一步操作（补充年报、深挖风险、估值分析等）

### 输出契约

`run_seven_looks.py` 支持两种最终输出格式：

- **markdown**: 面向人工阅读。报告顺序为综合评分、红旗预警、七看概览、待人工补充信息、行动建议、量化评语，最后追加“分项原始分析透传”章节，把每个 look 的原始 JSON 输出附在总结后面，便于核对汇总是否有遗漏或失真。
- **json**: 面向程序消费。顶层结构稳定包含 `framework`、`stock`、`as_of_date`、`lookback_years`、`quality_score`、`red_flags`、`commentary`、`human_in_loop_requests`、`recommendations`、`results`、`look_results`、`raw_results`、`intermediate_files`。

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

### 质量评分规则

- 起始 100 分，每个严重红旗（critical）扣 15 分，每个警示（warning）扣 5 分
- A (≥80): 财务质量良好
- B (60-79): 财务质量一般，存在部分隐患
- C (40-59): 财务质量较差，多项红旗预警
- D (<40): 财务质量极差，建议高度警惕

## 适用场景

- **一键七看分析**：对目标公司快速执行全部 7 个财务维度分析
- 快速执行已经落地的七看八问规则
- 为某一条规则补充数据映射、计算逻辑和 Python 脚本
- 判断某个分析动作能否直接由当前 docs 支撑
- 为每条规则执行“设计 -> 实测 -> code review -> 注册”的闭环

## 首要约束

1. 先读取 ./references/data-coverage.md，确认当前 docs 能直接支撑的数据范围。
2. 如果规则需要 docs 中没有的数据，或者需要派生指标但公式和窗口未定义，先和用户讨论，不得擅自假设。
3. 交付单元始终是一条规则，不是整个 15 步一起落地。
4. 每条规则完成后，必须经过一次真实样本实测和一次 code review。
5. 尽量把规则固化为 Python 脚本，不把核心逻辑埋在自然语言里。
6. 如果目标公司 `comp_type` 属于银行、保险、证券，先提示“金融类公司不适用”，不要继续执行当前规则。

## 执行单条规则

1. 读取 ./assets/rule_registry.json，确认哪些规则已经实现、哪些仍待定义。
2. 读取 ./references/data-coverage.md，确认目标规则依赖的表、字段和派生项。
3. 如果用户只指定部分规则，只执行这些规则；如果未指定，使用 `./scripts/run_seven_looks.py` 统一执行全部七看。
4. 参考 ./references/parallelization-guide.md，把规则按数据依赖拆成并行批次。
5. 汇总输出时，每条规则必须包含以下最小信息：
   - 规则编号
   - 结论
   - 证据字段或查询来源
   - 计算方法或阈值
   - 数据缺口与不确定性

## 新增或修订一条规则

1. 先读取 ./references/rule-delivery-workflow.md，按单条规则的交付清单推进。
2. 先向用户确认这条规则的业务问题、判断口径、时间窗口、阈值和输出格式。
3. 把规则拆成三层：
   - 直接可查字段
   - 需要派生计算的指标
   - 当前 docs 缺失、必须讨论的数据
4. 如果数据足够，新增或修改对应 Python 脚本。
5. 至少选择一个真实股票样本做实测；如果规则有明显正反案例，优先测两个样本。
6. 使用现有 code-review skill 做 review，重点看公式口径、日期对齐、空值处理和可解释性。
7. 更新 ./assets/rule_registry.json 的状态、脚本路径、测试状态和 review 状态。

## 推荐目录约定

- 规则注册表：./assets/rule_registry.json
- **七看统一入口**：./scripts/run_seven_looks.py
- 旧版分析入口：./scripts/run_analysis.py
- 拆分路线图：./references/skill-roadmap.md
- 数据边界：./references/data-coverage.md
- 单规则交付标准：./references/rule-delivery-workflow.md
- 并行拆分建议：./references/parallelization-guide.md

## 什么时候应该暂停并讨论

- 规则依赖年报附注、分部收入、管理层文字表述、机构一致预期等当前 docs 不含的数据
- 规则需要行业对比，但基准指数、同业池或对标口径尚未定义
- 规则需要长期分位、滚动窗口或复合指标，但窗口、复权口径、缺失值处理未确认
- 规则涉及“好/坏”的阈值判断，但用户尚未给出判定标准

## 立即可用的辅助资源

- ./references/data-coverage.md
- ./references/rule-delivery-workflow.md
- ./references/parallelization-guide.md
- ./references/skill-roadmap.md
- ./assets/rule_registry.json
- ./scripts/run_seven_looks.py
- ./scripts/run_analysis.py
- ./scripts/run_analysis.py