# 2min-company-analysis

`2min-company-analysis` 是“七看八问”能力模块的唯一主入口，用于对 A 股上市公司做结构化基本面快审。

该模块的结构化财务分析依赖本仓库内的 DuckDB 数据；涉及公告、年报正文、行业政策、IR 纪要等外部证据时，推荐配合同仓库子模块 [nano-search-mcp](../nano-search-mcp/README.md) 使用。

## 模块目标

- 用 7 个定量维度（看）快速体检财务质量。
- 用 8 个定性问题（问）补齐行业、管理层、风险与战略证据。
- 通过总编排脚本统一产出可复核的 JSON/Markdown 报告。

## 能力边界

- 适用对象：A 股非金融类公司。
- 金融类公司（银行/保险/证券）会在分项规则中返回 `not-applicable`。
- `look-04` 与 `look-05` 涉及年报文本证据，未提供报告文本包时会进入 human-in-loop。

## 目录地图

- `seven-look-eight-question/`：总编排 skill（七看总入口，可选接入八问）。
- `look-01-*` 到 `look-07-*`：七看 7 个独立规则 skill。
- `ask-q1-*` 到 `ask-q8-*`：八问 8 个独立问答 skill。

## 15+1 能力索引

### 七看（定量规则）

| 规则 | 目录 | 关注点 |
|---|---|---|
| look-01 | `look-01-profit-quality/` | 盈收与利润质量 |
| look-02 | `look-02-cost-structure/` | 费用成本结构 |
| look-03 | `look-03-growth-trend/` | 增长率趋势 |
| look-04 | `look-04-business-market-distribution/` | 业务构成与市场分布 |
| look-05 | `look-05-balance-sheet-health/` | 资产负债健康度 |
| look-06 | `look-06-input-output-efficiency/` | 投入产出效率 |
| look-07 | `look-07-roe-capital-return/` | 收益率与资本回报 |

### 八问（定性证据）

| 问题 | 目录 | 关注点 |
|---|---|---|
| Q1 | `ask-q1-industry-prospect/` | 行业前景与市场规模 |
| Q2 | `ask-q2-moat/` | 竞争优势与护城河 |
| Q3 | `ask-q3-management/` | 管理团队与股权结构 |
| Q4 | `ask-q4-financial-integrity/` | 财务真实性与会计质量 |
| Q5 | `ask-q5-market-position/` | 市场地位与客户集中度 |
| Q6 | `ask-q6-business-model/` | 业务模式与第二曲线 |
| Q7 | `ask-q7-risk-factors/` | 风险因素与应对机制 |
| Q8 | `ask-q8-future-plan/` | 未来规划与战略兑现率 |

### 总编排（1 个）

| 入口 | 目录 | 说明 |
|---|---|---|
| seven-look-eight-question | `seven-look-eight-question/` | 一键执行七看，可选接入八问并输出综合报告 |

## 三种使用路径

### 1) 总编排（推荐）

从仓库根目录运行：

```bash
python 2min-company-analysis/seven-look-eight-question/scripts/seven_looks_orchestrator.py \
  --stock 000002.SZ \
  --as-of-date 2025-04-30
```

追加 `--include-eight-questions` 可并入八问结果：

```bash
python 2min-company-analysis/seven-look-eight-question/scripts/seven_looks_orchestrator.py \
  --stock 000002.SZ \
  --as-of-date 2025-04-30 \
  --include-eight-questions \
  --format json
```

### 2) 单独执行某个 look

```bash
python 2min-company-analysis/look-01-profit-quality/scripts/look_01_profit_quality.py \
  --stock 000002.SZ \
  --as-of-date 2025-04-30
```

### 3) 单独执行某个 ask

```bash
python 2min-company-analysis/ask-q1-industry-prospect/scripts/q01_industry.py \
  --ts-code 000002.SZ \
  --as-of-date 2025-04-30
```

## 输入输出约定

- 输入核心参数：股票代码、分析日期、DuckDB 路径。
- 七看综合输出：质量评分（A/B/C/D）、红旗预警、维度汇总、行动建议。
- 八问并入方式：作为扩展字段合并到总输出；七看评分体系保持独立。
- 最终落盘：可通过 `--final-output` 指定最终 JSON/Markdown 文件。

## 与其它子模块关系

- 结构化数据来源：[tushare_duckdb_sync_skills](../tushare_duckdb_sync_skills/README.md) 与 [tushare_duckdb_sync_scripts](../tushare_duckdb_sync_scripts/README.md)
- 外部证据搜索：[nano-search-mcp](../nano-search-mcp/README.md)
- 依赖关系：`tushare_duckdb_sync_skills`（及其脚本）为必选；`nano-search-mcp` 为可选增强（启用外部证据时建议安装）

若要启用八问中的外部证据取证链路，建议先在仓库根目录安装搜索子模块：

```bash
conda activate legonanobot
pip install -e ./nano-search-mcp
```

如果需要测试百炼相关能力（如行业政策检索），还需在执行前设置：

```bash
export DASHSCOPE_API_KEY=your_token
```

## 维护边界

- 调整子规则脚本前，先确认是否影响总编排输出契约。
- 修改证据口径时，同步更新对应 skill 文档与引用说明。
- 涉及七看八问通用逻辑时，优先在 `seven-look-eight-question/` 下集中维护。

## 推荐流程

1. 先用 `tushare_duckdb_sync_skills` / `tushare_duckdb_sync_scripts` 同步结构化数据。
2. 如需外部证据，安装 `nano-search-mcp` 并启动或导入使用。
3. 再执行本模块的总编排或单项规则。