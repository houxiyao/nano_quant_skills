# Nano Quant Skills

面向 AI Agent 的量化分析技能仓库。当前仓库按模块化分层维护，核心分析能力已收敛到子模块目录，便于与其它技能区分与长期维护。

## 模块依赖关系

- `ts2ck`：上游数据同步引擎，将 Tushare 数据写入 ClickHouse（替代原 `tushare_to_duckdb`）。
- `tushare_to_duckdb/tushare_duckdb_sync_skills`：~~上游数据生产 skill 文档~~（已弃用，迁移至 `ts2ck`）。
- `tushare_to_duckdb/tushare_duckdb_sync_scripts`：~~cron 调度脚本~~（已弃用，迁移至 `ts2ck`）。
- `nano-search-mcp`：外部证据模块，提供公告/年报/政策/IR 等检索与正文抓取。
- `2min-company-analysis`：分析与编排模块，消费 ClickHouse 数据，并可选消费 `nano-search-mcp` 的外部证据。

推荐依赖链路：`ts2ck -> nano-search-mcp -> 2min-company-analysis`。

## 环境搭建（uv workspace）

本项目使用 [uv](https://docs.astral.sh/uv/) 管理依赖，采用 workspace 模式组织子项目。

```bash
# 安装 uv（如尚未安装）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 安装所有 workspace 成员
uv sync --all-packages

# 只安装指定子项目
uv sync --package ts2ck
```

配置文件位于 `config.yaml`，初始化：

```bash
uv run ts2ck init
```

## 文档索引（层级）

### 1) 数据同步模块

- [ts2ck](ts2ck/)
  - 作用：将 Tushare Pro 数据同步到 ClickHouse，支持全量/增量/断点续传。
  - 文档入口：[ts2ck/README.md](ts2ck/README.md)
- ~~[tushare_to_duckdb/tushare_duckdb_sync_skills](tushare_to_duckdb/tushare_duckdb_sync_skills/)~~（已弃用，迁移至 `ts2ck`）
- ~~[tushare_to_duckdb/tushare_duckdb_sync_scripts](tushare_to_duckdb/tushare_duckdb_sync_scripts/)~~（已弃用，迁移至 `ts2ck`）

### 2) 数据搜索模块

- [nano-search-mcp](nano-search-mcp/)
  - 作用：提供公告、年报、行业研报、IR 纪要、监管处罚、行业政策等外部数据搜索与正文抓取能力。
  - 文档入口：[nano-search-mcp/README.md](nano-search-mcp/README.md)

### 3) 公司分析模块（主入口）

- [2min-company-analysis](2min-company-analysis/)
  - 作用：封装“七看八问”15 个子 skill + 1 个总编排 skill。
  - 模块总文档：[2min-company-analysis/README.md](2min-company-analysis/README.md)

#### 3.1 七看（7 个定量 skill）

- [look-01-profit-quality](2min-company-analysis/look-01-profit-quality/)
- [look-02-cost-structure](2min-company-analysis/look-02-cost-structure/)
- [look-03-growth-trend](2min-company-analysis/look-03-growth-trend/)
- [look-04-business-market-distribution](2min-company-analysis/look-04-business-market-distribution/)
- [look-05-balance-sheet-health](2min-company-analysis/look-05-balance-sheet-health/)
- [look-06-input-output-efficiency](2min-company-analysis/look-06-input-output-efficiency/)
- [look-07-roe-capital-return](2min-company-analysis/look-07-roe-capital-return/)

#### 3.2 八问（8 个定性 skill）

- [ask-q1-industry-prospect](2min-company-analysis/ask-q1-industry-prospect/)
- [ask-q2-moat](2min-company-analysis/ask-q2-moat/)
- [ask-q3-management](2min-company-analysis/ask-q3-management/)
- [ask-q4-financial-integrity](2min-company-analysis/ask-q4-financial-integrity/)
- [ask-q5-market-position](2min-company-analysis/ask-q5-market-position/)
- [ask-q6-business-model](2min-company-analysis/ask-q6-business-model/)
- [ask-q7-risk-factors](2min-company-analysis/ask-q7-risk-factors/)
- [ask-q8-future-plan](2min-company-analysis/ask-q8-future-plan/)

#### 3.3 总编排（1 个）

- [seven-look-eight-question](2min-company-analysis/seven-look-eight-question/)
  - 入口脚本：`seven_looks_orchestrator.py`
  - 可选并入八问：`--include-eight-questions`

## 推荐使用顺序

1. 先按 [ts2ck](ts2ck/) 完成环境初始化和 ClickHouse 数据同步。
2. 如需公告、年报、政策、IR 等外部证据，安装并使用 [nano-search-mcp](nano-search-mcp/)。
3. 再进入 [2min-company-analysis/README.md](2min-company-analysis/README.md) 选择总编排或单独 look/ask。

最小联动示例：

```bash
# 1) 初始化 ts2ck 配置
uv run ts2ck init

# 2) 同步数据（单表）
uv run ts2ck sync \
  --endpoint daily --target-table stk_daily \
  --dimension-type trade_date --start-date 20240101 --sync-all

# 3) 或批量同步注册表中所有表
uv run ts2ck sync-all --registry ts2ck/registry.yaml

# 4) 执行公司分析总编排
uv run python 2min-company-analysis/seven-look-eight-question/scripts/seven_looks_orchestrator.py \
  --stock 000001.SZ \
  --as-of-date 2026-04-24 \
  --include-eight-questions
```

## 目录结构

```text
nano_quant_skills/
├── README.md
├── pyproject.toml                 # uv workspace root
├── uv.lock
├── ts2ck/         # Tushare → ClickHouse 同步引擎（uv workspace member）
├── nano-search-mcp/
├── 2min-company-analysis/
│   ├── README.md
│   ├── look-01-profit-quality/ ... look-07-roe-capital-return/
│   ├── ask-q1-industry-prospect/ ... ask-q8-future-plan/
│   └── seven-look-eight-question/
└── tushare_to_duckdb/             # （已弃用，迁移至 `ts2ck`）
```

## 作为 AI Skill 使用

- 技能型目录（如 `tushare_duckdb_sync_skills`、`2min-company-analysis` 下各子 skill）：可通过 `SKILL.md` 被 AI Agent 直接引用。
- `nano-search-mcp`：这是 MCP 服务子模块，不是单个 `SKILL.md` 目录；应按其 README 安装为 Python 包或 MCP 服务，而不是直接复制进 `.github/skills/`。

## 许可

脚本可自由复制使用。数据来源须遵守各数据提供方使用条款。
