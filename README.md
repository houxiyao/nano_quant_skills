# Nano Quant Skills

面向 AI Agent 的量化分析技能集合。每个 Skill 均为独立目录，包含工作流文档、可执行脚本和模板，可被 GitHub Copilot、Claude Code、Qoder 等 AI 编程工具直接调用。

## Skills 列表

| Skill | 说明 | 脚本 |
|---|---|---|
| [tushare-duckdb-sync](tushare-duckdb-sync/) | 从 Tushare Pro 同步数据到本地 DuckDB，支持全量/增量模式、质检、元数据文档生成 | `sync_table.py` · `check_quality.py` |
| [look-01-profit-quality](look-01-profit-quality/) | 一看：盈收与利润质量 — 扣非利润、毛利率、净利率、经营现金流 | `look_01_profit_quality.py` |
| [look-02-cost-structure](look-02-cost-structure/) | 二看：费用成本结构 — 四大费用率及费用与营收匹配度 | `look_02_cost_structure.py` |
| [look-03-growth-trend](look-03-growth-trend/) | 三看：增长率趋势 — 营收/净利润 CAGR、内生 vs 并购增长信号 | `look_03_growth_trend.py` |
| [look-04-business-market-distribution](look-04-business-market-distribution/) | 四看：业务构成与市场分布 — 主营占比、海外销售、客户集中度（需年报文本） | `look_04_business_market_distribution.py` |
| [look-05-balance-sheet-health](look-05-balance-sheet-health/) | 五看：资产负债健康度 — 现金流覆盖、有息负债、偿债能力、隐性负债（需年报附注） | `look_05_balance_sheet_health.py` |
| [look-06-input-output-efficiency](look-06-input-output-efficiency/) | 六看：投入产出效率 — 营运资金/收入、固定资产效率、人均投入产出 | `look_06_input_output_efficiency.py` |
| [look-07-roe-capital-return](look-07-roe-capital-return/) | 七看：收益率与资本回报 — ROE 杜邦三要素拆解、驱动类型、趋势 | `look_07_roe_capital_return.py` |
| [seven-look-eight-question](seven-look-eight-question/) | **七看一键编排** — 顺序执行 7 个维度、汇总红旗、评分、生成行动建议 | `run_seven_looks.py` |

## 目录结构

```
nano_quant_skills/
├── README.md                              ← 本文件
├── tushare-duckdb-sync/                   ← Skill：Tushare → DuckDB 数据同步
│   ├── SKILL.md
│   ├── README.md
│   ├── scripts/
│   ├── templates/
│   └── examples/
├── look-01-profit-quality/                ← 一看：盈收与利润质量
│   ├── SKILL.md
│   └── scripts/
├── look-02-cost-structure/                ← 二看：费用成本结构
│   ├── SKILL.md
│   └── scripts/
├── look-03-growth-trend/                  ← 三看：增长率趋势
│   ├── SKILL.md
│   └── scripts/
├── look-04-business-market-distribution/  ← 四看：业务构成与市场分布
│   ├── SKILL.md
│   └── scripts/
├── look-05-balance-sheet-health/          ← 五看：资产负债健康度
│   ├── SKILL.md
│   └── scripts/
├── look-06-input-output-efficiency/       ← 六看：投入产出效率
│   ├── SKILL.md
│   └── scripts/
├── look-07-roe-capital-return/            ← 七看：收益率与资本回报
│   ├── SKILL.md
│   └── scripts/
└── seven-look-eight-question/             ← 七看一键编排
    ├── SKILL.md
    ├── scripts/run_seven_looks.py
    ├── assets/rule_registry.json
    └── references/
```

## 设计原则

1. **自包含**：每个 Skill 的脚本不依赖本仓库其它模块，复制即用。
2. **双入口**：`SKILL.md` 面向 AI Agent 编排工作流；`README.md` 面向人类开发者。
3. **三项资产**：数据同步类 Skill 要求每次执行产出数据、元数据文档、运维记录三项资产。
4. **可扩展**：新增 Skill 只需创建新目录并遵循相同结构。

## 七看财务分析使用指南

"七看"是一套面向 A 股基本面分析的量化框架，从 7 个维度（盈利质量、费用结构、增长趋势、业务分布、资产负债、投入产出、资本回报）对上市公司进行系统性财务体检。

### 前置条件

- Python 3.10+，安装 `duckdb` 包
- 本地 DuckDB 数据库文件（由 `tushare-duckdb-sync` 同步生成），默认路径 `data/ashare.duckdb`
- 银行/保险/证券类公司会返回 `not-applicable`（财务报表结构不适用）

### 单维度执行

每个 look 都可以独立运行，输出 JSON 到 stdout：

```bash
# 示例：分析 000002.SZ（万科A）的盈利质量
python look-01-profit-quality/scripts/look_01_profit_quality.py \
    --stock 000002.SZ \
    --as-of-date 2025-04-30 \
    --lookback-years 3 \
    --db-path data/ashare.duckdb

# 示例：分析 ROE 与资本回报
python look-07-roe-capital-return/scripts/look_07_roe_capital_return.py \
    --stock 000002.SZ \
    --as-of-date 2025-04-30
```

### 一键七看

使用编排脚本顺序执行全部 7 个维度，自动汇总红旗预警、计算质量评分、生成行动建议：

```bash
# Markdown 报告（默认）
python seven-look-eight-question/scripts/run_seven_looks.py \
    --stock 000002.SZ \
    --as-of-date 2025-04-30

# JSON 输出 + 保存 7 份中间文件
python seven-look-eight-question/scripts/run_seven_looks.py \
    --stock 000002.SZ \
    --as-of-date 2025-04-30 \
    --format json \
    --output-dir ./output/000002
```

**主要参数：**

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--stock` | 股票代码（如 000002.SZ） | 必填 |
| `--as-of-date` | 分析截止日（YYYY-MM-DD） | 今天 |
| `--db-path` | DuckDB 数据库路径 | `data/ashare.duckdb` |
| `--format` | 输出格式：`json` 或 `markdown` | `markdown` |
| `--output-dir` | 中间 JSON 保存目录 | 系统临时目录 |
| `--report-bundle-04` | look-04 年报文本路径（可选） | — |
| `--report-bundle-05` | look-05 年报附注路径（可选） | — |

**质量评分规则：** 基础 100 分，严重红旗 −15，一般预警 −5。等级：A(≥80) / B(60-79) / C(40-59) / D(<40)。

### 作为 AI Agent Skill 使用

将整个目录放入 `.github/skills/`（GitHub Copilot）或在对话中引用 `SKILL.md` 路径，AI Agent 即可理解并编排执行。

## 快速开始

### 环境要求

- Python 3.10+
- 各 Skill 的依赖见其 `README.md`

### 作为 AI Skill 使用

通过如下自然语言命令，让大模型分析并安装 Skill：

> 请阅读 `nano_quant_skills/tushare-duckdb-sync/SKILL.md`，将其安装为你可调用的 Skill。

**各工具安装方式：**

| AI 工具 | 推荐安装方式 | 说明 |
|---|---|---|
| **Claude Code** | 安装为 Command（`/slash` 命令） | 将 SKILL.md 内容适配到 `.claude/commands/` 目录，之后通过 `/tushare-sync` 直接调用 |
| **GitHub Copilot** | 配置为 Skill | 将目录放入 `.github/skills/`，Copilot Chat 自动识别 |
| **Qoder** | 配置为 Prompt | 将 SKILL.md 适配到 `.qoder/prompts/` 目录 |
| **OpenCode** | 配置为 Prompt | 将 SKILL.md 适配到 `.opencode/prompts/` 目录 |
| **其它 Agent / OpenClaw / Nanobot** | 直接引用 | 在对话中指向 `SKILL.md` 路径即可 |

最稳妥的安装方案为复制如下命令给 AI Agent：
```
1. 阅读 `https://github.com/shadowinlife/nano_quant_skills` 项目，理解其文件结构和设计原则。
2. 将仓库中所有的SKILLS安装为你可调用的 Skills 或者 Commands，确保用户能使用它们
3. 如果本地已经存在 `tushare-duckdb-sync` 目录，比较内容差异后进行升级,**必须保留本地修改备份以便恢复**。
```

## 许可

脚本可自由复制使用。数据来源须遵守各数据提供方的使用条款。
