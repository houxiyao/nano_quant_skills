# Company Analysis Skills

基于 `company-analysis-mcp` MCP 服务的 A 股上市公司"七看八问"分析技能集。

## 前置条件

- `company-analysis-mcp` 服务已启动并可通过 MCP 协议访问
- ClickHouse 中已有 tushare 数据（由 `ts2ck` 同步）

## 技能索引

### 七看（定量财务分析）

| 技能 | MCP 工具 | 分析维度 |
|------|----------|----------|
| look-01-profit-quality | `look_01_profit_quality` | 盈收与利润质量 |
| look-02-cost-structure | `look_02_cost_structure` | 费用成本结构 |
| look-03-growth-trend | `look_03_growth_trend` | 增长率趋势 |
| look-04-business-distribution | `look_04_business_market_distribution` | 业务构成与市场分布 |
| look-05-balance-sheet-health | `look_05_balance_sheet_health` | 资产负债健康度 |
| look-06-efficiency | `look_06_input_output_efficiency` | 投入产出效率 |
| look-07-roe-capital-return | `look_07_roe_capital_return` | ROE 与资本回报 |

### 八问（定性证据分析）

| 技能 | MCP 工具 | 问题维度 |
|------|----------|----------|
| ask-q1-industry-prospect | `ask_q1_industry_prospect` | 行业前景 |
| ask-q2-moat | `ask_q2_moat` | 竞争优势 |
| ask-q3-management | `ask_q3_management` | 管理团队 |
| ask-q4-financial-integrity | `ask_q4_financial_integrity` | 财务真实性 |
| ask-q5-market-position | `ask_q5_market_position` | 市场地位 |
| ask-q6-business-model | `ask_q6_business_model` | 业务模式 |
| ask-q7-risk-factors | `ask_q7_risk_factors` | 风险因素 |
| ask-q8-future-plan | `ask_q8_future_plan` | 未来规划 |

### 编排

| 技能 | MCP 工具 | 用途 |
|------|----------|------|
| seven-look-eight-question | `orchestrate_seven_looks` + `orchestrate_eight_questions` | 一键综合分析 |

## 使用方式

### 单维度分析

直接调用对应 skill，输入股票代码即可。

### 综合分析

使用 `seven-look-eight-question` skill 一键执行全部维度并获取质量评分。
