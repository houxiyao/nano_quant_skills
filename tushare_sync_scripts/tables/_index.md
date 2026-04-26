# 表/视图索引

DuckDB 路径: `/Users/mgong/LegoNanoBot/Qdata/duckdb/ashare.duckdb`

| 模块 | 表名 | 列数 | 粒度 | 同步维度 | 描述 |
|---|---|---:|---|---|---|
| basic | `stk_ah_comparison` | 11 | 标的-交易日 | `trade_date` | A/H 股比价数据 |
| basic | `stk_bse_mapping` | 4 | 全量快照/字典 | `none` | 北交所旧码与新码映射 |
| basic | `stk_info` | 10 | 全量快照 | `none` | A股基本信息，含上市状态、行业、实控人 |
| basic | `stk_name_history` | 6 | 标的-报告期 | `none` | 股票历史曾用名及变更原因 |
| basic | `stk_st_daily` | 5 | 标的-交易日 | `trade_date` | 股票 ST/退市风险状态日历 |
| basic | `trade_calendar` | 4 | 全量快照 | `none` | SSE 交易日历 2000-2030，含 is_open 与 pretrade_date |
| factor | `stk_auction_open` | 9 | 标的-交易日 | `trade_date` | 集合竞价（开盘）成交数据 |
| factor | `stk_cyq_chips` | 4 | 标的-交易日-价格 | `trade_date` | 逐价位筹码占比分布（每行一个价位） |
| factor | `stk_cyq_perf` | 11 | 标的-交易日 | `trade_date` | 筹码分布：成本分位 + 获利盘比率 |
| factor | `stk_factor_pro` | 199 | 标的-交易日 | `trade_date` | 个股日行情 + 估值 + 复权价格 + 200+ 技术指标（261列） |
| factor | `stk_margin` | 11 | 标的-交易日 | `trade_date` | 融资融券明细：融资余额、融券余量、偿还量 |
| factor | `stk_moneyflow` | 20 | 标的-交易日 | `trade_date` | 个股大中小单资金净流向 |
| factor | `stk_moneyflow_ths` | 13 | 标的-交易日 | `trade_date` | 同花顺资金流向（大单/中单/小单净额） |
| factor | `stk_suspend` | 4 | 标的-交易日 | `trade_date` | 个股停复牌日历 |
| finance | `fin_balance` | 158 | 标的-报告期 | `period` | 资产负债表：总资产、负债、股东权益 |
| finance | `fin_cashflow` | 97 | 标的-报告期 | `period` | 现金流量表：经营/投资/筹资活动现金流 |
| finance | `fin_express` | 32 | 标的-报告期 | `period` | 业绩快报：提前发布的净利润/营收预估 |
| finance | `fin_forecast` | 12 | 标的-报告期 | `period` | 业绩预告：预增/预减/扭亏/续盈类型 |
| finance | `fin_income` | 94 | 标的-报告期 | `period` | 利润表：营收、净利润、EPS |
| finance | `fin_indicator` | 168 | 标的-报告期 | `period` | 财务指标：ROE/ROA/CFPS/毛利率等衍生指标 |
| finance | `fin_top10_float_holders` | 9 | 标的-报告期-股东 | `period` | 前十大流通股东持股 |
| finance | `fin_top10_holders` | 9 | 标的-报告期-股东 | `period` | 前十大股东持股 |
| index | `idx_daily_dc` | 12 | 标的-交易日 | `trade_date` | 大盘/板块指数日行情：开高低收量额波动率 |
| index | `idx_factor_pro` | 89 | 标的-交易日 | `trade_date` | 指数日行情 + 80+ 技术指标 |
| index | `idx_info` | 8 | 全量快照 | `none` | 指数基本信息：发布机构、基期、成分数 |
| index | `idx_member_daily` | 4 | 交易日-成分股 | `trade_date` | 指数成分股每日快照 |
| index | `idx_quote_dc` | 11 | 标的-交易日 | `trade_date` | 大盘指数行情概览：领涨股、涨跌家数、总市值 |
| index | `idx_sw_classify` | 7 | 行业层级字典 | `none` | 申万行业分类字典（SW2021，L1/L2/L3） |
| index | `idx_sw_member_all` | 11 | 行业层级-成分股快照 | `none` | 申万行业成分股全量快照（按 L1/L2/L3 分层） |
| index | `idx_sw_l3_peers` | 13 | 锚股票-同 L3 成分股 | `derived` | 标准视图：按当前申万 L3 分类展开同分类股票列表 |
