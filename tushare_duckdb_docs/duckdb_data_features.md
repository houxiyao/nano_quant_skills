# DuckDB 数据特征

## 当前定位

本仓库当前只维护 Tushare 直连 DuckDB 的接入与说明，不再维护 Spark / Paimon 同步链路。

## 表命名规范

DuckDB 中只保留最终清洗表，全部按业务前缀命名（无 `raw_*` 层）：

- `stk_*` — 个股基础信息与量价因子（`stk_info`, `stk_name_history`, `stk_st_daily`, `stk_factor_pro`, `stk_moneyflow`, `stk_moneyflow_ths`, `stk_cyq_perf`, `stk_cyq_chips`, `stk_margin`, `stk_suspend`, `stk_auction_open`, `stk_bse_mapping`, `stk_ah_comparison`）
- `idx_*` — 指数行情、分类与成分（`idx_info`, `idx_daily_dc`, `idx_quote_dc`, `idx_member_daily`, `idx_weight`, `idx_factor_pro`, `idx_sw_classify`, `idx_sw_member_all`）
- `fin_*` — 财务报表（`fin_income`, `fin_balance`, `fin_cashflow`, `fin_indicator`, `fin_express`, `fin_forecast`, `fin_top10_holders`, `fin_top10_float_holders`）

完整字段列表见 [docs/tables/](tables/_index.md)。

## 同步粒度

仓库现有同步脚本只支持三种同步维度：

| 维度 | 含义 | 典型 DuckDB 表 |
|---|---|---|
| `none` | 全量快照 | `stk_info`, `idx_info`, `idx_sw_classify`, `idx_sw_member_all`, `stk_bse_mapping` |
| `trade_date` | 按交易日增量 | `stk_moneyflow`, `stk_factor_pro`, `idx_daily_dc` |
| `period` | 按报告期增量 | `fin_income`, `fin_balance`, `fin_cashflow`, `fin_indicator` |

补充约定：

- 标准视图在表文档中使用 `derived` 表示，它们由现有表计算得到，不参与 Tushare 同步状态管理

## 表族特征

| 表族 | 数量 | 典型主键特征 | 典型时间列 |
|---|---:|---|---|
| stk_* (basic) | 5 | `ts_code` 或字典映射键 | `list_date`, `trade_date` |
| stk_* (factor) | 8 | `ts_code + trade_date` | `trade_date` |
| fin_* | 8 | `ts_code + end_date`，部分带 `ann_date` / `report_type` | `end_date`, `ann_date`, `f_ann_date` |
| idx_* | 8 | `ts_code + trade_date`、`index_code` 或分层行业代码组合 | `trade_date`, `list_date`, `in_date` |

## 时间字段约定

- Tushare 源数据常见日期格式为 `YYYYMMDD`
- 同步脚本接受 `YYYYMMDD` 与 `YYYY-MM-DD`
- DuckDB 内建议将业务日期字段收敛为 `DATE`
- 财务表查询时通常需要同时关注 `end_date` 与 `ann_date`

## 状态表约定

同步状态记录在：

```sql
table_sync_state
```

字段：

| 字段 | 含义 |
|---|---|
| `source_table` | 源表名（Tushare 接口名） |
| `dimension_type` | 同步维度类型（`none` / `trade_date` / `period`） |
| `dimension_value` | 维度值（交易日 `YYYYMMDD` 或报告期） |
| `is_sync` | 是否成功 |
| `error_message` | 错误信息 |
| `updated_at` | 更新时间 |

## 数据质量侧重点

- 财务表最容易出现重复键与公告时点差异
- 因子表最容易出现整列空值、字段扩展或接口字段变更
- 枚举类与字典类表适合全量重刷
- 增量表应优先依赖状态表去重，而不是假设接口天然幂等

## 维护建议

1. 新增 DuckDB 表时，按命名规范选择前缀（`stk_` / `idx_` / `fin_`）
2. 新接入 Tushare 接口时，必须先确认官方文档地址
3. 若只是历史过程材料、采样输出、验收报告，统一放到 `archive/`，不要放回 `docs/`
4. 更新表结构后，重新运行 `_gen_table_docs.py` 刷新 `docs/tables/`