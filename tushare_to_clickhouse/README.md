# Tushare to ClickHouse

将 [Tushare Pro](https://tushare.pro) 数据同步到 ClickHouse，支持全量覆盖与增量追加。

## 特性

- **YAML 配置**：单一配置文件管理所有参数（ClickHouse 连接、Tushare Token、同步策略）
- **批量同步**：通过注册表一键同步多张表，支持积分过滤
- **断点续传**：同步状态记录在 ClickHouse `ReplacingMergeTree` 状态表中，中断后自动跳过已同步维度
- **自动建表**：从 pandas DataFrame 推断 ClickHouse Schema，首次同步自动建表
- **数据质检**：内置 8 项质量检查（PK 唯一性、NULL 检测、NaN 污染、空值率等）
- **交易日安全窗口**：18:00 前默认只同步到上一个开放交易日，避免空 payload 误记成功

## 安装

```bash
cd tushare_to_clickhouse
pip install -e .
```

依赖：`tushare`, `clickhouse-connect`, `pandas`, `loguru`, `pyyaml`, `click`

## 快速开始

### 1. 初始化配置

```bash
tushare-to-clickhouse init
```

编辑生成的配置文件 `config.yaml`：

```yaml
clickhouse:
  host: localhost
  port: 8123
  user: default
  password: ""
  database: tushare_data

tushare:
  token: "your_tushare_token_here"

sync:
  default_start_date: "20100101"
  publish_cutoff_hour: 18
  default_sleep: 0.3
  max_retries: 3
```

### 2. 单表同步

```bash
# 全量覆盖（无维度表，如股票列表）
tushare-to-clickhouse sync \
  --endpoint stock_basic --target-table stk_info \
  --dimension-type none --mode overwrite

# 增量同步（按交易日维度，如日线行情）
tushare-to-clickhouse sync \
  --endpoint daily --target-table stk_daily \
  --dimension-type trade_date --start-date 20240101 --sync-all --sleep 0.3
```

### 3. 批量同步（注册表）

```bash
# 同步注册表中 5000 积分以内的所有表
tushare-to-clickhouse sync-all --registry registry/full_sync_registry.yaml

# 只同步指定表
tushare-to-clickhouse sync-all --tables stk_daily,moneyflow

# 按积分过滤（如只同步 2000 积分以内）
tushare-to-clickhouse sync-all --max-points 2000
```

### 4. 数据质检

```bash
tushare-to-clickhouse check \
  --table stk_daily --pk ts_code,trade_date --date-col trade_date --format markdown
```

### 5. 查看同步状态

```bash
# 查看所有同步状态
tushare-to-clickhouse status

# 查看指定表的状态
tushare-to-clickhouse status --source-table daily
```

## 注册表格式

注册表为 YAML 格式，每条记录定义一张表的同步参数：

```yaml
tables:
  - source_table: daily
    target_table: stk_daily
    endpoint: daily
    dimension_type: trade_date      # none / trade_date / period
    method: query                   # query 或方法名（如 suspend_d）
    pk: [ts_code, trade_date]
    order_by: [ts_code, trade_date]
    partition_by: "toYYYYMM(trade_date)"   # ClickHouse 分区（可选）
    mode: append                    # overwrite / append
    start_date: "20100101"
    sleep: 0.3
    points: 120                     # 积分要求（参考）
    description: "A股日线行情"
```

## 三种维度类型

| 维度类型 | 典型表 | 同步方式 |
|---------|--------|---------|
| `none` | `stock_basic`（股票列表） | 每次全量覆盖 |
| `trade_date` | `daily`（日线）、`moneyflow`（资金流） | 按交易日逐日拉取 |
| `period` | `income`（利润表）、`balancesheet`（资产负债表） | 按季末报告期拉取 |

## ClickHouse Schema 设计

- **表名**：`{类别}_{业务名}`，如 `stk_daily`、`fin_income`
- **引擎**：`MergeTree()`，状态表用 `ReplacingMergeTree(updated_at)`
- **ORDER BY**：业务主键（如 `(ts_code, trade_date)`）
- **PARTITION BY**：日期维度表建议按 `toYYYYMM(trade_date)` 分区
- **类型**：String / Nullable(String) / Float64 / Int64 / Date / Nullable(Date) / DateTime
- **去重**：pandas 层 `drop_duplicates`，因为 ClickHouse MergeTree 不保证唯一性

## 状态表

同步状态存储在 `table_sync_state`（ReplacingMergeTree）：

```sql
SELECT * FROM table_sync_state FINAL
WHERE source_table = 'daily' AND is_sync = 1
```

## CLI 完整命令参考

```
tushare-to-clickhouse init [--config PATH] [--force]
tushare-to-clickhouse sync --endpoint ENDPOINT [options...]
tushare-to-clickhouse sync-all [--registry PATH] [--tables A,B] [--max-points N]
tushare-to-clickhouse check --table TABLE --pk COL1,COL2 [--date-col COL] [--format text|json|markdown]
tushare-to-clickhouse status [--source-table TABLE]
```

## 许可

数据来源及使用须遵守 [Tushare 使用条款](https://tushare.pro/about/agreement)。
