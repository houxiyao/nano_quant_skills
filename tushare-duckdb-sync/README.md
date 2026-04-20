# Tushare → DuckDB 数据同步工具

将 [Tushare Pro](https://tushare.pro) 数据同步到本地 DuckDB，支持全量覆盖与增量追加。

## 快速开始

### 环境要求

```bash
pip install tushare duckdb pandas loguru
```

### 设置 Token

```bash
export TUSHARE_TOKEN=你的token  # https://tushare.pro 注册后在个人主页获取
```

### 交易日安全窗口

- 对 `daily`、`moneyflow`、`stk_factor_pro`、`idx_factor_pro` 等盘后更新表，建议在 `Asia/Shanghai 18:00` 后同步当天数据。
- 当 `--dimension-type trade_date` 且未显式传入 `--end-date` 时，脚本默认会在 18:00 前把截止日收敛到上一个开放交易日，避免把当天空 payload 误记成功。
- 如果你显式强拉今天，而上游还没出数，脚本会把该维度写成失败状态，等待后续重试。
- 只有在“0 行结果本来就是正确业务语义”的场景下，才应显式使用 `--allow-empty-result`。

### 全量同步（无维度表，如股票列表）

```bash
python sync_table.py \
  --endpoint stock_basic \
  --duckdb-path ./ashare.duckdb \
  --target-table stk_info \
  --mode overwrite \
  --dimension-type none
```

### 增量同步（按交易日维度，如日线行情）

```bash
python sync_table.py \
  --endpoint daily \
  --duckdb-path ./ashare.duckdb \
  --target-table stk_daily \
  --mode append \
  --dimension-type trade_date \
  --start-date 20240101 \
  --sync-all \
  --sleep 0.3
```

`--sync-all` 启用断点续传：已同步的交易日会自动跳过，中断后重跑即可继续。

如果在 18:00 前执行且未传 `--end-date`，脚本默认只会同步到上一个开放交易日。

### 按报告期同步（如财务报表）

```bash
python sync_table.py \
  --endpoint income_vip \
  --duckdb-path ./ashare.duckdb \
  --target-table fin_income \
  --mode append \
  --dimension-type period \
  --start-date 20100331 \
  --sync-all
```

### 批量同步

创建 `tasks.json`：

```json
[
  {
    "endpoint": "stock_basic",
    "target_table": "stk_info",
    "mode": "overwrite",
    "dimension_type": "none"
  },
  {
    "endpoint": "daily",
    "target_table": "stk_daily",
    "mode": "append",
    "dimension_type": "trade_date",
    "start_date": "20240101",
    "sync_all": true
  }
]
```

```bash
python sync_table.py --tasks-file tasks.json --duckdb-path ./ashare.duckdb
```

## 数据质检

```bash
python check_quality.py \
  --duckdb-path ./ashare.duckdb \
  --table stk_daily \
  --pk ts_code,trade_date \
  --date-col trade_date \
  --format markdown
```

检查项：行数、PK 唯一性、PK 非空、日期范围、NaN 字符串污染、度量列空值率。

输出格式：`text`（默认）、`json`、`markdown`（可直接嵌入文档）。

## sync_table.py 参数说明

| 参数 | 必填 | 默认值 | 说明 |
|---|---|---|---|
| `--endpoint` | 是* | — | Tushare 接口名（如 `daily`、`stock_basic`） |
| `--duckdb-path` | 是 | — | DuckDB 文件路径 |
| `--target-table` | 否 | 同 endpoint | DuckDB 目标表名 |
| `--source-table` | 否 | 同 endpoint | 同步状态跟踪用的逻辑名 |
| `--mode` | 否 | `overwrite` | `overwrite`（全量覆盖）或 `append`（增量追加） |
| `--dimension-type` | 否 | `none` | `none` / `trade_date` / `period` |
| `--dimension-field` | 否 | 同 dimension-type | API 调用时维度参数名 |
| `--method` | 否 | `query` | Tushare 调用方式（`query` 或方法名如 `suspend_d`） |
| `--start-date` | 否 | `20100101` | 起始日期 YYYYMMDD |
| `--end-date` | 否 | 今天 | 截止日期 YYYYMMDD |
| `--sync-all` | 否 | false | 启用断点续传（跳过已同步维度，失败时继续） |
| `--params` | 否 | — | 额外 Tushare 参数（JSON 字符串） |
| `--sleep` | 否 | `0.3` | 每次调用间隔（秒），防限频 |
| `--max-retries` | 否 | `3` | 失败重试次数 |
| `--allow-empty-result` | 否 | false | 允许空 payload 记成功，仅适用于 0 行本来就是合法结果的接口 |
| `--disable-safe-trade-date` | 否 | false | 关闭交易日安全截止规则；未传 `--end-date` 时允许直接尝试今天 |
| `--publish-cutoff-hour` | 否 | `18` | `Asia/Shanghai` 的发布截止小时；早于该时间默认只同步到上一个开放交易日 |
| `--tasks-file` | 否 | — | 批量任务 JSON 文件路径（此时 `--endpoint` 非必填） |

## 同步状态

脚本在 DuckDB 内维护 `table_sync_state` 表，记录每个维度值的同步状态，支持：

- **断点续传**：配合 `--sync-all`，已同步的维度自动跳过。
- **失败追踪**：`is_sync=0` 的记录包含错误信息，可定向重试。
- **空 payload 保护**：增量维度默认把空返回记为失败，避免把“上游未发布”误当成“同步成功”。

## 三种维度类型

| 维度类型 | 典型表 | 同步方式 |
|---|---|---|
| `none` | `stock_basic`（股票列表） | 每次全量覆盖 |
| `trade_date` | `daily`（日线）、`moneyflow`（资金流） | 按交易日逐日拉取 |
| `period` | `income`（利润表）、`balancesheet`（资产负债表） | 按季末报告期拉取 |

## 许可

脚本为自包含文件，可自由复制使用。数据来源及使用须遵守 [Tushare 使用条款](https://tushare.pro/about/agreement)。
