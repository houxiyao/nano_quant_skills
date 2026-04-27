# 示例：daily — 交易日增量更新表

> 本示例演示如何同步一张 **按交易日增量追加** 的 Tushare 表。
> `daily` 返回 A 股日线行情，每个交易日约 5000+ 条，需按 trade_date 逐日拉取。

## 1. Tushare 文档信息

- **文档地址**：https://tushare.pro/document/2?doc_id=27
- **接口名称**：`daily`
- **积分要求**：基础积分
- **限量**：每分钟 500 次，每次 6000 条
- **更新频率**：交易日盘后更新；社区实践建议按 `Asia/Shanghai 18:00` 后同步当天数据
- **调用方式**：`pro.query('daily', trade_date='20240101')` 或 `pro.daily(trade_date='20240101')`
- **说明**：未复权行情，停牌期间不提供数据

## 2. 首次全量同步

```bash
TUSHARE_TOKEN=你的token python sync_table.py \
    --endpoint daily \
    --duckdb-path ./ashare.duckdb \
    --target-table stk_daily \
    --mode append \
    --dimension-type trade_date \
    --start-date 20100101 \
    --sync-all \
    --sleep 0.3
```

- `--sync-all`：启用断点续传，已同步日期自动跳过
- `--sleep 0.3`：每次 API 调用间隔 0.3s，避免限频
- 首次同步约需 3500+ 个交易日，耗时较长

## 3. 增量同步（日常使用）

```bash
TUSHARE_TOKEN=你的token python sync_table.py \
    --endpoint daily \
    --duckdb-path ./ashare.duckdb \
    --target-table stk_daily \
    --mode append \
    --dimension-type trade_date \
    --start-date 20260415 \
    --sleep 0.3
```

对应 task JSON：

```json
[
  {
    "endpoint": "daily",
    "source_table": "daily",
    "target_table": "stk_daily",
    "mode": "append",
    "dimension_type": "trade_date",
    "start_date": "20260415",
    "sync_all": false,
    "sleep_seconds": 0.3
  }
]
```

### 交易日发布时间注意事项

- 如果命令在 `Asia/Shanghai 18:00` 前执行，且没有显式传 `end_date`，社区版 `sync_table.py` 会自动把截止日收敛到上一个开放交易日。
- 如果你显式强拉今天，而上游还没出数，脚本会把该维度写成失败状态，等待后续重试，而不是误记成功。
- 只有在你确认“当天 0 行是合法业务结果”时，才应添加 `--allow-empty-result`。

## 4. 建表 DDL（首次同步后执行）

```sql
-- 添加主键（确保 ts_code + trade_date 唯一）
ALTER TABLE stk_daily ADD PRIMARY KEY (ts_code, trade_date);

-- 添加交易日索引（加速按日期范围查询）
CREATE INDEX IF NOT EXISTS idx_stk_daily_trade_date ON stk_daily (trade_date);
```

## 5. 质检命令

```bash
python check_quality.py \
    --duckdb-path ./ashare.duckdb \
    --table stk_daily \
    --pk ts_code,trade_date \
    --date-col trade_date \
    --format markdown
```

## 6. 期望的元数据文档

---

# A股日线行情

## 基本信息
| 属性 | 值 |
|---|---|
| DuckDB 表名 | `stk_daily` |
| Tushare 接口 | `daily` |
| Tushare 文档 | https://tushare.pro/document/2?doc_id=27 |
| Tushare doc_id | 27 |
| 同步维度 | trade_date / trade_date |
| 同步模式 | append |
| PK | `(ts_code, trade_date)` |
| 索引 | `idx_stk_daily_trade_date` ON `(trade_date)` |
| 积分要求 | 基础积分 |

## 字段详情

> 此表覆盖目标表的 **全部列**。零遗漏是硬性要求。

| # | 列名 | DuckDB 类型 | Tushare 原始类型 | 数据角色 | 中文说明 | 量化用途 | 取值范围/格式 | 注意事项 |
|---|---|---|---|---|---|---|---|---|
| 1 | ts_code | VARCHAR | str | 标识 | 股票代码 | 唯一标识 | 000001.SZ 格式 | — |
| 2 | trade_date | DATE | str(YYYYMMDD) | 维度 | 交易日期 | 时间维度 | 2010-01-04 ~ 至今 | ETL 自动从 VARCHAR 转 DATE |
| 3 | open | DOUBLE | float | 度量 | 开盘价 | OHLC 行情 | > 0 | 未复权；停牌日无数据 |
| 4 | high | DOUBLE | float | 度量 | 最高价 | OHLC 行情 | ≥ open | 未复权 |
| 5 | low | DOUBLE | float | 度量 | 最低价 | OHLC 行情 | ≤ open | 未复权 |
| 6 | close | DOUBLE | float | 度量 | 收盘价 | OHLC 行情 | > 0 | 未复权；是涨跌幅计算基准 |
| 7 | pre_close | DOUBLE | float | 度量 | 昨收价（除权价） | OHLC 行情 | > 0 | 除权后的昨收，非实际昨收 |
| 8 | change | DOUBLE | float | 度量 | 涨跌额 | 成交量价 | 可为负 | close - pre_close |
| 9 | pct_chg | DOUBLE | float | 度量 | 涨跌幅(%) | 成交量价 | 通常 -20 ~ +20 | 基于除权昨收计算；涨跌停±10%/±20% |
| 10 | vol | DOUBLE | float | 度量 | 成交量（手） | 成交量价 | ≥ 0 | 单位：手（100股） |
| 11 | amount | DOUBLE | float | 度量 | 成交额（千元） | 成交量价 | ≥ 0 | 单位：千元 |

### 列角色分类汇总
- **标识字段**：ts_code
- **维度字段**：trade_date
- **度量字段**：open, high, low, close, pre_close, change, pct_chg, vol, amount
- **辅助字段**：无

### 特殊取值说明
- `pre_close` 是除权价，不等于实际前一日收盘价（分红送股后有差异）。
- `pct_chg` 基于除权昨收计算：`(close - pre_close) / pre_close * 100`。
- `change` / `pct_chg` 可以为负数，非脏数据。
- 停牌期间不提供数据（非 NULL 行，而是整天无数据）。
- 新股上市首日 `pre_close` 为发行价。

### Tushare ↔ DuckDB 字段差异
- 与 Tushare 文档一致，无差异。

## 数据质量快照

| 指标 | 值 | 检查时间 |
|---|---|---|
| 总行数 | ~18,000,000 | {同步后填写} |
| 数据起始 | 2010-01-04 | {同步后填写} |
| 数据截止 | {最新交易日} | {同步后填写} |
| 股票数 (DISTINCT ts_code) | ~5500 | {同步后填写} |
| 交易日/报告期数 | ~3800 | {同步后填写} |
| PK 重复数 | 0 | {同步后填写} |
| PK 列 NULL 数 | ts_code=0, trade_date=0 | {同步后填写} |
| NaN 字符串污染列 | 无 | {同步后填写} |
| 度量列全 NULL 率 > 50% | 无 | {同步后填写} |

## 同步记录

| 时间 | 操作 | 新增行数 | 数据截止 | 备注 |
|---|---|---|---|---|
| {datetime} | 全量 | ~18,000,000 | {最新交易日} | 首次同步 --sync-all |
