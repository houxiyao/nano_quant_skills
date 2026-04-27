# 示例：stock_basic — 全量覆盖表（无维度）

> 本示例演示如何同步一张 **无维度、全量覆盖** 的 Tushare 表。
> `stock_basic` 返回全市场 A 股列表，每次拉取完整数据后直接覆盖本地表。

## 1. Tushare 文档信息

- **文档地址**：https://tushare.pro/document/2?doc_id=25
- **接口名称**：`stock_basic`
- **积分要求**：2000 积分起
- **限量**：每次最多 6000 行（覆盖全市场）
- **更新频率**：有新股上市或退市时更新
- **调用方式**：`pro.query('stock_basic', ...)`

## 2. 同步命令

```bash
TUSHARE_TOKEN=你的token python sync_table.py \
    --endpoint stock_basic \
    --duckdb-path ./ashare.duckdb \
    --target-table stk_info \
    --mode overwrite \
    --dimension-type none
```

对应的 task JSON（用于批量模式）：

```json
[
  {
    "endpoint": "stock_basic",
    "source_table": "stock_basic",
    "target_table": "stk_info",
    "mode": "overwrite",
    "dimension_type": "none",
    "method": "query"
  }
]
```

## 3. 同步后建立 PK 与索引

`stock_basic` 同步使用 `overwrite` 模式，首次写入由 `CREATE TABLE AS SELECT` 创建表，
不含 PK 和索引。同步完成后需手动执行以下 DDL：

```sql
-- 添加主键
ALTER TABLE stk_info ADD PRIMARY KEY (ts_code);

-- 添加常用查询索引
CREATE INDEX IF NOT EXISTS idx_stk_info_list_date ON stk_info (list_date);
CREATE INDEX IF NOT EXISTS idx_stk_info_industry ON stk_info (industry);
```

> **Agent 工作流**：生成上述 DDL 后展示给用户确认，确认后执行。

## 4. 质检命令

```bash
python check_quality.py \
    --duckdb-path ./ashare.duckdb \
    --table stk_info \
    --pk ts_code \
    --format markdown
```

## 5. 期望的元数据文档

以下是 `stock_basic` 同步完成后应产出的完整元数据文档：

---

# 股票基本信息

## 基本信息
| 属性 | 值 |
|---|---|
| DuckDB 表名 | `stk_info` |
| Tushare 接口 | `stock_basic` |
| Tushare 文档 | https://tushare.pro/document/2?doc_id=25 |
| Tushare doc_id | 25 |
| 同步维度 | none |
| 同步模式 | overwrite |
| PK | `(ts_code)` |
| 索引 | `idx_stk_info_list_date` ON `(list_date)` |
| 积分要求 | 2000 |

## 字段详情

> 此表覆盖目标表的 **全部列**。零遗漏是硬性要求。

| # | 列名 | DuckDB 类型 | Tushare 原始类型 | 数据角色 | 中文说明 | 量化用途 | 取值范围/格式 | 注意事项 |
|---|---|---|---|---|---|---|---|---|
| 1 | ts_code | VARCHAR | str | 标识 | TS代码 | 唯一标识 | 000001.SZ 格式 | — |
| 2 | symbol | VARCHAR | str | 辅助 | 股票代码 | 辅助信息 | 纯数字6位 | 不含交易所后缀 |
| 3 | name | VARCHAR | str | 辅助 | 股票名称 | 辅助信息 | 中文名 | 可能包含 ST / *ST 前缀 |
| 4 | area | VARCHAR | str | 维度 | 地域 | 分类维度 | 省份/城市名 | — |
| 5 | industry | VARCHAR | str | 维度 | 所属行业 | 分类维度 | 行业名称 | — |
| 6 | fullname | VARCHAR | str | 辅助 | 股票全称 | 辅助信息 | — | 可能为 NULL |
| 7 | enname | VARCHAR | str | 辅助 | 英文全称 | 辅助信息 | — | 可能为 NULL |
| 8 | cnspell | VARCHAR | str | 辅助 | 拼音缩写 | 辅助信息 | 大写字母 | — |
| 9 | market | VARCHAR | str | 维度 | 市场类型 | 分类维度 | 主板/创业板/科创板/CDR/北交所 | — |
| 10 | exchange | VARCHAR | str | 维度 | 交易所代码 | 分类维度 | SSE/SZSE/BSE | 可能为 NULL |
| 11 | curr_type | VARCHAR | str | 辅助 | 交易货币 | 辅助信息 | CNY | 可能为 NULL |
| 12 | list_status | VARCHAR | str | 维度 | 上市状态 | 状态标记 | L/D/G/P | L=上市 D=退市 G=过会 P=暂停 |
| 13 | list_date | DATE | str(YYYYMMDD) | 维度 | 上市日期 | 时间维度 | 19901210 ~ 至今 | ETL 自动转 DATE |
| 14 | delist_date | DATE | str(YYYYMMDD) | 维度 | 退市日期 | 时间维度 | — | 多数为 NULL（未退市） |
| 15 | is_hs | VARCHAR | str | 维度 | 是否沪深港通 | 分类维度 | N/H/S | N=否 H=沪股通 S=深股通；可能为 NULL |
| 16 | act_name | VARCHAR | str | 辅助 | 实控人名称 | 辅助信息 | — | — |
| 17 | act_ent_type | VARCHAR | str | 辅助 | 实控人企业性质 | 辅助信息 | — | — |

### 列角色分类汇总
- **标识字段**：ts_code
- **维度字段**：area, industry, market, exchange, list_status, list_date, delist_date, is_hs
- **度量字段**：无
- **辅助字段**：symbol, name, fullname, enname, cnspell, curr_type, act_name, act_ent_type

### 特殊取值说明
- `list_status`：`L`=上市, `D`=退市, `G`=过会未交易, `P`=暂停上市。默认查询返回 `L`。
- `is_hs`：`N`=否, `H`=沪股通, `S`=深股通。部分旧股票可能为 NULL。
- `delist_date`：大部分股票为 NULL（未退市），仅退市股票有值。
- `fullname` / `enname`：少量老股票可能为 NULL。

### Tushare ↔ DuckDB 字段差异
- 与 Tushare 文档一致，无差异。

## 数据质量快照

| 指标 | 值 | 检查时间 |
|---|---|---|
| 总行数 | ~5700 | {同步后填写} |
| 数据起始 | — | {无日期维度} |
| 数据截止 | — | {无日期维度} |
| 股票数 (DISTINCT ts_code) | ~5700 | {同步后填写} |
| 交易日/报告期数 | — | — |
| PK 重复数 | 0 | {同步后填写} |
| PK 列 NULL 数 | ts_code=0 | {同步后填写} |
| NaN 字符串污染列 | 无 | {同步后填写} |
| 度量列全 NULL 率 > 50% | 无 | {同步后填写} |

## 同步记录

| 时间 | 操作 | 新增行数 | 数据截止 | 备注 |
|---|---|---|---|---|
| {datetime} | 全量覆盖 | ~5700 | — | 首次同步 |
