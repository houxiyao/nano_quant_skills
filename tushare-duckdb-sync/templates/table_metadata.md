# {表中文名称}

## 基本信息
| 属性 | 值 |
|---|---|
| DuckDB 表名 | `{target_table}` |
| Tushare 接口 | `{endpoint}` |
| Tushare 文档 | https://tushare.pro/document/2?doc_id={doc_id} |
| Tushare doc_id | {doc_id} |
| 同步维度 | {dimension_type} / {dimension_column} |
| 同步模式 | {overwrite 或 append} |
| PK | `({pk_columns})` |
| 索引 | `{index_name}` ON `({index_columns})` |
| 积分要求 | {积分等级} |

## 字段详情

> 此表覆盖目标表的 **全部列**。零遗漏是硬性要求。

| # | 列名 | DuckDB 类型 | Tushare 原始类型 | 数据角色 | 中文说明 | 量化用途 | 取值范围/格式 | 注意事项 |
|---|---|---|---|---|---|---|---|---|
| 1 | {col_name} | {VARCHAR/DOUBLE/DATE/...} | {str/float/int} | {标识/维度/度量/辅助} | {说明} | {用途} | {范围} | {注意事项或 —} |

<!-- 
数据角色定义：
  - 标识：唯一识别记录（如 ts_code）
  - 维度：分类/分组/过滤条件（如 trade_date, industry）
  - 度量：数值型、可聚合（如 open, close, vol）
  - 辅助：辅助显示/非核心字段（如 name, fullname）

DuckDB 类型选择指南：
  - Tushare str → VARCHAR
  - Tushare str(YYYYMMDD) → DATE  (ETL 自动转换)
  - Tushare float → DOUBLE
  - Tushare int → BIGINT
-->

### 列角色分类汇总
- **标识字段**：{列名列表}
- **维度字段**：{列名列表}
- **度量字段**：{列名列表}
- **辅助字段**：{列名列表}

### 特殊取值说明
- {描述枚举值、NULL 含义、边界情况等}

### Tushare ↔ DuckDB 字段差异
- {如有字段重命名或类型转换差异在此说明；无差异写"与 Tushare 文档一致，无差异"}

## 数据质量快照

> 由 `check_quality.py --format markdown` 生成后粘贴于此。

| 指标 | 值 | 检查时间 |
|---|---|---|
| 总行数 | {n} | {datetime} |
| 数据起始 | {min_date} | {datetime} |
| 数据截止 | {max_date} | {datetime} |
| 股票数 (DISTINCT ts_code) | {n} | {datetime} |
| 交易日/报告期数 | {n} | {datetime} |
| PK 重复数 | 0 | {datetime} |
| PK 列 NULL 数 | {col}=0 | {datetime} |
| NaN 字符串污染列 | 无 | {datetime} |
| 度量列全 NULL 率 > 50% | 无 | {datetime} |

## 同步记录

> 每次同步追加一行。

| 时间 | 操作 | 新增行数 | 数据截止 | 备注 |
|---|---|---|---|---|
| {datetime} | {全量覆盖/增量追加} | {n} | {date} | {备注} |
