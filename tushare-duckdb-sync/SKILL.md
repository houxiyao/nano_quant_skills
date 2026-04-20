---
name: tushare-duckdb-sync
description: '从 Tushare Pro 同步数据到本地 DuckDB，支持全量/增量模式。包含环境初始化、数据同步、质检、文档生成完整流程。关键词: tushare, duckdb, 数据同步, ETL, 增量更新, 全量同步, 数据质检。'
argument-hint: '输入 Tushare 文档地址或表名，可选同步模式（全量/增量）。'
user-invocable: true
---

# Tushare → DuckDB 数据同步工作流

## 根本目的

**自动完成从 Tushare 到本地 DuckDB 的数据同步，同时产出三份同等重要的资产：**

1. **数据本身** — DuckDB 中结构合理、可直接用于量化计算的表。
2. **数据元数据** — 每张表的完整文档，详细到每个列的含义、类型、取值范围、量化用途、注意事项。
3. **运维记录** — 同步状态（`table_sync_state`）、数据质量快照、同步历史日志。

> 数据没有元数据就无法被正确使用。同步没有状态记录就无法增量续传。
> **三者缺一，则同步工作不算完成。**

---

## Skill 目录结构

本 Skill 采用「主工作流 + 子文件」组织，社区用户只需将 `scripts/` 复制到工作区即可使用。

```
tushare-duckdb-sync/
├── SKILL.md                                ← 本文件：编排工作流（Agent 阅读）
├── scripts/
│   ├── sync_table.py                       ← 自包含同步脚本（无项目内部依赖）
│   └── check_quality.py                    ← 自包含质检脚本
├── examples/
│   ├── stock_basic_overwrite.md            ← 全量覆盖示例（none 维度）
│   └── daily_incremental.md               ← 增量同步示例（trade_date 维度）
└── templates/
    ├── mapping_registry.json               ← 映射注册表种子（空 tables 数组）
    ├── table_metadata.md                   ← 元数据文档模板
    └── task_config.json                    ← 任务配置 JSON 模板
```

**Agent 使用规则：**
- **同步数据**：将 `scripts/sync_table.py` 复制到用户工作区，填入参数后执行。
- **质检数据**：将 `scripts/check_quality.py` 复制到用户工作区执行。
- **生成元数据文档**：以 `templates/table_metadata.md` 为模板，逐项填写。
- **学习正确输出格式**：阅读 `examples/` 下的完整示例。

---

## 先决条件检查

> **进入工作流之前，必须先确认初始化状态。**

询问用户：**"是否已完成 Tushare-DuckDB 同步环境的初始化？"**

### 已初始化 → 快速恢复
要求用户给出文档目录路径（默认 `./docs/tushare_sync/`），然后：
1. 读取该目录下的 `计划书.md` 或 `_index.md` 恢复上下文。
2. 读取 `tables/` 下已有表文档，了解已同步表及其元数据覆盖情况。
3. 查询 DuckDB 中 `table_sync_state` 表确认同步进度。
4. 跳转到 → [工作流程](#工作流程)。

### 未初始化 → 执行初始化

#### Step 1：确认 Python 执行环境
询问用户，提供选项：
- **使用已有 conda 环境**（需提供环境名称）
- **使用已有 virtualenv**（需提供路径）
- **新建 conda 环境**（由 Agent 创建）
- **新建 virtualenv**（由 Agent 创建）

同时确认虚拟环境文件放置地址（默认选项：项目根目录下 `.venv/`）。

#### Step 2：确认 DuckDB 数据放置地址
询问用户 DuckDB 文件路径，提供默认选项：
- `./duckdb/tushare.duckdb`（相对项目根目录）

#### Step 3：确认文档放置地址
询问用户文档根目录，提供默认选项：
- `./docs/tushare_sync/`

#### Step 4：初始化工作
1. **部署脚本**：将 `scripts/sync_table.py` 和 `scripts/check_quality.py` 复制到用户工作区根目录（或用户指定位置）。
2. **环境描述**：创建 `{文档目录}/环境说明.md`，记录 Python 环境、DuckDB 路径、脚本位置。
3. **任务文档**：创建 `{文档目录}/计划书.md`，包含表格跟踪模板。
4. **依赖包**：确保以下包可用（缺失则安装）：`tushare`、`duckdb`、`pandas`、`loguru`。
5. **目录结构**：创建 `{文档目录}/tables/`、`{文档目录}/sql/`、`{文档目录}/archive/`。
6. **映射注册表**：将 `templates/mapping_registry.json` 复制到 `{文档目录}/mapping_registry.json`。这是空数据库的初始状态 — `tables` 数组为空，每同步一张新表后由 Agent 追加条目。

#### Step 5：配置 Tushare Token
询问用户输入 Tushare Pro Token（注册 https://tushare.pro 后在个人主页获取）。
验证 Token 有效性（调用 `trade_cal` 接口测试）。运行时通过 `TUSHARE_TOKEN` 环境变量传入。

---

## 工作流程

### Step 1：接收 Tushare 原始文档地址 — 元数据采集

要求用户提供 Tushare 官方文档 URL，格式如：
- `https://tushare.pro/document/2?doc_id=32`

使用 `fetch_webpage` 从文档页面 **逐列提取** 以下信息（这是元数据的唯一源头，必须完整采集）：
- **接口名称**（endpoint）
- **完整字段列表** — 每一列的：列名、Tushare 原始数据类型（str/float/int）、中文说明
- **输入参数**（哪些字段可作为查询维度）
- **维度类型**（按交易日 `trade_date` / 按报告期 `period` / 无维度 `none`）
- **维度字段名**（API 调用时传入的参数名）
- **数据更新频率**
- **积分权限要求**

> **如果 Tushare 文档页面无法访问或解析不完整，必须告知用户并暂停，不得跳过元数据采集。**

#### Step 1 备选路径：手动输入元数据

当网页无法访问（登录墙、网络故障、页面改版）时，Agent 应切换到手动采集模式：

1. 告知用户文档解析失败的原因。
2. 向用户依次确认以下信息（可接受自然语言描述，由 Agent 结构化）：
   - 接口名称（endpoint）
   - 字段列表（列名 + 类型 + 说明，可粘贴 Tushare 页面截图或已有文档）
   - 维度类型（按交易日 / 按报告期 / 无维度）
   - 积分要求（不确定可留空）
3. Agent 将用户输入整理为与网页解析相同的结构化格式，继续后续步骤。

> 手动输入的元数据同样写入表文档和映射注册表，与网页采集的数据享有相同地位。

### Step 2：生成表元数据文档（同步前必须完成）

> **在执行任何数据同步之前，先完成元数据文档。**
> 这确保即使同步中断，元数据也已就绪供后续使用。

在 `{文档目录}/tables/{表中文名}.md` 创建文档。

**使用模板**：以 `templates/table_metadata.md` 为基础，**必须包含全部章节**。
**参考示例**：阅读 `examples/stock_basic_overwrite.md` 或 `examples/daily_incremental.md` 了解正确的填写方式。

**字段详情的填写规则：**
1. `DuckDB 类型`：从实际 DuckDB 表的 `information_schema.columns` 查询，不是从文档猜测。若表尚不存在则标注 `(待建表确认)`。
2. `Tushare 原始类型`：从 Tushare 文档提取（str / float / int）。
3. `数据角色`：必须为以下之一 — `标识`、`维度`、`度量`、`辅助`。
4. `中文说明`：优先使用 Tushare 文档原文，不得省略或改写。
5. `量化用途`：Agent 根据列含义判断，可选类别：
   - `唯一标识`、`时间维度`、`OHLC 行情`、`成交量价`、`资金流向`、`财务指标`、`技术因子`、`持仓明细`、`状态标记`、`分类维度`、`辅助信息`。
6. `取值范围/格式`：记录数据格式（如日期格式）、已知枚举值（如 `P=暂停上市, D=退市`）、常见数值范围。
7. `注意事项`：记录类型转换、特殊取值、精度问题等。若无特殊情况写 `—`。

### Step 3：判断全量 vs 增量同步

按以下优先级自动判断：

1. **查 `table_sync_state` 表**：如果目标 `source_table` 存在已同步记录 → 增量模式。
2. **查 DuckDB 目标表**：如果目标表已存在且有数据 → 增量模式。
3. **查文档中的数据质量快照**：读取 `{文档目录}/tables/{表名}.md` → 辅助判断。
4. **以上均无记录** → 全量模式。

#### 全量模式
- 询问用户是否需要划定起始时间（默认 `20100101`）。
- 对于 `none` 维度表（如 `stock_basic`），直接 `overwrite`，同步完成后自动重建 PK 和索引。
- 对于 `trade_date` 维度表，设置 `sync_all=true` 以支持断点续传。
- 对于 `period` 维度表，设置 `sync_all=true`，起始期默认 `20100331`。

#### 增量模式
- `start_date` 设为已同步最大日期的下一天。
- `sync_all=false`（拉取 start_date 到今天的全部维度值）。
- `mode=append`。

#### 交易日安全截止规则

- 对 `trade_date` 维度的盘后行情、资金流、因子类接口，默认以 `Asia/Shanghai 18:00` 作为当天数据的安全发布时间。
- 如果当前时间早于 18:00 且用户没有显式提供 `end_date`，Agent 或脚本必须把有效截止日收敛到 **上一个开放交易日**，而不是盲目拉今天。
- 如果用户显式要求同步今天，而 Tushare 返回空 payload，必须将该维度写为 **失败状态**，不得写成成功。
- 只有在“0 行结果本来就符合业务语义”的接口上，才允许显式开启 `allow_empty_result` 把空结果记成功。

### Step 4：执行数据同步

#### 4a. 构建同步参数

**查阅映射注册表**：读取 `{文档目录}/mapping_registry.json`，检查目标表是否已有映射记录。

- **已有记录** → 直接使用注册表中的 `endpoint`、`method`、`dimension_type`、`pk` 等参数。
- **无记录（新表）** → 根据 Step 1 采集的 Tushare 文档信息推断参数，遵循以下命名与调用规则：

**命名规则：**
- `source_table`：通常与 Tushare 接口名一致（如 `daily`、`moneyflow`）。若用户已有 `table_sync_state` 历史记录，必须与之一致。
- `target_table`：遵循 `{类别}_{业务名}` 格式（如 `stk_daily`、`fin_balance`）。参见 [DuckDB Schema 设计规范](#duckdb-schema-设计规范)。

**Tushare API 调用规则（从文档推断）：**
- 默认 `method=query`，即 `pro.query('{endpoint}', ...)`。
- 若 Tushare 文档标注为"限量"接口或需要更高积分 → 尝试 `_vip` 后缀 endpoint（如 `income` → `income_vip`）。
- 若 `pro.query()` 返回空但直接方法调用 `pro.{endpoint}()` 有数据 → 切换 `method` 为 `{endpoint}`（如 `suspend_d`、`fina_indicator_vip`）。
- 调用失败时在日志中记录实际使用的 endpoint 和 method，便于排查。

> **同步完成后，必须将新表的映射关系追加到 `{文档目录}/mapping_registry.json`**（见 Step 7）。
> 这是新用户从空目录逐步积累本地知识的唯一途径。

#### 4b. 调用同步脚本

**单表同步**（直接命令行）：
```bash
TUSHARE_TOKEN=xxx python sync_table.py \
  --endpoint {endpoint} \
  --duckdb-path {duckdb_path} \
  --target-table {target_table} \
  --mode {overwrite|append} \
  --dimension-type {none|trade_date|period} \
  --start-date {YYYYMMDD} \
  --sleep 0.3
```

**批量同步**（任务文件）：
```bash
TUSHARE_TOKEN=xxx python sync_table.py \
  --tasks-file tasks.json \
  --duckdb-path {duckdb_path}
```

任务文件格式参见 `templates/task_config.json`。

> `sync_table.py` 是自包含脚本，不依赖本项目其它模块。Agent 只需填入参数即可执行。

社区脚本的默认安全行为：
- `trade_date` 且未传 `end_date` 时，18:00 前自动只同步到上一个开放交易日。
- 增量维度拿到空 payload 时，默认写失败状态，等待重试。
- 如确需接受 0 行成功，显式加 `--allow-empty-result`。

#### 4c. 处理常见错误
- **网络超时**：自动重试（已内置 `max_retries`），失败后单独重跑该表。
- **API 限频**：调整 `--sleep`（默认 0.3s）。
- **字段不匹配**：脚本自动丢弃目标表不存在的列（warning 日志），在元数据文档的「Tushare ↔ DuckDB 字段差异」中记录。
- **VARCHAR→DATE 类型冲突**：脚本已内置 `_coerce_dates` 自动转换。
- **空 payload**：对增量维度默认记失败，不记成功。先确认是否早于 `Asia/Shanghai 18:00`，或是否需要把 `end_date` 改成上一个开放交易日。

### Step 5：写入同步状态

> `table_sync_state` 是增量续传的基础，是三项资产之一（运维记录），必须及时更新。

`sync_table.py` 在 `dimension_type != "none"` 时自动写入 `table_sync_state`。

补充规则：对增量维度，**空 payload 默认写 `is_sync=0`**，避免把“上游未发布”误写成“成功同步”。

对于 `none` 维度表，手动执行：
```sql
INSERT INTO table_sync_state
(source_table, dimension_type, dimension_value, is_sync, error_message, updated_at)
VALUES ('{source_table}', 'none', '{today}', 1, '', NOW());
```

`table_sync_state` 表结构：
| 列名 | 类型 | 说明 |
|---|---|---|
| source_table | VARCHAR | Tushare 接口名 / 历史表名 |
| dimension_type | VARCHAR | trade_date / period / none |
| dimension_value | VARCHAR | 具体日期值（如 20260416） |
| is_sync | INTEGER | 1=成功, 0=失败 |
| error_message | VARCHAR | 失败原因（成功时为空） |
| updated_at | TIMESTAMP | 写入时间 |

### Step 6：质检同步结果

> **质检结果直接写入元数据文档的「数据质量快照」段落，使之成为该表的持久化质量档案。**

使用 `check_quality.py` 对每张新同步的表执行质检：

```bash
python check_quality.py \
  --duckdb-path {duckdb_path} \
  --table {target_table} \
  --pk {pk_col1,pk_col2} \
  --date-col {date_col} \
  --format markdown
```

脚本执行以下检查项：

| 检查项 | 通过条件 |
|---|---|
| 行数 | > 0（除已知空表） |
| PK 唯一 | 0 重复行 |
| PK 非空 | 每列 NULL 数 = 0 |
| 日期范围 | max ≥ 最新交易日 |
| NaN 污染 | VARCHAR 列无字面量 `'nan'` / `'NaN'` |
| 股票覆盖 | 记录 DISTINCT ts_code 数 |
| 交易日覆盖 | 记录 DISTINCT date 数 |
| 度量列空值率 | 标记 > 50% 的列 |

检查完成后：
1. 将 `--format markdown` 输出写入文档的「数据质量快照」表格（覆盖旧值）。
2. 若有失败项 → 报告并等待人工确认。
3. 全部通过 → 继续。

> 对于增量连续性检查（缺失交易日），Agent 需额外对比 `trade_cal` 表手动验证。

### Step 7：更新元数据文档

同步完成后，回到 Step 2 创建的文档，更新：
1. **数据质量快照** — 用 Step 6 的最新质检结果覆盖。
2. **同步记录** — 追加本次同步条目。
3. **字段详情** — 如果同步中发现字段差异（脚本 warning 中的 dropped columns），更新「Tushare ↔ DuckDB 字段差异」和对应列的「注意事项」。
4. **DuckDB 类型** — 如果是新建表，此时从 `information_schema.columns` 补全实际类型。
5. **映射注册表** — 若为首次同步的新表，向 `{文档目录}/mapping_registry.json` 的 `tables` 数组追加一条记录：
   ```json
   {
     "source_table": "{source_table}",
     "target_table": "{target_table}",
     "endpoint": "{endpoint}",
     "dimension_type": "{none|trade_date|period}",
     "method": "{query 或 方法名}",
     "pk": "{pk_col1,pk_col2}",
     "doc_id": "{doc_id}",
     "note": "{备注}"
   }
   ```
   > 映射注册表是跨会话持久化的本地知识，确保下次同步或增量更新时 Agent 无需重新推断参数。

### Step 8：输出任务纪要

```
✅ {表中文名} ({target_table}) 同步完成
  - 模式: {增量/全量}
  - 新增: {loaded_rows} 行
  - 总计: {total_rows} 行
  - 数据范围: {min_date} ~ {max_date}
  - 质检: 通过
  - 元数据文档: {文档路径} ✓（{n} 列全覆盖）
  - 同步状态: table_sync_state ✓
```

---

## 三项资产完整性自检

> **每次同步任务结束前，按顺序逐项检查。任何一项缺失，任务不算完成。**

### 资产一：数据
- ☐ DuckDB 目标表数据已到最新交易日/报告期
- ☐ PK 无重复
- ☐ PK 列无 NULL
- ☐ 日期维度无跳跃（trade_date 类型）

### 资产二：元数据文档
- ☐ `tables/{表名}.md` 已创建/更新
- ☐ 字段详情覆盖全部列（0 遗漏）
- ☐ 每列有 `数据角色`、`量化用途`、`取值范围/格式`、`注意事项`
- ☐ 特殊取值已说明（负值含义、NULL 含义、枚举值等）
- ☐ Tushare ↔ DuckDB 字段差异已记录
- ☐ 数据质量快照已更新为本次质检值

### 资产三：运维记录
- ☐ `table_sync_state` 已写入最新同步状态
- ☐ 同步记录已追加到文档
- ☐ `mapping_registry.json` 已包含本次同步表的映射条目
- ☐ 无残留临时文件

**三项全部通过后方可输出任务纪要。**

---

## DuckDB Schema 设计规范

- **表名规范**：`{类别}_{业务名}`，如 `stk_moneyflow`、`fin_balance`、`idx_daily_dc`。
- **日期列**：统一使用 `DATE` 类型（非 VARCHAR），同步脚本自动转换。
- **PK 约束**：每张表必须有 `PRIMARY KEY`（通常为 `ts_code + trade_date` 或 `ts_code + end_date`）。
- **索引**：trade_date / end_date 列必须有独立索引，命名 `idx_{table}_{col}`。
- **ORDER BY**：建表时按 `(ts_code, trade_date)` 物理排序以优化范围查询。
- **NULL 处理**：Tushare 的 NaN 统一转为 SQL NULL。
- **新表 DDL**：若目标表不存在，同步脚本先自动建表（CREATE TABLE AS SELECT），然后 Agent 生成 PK/索引 DDL 到 `{docs}/sql/`，展示给用户确认后执行。

---

## 工作日志管理

- 每张表文档的「同步记录」部分只保留最近 10 条。
- 超过 10 条的旧记录迁移到 `{文档目录}/archive/` 下按年月归档。
- 删除过时的临时文件、废弃的 SQL 脚本。
- 数据质量快照只保留最新一份（每次质检覆盖写入）。

---

## 映射注册表机制

> **新用户从空目录开始**：初始化时 `mapping_registry.json` 的 `tables` 数组为空。
> 每同步一张表，Agent 自动向其追加一条映射记录。随着使用积累，注册表逐步成长为该用户的完整映射知识库。

### 查阅顺序

Agent 在构建同步参数时按以下优先级查阅映射：
1. **`{文档目录}/mapping_registry.json`** — 用户本地注册表（首选，最准确）。
2. **本文档末尾的「种子映射参考」** — 仅在注册表无记录且为已知表时作为 fallback。
3. **Tushare 官方文档** — 注册表和种子表均无记录时，从文档推断。

### 种子映射参考

> 以下为已验证的常用表映射，**仅供 Agent 在用户注册表无对应记录时参考**。
> 同步完成后仍须将实际使用的参数写入 `mapping_registry.json`。

| source_table | target_table | endpoint | 维度 | method | pk | doc_id |
|---|---|---|---|---|---|---|
| stock_basic | stk_info | stock_basic | none | query | ts_code | 25 |
| daily | stk_daily | daily | trade_date | query | ts_code,trade_date | 27 |
| dc_daily | idx_daily_dc | dc_daily | trade_date | query | ts_code,trade_date | — |
| dc_index | idx_quote_dc | dc_index | trade_date | query | ts_code,trade_date | — |
| dc_member | idx_member_daily | dc_member | trade_date | query | ts_code,trade_date | — |
| idx_factor_pro | idx_factor_pro | idx_factor_pro | trade_date | query | ts_code,trade_date | — |
| stk_factor_pro | stk_factor_pro | stk_factor_pro | trade_date | query | ts_code,trade_date | — |
| moneyflow | stk_moneyflow | moneyflow | trade_date | query | ts_code,trade_date | 32 |
| moneyflow_ths | stk_moneyflow_ths | moneyflow_ths | trade_date | query | ts_code,trade_date | — |
| cyq_perf | stk_cyq_perf | cyq_perf | trade_date | query | ts_code,trade_date | — |
| margin_detail | stk_margin | margin_detail | trade_date | query | ts_code,trade_date | — |
| suspend_d | stk_suspend | suspend_d | trade_date | suspend_d | ts_code,trade_date | — |
| stock_st | stk_st_daily | stock_st | trade_date | stock_st | ts_code,trade_date | — |
| namechange | stk_name_history | namechange | none | namechange | ts_code | — |
| bse_mapping | stk_bse_mapping | bse_mapping | none | bse_mapping | ts_code | — |
| balancesheet | fin_balance | balancesheet_vip | period | query | ts_code,end_date | — |
| cashflow | fin_cashflow | cashflow_vip | period | query | ts_code,end_date | — |
| income | fin_income | income_vip | period | query | ts_code,end_date | — |
| fina_indicator | fin_indicator | fina_indicator_vip | period | fina_indicator_vip | ts_code,end_date | — |
| express | fin_express | express_vip | period | query | ts_code,end_date | — |
| forecast | fin_forecast | forecast_vip | period | query | ts_code,end_date | — |
| top10_holders | fin_top10_holders | top10_holders | period | query | ts_code,end_date | — |
| top10_floatholders | fin_top10_float_holders | top10_floatholders | period | query | ts_code,end_date | — |
| stk_ah_comparison | stk_ah_comparison | stk_ah_comparison | trade_date | query | ts_code,trade_date | — |

## 决策分支
- 若 Tushare 接口返回空数据且非已知空表 → 告警并暂停，等待人工确认。
- 若 PK 重复 → 报告重复样本，询问是否需要去重后重建。
- 若目标表不存在（新表）→ 同步脚本自动建表后，Agent 生成 PK/索引 DDL 展示给用户确认后执行。
- 若 `table_sync_state` 中无记录但目标表已有数据 → 以目标表实际 max_date 为基准做增量。
- 若 Tushare 文档字段与本地表字段有差异 → 在元数据文档中标注差异列，不擅自修改表结构。
