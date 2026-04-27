# Tushare Sync Scripts

这个目录是从旧仓库调度链路中拆出来的可独立运行工具集，负责按当前 mapping registry 批量同步 Tushare 数据到本地 DuckDB，并适配 crontab 周期调度。

> 本目录是按 [`../tushare_duckdb_sync_skills/SKILL.md`](../tushare_duckdb_sync_skills/SKILL.md) 生成的运行脚本；如需修改同步策略、表映射或质检逻辑，请优先回流到 skill 后重新生成。

如果你已经按 [../tushare_duckdb_sync_skills/README.md](../tushare_duckdb_sync_skills/README.md) 完成了 Python 依赖和 DuckDB 初始化，那么后续只需要提供 `TUSHARE_TOKEN`，就可以直接使用这里的调度脚本。

## 当前目录提供什么

- `run_trade_date_incremental.py` / `.sh`
  - 负责所有 `trade_date` 增量表。
  - 默认回看最近 7 天。
  - 18:00 前默认只同步到前一个安全交易日。

- `run_financial_period_overwrite.py` / `.sh`
  - 负责所有 `period` 财报表。
  - 默认总是清理并重刷最近 2 个财报期。

- `run_snapshot_refresh.py` / `.sh`
  - 负责所有 `none` 维度的快照表。
  - 每次直接 `overwrite`。

- `run_all.py` / `run_all.sh`
  - 顺序调度 `trade-date`、`financial`、`snapshot` 三组任务。
  - 默认继续执行剩余组，最后统一返回非零状态码。

- `bootstrap.sh`
  - cron 自举入口。
  - 优先使用 `CONDA_SH_PATH` 或当前机器可发现的 conda 安装；找不到 conda 时回退到 `PYTHON_BIN` 或当前 `python`。

## 默认目录约定

- 映射注册表默认读取当前目录下的 `mapping_registry.json`。
- 兼容旧布局：如果当前目录不存在注册表，会回退查找仓库根目录下的 `docs/mapping_registry.json`。
- DuckDB 默认路径：仓库根目录下的 `data/ashare.duckdb`（与下游 `2min-company-analysis` 共享，避免出现两份数据库）。
- 日志目录默认路径：`tushare_duckdb_sync_scripts/logs/tushare_sync`
- 锁目录默认路径：`tushare_duckdb_sync_scripts/temporary/locks`

可显式设置环境变量覆盖默认路径：

```bash
export TUSHARE_SYNC_REGISTRY_PATH=/path/to/mapping_registry.json
export TUSHARE_SYNC_DUCKDB_PATH=/path/to/ashare.duckdb   # 同步脚本写入的目标库
export ASHARE_DUCKDB_PATH=/path/to/ashare.duckdb         # 下游分析模块读取的库
```

> 单机使用时建议两者保持一致；分离仅在双机/双库场景下有意义。

## 运行方式 / Python 路径

脚本统一以包路径导入（`from tushare_duckdb_sync_scripts...`），因此调用前需要让 Python 能找到本仓库。任选其一：

- 在仓库根目录下运行（最简，`cwd=/path/to/nano_quant_skills`），所有 `run_*.sh` 与 `bootstrap.sh` 都基于此假设。
- 或者 `pip install -e .`（如果把仓库根做成可安装包）/ `export PYTHONPATH=/path/to/nano_quant_skills` 后，可在任意 cwd 调用。

## 必要环境变量

- `TUSHARE_TOKEN`
  - 运行时唯一必填的业务凭证。
  - 脚本不会主动扫描 `.env` 文件，cron 里要显式提供。

## 可选运行环境变量

- `CONDA_SH_PATH`
  - 指向 `conda.sh`，适合 cron 环境。

- `CONDA_ENV_NAME`
  - 默认为 `legonanobot`。

- `PYTHON_BIN`
  - 不使用 conda 时，可指定 Python 可执行文件。

## 快速开始

先做一次 dry-run，确认注册表和任务解析正常：

```bash
export TUSHARE_TOKEN=your_token
./tushare_duckdb_sync_scripts/run_all.sh --dry-run
```

只看快照组会跑哪些表：

```bash
./tushare_duckdb_sync_scripts/run_snapshot_refresh.sh --dry-run
```

手工补某天的交易日任务：

```bash
./tushare_duckdb_sync_scripts/run_trade_date_incremental.sh --date 20260426
```

手工重刷最近 2 期财报：

```bash
./tushare_duckdb_sync_scripts/run_financial_period_overwrite.sh
```

只跑部分表：

```bash
./tushare_duckdb_sync_scripts/run_all.sh --tables fin_balance,fin_cashflow,stk_moneyflow
```

## 推荐 crontab

整套任务每天晚间跑一次：

```cron
35 18 * * 1-5 TUSHARE_TOKEN=your_token CONDA_SH_PATH=/opt/miniforge3/etc/profile.d/conda.sh CONDA_ENV_NAME=legonanobot /path/to/nano_quant_skills/tushare_duckdb_sync_scripts/run_all.sh >> /path/to/nano_quant_skills/tushare_duckdb_sync_scripts/logs/tushare_sync/cron_suite.out 2>&1
```

如果想拆开调度：

```cron
35 18 * * 1-5 TUSHARE_TOKEN=your_token CONDA_SH_PATH=/opt/miniforge3/etc/profile.d/conda.sh CONDA_ENV_NAME=legonanobot /path/to/nano_quant_skills/tushare_duckdb_sync_scripts/run_trade_date_incremental.sh >> /path/to/nano_quant_skills/tushare_duckdb_sync_scripts/logs/tushare_sync/trade_date_cron.out 2>&1
10 19 * * 1-5 TUSHARE_TOKEN=your_token CONDA_SH_PATH=/opt/miniforge3/etc/profile.d/conda.sh CONDA_ENV_NAME=legonanobot /path/to/nano_quant_skills/tushare_duckdb_sync_scripts/run_financial_period_overwrite.sh >> /path/to/nano_quant_skills/tushare_duckdb_sync_scripts/logs/tushare_sync/financial_cron.out 2>&1
30 19 * * 1-5 TUSHARE_TOKEN=your_token CONDA_SH_PATH=/opt/miniforge3/etc/profile.d/conda.sh CONDA_ENV_NAME=legonanobot /path/to/nano_quant_skills/tushare_duckdb_sync_scripts/run_snapshot_refresh.sh >> /path/to/nano_quant_skills/tushare_duckdb_sync_scripts/logs/tushare_sync/snapshot_cron.out 2>&1
```

如果你不用 conda，可以在 cron 里直接指定解释器：

```cron
35 18 * * 1-5 TUSHARE_TOKEN=your_token PYTHON_BIN=/path/to/venv/bin/python /path/to/nano_quant_skills/tushare_duckdb_sync_scripts/run_all.sh >> /path/to/nano_quant_skills/tushare_duckdb_sync_scripts/logs/tushare_sync/cron_suite.out 2>&1
```
