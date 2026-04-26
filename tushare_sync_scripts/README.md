# Tushare Sync Scripts

这个目录提供可直接交给 crontab 的整套同步脚本，覆盖当前 mapping registry 中的全部 DuckDB 目标表。

## 目录说明

- run_trade_date_incremental.py / .sh
  - 负责所有 trade_date 增量表。
  - 默认回看最近 7 天。
  - 18:00 前默认只同步到前一天，避免收盘后数据未稳定。
  - 始终使用 sync_all=true，只补状态表中未成功的日期。

- run_financial_period_overwrite.py / .sh
  - 负责所有 period 财报表。
  - 默认总是清理并重刷最近 2 个财报期。
  - 这是为了解决财报在一段时间内持续披露的问题，不能因为某期第一次抓到数据就永久标记成功。
  - 重刷步骤是：先删目标表近 2 期数据，再删 table_sync_state 对应 period 状态，再逐期 append 回灌。

- run_snapshot_refresh.py / .sh
  - 负责所有 none 维度的快照表。
  - 每次直接 overwrite，适合做字典表/基础资料表日更或周更。

- run_all.py / run_all.sh
  - 顺序调度 trade-date、financial、snapshot 三组脚本。
  - 默认继续执行剩余组，即使某一组偶发失败也会留下日志和返回码。

- bootstrap.sh
  - 统一 cron 环境启动。
  - 会 source conda.sh、activate legonanobot，并切到仓库根目录。

## 日志与锁

- 日志目录默认是 logs/tushare_sync。
- 每个脚本按天滚动写日志，例如 trade_date_incremental_20260426.log。
- 锁文件目录默认是 temporary/locks。
- 同组脚本不会并发执行，避免两个 cron 同时改同一批状态表。

## 必要环境变量

- TUSHARE_TOKEN
  - 脚本不会自动扫描 .env 文件。
  - cron 里必须显式 export，或在 crontab 行前直接写。

## 推荐 crontab

每天晚间跑全量调度：

```cron
35 18 * * 1-5 TUSHARE_TOKEN=your_token /Users/mgong/LegoNanoBot/SparkRDAgent/tushare_sync_scripts/run_all.sh >> /Users/mgong/LegoNanoBot/SparkRDAgent/logs/tushare_sync/cron_suite.out 2>&1
```

如果想拆开调度：

```cron
35 18 * * 1-5 TUSHARE_TOKEN=your_token /Users/mgong/LegoNanoBot/SparkRDAgent/tushare_sync_scripts/run_trade_date_incremental.sh >> /Users/mgong/LegoNanoBot/SparkRDAgent/logs/tushare_sync/trade_date_cron.out 2>&1
10 19 * * 1-5 TUSHARE_TOKEN=your_token /Users/mgong/LegoNanoBot/SparkRDAgent/tushare_sync_scripts/run_financial_period_overwrite.sh >> /Users/mgong/LegoNanoBot/SparkRDAgent/logs/tushare_sync/financial_cron.out 2>&1
30 19 * * 1-5 TUSHARE_TOKEN=your_token /Users/mgong/LegoNanoBot/SparkRDAgent/tushare_sync_scripts/run_snapshot_refresh.sh >> /Users/mgong/LegoNanoBot/SparkRDAgent/logs/tushare_sync/snapshot_cron.out 2>&1
```

## 常用手动命令

只看今天会跑哪些 trade_date 任务：

```bash
./tushare_sync_scripts/run_trade_date_incremental.sh --dry-run
```

手工指定补某天：

```bash
./tushare_sync_scripts/run_trade_date_incremental.sh --date 20260426
```

手工重刷最近 2 期财报：

```bash
./tushare_sync_scripts/run_financial_period_overwrite.sh
```

手工重刷指定 period：

```bash
./tushare_sync_scripts/run_financial_period_overwrite.sh --periods 20251231,20260331
```

只跑部分表：

```bash
./tushare_sync_scripts/run_all.sh --tables fin_balance,fin_cashflow,stk_moneyflow
```
