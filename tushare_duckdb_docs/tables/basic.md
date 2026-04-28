# basic 模块 — 基础信息类（全量快照 / 字典映射 / 状态日历）

## trade_calendar

- **描述**: 上交所交易日历（SSE，2000-2030），含是否交易及前一交易日
- **粒度**: 全量快照（每次 overwrite）
- **同步维度**: `none`
- **主键**: `cal_date`
- **列数**: 4
- **Tushare 文档**: https://tushare.pro/document/2?doc_id=26

**字段列表**:
```
  exchange       VARCHAR   -- 交易所（SSE 上交所）
  cal_date       VARCHAR   -- 日历日期（YYYYMMDD）
  is_open        BIGINT    -- 是否交易日：1=交易 0=休市
  pretrade_date  VARCHAR   -- 上一个交易日（YYYYMMDD）
```

**查询示例**:
```sql
-- 判断某天是否为交易日
SELECT is_open FROM trade_calendar WHERE cal_date = '20260417';

-- 获取某交易日的前一交易日
SELECT pretrade_date FROM trade_calendar WHERE cal_date = '20260417';

-- 获取指定范围内所有交易日（按升序）
SELECT cal_date FROM trade_calendar
WHERE cal_date BETWEEN '20260401' AND '20260430' AND is_open = 1
ORDER BY cal_date;
```

## stk_ah_comparison

- **描述**: A/H 股比价数据
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 11

**字段列表**:
```
  hk_code                                   VARCHAR
  ts_code                                   VARCHAR
  trade_date                                DATE
  hk_name                                   VARCHAR
  hk_pct_chg                                DOUBLE
  hk_close                                  DOUBLE
  name                                      VARCHAR
  close                                     DOUBLE
  pct_chg                                   DOUBLE
  ah_comparison                             DOUBLE
  ah_premium                                DOUBLE
```

**查询示例**:
```sql
SELECT *
FROM stk_ah_comparison
WHERE 1=1
LIMIT 100;
```

## stk_bse_mapping

- **描述**: 北交所旧码与新码映射
- **粒度**: 全量快照/字典
- **同步维度**: `none`
- **主键**: `o_code, n_code`
- **列数**: 4

**字段列表**:
```
  name                                      VARCHAR  -- 股票名称
  o_code                                    VARCHAR  -- 原代码
  n_code                                    VARCHAR  -- 新代码
  list_date                                 DATE  -- 上市日期（格式：yyyyMMdd）
```

**查询示例**:
```sql
SELECT *
FROM stk_bse_mapping
WHERE 1=1
LIMIT 100;
```

## stk_info

- **描述**: A股基本信息，含上市状态、行业、实控人
- **粒度**: 全量快照
- **同步维度**: `none`
- **主键**: `ts_code`
- **列数**: 10

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS代码
  symbol                                    VARCHAR  -- 股票代码
  name                                      VARCHAR  -- 股票名称
  area                                      VARCHAR  -- 地域
  industry                                  VARCHAR  -- 所属行业
  cnspell                                   VARCHAR  -- 拼音缩写
  market                                    VARCHAR  -- 市场类型（主板/创业板/科创板/CDR）
  list_date                                 DATE  -- 上市日期
  act_name                                  VARCHAR  -- 实控人名称
  act_ent_type                              VARCHAR  -- 实控人企业性质
```

**查询示例**:
```sql
SELECT *
FROM stk_info
WHERE 1=1
LIMIT 100;
```

## stk_name_history

- **描述**: 股票历史曾用名及变更原因
- **粒度**: 标的-报告期
- **同步维度**: `none`
- **主键**: `ts_code, ann_date`
- **列数**: 6

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS代码
  name                                      VARCHAR  -- 证券名称
  start_date                                DATE  -- 开始日期（格式：yyyyMMdd）
  end_date                                  DATE  -- 结束日期（格式：yyyyMMdd）
  ann_date                                  DATE  -- 公告日期（格式：yyyyMMdd）
  change_reason                             VARCHAR  -- 变更原因
```

**查询示例**:
```sql
SELECT *
FROM stk_name_history
WHERE 1=1
LIMIT 100;
```

## stk_st_daily

- **描述**: 股票 ST/退市风险状态日历
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 5

**字段列表**:
```
  ts_code                                   VARCHAR  -- 股票代码（如：000001.SZ）
  name                                      VARCHAR  -- 股票名称
  trade_date                                DATE  -- 交易日期（格式：yyyyMMdd）
  type                                      VARCHAR  -- ST 类型代码（如：ST、*ST、退市风险警示等编码）
  type_name                                 VARCHAR  -- ST 类型名称（如：其他风险警示、退市风险警示等）
```

**查询示例**:
```sql
SELECT *
FROM stk_st_daily
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

