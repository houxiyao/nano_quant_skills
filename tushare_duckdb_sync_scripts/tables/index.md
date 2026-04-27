# index 模块 — 指数行情 / 行业分类 / 成分

## idx_daily_dc

- **描述**: 大盘/板块指数日行情：开高低收量额波动率
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 12

**字段列表**:
```
  ts_code                                   VARCHAR  -- 板块代码
  trade_date                                DATE  -- 交易日
  close                                     DOUBLE  -- 收盘点位
  open                                      DOUBLE  -- 开盘点位
  high                                      DOUBLE  -- 最高点位
  low                                       DOUBLE  -- 最低点位
  change                                    DOUBLE  -- 涨跌点位
  pct_change                                DOUBLE  -- 涨跌幅
  vol                                       DOUBLE  -- 成交量(股)
  amount                                    DOUBLE  -- 成交额(元)
  swing                                     DOUBLE  -- 振幅
  turnover_rate                             DOUBLE  -- 换手率
```

**查询示例**:
```sql
SELECT *
FROM idx_daily_dc
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## idx_factor_pro

- **描述**: 指数日行情 + 80+ 技术指标
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 89

**字段列表**:
```
  ts_code                                   VARCHAR  -- 指数代码
  trade_date                                DATE  -- 交易日期
  open                                      DOUBLE  -- 开盘价
  high                                      DOUBLE  -- 最高价
  low                                       DOUBLE  -- 最低价
  close                                     DOUBLE  -- 收盘价
  pre_close                                 DOUBLE  -- 昨收价
  change                                    DOUBLE  -- 涨跌额
  pct_change                                DOUBLE  -- 涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
  vol                                       DOUBLE  -- 成交量 （手）
  amount                                    DOUBLE  -- 成交额 （千元）
  asi_bfq                                   DOUBLE  -- 振动升降指标-OPEN, CLOSE, HIGH, LOW, M1=26, M2=10
  asit_bfq                                  DOUBLE  -- 振动升降指标-OPEN, CLOSE, HIGH, LOW, M1=26, M2=10
  atr_bfq                                   DOUBLE  -- 真实波动N日平均值-CLOSE, HIGH, LOW, N=20
  bbi_bfq                                   DOUBLE  -- BBI多空指标-CLOSE, M1=3, M2=6, M3=12, M4=20
  bias1_bfq                                 DOUBLE  -- BIAS乖离率-CLOSE, L1=6, L2=12, L3=24
  bias2_bfq                                 DOUBLE  -- BIAS乖离率-CLOSE, L1=6, L2=12, L3=24
  bias3_bfq                                 DOUBLE  -- BIAS乖离率-CLOSE, L1=6, L2=12, L3=24
  boll_lower_bfq                            DOUBLE  -- BOLL指标，布林带-CLOSE, N=20, P=2
  boll_mid_bfq                              DOUBLE  -- BOLL指标，布林带-CLOSE, N=20, P=2
  boll_upper_bfq                            DOUBLE  -- BOLL指标，布林带-CLOSE, N=20, P=2
  brar_ar_bfq                               DOUBLE  -- BRAR情绪指标-OPEN, CLOSE, HIGH, LOW, M1=26
  brar_br_bfq                               DOUBLE  -- BRAR情绪指标-OPEN, CLOSE, HIGH, LOW, M1=26
  cci_bfq                                   DOUBLE  -- 顺势指标又叫CCI指标-CLOSE, HIGH, LOW, N=14
  cr_bfq                                    DOUBLE  -- CR价格动量指标-CLOSE, HIGH, LOW, N=20
  dfma_dif_bfq                              DOUBLE  -- 平行线差指标-CLOSE, N1=10, N2=50, M=10
  dfma_difma_bfq                            DOUBLE  -- 平行线差指标-CLOSE, N1=10, N2=50, M=10
  dmi_adx_bfq                               DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  dmi_adxr_bfq                              DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  dmi_mdi_bfq                               DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  dmi_pdi_bfq                               DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  downdays                                  DOUBLE  -- 连跌天数
  updays                                    DOUBLE  -- 连涨天数
  dpo_bfq                                   DOUBLE  -- 区间震荡线-CLOSE, M1=20, M2=10, M3=6
  madpo_bfq                                 DOUBLE  -- 区间震荡线-CLOSE, M1=20, M2=10, M3=6
  ema_bfq_10                                DOUBLE  -- 指数移动平均-N=10
  ema_bfq_20                                DOUBLE  -- 指数移动平均-N=20
  ema_bfq_250                               DOUBLE  -- 指数移动平均-N=250
  ema_bfq_30                                DOUBLE  -- 指数移动平均-N=30
  ema_bfq_5                                 DOUBLE  -- 指数移动平均-N=5
  ema_bfq_60                                DOUBLE  -- 指数移动平均-N=60
  ema_bfq_90                                DOUBLE  -- 指数移动平均-N=90
  emv_bfq                                   DOUBLE  -- 简易波动指标-HIGH, LOW, VOL, N=14, M=9
  maemv_bfq                                 DOUBLE  -- 简易波动指标-HIGH, LOW, VOL, N=14, M=9
  expma_12_bfq                              DOUBLE  -- EMA指数平均数指标-CLOSE, N1=12, N2=50
  expma_50_bfq                              DOUBLE  -- EMA指数平均数指标-CLOSE, N1=12, N2=50
  kdj_bfq                                   DOUBLE  -- KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3
  kdj_d_bfq                                 DOUBLE  -- KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3
  kdj_k_bfq                                 DOUBLE  -- KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3
  ktn_down_bfq                              DOUBLE  -- 肯特纳交易通道, N选20日，ATR选10日-CLOSE, HIGH, LOW, N=20, M=10
  ktn_mid_bfq                               DOUBLE  -- 肯特纳交易通道, N选20日，ATR选10日-CLOSE, HIGH, LOW, N=20, M=10
  ktn_upper_bfq                             DOUBLE  -- 肯特纳交易通道, N选20日，ATR选10日-CLOSE, HIGH, LOW, N=20, M=10
  lowdays                                   DOUBLE  -- LOWRANGE(LOW)表示当前最低价是近多少周期内最低价的最小值
  topdays                                   DOUBLE  -- TOPRANGE(HIGH)表示当前最高价是近多少周期内最高价的最大值
  ma_bfq_10                                 DOUBLE  -- 简单移动平均-N=10
  ma_bfq_20                                 DOUBLE  -- 简单移动平均-N=20
  ma_bfq_250                                DOUBLE  -- 简单移动平均-N=250
  ma_bfq_30                                 DOUBLE  -- 简单移动平均-N=30
  ma_bfq_5                                  DOUBLE  -- 简单移动平均-N=5
  ma_bfq_60                                 DOUBLE  -- 简单移动平均-N=60
  ma_bfq_90                                 DOUBLE  -- 简单移动平均-N=90
  macd_bfq                                  DOUBLE  -- MACD指标-CLOSE, SHORT=12, LONG=26, M=9
  macd_dea_bfq                              DOUBLE  -- MACD指标-CLOSE, SHORT=12, LONG=26, M=9
  macd_dif_bfq                              DOUBLE  -- MACD指标-CLOSE, SHORT=12, LONG=26, M=9
  mass_bfq                                  DOUBLE  -- 梅斯线-HIGH, LOW, N1=9, N2=25, M=6
  ma_mass_bfq                               DOUBLE  -- 梅斯线-HIGH, LOW, N1=9, N2=25, M=6
  mfi_bfq                                   DOUBLE  -- MFI指标是成交量的RSI指标-CLOSE, HIGH, LOW, VOL, N=14
  mtm_bfq                                   DOUBLE  -- 动量指标-CLOSE, N=12, M=6
  mtmma_bfq                                 DOUBLE  -- 动量指标-CLOSE, N=12, M=6
  obv_bfq                                   DOUBLE  -- 能量潮指标-CLOSE, VOL
  psy_bfq                                   DOUBLE  -- 投资者对股市涨跌产生心理波动的情绪指标-CLOSE, N=12, M=6
  psyma_bfq                                 DOUBLE  -- 投资者对股市涨跌产生心理波动的情绪指标-CLOSE, N=12, M=6
  roc_bfq                                   DOUBLE  -- 变动率指标-CLOSE, N=12, M=6
  maroc_bfq                                 DOUBLE  -- 变动率指标-CLOSE, N=12, M=6
  rsi_bfq_12                                DOUBLE  -- RSI指标-CLOSE, N=12
  rsi_bfq_24                                DOUBLE  -- RSI指标-CLOSE, N=24
  rsi_bfq_6                                 DOUBLE  -- RSI指标-CLOSE, N=6
  taq_down_bfq                              DOUBLE  -- 唐安奇通道(海龟)交易指标-HIGH, LOW, 20
  taq_mid_bfq                               DOUBLE  -- 唐安奇通道(海龟)交易指标-HIGH, LOW, 20
  taq_up_bfq                                DOUBLE  -- 唐安奇通道(海龟)交易指标-HIGH, LOW, 20
  trix_bfq                                  DOUBLE  -- 三重指数平滑平均线-CLOSE, M1=12, M2=20
  trma_bfq                                  DOUBLE  -- 三重指数平滑平均线-CLOSE, M1=12, M2=20
  vr_bfq                                    DOUBLE  -- VR容量比率-CLOSE, VOL, M1=26
  wr_bfq                                    DOUBLE  -- W&R 威廉指标-CLOSE, HIGH, LOW, N=10, N1=6
  wr1_bfq                                   DOUBLE  -- W&R 威廉指标-CLOSE, HIGH, LOW, N=10, N1=6
  xsii_td1_bfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
  xsii_td2_bfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
  xsii_td3_bfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
  xsii_td4_bfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
```

**查询示例**:
```sql
SELECT *
FROM idx_factor_pro
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## idx_info

- **描述**: 指数基本信息：发布机构、基期、成分数
- **粒度**: 全量快照
- **同步维度**: `none`
- **主键**: `ts_code`
- **列数**: 8

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS代码
  name                                      VARCHAR  -- 简称
  market                                    VARCHAR  -- 市场(MSCI/CSI/SSE/SZSE/CICC/SW/OTH)
  publisher                                 VARCHAR  -- 发布方
  category                                  VARCHAR  -- 指数类别
  base_date                                 DATE  -- 基期
  base_point                                DOUBLE  -- 基点
  list_date                                 DATE  -- 发布日期
```

**查询示例**:
```sql
SELECT *
FROM idx_info
WHERE 1=1
LIMIT 100;
```

## idx_member_daily

- **描述**: 指数成分股每日快照
- **粒度**: 交易日-成分股
- **同步维度**: `trade_date`
- **主键**: `trade_date, ts_code, con_code`
- **列数**: 4

**字段列表**:
```
  trade_date                                DATE  -- 交易日期
  ts_code                                   VARCHAR  -- 概念代码
  con_code                                  VARCHAR  -- 成分代码
  name                                      VARCHAR  -- 成分股名称
```

**查询示例**:
```sql
SELECT *
FROM idx_member_daily
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## idx_quote_dc

- **描述**: 大盘指数行情概览：领涨股、涨跌家数、总市值
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 11

**字段列表**:
```
  ts_code                                   VARCHAR  -- 概念代码
  trade_date                                DATE  -- 交易日期
  name                                      VARCHAR  -- 概念名称
  leading                                   VARCHAR  -- 领涨股票名称
  leading_code                              VARCHAR  -- 领涨股票代码
  pct_change                                DOUBLE  -- 涨跌幅
  leading_pct                               DOUBLE  -- 领涨股票涨跌幅
  total_mv                                  DOUBLE  -- 总市值（万元）
  turnover_rate                             DOUBLE  -- 换手率
  up_num                                    BIGINT  -- 上涨家数
  down_num                                  BIGINT  -- 下降家数
```

**查询示例**:
```sql
SELECT *
FROM idx_quote_dc
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## idx_sw_classify

- **描述**: 申万行业分类字典（当前同步 SW2021 版本，覆盖 L1/L2/L3）
- **粒度**: 行业层级字典
- **同步维度**: `none`
- **主键**: `src, level, index_code`
- **列数**: 7

**字段列表**:
```
  index_code                                VARCHAR  -- 行业指数代码
  industry_name                             VARCHAR  -- 行业名称
  level                                     VARCHAR  -- 行业层级（L1/L2/L3）
  industry_code                             VARCHAR  -- 行业编码
  is_pub                                    VARCHAR  -- 是否发布
  parent_code                               VARCHAR  -- 上级行业代码，L1 为空
  src                                       VARCHAR  -- 分类版本来源（当前为 SW2021）
```

**查询示例**:
```sql
SELECT *
FROM idx_sw_classify
WHERE src = 'SW2021' AND level = 'L2'
ORDER BY index_code
LIMIT 100;
```

## idx_sw_member_all

- **描述**: 申万行业成分股全量快照（按一级行业扇出拉取，保留 L1/L2/L3 层级）
- **粒度**: 行业层级-成分股快照
- **同步维度**: `none`
- **主键**: `l1_code, l2_code, l3_code, ts_code, in_date, out_date, is_new`
- **列数**: 11

**字段列表**:
```
  l1_code                                   VARCHAR  -- 一级行业代码
  l1_name                                   VARCHAR  -- 一级行业名称
  l2_code                                   VARCHAR  -- 二级行业代码
  l2_name                                   VARCHAR  -- 二级行业名称
  l3_code                                   VARCHAR  -- 三级行业代码
  l3_name                                   VARCHAR  -- 三级行业名称
  ts_code                                   VARCHAR  -- 成分股 TS 代码
  name                                      VARCHAR  -- 成分股名称
  in_date                                   VARCHAR  -- 纳入日期（YYYYMMDD）
  out_date                                  INTEGER  -- 剔除日期；未剔除通常为空
  is_new                                    VARCHAR  -- 分类版本标记（Y 表示新版口径）
```

**查询示例**:
```sql
SELECT *
FROM idx_sw_member_all
WHERE l1_name = '电子'
ORDER BY l2_name, l3_name, ts_code
LIMIT 100;
```

## idx_sw_l3_peers

- **对象类型**: 标准视图
- **描述**: 以股票为锚点，展开当前申万三级行业下的全部同分类股票，便于快速取“同品种股票”
- **粒度**: 锚股票-同 L3 成分股
- **同步维度**: `derived`
- **主键**: `anchor_ts_code, l3_code, peer_ts_code`
- **列数**: 13
- **定义 SQL**: `docs/sql/create_idx_sw_l3_peers.sql`

**字段列表**:
```
  anchor_ts_code                             VARCHAR  -- 查询锚股票 TS 代码
  anchor_name                                VARCHAR  -- 查询锚股票名称
  anchor_l3_count                            BIGINT  -- 锚股票当前命中的 L3 分类数量；大多数股票为 1
  l1_code                                    VARCHAR  -- 一级行业代码
  l1_name                                    VARCHAR  -- 一级行业名称
  l2_code                                    VARCHAR  -- 二级行业代码
  l2_name                                    VARCHAR  -- 二级行业名称
  l3_code                                    VARCHAR  -- 三级行业代码（最细粒度申万分类）
  l3_name                                    VARCHAR  -- 三级行业名称
  peer_group_size                            BIGINT  -- 当前该 L3 分类下的股票数量
  peer_ts_code                               VARCHAR  -- 同分类股票 TS 代码
  peer_name                                  VARCHAR  -- 同分类股票名称
  peer_is_self                               BOOLEAN  -- 是否为锚股票自身
```

**使用约定**:

- 该视图基于 `idx_sw_member_all WHERE out_date IS NULL` 生成，只覆盖当前有效的申万分类关系
- 若某只股票当前命中多个 L3 分类，`anchor_l3_count` 会大于 1，查询结果会返回多个分类下的同类股票
- 若只需要排除锚股票自身，可额外过滤 `peer_is_self = false`

**查询示例**:
```sql
SELECT *
FROM idx_sw_l3_peers
WHERE anchor_ts_code = '000001.SZ'
  AND peer_is_self = false
ORDER BY peer_ts_code;
```

```sql
SELECT DISTINCT
    anchor_ts_code,
    anchor_name,
    l3_code,
    l3_name,
    anchor_l3_count,
    peer_group_size
FROM idx_sw_l3_peers
WHERE anchor_ts_code = '000001.SZ'
ORDER BY l3_code;
```

