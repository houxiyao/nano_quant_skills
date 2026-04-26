# factor 模块 — 量价因子类（日频，按交易日增量）

## stk_auction_open

- **描述**: 集合竞价（开盘）成交数据
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 9

**字段列表**:
```
  ts_code                                   VARCHAR  -- 股票代码
  trade_date                                DATE  -- 交易日期
  close                                     DOUBLE  -- 收盘集合竞价收盘价
  open                                      DOUBLE  -- 收盘集合竞价开盘价
  high                                      DOUBLE  -- 收盘集合竞价最高价
  low                                       DOUBLE  -- 收盘集合竞价最低价
  vol                                       DOUBLE  -- 开盘集合竞价成交量
  amount                                    DOUBLE  -- 开盘集合竞价成交额
  vwap                                      DOUBLE  -- 开盘集合竞价均价
```

**查询示例**:
```sql
SELECT *
FROM stk_auction_open
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## stk_cyq_chips

- **描述**: 逐价位筹码占比分布（每行一个价位）
- **粒度**: 标的-交易日-价格
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date, price`
- **列数**: 4

**字段列表**:
```
  ts_code                                   VARCHAR  -- 股票代码
  trade_date                                DATE  -- 交易日期
  price                                     DOUBLE  -- 成本价格
  percent                                   DOUBLE  -- 价格占比(%)
```

**查询示例**:
```sql
SELECT *
FROM stk_cyq_chips
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## stk_cyq_perf

- **描述**: 筹码分布：成本分位 + 获利盘比率
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 11

**字段列表**:
```
  ts_code                                   VARCHAR  -- 股票代码
  trade_date                                DATE  -- 交易日期
  his_low                                   DOUBLE  -- 历史最低价
  his_high                                  DOUBLE  -- 历史最高价
  cost_5pct                                 DOUBLE  -- 5分位成本
  cost_15pct                                DOUBLE  -- 15分位成本
  cost_50pct                                DOUBLE  -- 50分位成本
  cost_85pct                                DOUBLE  -- 85分位成本
  cost_95pct                                DOUBLE  -- 95分位成本
  weight_avg                                DOUBLE  -- 加权平均成本
  winner_rate                               DOUBLE  -- 胜率
```

**查询示例**:
```sql
SELECT *
FROM stk_cyq_perf
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## stk_factor_pro

- **描述**: 个股日行情 + 估值 + 复权价格 + 200+ 技术指标（261列）
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 199

**字段列表**:
```
  ts_code                                   VARCHAR  -- 股票代码
  trade_date                                DATE  -- 交易日期
  open                                      DOUBLE  -- 开盘价
  open_hfq                                  DOUBLE  -- 开盘价（后复权）
  high                                      DOUBLE  -- 最高价
  high_hfq                                  DOUBLE  -- 最高价（后复权）
  low                                       DOUBLE  -- 最低价
  low_hfq                                   DOUBLE  -- 最低价（后复权）
  close                                     DOUBLE  -- 收盘价
  close_hfq                                 DOUBLE  -- 收盘价（后复权）
  change                                    DOUBLE  -- 涨跌额
  pct_chg                                   DOUBLE  -- 涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
  vol                                       DOUBLE  -- 成交量 （手）
  amount                                    DOUBLE  -- 成交额 （千元）
  turnover_rate                             DOUBLE  -- 换手率（%）
  turnover_rate_f                           DOUBLE  -- 换手率（自由流通股）
  volume_ratio                              DOUBLE  -- 量比
  pe                                        DOUBLE  -- 市盈率（总市值/净利润， 亏损的PE为空）
  pe_ttm                                    DOUBLE  -- 市盈率（TTM，亏损的PE为空）
  pb                                        DOUBLE  -- 市净率（总市值/净资产）
  ps                                        DOUBLE  -- 市销率
  ps_ttm                                    DOUBLE  -- 市销率（TTM）
  dv_ratio                                  DOUBLE  -- 股息率 （%）
  dv_ttm                                    DOUBLE  -- 股息率（TTM）（%）
  total_share                               DOUBLE  -- 总股本 （万股）
  float_share                               DOUBLE  -- 流通股本 （万股）
  free_share                                DOUBLE  -- 自由流通股本 （万）
  total_mv                                  DOUBLE  -- 总市值 （万元）
  circ_mv                                   DOUBLE  -- 流通市值（万元）
  adj_factor                                DOUBLE  -- 复权因子
  asi_bfq                                   DOUBLE  -- 振动升降指标-OPEN, CLOSE, HIGH, LOW, M1=26, M2=10
  asi_hfq                                   DOUBLE  -- 振动升降指标-OPEN, CLOSE, HIGH, LOW, M1=26, M2=10
  asit_bfq                                  DOUBLE  -- 振动升降指标-OPEN, CLOSE, HIGH, LOW, M1=26, M2=10
  asit_hfq                                  DOUBLE  -- 振动升降指标-OPEN, CLOSE, HIGH, LOW, M1=26, M2=10
  atr_bfq                                   DOUBLE  -- 真实波动N日平均值-CLOSE, HIGH, LOW, N=20
  atr_hfq                                   DOUBLE  -- 真实波动N日平均值-CLOSE, HIGH, LOW, N=20
  bbi_bfq                                   DOUBLE  -- BBI多空指标-CLOSE, M1=3, M2=6, M3=12, M4=20
  bbi_hfq                                   DOUBLE  -- BBI多空指标-CLOSE, M1=3, M2=6, M3=12, M4=21
  bias1_bfq                                 DOUBLE  -- BIAS乖离率-CLOSE, L1=6, L2=12, L3=24
  bias1_hfq                                 DOUBLE  -- BIAS乖离率-CLOSE, L1=6, L2=12, L3=24
  bias2_bfq                                 DOUBLE  -- BIAS乖离率-CLOSE, L1=6, L2=12, L3=24
  bias2_hfq                                 DOUBLE  -- BIAS乖离率-CLOSE, L1=6, L2=12, L3=24
  bias3_bfq                                 DOUBLE  -- BIAS乖离率-CLOSE, L1=6, L2=12, L3=24
  bias3_hfq                                 DOUBLE  -- BIAS乖离率-CLOSE, L1=6, L2=12, L3=24
  boll_lower_bfq                            DOUBLE  -- BOLL指标，布林带-CLOSE, N=20, P=2
  boll_lower_hfq                            DOUBLE  -- BOLL指标，布林带-CLOSE, N=20, P=2
  boll_mid_bfq                              DOUBLE  -- BOLL指标，布林带-CLOSE, N=20, P=2
  boll_mid_hfq                              DOUBLE  -- BOLL指标，布林带-CLOSE, N=20, P=2
  boll_upper_bfq                            DOUBLE  -- BOLL指标，布林带-CLOSE, N=20, P=2
  boll_upper_hfq                            DOUBLE  -- BOLL指标，布林带-CLOSE, N=20, P=2
  brar_ar_bfq                               DOUBLE  -- BRAR情绪指标-OPEN, CLOSE, HIGH, LOW, M1=26
  brar_ar_hfq                               DOUBLE  -- BRAR情绪指标-OPEN, CLOSE, HIGH, LOW, M1=26
  brar_br_bfq                               DOUBLE  -- BRAR情绪指标-OPEN, CLOSE, HIGH, LOW, M1=26
  brar_br_hfq                               DOUBLE  -- BRAR情绪指标-OPEN, CLOSE, HIGH, LOW, M1=26
  cci_bfq                                   DOUBLE  -- 顺势指标又叫CCI指标-CLOSE, HIGH, LOW, N=14
  cci_hfq                                   DOUBLE  -- 顺势指标又叫CCI指标-CLOSE, HIGH, LOW, N=14
  cr_bfq                                    DOUBLE  -- CR价格动量指标-CLOSE, HIGH, LOW, N=20
  cr_hfq                                    DOUBLE  -- CR价格动量指标-CLOSE, HIGH, LOW, N=20
  dfma_dif_bfq                              DOUBLE  -- 平行线差指标-CLOSE, N1=10, N2=50, M=10
  dfma_dif_hfq                              DOUBLE  -- 平行线差指标-CLOSE, N1=10, N2=50, M=10
  dfma_difma_bfq                            DOUBLE  -- 平行线差指标-CLOSE, N1=10, N2=50, M=10
  dfma_difma_hfq                            DOUBLE  -- 平行线差指标-CLOSE, N1=10, N2=50, M=10
  dmi_adx_bfq                               DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  dmi_adx_hfq                               DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  dmi_adxr_bfq                              DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  dmi_adxr_hfq                              DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  dmi_mdi_bfq                               DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  dmi_mdi_hfq                               DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  dmi_pdi_bfq                               DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  dmi_pdi_hfq                               DOUBLE  -- 动向指标-CLOSE, HIGH, LOW, M1=14, M2=6
  downdays                                  DOUBLE  -- 连跌天数
  updays                                    DOUBLE  -- 连涨天数
  dpo_bfq                                   DOUBLE  -- 区间震荡线-CLOSE, M1=20, M2=10, M3=6
  dpo_hfq                                   DOUBLE  -- 区间震荡线-CLOSE, M1=20, M2=10, M3=6
  madpo_bfq                                 DOUBLE  -- 区间震荡线-CLOSE, M1=20, M2=10, M3=6
  madpo_hfq                                 DOUBLE  -- 区间震荡线-CLOSE, M1=20, M2=10, M3=6
  ema_bfq_10                                DOUBLE  -- 指数移动平均-N=10
  ema_bfq_20                                DOUBLE  -- 指数移动平均-N=20
  ema_bfq_250                               DOUBLE  -- 指数移动平均-N=250
  ema_bfq_30                                DOUBLE  -- 指数移动平均-N=30
  ema_bfq_5                                 DOUBLE  -- 指数移动平均-N=5
  ema_bfq_60                                DOUBLE  -- 指数移动平均-N=60
  ema_bfq_90                                DOUBLE  -- 指数移动平均-N=90
  ema_hfq_10                                DOUBLE  -- 指数移动平均-N=10
  ema_hfq_20                                DOUBLE  -- 指数移动平均-N=20
  ema_hfq_250                               DOUBLE  -- 指数移动平均-N=250
  ema_hfq_30                                DOUBLE  -- 指数移动平均-N=30
  ema_hfq_5                                 DOUBLE  -- 指数移动平均-N=5
  ema_hfq_60                                DOUBLE  -- 指数移动平均-N=60
  ema_hfq_90                                DOUBLE  -- 指数移动平均-N=90
  ema_qfq_10                                DOUBLE  -- 指数移动平均-N=10
  ema_qfq_20                                DOUBLE  -- 指数移动平均-N=20
  ema_qfq_250                               DOUBLE  -- 指数移动平均-N=250
  ema_qfq_30                                DOUBLE  -- 指数移动平均-N=30
  ema_qfq_5                                 DOUBLE  -- 指数移动平均-N=5
  ema_qfq_60                                DOUBLE  -- 指数移动平均-N=60
  ema_qfq_90                                DOUBLE  -- 指数移动平均-N=90
  emv_bfq                                   DOUBLE  -- 简易波动指标-HIGH, LOW, VOL, N=14, M=9
  emv_hfq                                   DOUBLE  -- 简易波动指标-HIGH, LOW, VOL, N=14, M=9
  maemv_bfq                                 DOUBLE  -- 简易波动指标-HIGH, LOW, VOL, N=14, M=9
  maemv_hfq                                 DOUBLE  -- 简易波动指标-HIGH, LOW, VOL, N=14, M=9
  expma_12_bfq                              DOUBLE  -- EMA指数平均数指标-CLOSE, N1=12, N2=50
  expma_12_hfq                              DOUBLE  -- EMA指数平均数指标-CLOSE, N1=12, N2=50
  expma_50_bfq                              DOUBLE  -- EMA指数平均数指标-CLOSE, N1=12, N2=50
  expma_50_hfq                              DOUBLE  -- EMA指数平均数指标-CLOSE, N1=12, N2=50
  kdj_bfq                                   DOUBLE  -- KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3
  kdj_hfq                                   DOUBLE  -- KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3
  kdj_d_bfq                                 DOUBLE  -- KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3
  kdj_d_hfq                                 DOUBLE  -- KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3
  kdj_k_bfq                                 DOUBLE  -- KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3
  kdj_k_hfq                                 DOUBLE  -- KDJ指标-CLOSE, HIGH, LOW, N=9, M1=3, M2=3
  ktn_down_bfq                              DOUBLE  -- 肯特纳交易通道, N选20日，ATR选10日-CLOSE, HIGH, LOW, N=20, M=10
  ktn_down_hfq                              DOUBLE  -- 肯特纳交易通道, N选20日，ATR选10日-CLOSE, HIGH, LOW, N=20, M=10
  ktn_mid_bfq                               DOUBLE  -- 肯特纳交易通道, N选20日，ATR选10日-CLOSE, HIGH, LOW, N=20, M=10
  ktn_mid_hfq                               DOUBLE  -- 肯特纳交易通道, N选20日，ATR选10日-CLOSE, HIGH, LOW, N=20, M=10
  ktn_upper_bfq                             DOUBLE  -- 肯特纳交易通道, N选20日，ATR选10日-CLOSE, HIGH, LOW, N=20, M=10
  ktn_upper_hfq                             DOUBLE  -- 肯特纳交易通道, N选20日，ATR选10日-CLOSE, HIGH, LOW, N=20, M=10
  lowdays                                   DOUBLE  -- LOWRANGE(LOW)表示当前最低价是近多少周期内最低价的最小值
  topdays                                   DOUBLE  -- TOPRANGE(HIGH)表示当前最高价是近多少周期内最高价的最大值
  ma_bfq_10                                 DOUBLE  -- 简单移动平均-N=10
  ma_bfq_20                                 DOUBLE  -- 简单移动平均-N=20
  ma_bfq_250                                DOUBLE  -- 简单移动平均-N=250
  ma_bfq_30                                 DOUBLE  -- 简单移动平均-N=30
  ma_bfq_5                                  DOUBLE  -- 简单移动平均-N=5
  ma_bfq_60                                 DOUBLE  -- 简单移动平均-N=60
  ma_bfq_90                                 DOUBLE  -- 简单移动平均-N=90
  ma_hfq_10                                 DOUBLE  -- 简单移动平均-N=10
  ma_hfq_20                                 DOUBLE  -- 简单移动平均-N=20
  ma_hfq_250                                DOUBLE  -- 简单移动平均-N=250
  ma_hfq_30                                 DOUBLE  -- 简单移动平均-N=30
  ma_hfq_5                                  DOUBLE  -- 简单移动平均-N=5
  ma_hfq_60                                 DOUBLE  -- 简单移动平均-N=60
  ma_hfq_90                                 DOUBLE  -- 简单移动平均-N=90
  ma_qfq_10                                 DOUBLE  -- 简单移动平均-N=10
  ma_qfq_20                                 DOUBLE  -- 简单移动平均-N=20
  ma_qfq_250                                DOUBLE  -- 简单移动平均-N=250
  ma_qfq_30                                 DOUBLE  -- 简单移动平均-N=30
  ma_qfq_5                                  DOUBLE  -- 简单移动平均-N=5
  ma_qfq_60                                 DOUBLE  -- 简单移动平均-N=60
  ma_qfq_90                                 DOUBLE  -- 简单移动平均-N=90
  macd_bfq                                  DOUBLE  -- MACD指标-CLOSE, SHORT=12, LONG=26, M=9
  macd_hfq                                  DOUBLE  -- MACD指标-CLOSE, SHORT=12, LONG=26, M=9
  macd_dea_bfq                              DOUBLE  -- MACD指标-CLOSE, SHORT=12, LONG=26, M=9
  macd_dea_hfq                              DOUBLE  -- MACD指标-CLOSE, SHORT=12, LONG=26, M=9
  macd_dif_bfq                              DOUBLE  -- MACD指标-CLOSE, SHORT=12, LONG=26, M=9
  macd_dif_hfq                              DOUBLE  -- MACD指标-CLOSE, SHORT=12, LONG=26, M=9
  mass_bfq                                  DOUBLE  -- 梅斯线-HIGH, LOW, N1=9, N2=25, M=6
  mass_hfq                                  DOUBLE  -- 梅斯线-HIGH, LOW, N1=9, N2=25, M=6
  ma_mass_bfq                               DOUBLE  -- 梅斯线-HIGH, LOW, N1=9, N2=25, M=6
  ma_mass_hfq                               DOUBLE  -- 梅斯线-HIGH, LOW, N1=9, N2=25, M=6
  mfi_bfq                                   DOUBLE  -- MFI指标是成交量的RSI指标-CLOSE, HIGH, LOW, VOL, N=14
  mfi_hfq                                   DOUBLE  -- MFI指标是成交量的RSI指标-CLOSE, HIGH, LOW, VOL, N=14
  mtm_bfq                                   DOUBLE  -- 动量指标-CLOSE, N=12, M=6
  mtm_hfq                                   DOUBLE  -- 动量指标-CLOSE, N=12, M=6
  mtmma_bfq                                 DOUBLE  -- 动量指标-CLOSE, N=12, M=6
  mtmma_hfq                                 DOUBLE  -- 动量指标-CLOSE, N=12, M=6
  obv_bfq                                   DOUBLE  -- 能量潮指标-CLOSE, VOL
  obv_hfq                                   DOUBLE  -- 能量潮指标-CLOSE, VOL
  psy_bfq                                   DOUBLE  -- 投资者对股市涨跌产生心理波动的情绪指标-CLOSE, N=12, M=6
  psy_hfq                                   DOUBLE  -- 投资者对股市涨跌产生心理波动的情绪指标-CLOSE, N=12, M=6
  psyma_bfq                                 DOUBLE  -- 投资者对股市涨跌产生心理波动的情绪指标-CLOSE, N=12, M=6
  psyma_hfq                                 DOUBLE  -- 投资者对股市涨跌产生心理波动的情绪指标-CLOSE, N=12, M=6
  roc_bfq                                   DOUBLE  -- 变动率指标-CLOSE, N=12, M=6
  roc_hfq                                   DOUBLE  -- 变动率指标-CLOSE, N=12, M=6
  maroc_bfq                                 DOUBLE  -- 变动率指标-CLOSE, N=12, M=6
  maroc_hfq                                 DOUBLE  -- 变动率指标-CLOSE, N=12, M=6
  rsi_bfq_12                                DOUBLE  -- RSI指标-CLOSE, N=12
  rsi_bfq_24                                DOUBLE  -- RSI指标-CLOSE, N=24
  rsi_bfq_6                                 DOUBLE  -- RSI指标-CLOSE, N=6
  rsi_hfq_12                                DOUBLE  -- RSI指标-CLOSE, N=12
  rsi_hfq_24                                DOUBLE  -- RSI指标-CLOSE, N=24
  rsi_hfq_6                                 DOUBLE  -- RSI指标-CLOSE, N=6
  rsi_qfq_12                                DOUBLE  -- RSI指标-CLOSE, N=12
  rsi_qfq_24                                DOUBLE  -- RSI指标-CLOSE, N=24
  rsi_qfq_6                                 DOUBLE  -- RSI指标-CLOSE, N=6
  taq_down_bfq                              DOUBLE  -- 唐安奇通道(海龟)交易指标-HIGH, LOW, 20
  taq_down_hfq                              DOUBLE  -- 唐安奇通道(海龟)交易指标-HIGH, LOW, 20
  taq_mid_bfq                               DOUBLE  -- 唐安奇通道(海龟)交易指标-HIGH, LOW, 20
  taq_mid_hfq                               DOUBLE  -- 唐安奇通道(海龟)交易指标-HIGH, LOW, 20
  taq_up_bfq                                DOUBLE  -- 唐安奇通道(海龟)交易指标-HIGH, LOW, 20
  taq_up_hfq                                DOUBLE  -- 唐安奇通道(海龟)交易指标-HIGH, LOW, 20
  trix_bfq                                  DOUBLE  -- 三重指数平滑平均线-CLOSE, M1=12, M2=20
  trix_hfq                                  DOUBLE  -- 三重指数平滑平均线-CLOSE, M1=12, M2=20
  trma_bfq                                  DOUBLE  -- 三重指数平滑平均线-CLOSE, M1=12, M2=20
  trma_hfq                                  DOUBLE  -- 三重指数平滑平均线-CLOSE, M1=12, M2=20
  vr_bfq                                    DOUBLE  -- VR容量比率-CLOSE, VOL, M1=26
  vr_hfq                                    DOUBLE  -- VR容量比率-CLOSE, VOL, M1=26
  wr_bfq                                    DOUBLE  -- W&R 威廉指标-CLOSE, HIGH, LOW, N=10, N1=6
  wr_hfq                                    DOUBLE  -- W&R 威廉指标-CLOSE, HIGH, LOW, N=10, N1=6
  wr1_bfq                                   DOUBLE  -- W&R 威廉指标-CLOSE, HIGH, LOW, N=10, N1=6
  wr1_hfq                                   DOUBLE  -- W&R 威廉指标-CLOSE, HIGH, LOW, N=10, N1=6
  xsii_td1_bfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
  xsii_td1_hfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
  xsii_td2_bfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
  xsii_td2_hfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
  xsii_td3_bfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
  xsii_td3_hfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
  xsii_td4_bfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
  xsii_td4_hfq                              DOUBLE  -- 薛斯通道II-CLOSE, HIGH, LOW, N=102, M=7
```

**查询示例**:
```sql
SELECT *
FROM stk_factor_pro
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## stk_margin

- **描述**: 融资融券明细：融资余额、融券余量、偿还量
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 11

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS股票代码
  trade_date                                DATE  -- 交易日期
  name                                      VARCHAR  -- 股票名称（20190910后有数据）
  rzye                                      DOUBLE  -- 融资余额(元)
  rqye                                      DOUBLE  -- 融券余额(元)
  rzmre                                     DOUBLE  -- 融资买入额(元)
  rqyl                                      DOUBLE  -- 融券余量（股）
  rzche                                     DOUBLE  -- 融资偿还额(元)
  rqchl                                     DOUBLE  -- 融券偿还量(股)
  rqmcl                                     DOUBLE  -- 融券卖出量(股,份,手)
  rzrqye                                    DOUBLE  -- 融资融券余额(元)
```

**查询示例**:
```sql
SELECT *
FROM stk_margin
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## stk_moneyflow

- **描述**: 个股大中小单资金净流向
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 20

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS代码
  trade_date                                DATE  -- 交易日期
  buy_sm_vol                                BIGINT  -- 小单买入量（手）
  buy_sm_amount                             DOUBLE  -- 小单买入金额（万元）
  sell_sm_vol                               BIGINT  -- 小单卖出量（手）
  sell_sm_amount                            DOUBLE  -- 小单卖出金额（万元）
  buy_md_vol                                BIGINT  -- 中单买入量（手）
  buy_md_amount                             DOUBLE  -- 中单买入金额（万元）
  sell_md_vol                               BIGINT  -- 中单卖出量（手）
  sell_md_amount                            DOUBLE  -- 中单卖出金额（万元）
  buy_lg_vol                                BIGINT  -- 大单买入量（手）
  buy_lg_amount                             DOUBLE  -- 大单买入金额（万元）
  sell_lg_vol                               BIGINT  -- 大单卖出量（手）
  sell_lg_amount                            DOUBLE  -- 大单卖出金额（万元）
  buy_elg_vol                               BIGINT  -- 特大单买入量（手）
  buy_elg_amount                            DOUBLE  -- 特大单买入金额（万元）
  sell_elg_vol                              BIGINT  -- 特大单卖出量（手）
  sell_elg_amount                           DOUBLE  -- 特大单卖出金额（万元）
  net_mf_vol                                BIGINT  -- 净流入量（手）
  net_mf_amount                             DOUBLE  -- 净流入额（万元）
```

**查询示例**:
```sql
SELECT *
FROM stk_moneyflow
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## stk_moneyflow_ths

- **描述**: 同花顺资金流向（大单/中单/小单净额）
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date`
- **列数**: 13

**字段列表**:
```
  trade_date                                DATE  -- 交易日期
  ts_code                                   VARCHAR  -- 股票代码
  name                                      VARCHAR  -- 股票名称
  pct_change                                DOUBLE  -- 涨跌幅
  latest                                    DOUBLE  -- 最新价
  net_amount                                DOUBLE  -- 资金净流入(万元)
  net_d5_amount                             DOUBLE  -- 5日主力净额(万元)
  buy_lg_amount                             DOUBLE  -- 今日大单净流入额(万元)
  buy_lg_amount_rate                        DOUBLE  -- 今日大单净流入占比(%)
  buy_md_amount                             DOUBLE  -- 今日中单净流入额(万元)
  buy_md_amount_rate                        DOUBLE  -- 今日中单净流入占比(%)
  buy_sm_amount                             DOUBLE  -- 今日小单净流入额(万元)
  buy_sm_amount_rate                        DOUBLE  -- 今日小单净流入占比(%)
```

**查询示例**:
```sql
SELECT *
FROM stk_moneyflow_ths
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

## stk_suspend

- **描述**: 个股停复牌日历
- **粒度**: 标的-交易日
- **同步维度**: `trade_date`
- **主键**: `ts_code, trade_date, suspend_type`
- **列数**: 4

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS代码
  trade_date                                DATE  -- 停复牌日期
  suspend_timing                            VARCHAR  -- 日内停牌时间段
  suspend_type                              VARCHAR  -- 停复牌类型：S-停牌，R-复牌
```

**查询示例**:
```sql
SELECT *
FROM stk_suspend
WHERE ts_code = '000001.SZ' AND trade_date >= '20240101'
LIMIT 100;
```

