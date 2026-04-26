# finance 模块 — 财务报表类（按报告期增量）

## fin_balance

- **描述**: 资产负债表：总资产、负债、股东权益
- **粒度**: 标的-报告期
- **同步维度**: `period`
- **主键**: `ts_code, end_date, report_type`
- **列数**: 158

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS股票代码
  ann_date                                  DATE  -- 公告日期
  f_ann_date                                DATE  -- 实际公告日期
  end_date                                  DATE  -- 报告期
  report_type                               VARCHAR  -- 报表类型
  comp_type                                 VARCHAR  -- 公司类型(1一般工商业2银行3保险4证券)
  end_type                                  VARCHAR  -- 报告期类型
  total_share                               DOUBLE  -- 期末总股本
  cap_rese                                  DOUBLE  -- 资本公积金
  undistr_porfit                            DOUBLE  -- 未分配利润
  surplus_rese                              DOUBLE  -- 盈余公积金
  special_rese                              DOUBLE  -- 专项储备
  money_cap                                 DOUBLE  -- 货币资金
  trad_asset                                DOUBLE  -- 交易性金融资产
  notes_receiv                              DOUBLE  -- 应收票据
  accounts_receiv                           DOUBLE  -- 应收账款
  oth_receiv                                DOUBLE  -- 其他应收款
  prepayment                                DOUBLE  -- 预付款项
  div_receiv                                DOUBLE  -- 应收股利
  int_receiv                                DOUBLE  -- 应收利息
  inventories                               DOUBLE  -- 存货
  amor_exp                                  DOUBLE  -- 待摊费用
  nca_within_1y                             DOUBLE  -- 一年内到期的非流动资产
  sett_rsrv                                 DOUBLE  -- 结算备付金
  loanto_oth_bank_fi                        DOUBLE  -- 拆出资金
  premium_receiv                            DOUBLE  -- 应收保费
  reinsur_receiv                            DOUBLE  -- 应收分保账款
  reinsur_res_receiv                        DOUBLE  -- 应收分保合同准备金
  pur_resale_fa                             DOUBLE  -- 买入返售金融资产
  oth_cur_assets                            DOUBLE  -- 其他流动资产
  total_cur_assets                          DOUBLE  -- 流动资产合计
  fa_avail_for_sale                         DOUBLE  -- 可供出售金融资产
  htm_invest                                DOUBLE  -- 持有至到期投资
  lt_eqt_invest                             DOUBLE  -- 长期股权投资
  invest_real_estate                        DOUBLE  -- 投资性房地产
  time_deposits                             DOUBLE  -- 定期存款
  oth_assets                                DOUBLE  -- 其他资产
  lt_rec                                    DOUBLE  -- 长期应收款
  fix_assets                                DOUBLE  -- 固定资产
  cip                                       DOUBLE  -- 在建工程
  const_materials                           DOUBLE  -- 工程物资
  fixed_assets_disp                         DOUBLE  -- 固定资产清理
  produc_bio_assets                         DOUBLE  -- 生产性生物资产
  oil_and_gas_assets                        DOUBLE  -- 油气资产
  intan_assets                              DOUBLE  -- 无形资产
  r_and_d                                   DOUBLE  -- 研发支出
  goodwill                                  DOUBLE  -- 商誉
  lt_amor_exp                               DOUBLE  -- 长期待摊费用
  defer_tax_assets                          DOUBLE  -- 递延所得税资产
  decr_in_disbur                            DOUBLE  -- 发放贷款及垫款
  oth_nca                                   DOUBLE  -- 其他非流动资产
  total_nca                                 DOUBLE  -- 非流动资产合计
  cash_reser_cb                             DOUBLE  -- 现金及存放中央银行款项
  depos_in_oth_bfi                          DOUBLE  -- 存放同业和其它金融机构款项
  prec_metals                               DOUBLE  -- 贵金属
  deriv_assets                              DOUBLE  -- 衍生金融资产
  rr_reins_une_prem                         DOUBLE  -- 应收分保未到期责任准备金
  rr_reins_outstd_cla                       DOUBLE  -- 应收分保未决赔款准备金
  rr_reins_lins_liab                        DOUBLE  -- 应收分保寿险责任准备金
  rr_reins_lthins_liab                      DOUBLE  -- 应收分保长期健康险责任准备金
  refund_depos                              DOUBLE  -- 存出保证金
  ph_pledge_loans                           DOUBLE  -- 保户质押贷款
  refund_cap_depos                          DOUBLE  -- 存出资本保证金
  indep_acct_assets                         DOUBLE  -- 独立账户资产
  client_depos                              DOUBLE  -- 其中：客户资金存款
  client_prov                               DOUBLE  -- 其中：客户备付金
  transac_seat_fee                          DOUBLE  -- 其中:交易席位费
  invest_as_receiv                          DOUBLE  -- 应收款项类投资
  total_assets                              DOUBLE  -- 资产总计
  lt_borr                                   DOUBLE  -- 长期借款
  st_borr                                   DOUBLE  -- 短期借款
  cb_borr                                   DOUBLE  -- 向中央银行借款
  depos_ib_deposits                         DOUBLE  -- 吸收存款及同业存放
  loan_oth_bank                             DOUBLE  -- 拆入资金
  trading_fl                                DOUBLE  -- 交易性金融负债
  notes_payable                             DOUBLE  -- 应付票据
  acct_payable                              DOUBLE  -- 应付账款
  adv_receipts                              DOUBLE  -- 预收款项
  sold_for_repur_fa                         DOUBLE  -- 卖出回购金融资产款
  comm_payable                              DOUBLE  -- 应付手续费及佣金
  payroll_payable                           DOUBLE  -- 应付职工薪酬
  taxes_payable                             DOUBLE  -- 应交税费
  int_payable                               DOUBLE  -- 应付利息
  div_payable                               DOUBLE  -- 应付股利
  oth_payable                               DOUBLE  -- 其他应付款
  acc_exp                                   DOUBLE  -- 预提费用
  deferred_inc                              DOUBLE  -- 递延收益
  st_bonds_payable                          DOUBLE  -- 应付短期债券
  payable_to_reinsurer                      DOUBLE  -- 应付分保账款
  rsrv_insur_cont                           DOUBLE  -- 保险合同准备金
  acting_trading_sec                        DOUBLE  -- 代理买卖证券款
  acting_uw_sec                             DOUBLE  -- 代理承销证券款
  non_cur_liab_due_1y                       DOUBLE  -- 一年内到期的非流动负债
  oth_cur_liab                              DOUBLE  -- 其他流动负债
  total_cur_liab                            DOUBLE  -- 流动负债合计
  bond_payable                              DOUBLE  -- 应付债券
  lt_payable                                DOUBLE  -- 长期应付款
  specific_payables                         DOUBLE  -- 专项应付款
  estimated_liab                            DOUBLE  -- 预计负债
  defer_tax_liab                            DOUBLE  -- 递延所得税负债
  defer_inc_non_cur_liab                    DOUBLE  -- 递延收益-非流动负债
  oth_ncl                                   DOUBLE  -- 其他非流动负债
  total_ncl                                 DOUBLE  -- 非流动负债合计
  depos_oth_bfi                             DOUBLE  -- 同业和其它金融机构存放款项
  deriv_liab                                DOUBLE  -- 衍生金融负债
  depos                                     DOUBLE  -- 吸收存款
  agency_bus_liab                           DOUBLE  -- 代理业务负债
  oth_liab                                  DOUBLE  -- 其他负债
  prem_receiv_adva                          DOUBLE  -- 预收保费
  depos_received                            DOUBLE  -- 存入保证金
  ph_invest                                 DOUBLE  -- 保户储金及投资款
  reser_une_prem                            DOUBLE  -- 未到期责任准备金
  reser_outstd_claims                       DOUBLE  -- 未决赔款准备金
  reser_lins_liab                           DOUBLE  -- 寿险责任准备金
  reser_lthins_liab                         DOUBLE  -- 长期健康险责任准备金
  indept_acc_liab                           DOUBLE  -- 独立账户负债
  pledge_borr                               DOUBLE  -- 其中:质押借款
  indem_payable                             DOUBLE  -- 应付赔付款
  policy_div_payable                        DOUBLE  -- 应付保单红利
  total_liab                                DOUBLE  -- 负债合计
  treasury_share                            DOUBLE  -- 减:库存股
  ordin_risk_reser                          DOUBLE  -- 一般风险准备
  forex_differ                              DOUBLE  -- 外币报表折算差额
  invest_loss_unconf                        DOUBLE  -- 未确认的投资损失
  minority_int                              DOUBLE  -- 少数股东权益
  total_hldr_eqy_exc_min_int                DOUBLE  -- 股东权益合计(不含少数股东权益)
  total_hldr_eqy_inc_min_int                DOUBLE  -- 股东权益合计(含少数股东权益)
  total_liab_hldr_eqy                       DOUBLE  -- 负债及股东权益总计
  lt_payroll_payable                        DOUBLE  -- 长期应付职工薪酬
  oth_comp_income                           DOUBLE  -- 其他综合收益
  oth_eqt_tools                             DOUBLE  -- 其他权益工具
  oth_eqt_tools_p_shr                       DOUBLE  -- 其他权益工具(优先股)
  lending_funds                             DOUBLE  -- 融出资金
  acc_receivable                            DOUBLE  -- 应收款项
  st_fin_payable                            DOUBLE  -- 应付短期融资款
  payables                                  DOUBLE  -- 应付款项
  hfs_assets                                DOUBLE  -- 持有待售的资产
  hfs_sales                                 DOUBLE  -- 持有待售的负债
  cost_fin_assets                           DOUBLE  -- 以摊余成本计量的金融资产
  fair_value_fin_assets                     DOUBLE  -- 以公允价值计量且其变动计入其他综合收益的金融资产
  cip_total                                 DOUBLE  -- 在建工程(合计)(元)
  oth_pay_total                             DOUBLE  -- 其他应付款(合计)(元)
  long_pay_total                            DOUBLE  -- 长期应付款(合计)(元)
  debt_invest                               DOUBLE  -- 债权投资(元)
  oth_debt_invest                           DOUBLE  -- 其他债权投资(元)
  oth_eq_invest                             DOUBLE  -- 其他权益工具投资(元)
  oth_illiq_fin_assets                      DOUBLE  -- 其他非流动金融资产(元)
  oth_eq_ppbond                             DOUBLE  -- 其他权益工具:永续债(元)
  receiv_financing                          DOUBLE  -- 应收款项融资
  use_right_assets                          DOUBLE  -- 使用权资产
  lease_liab                                DOUBLE  -- 租赁负债
  contract_assets                           DOUBLE  -- 合同资产
  contract_liab                             DOUBLE  -- 合同负债
  accounts_receiv_bill                      DOUBLE  -- 应收票据及应收账款
  accounts_pay                              DOUBLE  -- 应付票据及应付账款
  oth_rcv_total                             DOUBLE  -- 其他应收款(合计)（元）
  fix_assets_total                          DOUBLE  -- 固定资产(合计)(元)
  update_flag                               VARCHAR  -- 更新标识
```

**查询示例**:
```sql
SELECT *
FROM fin_balance
WHERE ts_code = '000001.SZ' AND end_date >= '20230101'
LIMIT 100;
```

## fin_cashflow

- **描述**: 现金流量表：经营/投资/筹资活动现金流
- **粒度**: 标的-报告期
- **同步维度**: `period`
- **主键**: `ts_code, end_date, report_type`
- **列数**: 97

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS股票代码
  ann_date                                  DATE  -- 公告日期
  f_ann_date                                DATE  -- 实际公告日期
  end_date                                  DATE  -- 报告期
  comp_type                                 VARCHAR  -- 公司类型(1一般工商业2银行3保险4证券)
  report_type                               VARCHAR  -- 报表类型
  end_type                                  VARCHAR  -- 报告期类型
  net_profit                                DOUBLE  -- 净利润
  finan_exp                                 DOUBLE  -- 财务费用
  c_fr_sale_sg                              DOUBLE  -- 销售商品、提供劳务收到的现金
  recp_tax_rends                            DOUBLE  -- 收到的税费返还
  n_depos_incr_fi                           DOUBLE  -- 客户存款和同业存放款项净增加额
  n_incr_loans_cb                           DOUBLE  -- 向中央银行借款净增加额
  n_inc_borr_oth_fi                         DOUBLE  -- 向其他金融机构拆入资金净增加额
  prem_fr_orig_contr                        DOUBLE  -- 收到原保险合同保费取得的现金
  n_incr_insured_dep                        DOUBLE  -- 保户储金净增加额
  n_reinsur_prem                            DOUBLE  -- 收到再保业务现金净额
  n_incr_disp_tfa                           DOUBLE  -- 处置交易性金融资产净增加额
  ifc_cash_incr                             DOUBLE  -- 收取利息和手续费净增加额
  n_incr_disp_faas                          DOUBLE  -- 处置可供出售金融资产净增加额
  n_incr_loans_oth_bank                     DOUBLE  -- 拆入资金净增加额
  n_cap_incr_repur                          DOUBLE  -- 回购业务资金净增加额
  c_fr_oth_operate_a                        DOUBLE  -- 收到其他与经营活动有关的现金
  c_inf_fr_operate_a                        DOUBLE  -- 经营活动现金流入小计
  c_paid_goods_s                            DOUBLE  -- 购买商品、接受劳务支付的现金
  c_paid_to_for_empl                        DOUBLE  -- 支付给职工以及为职工支付的现金
  c_paid_for_taxes                          DOUBLE  -- 支付的各项税费
  n_incr_clt_loan_adv                       DOUBLE  -- 客户贷款及垫款净增加额
  n_incr_dep_cbob                           DOUBLE  -- 存放央行和同业款项净增加额
  c_pay_claims_orig_inco                    DOUBLE  -- 支付原保险合同赔付款项的现金
  pay_handling_chrg                         DOUBLE  -- 支付手续费的现金
  pay_comm_insur_plcy                       DOUBLE  -- 支付保单红利的现金
  oth_cash_pay_oper_act                     DOUBLE  -- 支付其他与经营活动有关的现金
  st_cash_out_act                           DOUBLE  -- 经营活动现金流出小计
  n_cashflow_act                            DOUBLE  -- 经营活动产生的现金流量净额
  oth_recp_ral_inv_act                      DOUBLE  -- 收到其他与投资活动有关的现金
  c_disp_withdrwl_invest                    DOUBLE  -- 收回投资收到的现金
  c_recp_return_invest                      DOUBLE  -- 取得投资收益收到的现金
  n_recp_disp_fiolta                        DOUBLE  -- 处置固定资产、无形资产和其他长期资产收回的现金净额
  n_recp_disp_sobu                          DOUBLE  -- 处置子公司及其他营业单位收到的现金净额
  stot_inflows_inv_act                      DOUBLE  -- 投资活动现金流入小计
  c_pay_acq_const_fiolta                    DOUBLE  -- 购建固定资产、无形资产和其他长期资产支付的现金
  c_paid_invest                             DOUBLE  -- 投资支付的现金
  n_disp_subs_oth_biz                       DOUBLE  -- 取得子公司及其他营业单位支付的现金净额
  oth_pay_ral_inv_act                       DOUBLE  -- 支付其他与投资活动有关的现金
  n_incr_pledge_loan                        DOUBLE  -- 质押贷款净增加额
  stot_out_inv_act                          DOUBLE  -- 投资活动现金流出小计
  n_cashflow_inv_act                        DOUBLE  -- 投资活动产生的现金流量净额
  c_recp_borrow                             DOUBLE  -- 取得借款收到的现金
  proc_issue_bonds                          DOUBLE  -- 发行债券收到的现金
  oth_cash_recp_ral_fnc_act                 DOUBLE  -- 收到其他与筹资活动有关的现金
  stot_cash_in_fnc_act                      DOUBLE  -- 筹资活动现金流入小计
  free_cashflow                             DOUBLE  -- 企业自由现金流量
  c_prepay_amt_borr                         DOUBLE  -- 偿还债务支付的现金
  c_pay_dist_dpcp_int_exp                   DOUBLE  -- 分配股利、利润或偿付利息支付的现金
  incl_dvd_profit_paid_sc_ms                DOUBLE  -- 其中:子公司支付给少数股东的股利、利润
  oth_cashpay_ral_fnc_act                   DOUBLE  -- 支付其他与筹资活动有关的现金
  stot_cashout_fnc_act                      DOUBLE  -- 筹资活动现金流出小计
  n_cash_flows_fnc_act                      DOUBLE  -- 筹资活动产生的现金流量净额
  eff_fx_flu_cash                           DOUBLE  -- 汇率变动对现金的影响
  n_incr_cash_cash_equ                      DOUBLE  -- 现金及现金等价物净增加额
  c_cash_equ_beg_period                     DOUBLE  -- 期初现金及现金等价物余额
  c_cash_equ_end_period                     DOUBLE  -- 期末现金及现金等价物余额
  c_recp_cap_contrib                        DOUBLE  -- 吸收投资收到的现金
  incl_cash_rec_saims                       DOUBLE  -- 其中:子公司吸收少数股东投资收到的现金
  uncon_invest_loss                         DOUBLE  -- 未确认投资损失
  prov_depr_assets                          DOUBLE  -- 加:资产减值准备
  depr_fa_coga_dpba                         DOUBLE  -- 固定资产折旧、油气资产折耗、生产性生物资产折旧
  amort_intang_assets                       DOUBLE  -- 无形资产摊销
  lt_amort_deferred_exp                     DOUBLE  -- 长期待摊费用摊销
  decr_deferred_exp                         DOUBLE  -- 待摊费用减少
  incr_acc_exp                              DOUBLE  -- 预提费用增加
  loss_disp_fiolta                          DOUBLE  -- 处置固定、无形资产和其他长期资产的损失
  loss_scr_fa                               DOUBLE  -- 固定资产报废损失
  loss_fv_chg                               DOUBLE  -- 公允价值变动损失
  invest_loss                               DOUBLE  -- 投资损失
  decr_def_inc_tax_assets                   DOUBLE  -- 递延所得税资产减少
  incr_def_inc_tax_liab                     DOUBLE  -- 递延所得税负债增加
  decr_inventories                          DOUBLE  -- 存货的减少
  decr_oper_payable                         DOUBLE  -- 经营性应收项目的减少
  incr_oper_payable                         DOUBLE  -- 经营性应付项目的增加
  others                                    DOUBLE  -- 其他
  im_net_cashflow_oper_act                  DOUBLE  -- 经营活动产生的现金流量净额(间接法)
  conv_debt_into_cap                        DOUBLE  -- 债务转为资本
  conv_copbonds_due_within_1y               DOUBLE  -- 一年内到期的可转换公司债券
  fa_fnc_leases                             DOUBLE  -- 融资租入固定资产
  im_n_incr_cash_equ                        DOUBLE  -- 现金及现金等价物净增加额(间接法)
  net_dism_capital_add                      DOUBLE  -- 拆出资金净增加额
  net_cash_rece_sec                         DOUBLE  -- 代理买卖证券收到的现金净额(元)
  credit_impa_loss                          DOUBLE  -- 信用减值损失
  use_right_asset_dep                       DOUBLE  -- 使用权资产折旧
  oth_loss_asset                            DOUBLE  -- 其他资产减值损失
  end_bal_cash                              DOUBLE  -- 现金的期末余额
  beg_bal_cash                              DOUBLE  -- 减:现金的期初余额
  end_bal_cash_equ                          DOUBLE  -- 加:现金等价物的期末余额
  beg_bal_cash_equ                          DOUBLE  -- 减:现金等价物的期初余额
  update_flag                               VARCHAR  -- 更新标志(1最新）
```

**查询示例**:
```sql
SELECT *
FROM fin_cashflow
WHERE ts_code = '000001.SZ' AND end_date >= '20230101'
LIMIT 100;
```

## fin_express

- **描述**: 业绩快报：提前发布的净利润/营收预估
- **粒度**: 标的-报告期
- **同步维度**: `period`
- **主键**: `ts_code, end_date, ann_date`
- **列数**: 32

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS股票代码
  ann_date                                  DATE  -- 公告日期
  end_date                                  DATE  -- 报告期
  revenue                                   DOUBLE  -- 营业收入(元)
  operate_profit                            DOUBLE  -- 营业利润(元)
  total_profit                              DOUBLE  -- 利润总额(元)
  n_income                                  DOUBLE  -- 净利润(元)
  total_assets                              DOUBLE  -- 总资产(元)
  total_hldr_eqy_exc_min_int                DOUBLE  -- 股东权益合计(不含少数股东权益)(元)
  diluted_eps                               DOUBLE  -- 每股收益(摊薄)(元)
  diluted_roe                               DOUBLE  -- 净资产收益率(摊薄)(%)
  yoy_net_profit                            DOUBLE  -- 去年同期修正后净利润
  bps                                       DOUBLE  -- 每股净资产
  yoy_sales                                 DOUBLE  -- 同比增长率:营业收入
  yoy_op                                    DOUBLE  -- 同比增长率:营业利润
  yoy_tp                                    DOUBLE  -- 同比增长率:利润总额
  yoy_dedu_np                               DOUBLE  -- 同比增长率:归属母公司股东的净利润
  yoy_eps                                   DOUBLE  -- 同比增长率:基本每股收益
  yoy_roe                                   DOUBLE  -- 同比增减:加权平均净资产收益率
  growth_assets                             DOUBLE  -- 比年初增长率:总资产
  yoy_equity                                DOUBLE  -- 比年初增长率:归属母公司的股东权益
  growth_bps                                DOUBLE  -- 比年初增长率:归属于母公司股东的每股净资产
  or_last_year                              DOUBLE  -- 去年同期营业收入
  op_last_year                              DOUBLE  -- 去年同期营业利润
  tp_last_year                              DOUBLE  -- 去年同期利润总额
  np_last_year                              DOUBLE  -- 去年同期净利润
  eps_last_year                             DOUBLE  -- 去年同期每股收益
  open_net_assets                           DOUBLE  -- 期初净资产
  open_bps                                  DOUBLE  -- 期初每股净资产
  perf_summary                              VARCHAR  -- 业绩简要说明
  is_audit                                  INTEGER  -- 是否审计： 1是 0否
  remark                                    VARCHAR  -- 备注
```

**查询示例**:
```sql
SELECT *
FROM fin_express
WHERE ts_code = '000001.SZ' AND end_date >= '20230101'
LIMIT 100;
```

## fin_forecast

- **描述**: 业绩预告：预增/预减/扭亏/续盈类型
- **粒度**: 标的-报告期
- **同步维度**: `period`
- **主键**: `ts_code, end_date, ann_date`
- **列数**: 12

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS股票代码
  ann_date                                  DATE  -- 公告日期
  end_date                                  DATE  -- 报告期
  type                                      VARCHAR  -- 业绩预告类型(预增/预减/扭亏/首亏/续亏/续盈/略增/略减)
  p_change_min                              DOUBLE  -- 预告净利润变动幅度下限（%）
  p_change_max                              DOUBLE  -- 预告净利润变动幅度上限（%）
  net_profit_min                            DOUBLE  -- 预告净利润下限（万元）
  net_profit_max                            DOUBLE  -- 预告净利润上限（万元）
  last_parent_net                           DOUBLE  -- 上年同期归属母公司净利润
  first_ann_date                            DATE  -- 首次公告日
  summary                                   VARCHAR  -- 业绩预告摘要
  change_reason                             VARCHAR  -- 业绩变动原因
```

**查询示例**:
```sql
SELECT *
FROM fin_forecast
WHERE ts_code = '000001.SZ' AND end_date >= '20230101'
LIMIT 100;
```

## fin_income

- **描述**: 利润表：营收、净利润、EPS
- **粒度**: 标的-报告期
- **同步维度**: `period`
- **主键**: `ts_code, end_date, report_type`
- **列数**: 94

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS代码
  ann_date                                  DATE  -- 公告日期
  f_ann_date                                DATE  -- 实际公告日期
  end_date                                  DATE  -- 报告期
  report_type                               VARCHAR  -- 报告类型
  comp_type                                 VARCHAR  -- 公司类型(1一般工商业2银行3保险4证券)
  end_type                                  VARCHAR  -- 报告期类型
  basic_eps                                 DOUBLE  -- 基本每股收益
  diluted_eps                               DOUBLE  -- 稀释每股收益
  total_revenue                             DOUBLE  -- 营业总收入
  revenue                                   DOUBLE  -- 营业收入
  int_income                                DOUBLE  -- 利息收入
  prem_earned                               DOUBLE  -- 已赚保费
  comm_income                               DOUBLE  -- 手续费及佣金收入
  n_commis_income                           DOUBLE  -- 手续费及佣金净收入
  n_oth_income                              DOUBLE  -- 其他经营净收益
  n_oth_b_income                            DOUBLE  -- 加:其他业务净收益
  prem_income                               DOUBLE  -- 保险业务收入
  out_prem                                  DOUBLE  -- 减:分出保费
  une_prem_reser                            DOUBLE  -- 提取未到期责任准备金
  reins_income                              DOUBLE  -- 其中:分保费收入
  n_sec_tb_income                           DOUBLE  -- 代理买卖证券业务净收入
  n_sec_uw_income                           DOUBLE  -- 证券承销业务净收入
  n_asset_mg_income                         DOUBLE  -- 受托客户资产管理业务净收入
  oth_b_income                              DOUBLE  -- 其他业务收入
  fv_value_chg_gain                         DOUBLE  -- 加:公允价值变动净收益
  invest_income                             DOUBLE  -- 加:投资净收益
  ass_invest_income                         DOUBLE  -- 其中:对联营企业和合营企业的投资收益
  forex_gain                                DOUBLE  -- 加:汇兑净收益
  total_cogs                                DOUBLE  -- 营业总成本
  oper_cost                                 DOUBLE  -- 减:营业成本
  int_exp                                   DOUBLE  -- 减:利息支出
  comm_exp                                  DOUBLE  -- 减:手续费及佣金支出
  biz_tax_surchg                            DOUBLE  -- 减:营业税金及附加
  sell_exp                                  DOUBLE  -- 减:销售费用
  admin_exp                                 DOUBLE  -- 减:管理费用
  fin_exp                                   DOUBLE  -- 减:财务费用
  assets_impair_loss                        DOUBLE  -- 减:资产减值损失
  prem_refund                               DOUBLE  -- 退保金
  compens_payout                            DOUBLE  -- 赔付总支出
  reser_insur_liab                          DOUBLE  -- 提取保险责任准备金
  div_payt                                  DOUBLE  -- 保户红利支出
  reins_exp                                 DOUBLE  -- 分保费用
  oper_exp                                  DOUBLE  -- 营业支出
  compens_payout_refu                       DOUBLE  -- 减:摊回赔付支出
  insur_reser_refu                          DOUBLE  -- 减:摊回保险责任准备金
  reins_cost_refund                         DOUBLE  -- 减:摊回分保费用
  other_bus_cost                            DOUBLE  -- 其他业务成本
  operate_profit                            DOUBLE  -- 营业利润
  non_oper_income                           DOUBLE  -- 加:营业外收入
  non_oper_exp                              DOUBLE  -- 减:营业外支出
  nca_disploss                              DOUBLE  -- 其中:减:非流动资产处置净损失
  total_profit                              DOUBLE  -- 利润总额
  income_tax                                DOUBLE  -- 所得税费用
  n_income                                  DOUBLE  -- 净利润(含少数股东损益)
  n_income_attr_p                           DOUBLE  -- 净利润(不含少数股东损益)
  minority_gain                             DOUBLE  -- 少数股东损益
  oth_compr_income                          DOUBLE  -- 其他综合收益
  t_compr_income                            DOUBLE  -- 综合收益总额
  compr_inc_attr_p                          DOUBLE  -- 归属于母公司(或股东)的综合收益总额
  compr_inc_attr_m_s                        DOUBLE  -- 归属于少数股东的综合收益总额
  ebit                                      DOUBLE  -- 息税前利润
  ebitda                                    DOUBLE  -- 息税折旧摊销前利润
  insurance_exp                             DOUBLE  -- 保险业务支出
  undist_profit                             DOUBLE  -- 年初未分配利润
  distable_profit                           DOUBLE  -- 可分配利润
  rd_exp                                    DOUBLE  -- 研发费用
  fin_exp_int_exp                           DOUBLE  -- 财务费用:利息费用
  fin_exp_int_inc                           DOUBLE  -- 财务费用:利息收入
  transfer_surplus_rese                     DOUBLE  -- 盈余公积转入
  transfer_housing_imprest                  DOUBLE  -- 住房周转金转入
  transfer_oth                              DOUBLE  -- 其他转入
  adj_lossgain                              DOUBLE  -- 调整以前年度损益
  withdra_legal_surplus                     DOUBLE  -- 提取法定盈余公积
  withdra_legal_pubfund                     DOUBLE  -- 提取法定公益金
  withdra_biz_devfund                       DOUBLE  -- 提取企业发展基金
  withdra_rese_fund                         DOUBLE  -- 提取储备基金
  withdra_oth_ersu                          DOUBLE  -- 提取任意盈余公积金
  workers_welfare                           DOUBLE  -- 职工奖金福利
  distr_profit_shrhder                      DOUBLE  -- 可供股东分配的利润
  prfshare_payable_dvd                      DOUBLE  -- 应付优先股股利
  comshare_payable_dvd                      DOUBLE  -- 应付普通股股利
  capit_comstock_div                        DOUBLE  -- 转作股本的普通股股利
  net_after_nr_lp_correct                   DOUBLE  -- 扣除非经常性损益后的净利润（更正前）
  credit_impa_loss                          DOUBLE  -- 信用减值损失
  net_expo_hedging_benefits                 DOUBLE  -- 净敞口套期收益
  oth_impair_loss_assets                    DOUBLE  -- 其他资产减值损失
  total_opcost                              DOUBLE  -- 营业总成本（二）
  amodcost_fin_assets                       DOUBLE  -- 以摊余成本计量的金融资产终止确认收益
  oth_income                                DOUBLE  -- 其他收益
  asset_disp_income                         DOUBLE  -- 资产处置收益
  continued_net_profit                      DOUBLE  -- 持续经营净利润
  end_net_profit                            DOUBLE  -- 终止经营净利润
  update_flag                               VARCHAR  -- 更新标识
```

**查询示例**:
```sql
SELECT *
FROM fin_income
WHERE ts_code = '000001.SZ' AND end_date >= '20230101'
LIMIT 100;
```

## fin_indicator

- **描述**: 财务指标：ROE/ROA/CFPS/毛利率等衍生指标
- **粒度**: 标的-报告期
- **同步维度**: `period`
- **主键**: `ts_code, end_date, ann_date`
- **列数**: 168

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS代码
  ann_date                                  DATE  -- 公告日期
  end_date                                  DATE  -- 报告期
  ann_date_key                              DATE
  eps                                       DOUBLE  -- 基本每股收益
  dt_eps                                    DOUBLE  -- 稀释每股收益
  total_revenue_ps                          DOUBLE  -- 每股营业总收入
  revenue_ps                                DOUBLE  -- 每股营业收入
  capital_rese_ps                           DOUBLE  -- 每股资本公积
  surplus_rese_ps                           DOUBLE  -- 每股盈余公积
  undist_profit_ps                          DOUBLE  -- 每股未分配利润
  extra_item                                DOUBLE  -- 非经常性损益
  profit_dedt                               DOUBLE  -- 扣除非经常性损益后的净利润（扣非净利润）
  gross_margin                              DOUBLE  -- 毛利
  current_ratio                             DOUBLE  -- 流动比率
  quick_ratio                               DOUBLE  -- 速动比率
  cash_ratio                                DOUBLE  -- 保守速动比率
  invturn_days                              DOUBLE  -- 存货周转天数
  arturn_days                               DOUBLE  -- 应收账款周转天数
  inv_turn                                  DOUBLE  -- 存货周转率
  ar_turn                                   DOUBLE  -- 应收账款周转率
  ca_turn                                   DOUBLE  -- 流动资产周转率
  fa_turn                                   DOUBLE  -- 固定资产周转率
  assets_turn                               DOUBLE  -- 总资产周转率
  op_income                                 DOUBLE  -- 经营活动净收益
  valuechange_income                        DOUBLE  -- 价值变动净收益
  interst_income                            DOUBLE  -- 利息费用
  daa                                       DOUBLE  -- 折旧与摊销
  ebit                                      DOUBLE  -- 息税前利润
  ebitda                                    DOUBLE  -- 息税折旧摊销前利润
  fcff                                      DOUBLE  -- 企业自由现金流量
  fcfe                                      DOUBLE  -- 股权自由现金流量
  current_exint                             DOUBLE  -- 无息流动负债
  noncurrent_exint                          DOUBLE  -- 无息非流动负债
  interestdebt                              DOUBLE  -- 带息债务
  netdebt                                   DOUBLE  -- 净债务
  tangible_asset                            DOUBLE  -- 有形资产
  working_capital                           DOUBLE  -- 营运资金
  networking_capital                        DOUBLE  -- 营运流动资本
  invest_capital                            DOUBLE  -- 全部投入资本
  retained_earnings                         DOUBLE  -- 留存收益
  diluted2_eps                              DOUBLE  -- 期末摊薄每股收益
  bps                                       DOUBLE  -- 每股净资产
  ocfps                                     DOUBLE  -- 每股经营活动产生的现金流量净额
  retainedps                                DOUBLE  -- 每股留存收益
  cfps                                      DOUBLE  -- 每股现金流量净额
  ebit_ps                                   DOUBLE  -- 每股息税前利润
  fcff_ps                                   DOUBLE  -- 每股企业自由现金流量
  fcfe_ps                                   DOUBLE  -- 每股股东自由现金流量
  netprofit_margin                          DOUBLE  -- 销售净利率
  grossprofit_margin                        DOUBLE  -- 销售毛利率
  cogs_of_sales                             DOUBLE  -- 销售成本率
  expense_of_sales                          DOUBLE  -- 销售期间费用率
  profit_to_gr                              DOUBLE  -- 净利润/营业总收入
  saleexp_to_gr                             DOUBLE  -- 销售费用/营业总收入
  adminexp_of_gr                            DOUBLE  -- 管理费用/营业总收入
  finaexp_of_gr                             DOUBLE  -- 财务费用/营业总收入
  impai_ttm                                 DOUBLE  -- 资产减值损失/营业总收入
  gc_of_gr                                  DOUBLE  -- 营业总成本/营业总收入
  op_of_gr                                  DOUBLE  -- 营业利润/营业总收入
  ebit_of_gr                                DOUBLE  -- 息税前利润/营业总收入
  roe                                       DOUBLE  -- 净资产收益率
  roe_waa                                   DOUBLE  -- 加权平均净资产收益率
  roe_dt                                    DOUBLE  -- 净资产收益率(扣除非经常损益)
  roa                                       DOUBLE  -- 总资产报酬率
  npta                                      DOUBLE  -- 总资产净利润
  roic                                      DOUBLE  -- 投入资本回报率
  roe_yearly                                DOUBLE  -- 年化净资产收益率
  roa2_yearly                               DOUBLE  -- 年化总资产报酬率
  roe_avg                                   DOUBLE  -- 平均净资产收益率(增发条件)
  opincome_of_ebt                           DOUBLE  -- 经营活动净收益/利润总额
  investincome_of_ebt                       DOUBLE  -- 价值变动净收益/利润总额
  n_op_profit_of_ebt                        DOUBLE  -- 营业外收支净额/利润总额
  tax_to_ebt                                DOUBLE  -- 所得税/利润总额
  dtprofit_to_profit                        DOUBLE  -- 扣除非经常损益后的净利润/净利润
  salescash_to_or                           DOUBLE  -- 销售商品提供劳务收到的现金/营业收入
  ocf_to_or                                 DOUBLE  -- 经营活动产生的现金流量净额/营业收入
  ocf_to_opincome                           DOUBLE  -- 经营活动产生的现金流量净额/经营活动净收益
  capitalized_to_da                         DOUBLE  -- 资本支出/折旧和摊销
  debt_to_assets                            DOUBLE  -- 资产负债率
  assets_to_eqt                             DOUBLE  -- 权益乘数
  dp_assets_to_eqt                          DOUBLE  -- 权益乘数(杜邦分析)
  ca_to_assets                              DOUBLE  -- 流动资产/总资产
  nca_to_assets                             DOUBLE  -- 非流动资产/总资产
  tbassets_to_totalassets                   DOUBLE  -- 有形资产/总资产
  int_to_talcap                             DOUBLE  -- 带息债务/全部投入资本
  eqt_to_talcapital                         DOUBLE  -- 归属于母公司的股东权益/全部投入资本
  currentdebt_to_debt                       DOUBLE  -- 流动负债/负债合计
  longdeb_to_debt                           DOUBLE  -- 非流动负债/负债合计
  ocf_to_shortdebt                          DOUBLE  -- 经营活动产生的现金流量净额/流动负债
  debt_to_eqt                               DOUBLE  -- 产权比率
  eqt_to_debt                               DOUBLE  -- 归属于母公司的股东权益/负债合计
  eqt_to_interestdebt                       DOUBLE  -- 归属于母公司的股东权益/带息债务
  tangibleasset_to_debt                     DOUBLE  -- 有形资产/负债合计
  tangasset_to_intdebt                      DOUBLE  -- 有形资产/带息债务
  tangibleasset_to_netdebt                  DOUBLE  -- 有形资产/净债务
  ocf_to_debt                               DOUBLE  -- 经营活动产生的现金流量净额/负债合计
  ocf_to_interestdebt                       DOUBLE  -- 经营活动产生的现金流量净额/带息债务
  ocf_to_netdebt                            DOUBLE  -- 经营活动产生的现金流量净额/净债务
  ebit_to_interest                          DOUBLE  -- 已获利息倍数(EBIT/利息费用)
  longdebt_to_workingcapital                DOUBLE  -- 长期债务与营运资金比率
  ebitda_to_debt                            DOUBLE  -- 息税折旧摊销前利润/负债合计
  turn_days                                 DOUBLE  -- 营业周期
  roa_yearly                                DOUBLE  -- 年化总资产净利率
  roa_dp                                    DOUBLE  -- 总资产净利率(杜邦分析)
  fixed_assets                              DOUBLE  -- 固定资产合计
  profit_prefin_exp                         DOUBLE  -- 扣除财务费用前营业利润
  non_op_profit                             DOUBLE  -- 非营业利润
  op_to_ebt                                 DOUBLE  -- 营业利润／利润总额
  nop_to_ebt                                DOUBLE  -- 非营业利润／利润总额
  ocf_to_profit                             DOUBLE  -- 经营活动产生的现金流量净额／营业利润
  cash_to_liqdebt                           DOUBLE  -- 货币资金／流动负债
  cash_to_liqdebt_withinterest              DOUBLE  -- 货币资金／带息流动负债
  op_to_liqdebt                             DOUBLE  -- 营业利润／流动负债
  op_to_debt                                DOUBLE  -- 营业利润／负债合计
  roic_yearly                               DOUBLE  -- 年化投入资本回报率
  total_fa_trun                             DOUBLE  -- 固定资产合计周转率
  profit_to_op                              DOUBLE  -- 利润总额／营业收入
  q_opincome                                DOUBLE  -- 经营活动单季度净收益
  q_investincome                            DOUBLE  -- 价值变动单季度净收益
  q_dtprofit                                DOUBLE  -- 扣除非经常损益后的单季度净利润
  q_eps                                     DOUBLE  -- 每股收益(单季度)
  q_netprofit_margin                        DOUBLE  -- 销售净利率(单季度)
  q_gsprofit_margin                         DOUBLE  -- 销售毛利率(单季度)
  q_exp_to_sales                            DOUBLE  -- 销售期间费用率(单季度)
  q_profit_to_gr                            DOUBLE  -- 净利润／营业总收入(单季度)
  q_saleexp_to_gr                           DOUBLE  -- 销售费用／营业总收入 (单季度)
  q_adminexp_to_gr                          DOUBLE  -- 管理费用／营业总收入 (单季度)
  q_finaexp_to_gr                           DOUBLE  -- 财务费用／营业总收入 (单季度)
  q_impair_to_gr_ttm                        DOUBLE  -- 资产减值损失／营业总收入(单季度)
  q_gc_to_gr                                DOUBLE  -- 营业总成本／营业总收入 (单季度)
  q_op_to_gr                                DOUBLE  -- 营业利润／营业总收入(单季度)
  q_roe                                     DOUBLE  -- 净资产收益率(单季度)
  q_dt_roe                                  DOUBLE  -- 净资产单季度收益率(扣除非经常损益)
  q_npta                                    DOUBLE  -- 总资产净利润(单季度)
  q_opincome_to_ebt                         DOUBLE  -- 经营活动净收益／利润总额(单季度)
  q_investincome_to_ebt                     DOUBLE  -- 价值变动净收益／利润总额(单季度)
  q_dtprofit_to_profit                      DOUBLE  -- 扣除非经常损益后的净利润／净利润(单季度)
  q_salescash_to_or                         DOUBLE  -- 销售商品提供劳务收到的现金／营业收入(单季度)
  q_ocf_to_sales                            DOUBLE  -- 经营活动产生的现金流量净额／营业收入(单季度)
  q_ocf_to_or                               DOUBLE  -- 经营活动产生的现金流量净额／经营活动净收益(单季度)
  basic_eps_yoy                             DOUBLE  -- 基本每股收益同比增长率(%)
  dt_eps_yoy                                DOUBLE  -- 稀释每股收益同比增长率(%)
  cfps_yoy                                  DOUBLE  -- 每股经营活动产生的现金流量净额同比增长率(%)
  op_yoy                                    DOUBLE  -- 营业利润同比增长率(%)
  ebt_yoy                                   DOUBLE  -- 利润总额同比增长率(%)
  netprofit_yoy                             DOUBLE  -- 归属母公司股东的净利润同比增长率(%)
  dt_netprofit_yoy                          DOUBLE  -- 归属母公司股东的净利润-扣除非经常损益同比增长率(%)
  ocf_yoy                                   DOUBLE  -- 经营活动产生的现金流量净额同比增长率(%)
  roe_yoy                                   DOUBLE  -- 净资产收益率(摊薄)同比增长率(%)
  bps_yoy                                   DOUBLE  -- 每股净资产相对年初增长率(%)
  assets_yoy                                DOUBLE  -- 资产总计相对年初增长率(%)
  eqt_yoy                                   DOUBLE  -- 归属母公司的股东权益相对年初增长率(%)
  tr_yoy                                    DOUBLE  -- 营业总收入同比增长率(%)
  or_yoy                                    DOUBLE  -- 营业收入同比增长率(%)
  q_gr_yoy                                  DOUBLE  -- 营业总收入同比增长率(%)(单季度)
  q_gr_qoq                                  DOUBLE  -- 营业总收入环比增长率(%)(单季度)
  q_sales_yoy                               DOUBLE  -- 营业收入同比增长率(%)(单季度)
  q_sales_qoq                               DOUBLE  -- 营业收入环比增长率(%)(单季度)
  q_op_yoy                                  DOUBLE  -- 营业利润同比增长率(%)(单季度)
  q_op_qoq                                  DOUBLE  -- 营业利润环比增长率(%)(单季度)
  q_profit_yoy                              DOUBLE  -- 净利润同比增长率(%)(单季度)
  q_profit_qoq                              DOUBLE  -- 净利润环比增长率(%)(单季度)
  q_netprofit_yoy                           DOUBLE  -- 归属母公司股东的净利润同比增长率(%)(单季度)
  q_netprofit_qoq                           DOUBLE  -- 归属母公司股东的净利润环比增长率(%)(单季度)
  equity_yoy                                DOUBLE  -- 净资产同比增长率
  rd_exp                                    DOUBLE  -- 研发费用
  update_flag                               VARCHAR  -- 更新标识
```

**查询示例**:
```sql
SELECT *
FROM fin_indicator
WHERE ts_code = '000001.SZ' AND end_date >= '20230101'
LIMIT 100;
```

## fin_top10_float_holders

- **描述**: 前十大流通股东持股
- **粒度**: 标的-报告期-股东
- **同步维度**: `period`
- **主键**: `ts_code, end_date, ann_date, holder_name`
- **列数**: 9

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS股票代码
  ann_date                                  DATE  -- 公告日期
  end_date                                  DATE  -- 报告期
  holder_name                               VARCHAR  -- 股东名称
  hold_amount                               DOUBLE  -- 持有数量（股）
  hold_ratio                                DOUBLE  -- 占总股本比例(%)
  hold_float_ratio                          DOUBLE  -- 占流通股本比例(%)
  hold_change                               DOUBLE  -- 持股变动
  holder_type                               VARCHAR  -- 股东类型
```

**查询示例**:
```sql
SELECT *
FROM fin_top10_float_holders
WHERE ts_code = '000001.SZ' AND end_date >= '20230101'
LIMIT 100;
```

## fin_top10_holders

- **描述**: 前十大股东持股
- **粒度**: 标的-报告期-股东
- **同步维度**: `period`
- **主键**: `ts_code, end_date, ann_date, holder_name`
- **列数**: 9

**字段列表**:
```
  ts_code                                   VARCHAR  -- TS股票代码
  ann_date                                  DATE  -- 公告日期
  end_date                                  DATE  -- 报告期
  holder_name                               VARCHAR  -- 股东名称
  hold_amount                               DOUBLE  -- 持有数量（股）
  hold_ratio                                DOUBLE  -- 占总股本比例(%)
  hold_float_ratio                          DOUBLE  -- 占流通股本比例(%)
  hold_change                               DOUBLE  -- 持股变动
  holder_type                               VARCHAR  -- 股东类型
```

**查询示例**:
```sql
SELECT *
FROM fin_top10_holders
WHERE ts_code = '000001.SZ' AND end_date >= '20230101'
LIMIT 100;
```

