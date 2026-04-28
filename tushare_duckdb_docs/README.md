# Docs

本目录是 Quant LLM 与开发者读取 DuckDB 数据结构的主要入口。

## 文件结构

```
docs/
├── README.md                  # 本文件
├── duckdb_data_features.md    # 数据特征：命名规范、同步维度、时间字段、状态表
├── sql/                       # 可复用 DuckDB 对象定义 SQL
│   └── create_idx_sw_l3_peers.sql  # 申万 L3 同分类股票标准视图
└── tables/                    # 表文档（从 DuckDB 实际 schema 生成）
    ├── _index.md              # 全量表/视图索引（29 个对象）
    ├── basic.md               # stk_* 基础信息（5 表）
    ├── factor.md              # stk_* 量价因子（8 表）
    ├── finance.md             # fin_* 财务报表（8 表）
    └── index.md               # idx_* 指数行情/分类/成分（7 表 + 1 视图）
```

## 阅读顺序

1. **先看** `duckdb_data_features.md` — 了解命名规范、同步维度、状态表约定
2. **再查** `tables/_index.md` — 快速定位需要的表
3. **展开** `tables/<module>.md` — 查看具体字段定义与查询示例

## Tushare 文档入口

- 总入口：https://tushare.pro/document/2
- 数据索引：https://tushare.pro/document/2?doc_id=209

## 历史归档

历史优化报告、采样脚本、量化SQL 等过程材料在 `archive/docs/` 中。
