# Nano Quant Skills

面向 AI Agent 的量化分析技能集合。每个 Skill 均为独立目录，包含工作流文档、可执行脚本和模板，可被 GitHub Copilot、Claude Code、Qoder 等 AI 编程工具直接调用。

## Skills 列表

| Skill | 说明 | 脚本 |
|---|---|---|
| [tushare-duckdb-sync](tushare-duckdb-sync/) | 从 Tushare Pro 同步数据到本地 DuckDB，支持全量/增量模式、质检、元数据文档生成 | `sync_table.py` · `check_quality.py` |

## 目录结构

```
nano_quant_skills/
├── README.md                 ← 本文件
├── tushare-duckdb-sync/      ← Skill：Tushare → DuckDB 数据同步
│   ├── SKILL.md              ← Agent 工作流文档
│   ├── README.md             ← 人类可读的快速上手指南
│   ├── scripts/              ← 自包含可执行脚本
│   ├── templates/            ← 配置与文档模板
│   └── examples/             ← 完整示例
└── (更多 skill 待添加)
```

## 设计原则

1. **自包含**：每个 Skill 的脚本不依赖本仓库其它模块，复制即用。
2. **双入口**：`SKILL.md` 面向 AI Agent 编排工作流；`README.md` 面向人类开发者。
3. **三项资产**：数据同步类 Skill 要求每次执行产出数据、元数据文档、运维记录三项资产。
4. **可扩展**：新增 Skill 只需创建新目录并遵循相同结构。

## 快速开始

### 环境要求

- Python 3.10+
- 各 Skill 的依赖见其 `README.md`

### 作为 AI Skill 使用

将本仓库克隆到项目中，配置 AI 工具的 Skill 路径指向对应 `SKILL.md` 即可。

### 作为独立脚本使用

```bash
cd tushare-duckdb-sync/scripts
pip install tushare duckdb pandas loguru
export TUSHARE_TOKEN=你的token
python sync_table.py --endpoint daily --duckdb-path ./ashare.duckdb --target-table stk_daily --mode append --dimension-type trade_date --start-date 20240101 --sync-all
```

## 许可

脚本可自由复制使用。数据来源须遵守各数据提供方的使用条款。
