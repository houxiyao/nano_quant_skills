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

通过如下自然语言命令，让大模型分析并安装 Skill：

> 请阅读 `nano_quant_skills/tushare-duckdb-sync/SKILL.md`，将其安装为你可调用的 Skill。

**各工具安装方式：**

| AI 工具 | 推荐安装方式 | 说明 |
|---|---|---|
| **Claude Code** | 安装为 Command（`/slash` 命令） | 将 SKILL.md 内容适配到 `.claude/commands/` 目录，之后通过 `/tushare-sync` 直接调用 |
| **GitHub Copilot** | 配置为 Skill | 将目录放入 `.github/skills/`，Copilot Chat 自动识别 |
| **Qoder** | 配置为 Prompt | 将 SKILL.md 适配到 `.qoder/prompts/` 目录 |
| **其它 Agent / OpenClaw / Nanobot** | 直接引用 | 在对话中指向 `SKILL.md` 路径即可 |

最稳妥的安装方案为复制如下命令给 AI Agent：
```
1. 阅读 `https://github.com/shadowinlife/nano_quant_skills` 项目，理解其文件结构和设计原则。
2. 将仓库中所有的SKILLS安装为你可调用的 Skills 或者 Commands，确保用户能使用它们
3. 如果本地已经存在 `tushare-duckdb-sync` 目录，比较内容差异后进行升级,**必须保留本地修改备份以便恢复**。
```

## 许可

脚本可自由复制使用。数据来源须遵守各数据提供方的使用条款。
