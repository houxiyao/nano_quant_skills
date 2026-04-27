# NanoSearchMCP

NanoSearchMCP 是一个可独立部署、可被外部 Agent / Skill / MCP Client 复用的标准 MCP 服务。

当前它已作为 [nano_quant_skills](../README.md) 仓库中的数据搜索子模块发布；[2min-company-analysis](../2min-company-analysis/README.md) 只是现有下游之一，而不是唯一使用方。

## 在当前仓库中的定位

- 子模块目录：`nano-search-mcp/`
- 主要用途：为任意外部 Agent / Skill / MCP Client 提供搜索与抓取能力
- 典型搭配：作为独立 MCP 服务接入其它 Agent runtime，或作为 Python 包嵌入上层编排
- 注意：这是 MCP 服务包，不是单个 `SKILL.md` 技能目录
- 上游/下游关系：
  - 上游数据底座：`tushare_duckdb_sync_skills` / `tushare_duckdb_sync_scripts`（提供结构化指标，不直接依赖本模块）
  - 直接下游：`2min-company-analysis`（在外部证据采集阶段导入 `nano_search_mcp.tools.*`）

在当前 mono-repo 中推荐安装方式：

```bash
conda activate legonanobot
cd nano-search-mcp
pip install -e ".[dev]"
playwright install chromium
```

项目现在只保留 MCP 交付面，假设所有工具调用都可以在一次 HTTP 请求超时内完成；不再提供异步任务提交、轮询、归档和任务持久化恢复这套额外 API。

## 当前能力

服务按能力域提供 **13 个** MCP 工具（详细参数 / 返回结构见各工具 docstring）：

| 能力域 | 工具 | 说明 |
|--------|------|------|
| 通用检索 | `general_search` | 对外默认网页检索入口，适合开放式找资料；支持 `site` 约束与关键词包含/排除 |
| 通用检索 | `search` | 百炼 WebSearch 低层原始包装，主要用于兼容旧调用 |
| 通用检索 | `fetch_page` | 抓取任意 URL 正文（Markdown），带 SSRF 防护 + Playwright 渲染 |
| 通用检索 | `search_deferred_topic` | 按命名模板或自由查询的百炼 WebSearch 检索，支持 context 变量填充 |
| 定期报告 | `get_company_report` | 指定年份年报 / 半年报 / 一季报 / 三季报全文（新浪财经） |
| 临时公告 | `list_announcements` | A 股临时公告列表，支持按 `ann_type` 过滤 |
| 临时公告 | `get_announcement_text` | 单条公告正文 |
| 行业研报 | `list_industry_reports` | 券商行业研报列表，支持 `ts_code` 自动路由至申万二级行业 |
| 行业研报 | `get_report_text` | 单条研报正文 |
| 监管处罚 | `list_regulatory_penalties` | 公司违规处理 / 监管处罚记录 |
| 投资者关系 | `list_ir_meetings` | 机构调研 / 业绩说明会等 IR 活动列表 |
| 投资者关系 | `get_ir_meeting_text` | 单条 IR 纪要正文 + 参会机构名单 |
| 行业政策 | `list_industry_policies` | `*.gov.cn` 近 1 年内发布的行业政策文件（最多 5 条） |

## 外部 MCP Client 集成建议

把本项目视为独立 MCP 服务时，建议按“默认入口 + 专用工具”的方式集成，而不是从上层业务仓库复制封装逻辑。

### 工具选择建议

| 场景 | 优先工具 | 原因 |
|------|----------|------|
| 不确定该用哪个搜索工具 | `general_search` | 对外默认网页检索入口，适合开放式找资料 |
| 新闻、百科、公司资料、跨站检索 | `general_search` | 支持 `site` / `include_terms` / `exclude_terms` 约束 |
| 任意开放主题，例如“海豚的颜色” | `general_search` | 真正的通用网页搜索，不限于公司或 A 股主题 |
| 明确需要 `gov.cn` 产业政策/行业规范文件 | `list_industry_policies` | 政策专用接口，返回更适合政策取证的结果 |
| 已依赖旧版 `[{title, url, snippet}]` 原始返回结构 | `search` | 兼容旧调用，不建议作为新接入默认入口 |

### 对接外部 Skill / Agent 的建议

- 新接入方默认先尝试 `general_search`，只有在领域边界非常明确时再切到专用工具。
- 如果上层框架会根据 MCP tool description 自动选工具，请确保同步暴露本服务的 tool description 与 server instructions，不要自行重写成模糊摘要。
- 如果上层需要正文，请在检索拿到 URL 后继续调用 `fetch_page`。

### 宿主前缀命名与缓存刷新

部分 MCP 宿主不会直接显示原始工具名，而会在前面加上服务别名，例如：

- 推荐：`general_search` 显示为 `nano_search_general_search`
- 推荐：`list_industry_policies` 显示为 `nano_search_list_industry_policies`

这类前缀通常由宿主配置决定，不由本服务控制；判断工具能力时应看后缀语义，而不是只看前缀。

不建议把本服务挂载成 `company_report_search` 这类明显带有公司报告语义的别名。对于会根据工具名做路由或打分的 Agent 编排器，这会把 `general_search` 误导成“公司搜索域内的通用搜索”，削弱它作为真正开放式网页搜索入口的可发现性。

推荐使用中性前缀，例如：

1. `nano_search`
2. `web_search`
3. `open_web_search`

如果你升级到包含 `general_search` 的版本后，宿主侧仍然只显示旧的工具清单，请优先：

1. 重启 MCP 宿主 / Agent runtime
2. 刷新宿主的工具清单缓存
3. 重新拉取本服务的 `list_tools` 结果

只要宿主重新同步了工具清单，新的 `general_search` 就应对外可见。

**错误契约**：除 `search` / `get_company_report` 会在参数非法或网络彻底失败时抛异常外，
其余工具在失败时统一返回 `{source: "unavailable", error, fetch_time}` 字典。

**安全基线**：
- 所有域名构造均采用白名单校验（新浪财经 / gov.cn），防止 URL 注入
- `fetch_page` 拒绝 `file://`、loopback、RFC1918 私网、云元数据端点等 SSRF 向量
- HTTP 层均有指数退避重试 + 请求限频

## 环境要求

- Python 3.10+
- conda 环境：`legonanobot`
- Playwright Chromium 浏览器

## 安装

```bash
conda activate legonanobot
pip install -e ".[dev]"
playwright install chromium
```

如果只需要作为普通依赖安装，也可以使用：

```bash
conda activate legonanobot
pip install .
playwright install chromium
```

安装完成后，既可以把它当作命令行 MCP 服务启动，也可以在 Python 代码中直接导入包内对象。

## 启动方式

### 启动 MCP Server

```bash
conda activate legonanobot
nano-search-mcp
```

默认通过 streamable HTTP 方式监听 `http://127.0.0.1:8000/mcp`。

如果需要本地直连或被支持 stdio 的 MCP Client 直接拉起，可以切换到 stdio transport：

```bash
conda activate legonanobot
nano-search-mcp --transport stdio
```

等价写法：

```bash
conda activate legonanobot
python -m nano_search_mcp --transport stdio
```

如果你的 MCP Client、网关或反向代理有请求超时限制，需要把超时时间设到足够覆盖最慢的一次 `fetch_page` 或 `get_company_report` 调用。

## 配置

服务支持四层配置源，优先级从高到低：

| 优先级 | 来源 | 说明 |
|--------|------|------|
| 1 | CLI 参数 | `--transport`, `--host`, `--port`, `--config` |
| 2 | YAML 配置文件 | 通过 `--config` 指定或按默认路径自动发现 |
| 3 | 环境变量 | `NANO_SEARCH_MCP_{SECTION}_{FIELD}`（大写下划线） |
| 4 | 内置默认值 | 代码中的 dataclass 默认值，零配置即可启动 |

### 配置文件

生成示例配置文件：

```bash
nano-search-mcp --generate-config > config.yaml
```

配置文件自动查找顺序（未指定 `--config` 时）：

1. `./config.yaml`
2. `./nano-search-mcp.yaml`
3. `~/.config/nano-search-mcp/config.yaml`

完整配置结构见 [`config.example.yaml`](config.example.yaml)。主要配置段：

```yaml
api:
  dashscope_api_key: "sk-xxx"       # 百炼 API 密钥（推荐用环境变量）
  bailian_websearch_endpoint: "..."  # 百炼 WebSearch MCP 端点
  bailian_mcp_timeout: 30.0          # HTTP 请求超时（秒）

server:
  transport: "streamable-http"       # streamable-http 或 stdio
  host: "0.0.0.0"
  port: 8000

http:
  max_retries: 3                     # 网络请求最大重试次数
  backoff_base: 2.0                  # 指数退避基数（秒）
  request_interval: 1.0              # 相邻请求最小间隔（秒）

cache:
  cache_dir: "~/.cache/nano_search_mcp"  # 缓存根目录（支持 ~）
  list_cache_ttl: 3600                   # 列表页缓存 TTL（秒）
  detail_cache_ttl: 604800               # 详情页缓存 TTL（秒）

fetch:
  playwright_wait_ms: 2000           # 渲染后额外等待（毫秒）
  max_content_length: 500000         # 正文最大字符数

announcements:
  max_pages: 10                      # 公告列表最多翻页数

industry_reports:
  max_pages: 5

ir_meetings:
  max_pages: 20

industry_policies:
  max_per_query: 10
  top_n: 5
```

### 环境变量

两种命名格式均受支持：

**新格式**（推荐）：`NANO_SEARCH_MCP_{SECTION}_{FIELD}`

```bash
export NANO_SEARCH_MCP_API_DASHSCOPE_API_KEY="sk-xxx"
export NANO_SEARCH_MCP_HTTP_MAX_RETRIES=5
export NANO_SEARCH_MCP_CACHE_CACHE_DIR="/tmp/mcp_cache"
```

**旧格式**（向后兼容）：

```bash
export DASHSCOPE_API_KEY="sk-xxx"
export BAILIAN_WEBSEARCH_ENDPOINT="https://..."
export BAILIAN_MCP_TIMEOUT=60
```

### CLI 参数

```bash
nano-search-mcp --help

# 指定配置文件
nano-search-mcp --config /path/to/config.yaml

# 覆盖服务参数
nano-search-mcp --transport stdio
nano-search-mcp --host 127.0.0.1 --port 9000
```

### 作为 Python 包导入

项目本身是标准 Python 包，可以直接安装并导入：

```python
import nano_search_mcp
from nano_search_mcp.server import mcp
from nano_search_mcp.api import app

print(nano_search_mcp.__version__)
```

当前更适合将其作为“可安装的 MCP 服务包”复用：

- `nano_search_mcp.server:mcp`：标准 MCP 服务对象
- `nano_search_mcp.api:app`：streamable HTTP ASGI app
- `nano_search_mcp.__main__:main`：命令行入口

如果你的目标是把它当作稳定 SDK 直接调用内部抓取函数，建议优先通过 MCP 工具接口或 `mcp` / `app` 进行集成；`tools/` 下模块目前主要按服务注册组织，而不是按独立 SDK API 设计。

如果你的上层框架会基于 MCP 工具描述自行决定调用路径，请优先让它感知本服务原生暴露的 `general_search` / `list_industry_policies` 描述，而不是在上层再造一层不完整的路由摘要。

## MCP 调用示例

### 获取指定年份定期报告

调用 `get_company_report` 时，调用方必须显式提供 `year`；`report_type` 默认为 `annual`（年报），`semi` 表示半年报/中报，`q1` 表示一季报，`q3` 表示三季报，也支持这些中文别名。不支持“最近一期”“最新报告”这类含糊输入。

示例参数：

```python
get_company_report(
  stockid="300750",
  year=2023,
)

get_company_report(
  stockid="300750",
  year=2023,
  report_type="q1",
)
```

返回内容为 `300750` 在 `2023` 年的中文完整版年报、半年报、一季报或三季报正文；如果该年份不存在对应报告，工具会明确报错。

## 典型调用流程

对定期报告场景，上层 Agent 一般会按下面的 MCP 调用顺序工作：

1. 在调用侧先明确目标年份，例如 `2023`、`2024`、`2025`。
2. 如果还没有报告列表页 URL，先调用 `search` 搜索，例如：`新浪财经 宁德时代 年报 site:sina.com.cn`。
3. 用 `fetch_page` 抓取报告列表页正文，提取每个条目的详情页或 PDF 链接。
4. 如果目标是“直接获取某家公司某一年的定期报告正文”，优先调用 `get_company_report(stockid=..., year=..., report_type=...)`。
5. 针对后续详情页或 PDF 链接继续调用 `fetch_page`，或进入 PDF 下载与解析流程。
6. 将解析结果与本地链接仓库、缓存仓库关联保存。

## 测试

当前覆盖 MCP 工具层和关键抓取路径的单测，覆盖范围：

- MCP 服务启动与工具注册（契约断言）
- 各工具的参数校验、URL 构造、解析、缓存、错误路径
- `fetch_page` 的 SSRF 防护专项测试
- 新浪定期报告 / 公告 / 行业研报 / IR 纪要 / 监管处罚 / 行业政策等数据源接入

运行测试：

```bash
conda activate legonanobot
pytest
```

测试文件位于 `tests/` 目录。

## 目录结构

```text
config.example.yaml          示例配置文件（可复制为 config.yaml 直接使用）
src/nano_search_mcp/
  config.py       集中式配置管理（四层优先级合并）
  api.py          标准 MCP HTTP app 兼容入口
  server.py       MCP Server 入口（注册 13 个工具 + CLI 解析）
  tools/
    search.py                百炼 WebSearch 搜索
    fetch.py                 页面抓取（含 SSRF 防护）
    deferred_search.py       模板化检索
    sina_reports.py          定期报告
    announcements.py         临时公告
    industry_reports.py      行业研报
    ir_meetings.py           投资者关系活动
    regulatory_penalties.py  监管处罚
    industry_policies.py     行业政策（gov.cn）
tests/
  test_server.py           MCP 服务入口与工具注册契约
  test_fetch.py            SSRF 防护专项
  test_*.py                各数据源单测
```