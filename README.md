# astrbot_plugin_user_memory · 用户分层记忆

> 以用户为单位的分层记忆：**写时整理**，回复时**直接注入**，不检索、不嵌入。

## 它解决什么

传统 `memory_recall` 类方案是「读时检索」：每轮现算 embedding、现查，慢且贵。本插件反过来：

1. 消息进来只做**追加**（O(1)，无 LLM/网络）。
2. 会话**空闲**时，用 LLM 把新事件整理成用户画像 + 一段**可直接注入的成品文本**。
3. 回复时 `on_llm_request` 直接读这段文本注入 prompt —— 零检索、零嵌入。

## 特性

- 以 `user_id` 为单位归档，时间线 + 画像 + 预编译注入文本。
- 空闲触发整理（私聊/群聊阈值分开），状态机保证"下次对话前只触发一次"。
- 自动注入走 `extra_user_content_parts`，不污染 system prompt（不破坏缓存）。
- 读时按打分选择注入内容：`重要度 × 新鲜度 × 标签命中 × 置顶`，并强制执行条数上限与衰减淘汰。
- 冷热分层：超期记忆（默认 >30 天）迁入本地向量冷层，仅在命中线索时按需检索。
- 冷层单库 + `user_id` 过滤，天然隔离；可选接入 AstrBot 已配置的**重排模型**。
- **即时记忆**：用户说“记住…”时，立刻写入 pinned 事实，本轮即可被注入（无需等空闲整理）。
- **WebUI 可视化**：插件详情页自带「用户记忆」页面，可查看每个用户的事件 / 事实 / 语境 / 冷层块。
- 全程不进请求关键路径，不拖慢回复。

## 安装

把插件目录放入 AstrBot 插件目录并重启/重载：

- 打包版：`~/.astrbot/data/plugins/`
- 源码/多实例版：`<实例>/core/data/plugins/`

## 配置

见 `_conf_schema.json`。关键项：

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `enable` | true | 总开关 |
| `idle_threshold_private` | 180 | 私聊空闲触发（秒） |
| `idle_threshold_group` | 900 | 群聊空闲触发（秒） |
| `min_consolidate_interval` | 600 | 最小整理间隔（秒） |
| `cold_migrate_days` | 30 | 超期迁入冷层阈值（天） |
| `embedding_provider_id` | 空 | 冷层嵌入模型（留空=默认） |
| `consolidate_provider_id` | 空 | 整理用 LLM（留空=当前会话模型，建议用便宜模型） |

> 冷层需要先在 AstrBot 配置好**嵌入 provider**；未配置时冷层自动跳过，热层不受影响。

## 指令

- `/usermem`：查看状态（事件数、冷层块数、是否有注入文本）。
- `/usermem 整理`：立即触发一次整理。
- `/usermem 删除`：删除自己的全部记忆。
- `/usermem 模型`：列出 AstrBot 已配置的嵌入 / 重排 provider ID，便于填入配置。

## WebUI 记忆可视化

重载插件后，在 WebUI 的插件卡片 → 详情页 → Pages 里打开「用户记忆」页面：

1. 左侧列出所有用户（事件数、冷层块数、最后活动时间），支持按 user_id 过滤。
2. 右侧展示该用户的事实（含重要度/pinned）、语境文本（inject_text）、最近事件列表。

> 数据来自插件独立 SQLite，只读展示，不经 LLM。

## 数据

存放于 `data/plugin_data/astrbot_plugin_user_memory/memory.db`（SQLite，WAL）。随 AstrBot 的 `data/` 一起备份。

## 兼容性

- 目标平台：`aiocqhttp`（OneBot v11）。
- 已在 AstrBot 4.24.5 ~ 4.28.2 核对核心 API（`extra_user_content_parts` / `TextPart` / `mark_as_temp` / `llm_generate` / 嵌入 provider）。

## 已知限制

1. 注入决策是"写时打分 + 读时规则选择"的近似，不是每轮精确相关。
2. 冷层检索为规则线索触发（可配正则），不是每轮语义检索。
3. 摘要可能失真；原始事件保留为事实源。

## 版本与发布

- 版本号唯一来源：`metadata.yaml` 的 `version` 字段（同时同步 `main.py` 的 `__version__` 与 `CHANGELOG.md`）。
- 发新版本：改完代码 → 升 `version`（并同步 `__version__`）→ 在 `CHANGELOG.md` 记一笔 → 运行 `python build.py` 得到 `astrbot_plugin_user_memory-<version>.zip` → 覆盖上传。
- `git tag vX.Y.Z` 打标签便于回溯。
- 当前版本：`v1.1.0`（可用 `/usermem` 或 WebUI 页面查看）。

## License

MIT © 2026 lousha114514
