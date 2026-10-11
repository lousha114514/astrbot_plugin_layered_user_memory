# astrbot_plugin_layered_user_memory · 用户分层记忆

> 以用户为单位的分层记忆：**写时整理、读时直取**——回复时直接注入画像，不做检索、不调嵌入。
>
> 适用平台：aiocqhttp（OneBot v11）+ NapCat。

## 思路：为什么不用「检索式记忆」

传统 `memory_recall` 类方案是「读时检索」：每轮现算 embedding、现查、可能还要 rerank，慢且贵。本插件反过来：

1. 消息进来只做**追加**（O(1)，无 LLM、无网络）。
2. 会话**空闲**时，用 LLM 把新事件整理成用户画像 + 一段**可直接注入的成品文本**。
3. 回复时在 `on_llm_request` 直接读这段文本注入 prompt —— 零检索、零嵌入。
4. 超期记忆迁入**冷层**，只在命中线索时按需检索（可选重排）。

全程不进请求关键路径，不拖慢回复。

## 特性

- 以 `user_id` 归档：时间线（events）+ 画像（facts/narrative）+ 预编译注入文本（inject_cache）。
- **空闲触发整理**：私聊/群聊阈值分开，状态机保证「下次对话前只触发一次」，带最小间隔与每小时上限。
- **自动注入**：走 `extra_user_content_parts`，不写 system prompt（不破坏提示词缓存）。
- **读时打分选择**：`重要度 × 新鲜度 × 标签命中 × 置顶`，并强制条数上限与衰减淘汰。
- **冷热分层**：超期记忆迁入本地向量冷层，按 user_id 过滤隔离；可选接入 AstrBot 已配置的**重排模型**。
- **即时记忆**：用户说「记住…」时立刻写入 pinned 事实，本轮即可注入。
- **角色锚定整理**：整理 prompt 注入「本人 + 本机器人」身份，禁止编造角色/机器人编号；机器人发言作为上下文一并提供。
- **溯源 + 幻觉复核**：每条事实记录来源事件 id；无有效来源的标记为「未验证」，默认不注入（可配置放开）。
- **权限体系**：普通管理员 / 超管 / 受保护用户。
- **隐私设置**：用户可按「会话」或「整体」开关自己的记忆记录；超管可按群关闭记录。
- **WebUI 可视化**：插件详情页自带「用户记忆」页面，可增删改查、搜索、选模型、导出导入、重建库。
- **稳定性**：单连接单写者 + 事务 + WAL；数据库损坏自愈；每日自动备份。

## 安装

### 方式一：WebUI 上传（推荐）

1. 从 Releases 或仓库根目录下载 `astrbot_plugin_layered_user_memory-<版本>.zip`。
2. WebUI → 「插件」→ 安装插件 → 上传该 zip。

### 方式二：手动放置

把插件目录放入 AstrBot 插件目录并重启/重载：

- 打包版：`~/.astrbot/data/plugins/`
- 源码/多实例版：`<实例>/core/data/plugins/`

> 说明：插件**数据目录固定为 `astrbot_plugin_user_memory`**（见下方「数据」），即使插件改名也不会丢数据。

## 指令（`/usermem`，别名 `/用户记忆`）

**所有人**
- `/usermem`：查看自己的状态（事件数、冷层块数、语境、当前记录设置）。
- `/usermem 查看`：查看自己的完整记忆（事实 / 语境 / 最近事件）。
- `/usermem 整理`：立即触发一次整理。
- `/usermem 删除`：删除自己的全部记忆。
- `/usermem 模型`：列出 AstrBot 已配置的嵌入 / 重排 provider ID。
- `/usermem mode 全局 | 关闭当前会话 | 开启当前会话 | 关闭`：设置自己的记忆记录开关（见「隐私设置」）。

**管理员（`admin_ids` 或 AstrBot 全局管理员）**
- `/usermem 查看 <user_id>`：查看他人记忆（受保护用户除外）。
- `/usermem 列表`：用户列表（隐藏受保护用户）。
- `/usermem 清除 <user_id>`：清除某用户全部记忆。
- `/usermem mode <状态> <user_id>`：设置他人的记录开关。
- LLM 工具 `edit_memory`：用自然语言让 bot 修改/删除记忆（先预览、再执行；删除会写墓碑防复活）。

**超管（`superadmin_ids`）**
- 拥有以上全部权限，且对**受保护用户**同样有效。
- `/usermem 保护 <user_id>` / `/usermem 取消保护 <user_id>` / `/usermem 保护列表`。
- `/usermem group 开启 | 关闭`：切换本群是否记录记忆。
- `/usermem 重建`：重建数据库（旧库改名 `.bak.<时间戳>`）。

## 隐私设置

**按用户（会话维度）**
- `mode 全局`：所有会话都记录。
- `mode 关闭当前会话`：不记录「发命令所在的那个会话」（群聊关该群、私聊关私聊）。
- `mode 开启当前会话`：重新记录当前会话。
- `mode 关闭`：不记录该用户任何记忆。
- 本人可设自己；管理员/超管可带 `user_id` 设他人（受保护用户仅超管）。

**按群（超管 / 配置）**
- `/usermem group 关闭`、`/usermem group 开启`。
- 或在插件配置项 `disabled_groups`（群号列表）里维护。

> 关闭后**不再记录**对应记忆；已记录的内容不会自动删除，需用「删除 / 清除」处理。

## 权限体系

| 角色 | 配置项 | 能力 |
| --- | --- | --- |
| 普通用户 | — | 看自己、管理自己的隐私开关 |
| 管理员 | `admin_ids`（+ AstrBot 全局管理员） | 看/清除他人（受保护除外）、列表、`edit_memory` |
| 超管 | `superadmin_ids` | 全部权限 + 受保护用户 + 保护名单 + 群开关 + 重建 |

受保护用户：`protected_ids`（列表，普通管理员不可查看）。

## WebUI 记忆可视化

重载插件后，打开插件卡片 → 详情页 → Pages → 「用户记忆」：

1. 顶栏：搜索（按 user_id / 事实 / 事件内容）、筛选（全部 / 有冷层 / 受保护）、主题切换（浅 / 深 / 跟随系统）。
2. 左栏：用户列表（头像、事件数、冷层数、受保护锁、最后活动），支持删除。
3. 右栏：用户卡 → 事实（重要度条、置顶、编辑/删除、**溯源**）→ 语境（inject_text，带字数/保存状态）→ 事件时间线 → 冷层。
4. 设置抽屉：模型选择（嵌入 / 重排）、导出/导入记忆（JSON）、重建数据库。

## 配置项（摘要）

完整定义见 `_conf_schema.json`。

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `enable` | true | 总开关 |
| `idle_threshold_private` / `_group` | 180 / 900 | 空闲触发整理（秒） |
| `min_consolidate_interval` / `max_consolidate_per_hour` | 600 / 6 | 防抖与频率上限 |
| `event_min_importance` / `min_event_chars` / `strip_mentions` | 5 / 5 / true | 记录门槛与清洗 |
| `inject_max_facts` / `inject_max_timeline` / `inject_text_max_chars` | 12 / 8 / 1200 | 注入预算 |
| `decay_days` / `decay_importance_floor` / `timeline_min_importance` | 90 / 50 / 10 | 衰减与淘汰 |
| `score_w_importance/_freshness/_tag/_pinned` | 1.0 / .8 / .6 / .5 | 读时打分权重 |
| `inject_unverified` | false | 是否注入未验证事实 |
| `cold_enable` / `cold_migrate_days` / `cold_retrieve_top_k` / `cold_use_rerank` | true / 30 / 5 / true | 冷层 |
| `embedding_provider_id` / `rerank_provider_id` | 空 | 冷层模型（留空=默认） |
| `consolidate_provider_id` / `consolidate_max_events` | 空 / 40 | 整理用 LLM |
| `admin_ids` / `superadmin_ids` / `protected_ids` / `disabled_groups` | [] | 权限与群开关 |
| `backup_on_start` / `backup_keep_days` | true / 7 | 自动备份 |

> 冷层需要先在 AstrBot 配好**嵌入 provider**；未配置时冷层自动跳过，热层不受影响。

## 数据、备份与恢复

- 数据库：`data/plugin_data/astrbot_plugin_user_memory/memory.db`（SQLite + WAL）。
- 自动备份：每天首次加载写入 `backups/memory-YYYYMMDD.db`（`VACUUM INTO`），保留最近 N 份。
- WebUI：可整库**导出/导入** JSON，也可**按用户导出**；可一键**重建数据库**（旧库改名 `.bak.<时间戳>`）。
- 损坏自愈：启动或写入检测到损坏时，自动把旧库改名 `.corrupt.<时间戳>` 并重建空库。

## 兼容性

- 目标平台：`aiocqhttp`（OneBot v11）。
- 已在 AstrBot **4.24.5 ~ 4.28.2** 核对核心 API（`extra_user_content_parts` / `TextPart` / `mark_as_temp` / `llm_generate` / 嵌入与重排 provider）。
- Web API 双栈兼容：新版 `astrbot.api.web` 与 4.24.5 的 `quart`。

## 已知限制

1. 注入是「写时打分 + 读时规则选择」的近似，不是每轮精确相关。
2. 冷层检索默认由线索正则触发（可配），不是每轮语义检索。
3. 摘要可能失真；原始事件永远保留为事实源，且有溯源与「未验证」标记兜底。
4. 命名冲突提示：GitHub 上存在同名插件（现已改名为本仓库名以区分）。

## 版本与发布

- 版本号唯一来源：`metadata.yaml` 的 `version`（同步 `main.py` 的 `__version__` 与 `CHANGELOG.md`）。
- 发版：改代码 → 升 `version` → 记 `CHANGELOG.md` → `python build.py` 出包 → 提交推送。
- 标签：`git tag vX.Y.Z`。
- 当前版本：**v1.9.0**（可用 `/usermem` 或 WebUI 页面查看）。

```bash
# 发布新版本
cd /d D:\lobster\astrbot_plugin_user_memory
python build.py
git add -A
git commit -m "feat(vX.Y.Z): ..."
git tag vX.Y.Z
git push && git push --tags
```

## License

MIT © 2026 lousha114514
