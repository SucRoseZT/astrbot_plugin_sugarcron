# 蔗糖的定时任务~ (astrbot_plugin_sugarcron)

重写 AstraBot 自带的定时任务。核心动机只有一个：**AI 发出去的定时消息，AI 自己要记得**。

## 为什么重写

自带的定时任务到点把消息发出去就结束了。发出去的话不走「回复当前事件」的链路，AI 的
对话里没有它，记忆里也没有它——你换个会话问「你为什么发这个」，它只能瞎猜，猜不着就
否认自己发过。跟转述功能没做记忆联动时是同一个病：**它以为自己的话是假的**。

修法跟 sugarmemory 给转述做的一样，三步：

1. 发送统一走 `context.send_message`，sugarmemory 挂在那里的壳照常记一条
   `bot_outbound`（说什么）；
2. 发送前在 message_chain 上打 `_sugarmemory_relay` 显式标记，`cause` 带任务创建时
   用户说的原话（为什么发），`label` 把记录开头写成
   `[你按定时任务「早安」发给群 xxx]`——跟真转述区分开，不会把定时行为当成
   「替用户带话」；
3. sugarmemory 注入时这段在「你在其他会话亲口发过的话」里，跨会话可见，
   AI 被问「你发过什么 / 为什么发」直接有答案。

> 需要 sugarmemory 在跑（2026-10-27 之后的版本认识标记里的 `label` 字段）。
> 没装也能用：任务照跑，只是 AI 事后记不住。sugarmemory 那边的
> `record_outbound_other` 开不开都行——带了标记的一定记，不受那个开关影响。
>
> 注：分段插件若自己 `context.send_message` 直发拆出来的段（不经 `event.send`），
> 那些段会按 sugarmemory 默认规则记录，没有 label/cause；本插件能控制的最后一段
> 仍带完整标记。
>
> 合成事件还做了两件防污染的事：sender 写成「系统唤醒」、可见文本带 `[系统唤醒]`
> 前缀（模型实际收到的是干净的提示词），避免把系统提示词记成真人发言；
> `message_id` 留空，避免分段插件给第一段插一个指向不存在消息的「引用回复」。

## 任务怎么建

三条路随便挑：

- **聊天里说**：「每天早上 8 点提醒大家喝水」→ AI 调 `sugar_cron_add` 自己建，
  起因自动带你的原话；说「问候 / 问早 / 跟某人聊两句」会优先建真实的分段回复；
- **WebUI 面板**：`/api/plug/astrbot_plugin_sugarcron/panel`——任务增删改查、
  立即试跑、运行记录、总配置（改完立刻生效，不用重载插件）；
- **指令**（管理员）：`/定时任务 list | on <id> | off <id> | del <id> | run <id>`。

## 三种内容模式

| 模式 | 到点做什么 | 适合 |
|---|---|---|
| 固定文本 | 原样发配置的内容，零模型调用，**只发一条** | 提醒、固定话术 |
| 一坨的回复 | 把「任务说明」+ 目标会话的人设喂给模型现场写，**只发一条** | 问早、一句话通知 |
| **真实的分段回复** | 只喂**一条提示词**给模型，让它按正常对话链路回复；回复交给分段插件拆成**一连串**自然消息 | 真·早安问候、找人聊天、连着说几句 |

旧模式（固定文本 / 一坨的回复）一行没删，原逻辑照跑。

### 真实的分段回复是怎么「连成一串」的

到点生成一句提示词，例如：

```
已触发定时任务，内容为给某某用户发送早安问好
```

模板可在总配置里改（`{content}` / `{name}` / `{target}` / `{session}` / `{time}` / `{date}`）。
这条提示词作为**一次模型输入**，模型用人设 + 记忆按平时说话的方式回一整段；
附加风格提示词会要求它写成 2~5 句口语短句（句间用标点/换行分开）。这段风格词
挂在**本轮用户内容**上（`extra_user_content_parts` + `mark_as_temp()`），不写
`system_prompt`、也不写会话历史：`system_prompt` 一旦被这 150 字占住第一行，框架
随后追上去的 5k 人设/记忆全部错位，provider 从第一个 64-token 块起就整段不命中。

回复**不在这里被硬拆**：它被做成标准 `LLM_RESULT` 结果链，走正常的发送前钩子，
由你装的**任意分段插件**把它拆成一连串短消息发出去。所以「兼容任何分段插件」
不依赖本插件自己实现分段逻辑，也不挑分段插件是哪一家。

### 两条触发链路（可切换）

| 链路 | 做法 | 特点 |
|---|---|---|
| `pipeline`（默认） | 合成一条 `CronMessageEvent` 推进 `Context.get_event_queue()`，走完整消息管道 | 人设、记忆注入、唤醒检查、分段、表情、TTS、所有插件钩子照常参与，跟真人发消息一模一样；也会被其它插件的唤醒/白名单/注意力规则影响 |
| `agent` | 直接 `build_main_agent` 跑一次，再手动过 `on_llm_request` + `on_decorating_result` | 跳过管道前置阶段，不受其它插件拦截；分段仍走发送前钩子 |

总配置「真实的分段回复 → 触发链路」选全局，每个任务也能单独覆盖（留空 = 跟随全局）。
`pipeline` 需要目标会话能被唤醒：插件会按该会话配置自动补唤醒前缀（阶段会剥掉，
模型看到的是干净提示词），没配前缀的群聊改为补一个 @ 自己。
`agent` 链路跑完会把这次触发和回复写回目标会话历史。

## 两个总开关

「总配置 → 模式开关」里各一个 bool：

| 键 | 管什么 | 关掉之后 |
|---|---|---|
| `legacy_enabled` | 固定文本 + 一坨的回复 | 任务保留、面板可见，到点记一条「已跳过」，不能新建/改成该模式 |
| `greeting_enabled` | 真实的分段回复 | 同上 |

重新打开立刻恢复，**不用重建任务**。编辑一个已关模式的老任务的其它字段仍然允许。

## 给 AI 的工具

- `sugar_cron_add`：建任务（循环 cron / 单次 run_at），`mode=text|ai|agent`；
  非主人只能发到自己当前会话；`agent` 另有 `target_name` / `route` 两个可选参数；
- `sugar_cron_list`：列出任务（含总开关状态、链路），并附上最近已投递的记录；
- `sugar_cron_remove` / `sugar_cron_toggle`：删 / 启停，建任务的人或主人可以；
- `sugar_cron_run`：立刻试跑一次（仅主人）。

## 目标会话

存的是三段式 `platform_id:message_type:session_id`，例如
`aiocqhttp:GroupMessage:123456`（群）、`aiocqhttp:FriendMessage:888`（私聊）。
面板新建任务时会列出 sugarmemory 记录里最近活跃的会话，点一下就填上。

## 配置

面板「总配置」是**自动保存**（照 sugarmemory 面板的交互）：开关点一下就存，文本 / 数字失焦或回车存，长文本框停手约 1 秒存；只提交真正改了的字段，每行有「待存 / 已存 / 未存」状态，失败会出现「重试保存」。改完立刻生效，不用点保存也不用重载插件。左侧「_conf_schema.json」只是面板打不开时的兜底配置页，那边改完才需要保存并重载插件。

| 分组 | 键 | 默认 | 说明 |
|---|---|---|---|
| 模式开关 | legacy_enabled | true | 旧的单条定时（固定文本 / 一坨的回复）总开关 |
| 模式开关 | greeting_enabled | true | 新的真实的分段回复总开关 |
| 真实的分段回复 | greeting_route | pipeline | `pipeline` 完整消息管道 / `agent` 直接唤醒主 Agent |
| 真实的分段回复 | greeting_trigger_template | `已触发定时任务，内容为{content}` | 提示词模板，支持 6 个占位符 |
| 真实的分段回复 | greeting_system_prompt | 内置口语短句风格 | 附加风格提示词，挂在 user 内容上（不写 system、不进历史），让回复便于分段 |
| 真实的分段回复 | greeting_timeout_seconds | 180 | agent 链路生成超时 |
| 真实的分段回复 | greeting_max_steps | 20 | agent 链路工具循环上限 |
| 真实的分段回复 | greeting_llm_hooks | true | agent 链路手动跑 on_llm_request（记忆注入） |
| 真实的分段回复 | greeting_result_hooks | true | agent 链路手动跑 on_decorating_result（分段兼容） |
| 真实的分段回复 | greeting_save_history | true | agent 链路写回会话历史 |
| 发送 | ai_provider_id | 空 | 一坨的回复用的模型，留空跟目标会话走 |
| 发送 | ai_timeout_seconds | 120 | 生成超时，超时这次不发 |
| 发送 | ai_extra_prompt | 空 | 拼在人设后的全局附加提示词（只作用于「一坨的回复」） |
| 记忆联动 | memory_link | true | 打 sugarmemory 标记（label + cause） |
| 记忆联动 | memory_cause_max | 90 | 起因最长字数 |
| 给 AI 的工具 | tool_add | true | 关掉后 AI 不能建任务 |
| 给 AI 的工具 | tool_admin_only | false | 建任务只限主人 |
| 给 AI 的工具 | tool_max_per_session | 20 | 每会话任务上限 |
| 杂项 | log_keep | 200 | 运行记录保留条数 |
| 杂项 | default_timezone | 空 | 任务没填时区时的默认值 |

## 文件

```
main.py                 插件入口：调度 / 三种模式 / 记忆标记 / 工具 / 指令 / Web API
pages/定时任务/index.html  WebUI 面板（粉毛同款主题）
_conf_schema.json       WebUI 兜底配置页的 schema
data/plugin_data/astrbot_plugin_sugarcron/jobs.json   任务
data/plugin_data/astrbot_plugin_sugarcron/runs.json   运行记录
```

调度用 AstrBot 自带的 apscheduler，无新增依赖。插件停用/重载时调度器关闭，
任务数据保留，重新启用自动恢复。
