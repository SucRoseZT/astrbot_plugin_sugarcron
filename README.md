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

## 任务怎么建

三条路随便挑：

- **聊天里说**：「每天早上 8 点提醒大家喝水」→ AI 调 `sugar_cron_add` 自己建，
  起因自动带你的原话；
- **WebUI 面板**：`/api/plug/astrbot_plugin_sugarcron/panel`——任务增删改查、
  立即试跑、运行记录、总配置（改完立刻生效，不用重载）；
- **指令**（管理员）：`/定时任务 list | on <id> | off <id> | del <id> | run <id>`。

## 两种内容模式

| 模式 | 到点做什么 | 适合 |
|---|---|---|
| 固定文本 | 原样发配置的内容，零模型调用 | 提醒、固定话术 |
| AI 生成 | 把「任务说明」+ 目标会话的人设喂给模型现场写 | 问早、闲聊型定时消息，听起来不是机器人群发 |

AI 生成用的模型在配置里选（留空 = 目标会话当前模型），可以单独挂个便宜模型。

## 给 AI 的工具

- `sugar_cron_add`：建任务（循环 cron / 单次 run_at），非主人只能发到自己当前会话；
- `sugar_cron_list`：列出任务（非主人只看得到自己会话相关的）；
- `sugar_cron_remove` / `sugar_cron_toggle`：删 / 启停，建任务的人或主人可以；
- `sugar_cron_run`：立刻试跑一次（仅主人）。

## 目标会话

存的是三段式 `platform_id:message_type:session_id`，例如
`aiocqhttp:GroupMessage:123456`（群）、`aiocqhttp:FriendMessage:888`（私聊）。
面板新建任务时会列出 sugarmemory 记录里最近活跃的会话，点一下就填上。

## 配置

| 分组 | 键 | 默认 | 说明 |
|---|---|---|---|
| 发送 | ai_provider_id | 空 | AI 生成用的模型，留空跟目标会话走 |
| 发送 | ai_timeout_seconds | 120 | 生成超时，超时这次不发 |
| 发送 | ai_extra_prompt | 空 | 拼在人设后的全局附加提示词 |
| 记忆联动 | memory_link | true | 打 sugarmemory 标记（label + cause） |
| 记忆联动 | memory_cause_max | 90 | 起因最长字数 |
| 给 AI 的工具 | tool_add | true | 关掉后 AI 不能建任务 |
| 给 AI 的工具 | tool_admin_only | false | 建任务只限主人 |
| 给 AI 的工具 | tool_max_per_session | 20 | 每会话任务上限 |
| 杂项 | log_keep | 200 | 运行记录保留条数 |
| 杂项 | default_timezone | 空 | 任务没填时区时的默认值 |

## 文件

```
main.py                 插件入口：调度 / 发送 / 记忆标记 / 工具 / 指令 / Web API
pages/定时任务/index.html  WebUI 面板（粉毛同款主题）
data/plugin_data/astrbot_plugin_sugarcron/jobs.json   任务
data/plugin_data/astrbot_plugin_sugarcron/runs.json   运行记录
```

调度用 AstrBot 自带的 apscheduler，无新增依赖。插件停用/重载时调度器关闭，
任务数据保留，重新启用自动恢复。
