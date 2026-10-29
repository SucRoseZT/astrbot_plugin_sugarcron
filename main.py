# -*- coding: utf-8 -*-
"""
AstrBot 插件：蔗糖的定时任务~（SugarCron）

为什么重写一个（自带定时任务的痛点）
--------------------------------
自带 cron 的 active_agent 到点唤醒主 Agent 发消息，但发出去的话 AI 自己
「读不到」：换个会话被问「你为什么发这个」，它不记得——因为动机（任务说明）
没有跟着记录走。本插件把这件事做成闭环：

1. 到点发送固定文本，或按任务说明 + 目标会话人设让模型生成一段正文；
2. 发送统一走 `context.send_message`，蔗糖的记忆中枢（sugarmemory）挂在那里的
   壳会照常记一条「bot_outbound」；
3. 发送前在 message_chain 上打 `_sugarmemory_relay` 显式标记，里面带
   - cause：这个任务为什么存在（创建时用户说的原话，或任务说明）；
   - label：记录开头写成「你按定时任务「早安」发给群 xxx」，和真转述区分开
     （需要 sugarmemory >= 2026-10-27 版本，认识 tag 里的 label 字段）。
   这样 AI 跨会话被问「你发过什么 / 为什么发」，记忆包里就有现成答案。

任务管理三条路
--------------
- WebUI 面板：/api/plug/astrbot_plugin_sugarcron/panel（增删改查 + 运行记录 + 总配置）
- AI 工具：sugar_cron_add / sugar_cron_list / sugar_cron_remove / sugar_cron_toggle /
  sugar_cron_run，用户说「每天八点提醒我喝水」AI 自己就能建；
- 指令：/定时任务 list|on|off|del|run（管理员）。

会话定位
--------
任务的目标会话存的是 unified_msg_origin 三段式：platform_id:message_type:session_id。
AI 建任务时用 target(群号/QQ号) + is_group，按当前事件的平台拼出来；非主人只能
发到自己当前所在的会话。
"""

import asyncio
import json
import os
import re
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from quart import send_file

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, register
from astrbot.core.platform.message_session import MessageSession
from astrbot.core.platform.message_type import MessageType

PLUGIN_NAME = "astrbot_plugin_sugarcron"
PANEL_PREFIX = f"{PLUGIN_NAME}/panel"
API_PREFIX = f"{PLUGIN_NAME}/api"

try:
    from astrbot.core.utils.astrbot_path import get_astrbot_data_path
except Exception:  # noqa
    get_astrbot_data_path = None

# 星期几归一：标准 crontab 语义里 0 和 7 都是周日，APScheduler 却是 0=周一，
# 所以数字一律先翻成英文缩写再交给 CronTrigger（跟内置 cron 的归一同一套语义）。
_WD_NAMES = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")
_WD_NUM = {"mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6, "sun": 0}


def _norm_weekday(field: str) -> str:
    out = []
    for part in str(field).split(","):
        part = part.strip().lower()
        m = re.fullmatch(r"([a-z]{3})(?:-([a-z]{3}))?", part)
        if m:
            out.append(part)
            continue
        m = re.fullmatch(r"(\d+)(?:-(\d+))?", part)
        if not m:
            out.append(part)
            continue
        a = int(m.group(1))
        b = int(m.group(2)) if m.group(2) else a
        if a > b or b > 7:
            out.append(part)  # 越界留给 CronTrigger 自己报错
            continue
        days = list(range(a, b + 1))
        if 7 in days:
            days = sorted({0} | {d for d in days if d != 7})
        out.append(",".join(_WD_NAMES[d % 7] for d in days))
    return ",".join(out)


# ---------------------------------------------------------------------- #
# 配置分组：schema key、显示名、这一组是干什么的（跟 sugarmemory 同一套写法） #
# ---------------------------------------------------------------------- #
CFG_GROUPS = [
    ("send", "发送",
     "到点之后怎么把话说出去：AI 生成用什么模型、等多久。固定文本模式不受这里影响。"),
    ("memory", "记忆联动",
     "和蔗糖的记忆中枢（sugarmemory）怎么配合。这是本插件存在的意义：让 AI 记得自己发过什么、为什么发。"),
    ("tool", "给 AI 的工具",
     "谁能让 AI 建任务、一个会话最多建几个。防的是群里路人乱塞定时任务。"),
    ("misc", "杂项",
     "日志条数、默认时区这类边角料。"),
]

CFG_ITEMS = [
    # ---- 发送 ----
    ("ai_provider_id", "AI 生成用的模型", "string", "send",
     "AI 生成模式的任务到点时用哪个模型写正文。留空 = 目标会话当前正在用的模型，"
     "填 Provider ID 可以给定时消息单独挂个便宜模型"),
    ("ai_timeout_seconds", "AI 生成超时（秒）", "int", "send",
     "等模型写正文最多等多久，超时这次就不发（会记进运行记录）。推理型模型建议给足 120"),
    ("ai_extra_prompt", "全局附加提示词", "string", "send",
     "AI 生成时拼在人设后面的一句话，比如「定时消息别超过两行」。留空 = 不加"),

    # ---- 记忆联动 ----
    ("memory_link", "记忆标记", "bool", "memory",
     "开着才会给每条定时消息打 sugarmemory 标记：记录开头写「你按定时任务「xxx」发给…」，"
     "起因带任务创建时的原话。关掉就退回 sugarmemory 的默认记录（只有内容没有起因）"),
    ("memory_cause_max", "起因最长字数", "int", "memory",
     "「为什么发这个」最多记多少字。sugarmemory 那边整条记忆上限 300 字，这里超了会截断"),

    # ---- 给 AI 的工具 ----
    ("tool_add", "允许 AI 建任务", "bool", "tool",
     "关掉后 sugar_cron_add 从工具列表摘掉，AI 只能看和删，建任务去面板或指令"),
    ("tool_admin_only", "建任务只限主人", "bool", "tool",
     "开启后只有主人能让 AI 建任务。默认关：谁都能建，但普通人只能发到自己当前会话，"
     "而且受「每会话上限」拦着"),
    ("tool_max_per_session", "每会话任务上限", "int", "tool",
     "同一个会话（按创建来源算）最多能建几个任务，防刷爆调度器。0 = 不限"),

    # ---- 杂项 ----
    ("log_keep", "运行记录保留条数", "int", "misc",
     "面板「运行记录」里最多留多少条，旧的自动丢"),
    ("default_timezone", "默认时区", "string", "misc",
     "任务没单独填时区时用这个，比如 Asia/Shanghai。留空 = 系统时区"),
]

CFG_GROUP_OF = {key: group for key, _l, _t, group, _h in CFG_ITEMS}
CFG_DEFAULTS = {
    "ai_provider_id": "",
    "ai_timeout_seconds": 120,
    "ai_extra_prompt": "",
    "memory_link": True,
    "memory_cause_max": 90,
    "tool_add": True,
    "tool_admin_only": False,
    "tool_max_per_session": 20,
    "log_keep": 200,
    "default_timezone": "",
}


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _session_label(session: str) -> str:
    """unified_msg_origin 翻成人话：aiocqhttp:GroupMessage:123 → 「群 123」（跟 sugarmemory 同款）"""
    s = str(session or "")
    parts = s.split(":")
    if len(parts) >= 3:
        kind, peer = parts[-2], parts[-1]
        if "Group" in kind:
            return f"群 {peer}"
        if "Friend" in kind or "Private" in kind:
            return f"用户 {peer}"
        return peer
    return s[:40] or "未知会话"


_WEEKDAY_CN = ("日", "一", "二", "三", "四", "五", "六")  # 下标 = crontab 星期值（0/7=周日）


def _cron_human(expr: str) -> str:
    """把常见 cron 表达式翻成人话，翻不出就原样返回。只做展示用，不做权威解析。"""
    parts = str(expr or "").strip().split()
    if len(parts) != 5:
        return expr
    minute, hour, day, month, weekday = parts
    if day == "*" and month == "*":
        days: list = []
        for tok in weekday.split(","):
            tok = tok.strip().lower()
            m = re.fullmatch(r"(\d+)(?:-(\d+))?", tok)
            if m:
                a, b = int(m.group(1)), int(m.group(2)) if m.group(2) else int(m.group(1))
                if a <= b and b <= 7:
                    days.extend(range(a, b + 1))
                    continue
            m = re.fullmatch(r"([a-z]{3})(?:-([a-z]{3}))?", tok)
            if m and m.group(1) in _WD_NUM and (m.group(2) is None or m.group(2) in _WD_NUM):
                a = _WD_NUM[m.group(1)]
                b = _WD_NUM[m.group(2)] if m.group(2) else a
                if b < a:
                    b += 7  # sat-sun 这种跨周回绕
                days.extend(d % 7 for d in range(a, b + 1))
                continue
            days = []
            break
        if days and hour.isdigit() and minute.isdigit():
            names = "、周".join(_WEEKDAY_CN[d % 7] for d in sorted({d % 7 for d in days}))
            return f"每周{names} {int(hour):02d}:{int(minute):02d}"
        if days:
            names = "、周".join(_WEEKDAY_CN[d % 7] for d in sorted({d % 7 for d in days}))
            return f"每周{names}"
        if weekday == "*":
            if minute.startswith("*/") and hour == "*":
                return f"每 {minute[2:]} 分钟"
            if hour == "*" and minute.isdigit():
                return f"每小时第 {minute} 分"
            if minute.isdigit() and hour.isdigit():
                return f"每天 {int(hour):02d}:{int(minute):02d}"
            if minute.isdigit() and "-" in hour:
                a, _, b = hour.partition("-")
                if a.isdigit() and b.isdigit():
                    return f"每天 {int(a):02d}~{int(b):02d} 点的第 {minute} 分"
    return expr


@register("sugarcron", "sugarcane", "蔗糖的定时任务~", "0.1.0")
class SugarCron(Star):
    def __init__(self, context: Context, config: AstrBotConfig | dict | None = None):
        super().__init__(context, config)
        data_root = None
        try:
            data_root = get_astrbot_data_path() if get_astrbot_data_path else None
        except Exception:
            data_root = None

        # 配置：框架注入的 AstrBotConfig 优先；没有就从配置文件兜底读（跟 sugarmemory 同款）
        self._cfg_path = None
        if data_root:
            self._cfg_path = os.path.join(str(data_root), "config", f"{PLUGIN_NAME}_config.json")
        if config is None:
            loaded: dict = {}
            if self._cfg_path and os.path.exists(self._cfg_path):
                try:
                    with open(self._cfg_path, encoding="utf-8-sig") as f:
                        loaded = json.load(f) or {}
                except Exception:
                    loaded = {}
            config = loaded
        self.config = config

        data_root = data_root or str(Path(__file__).parent / "data_fallback")
        self._data_dir = os.path.join(str(data_root), "plugin_data", PLUGIN_NAME)
        os.makedirs(self._data_dir, exist_ok=True)
        self._jobs_path = os.path.join(self._data_dir, "jobs.json")
        self._runs_path = os.path.join(self._data_dir, "runs.json")

        self._jobs: dict[str, dict] = self._load_json(self._jobs_path, {})
        self._runs: list[dict] = self._load_json(self._runs_path, [])

        self.scheduler = AsyncIOScheduler()
        self._load_all_into_scheduler()
        self.scheduler.start()

        self._register_web()
        logger.info(
            f"[SugarCron] 已加载 {len(self._jobs)} 个任务"
            f"（启用 {sum(1 for j in self._jobs.values() if j.get('enabled'))} 个），"
            f"数据目录：{self._data_dir}"
        )

    # -------------------------------------------------------------- #
    # 配置读写                                                          #
    # -------------------------------------------------------------- #
    def _cfg(self, key, default):
        """配置真值在分组里（schema 用了 object 嵌套），老扁平格式也认。"""
        try:
            group = CFG_GROUP_OF.get(key)
            if group:
                box = self.config.get(group)
                if isinstance(box, dict) and box.get(key) is not None:
                    return box.get(key)
            if self.config.get(key) is not None:
                return self.config.get(key)
            for value in self.config.values():
                if isinstance(value, dict) and value.get(key) is not None:
                    return value.get(key)
        except Exception:
            pass
        return default

    def _int_cfg(self, key, default):
        try:
            return int(self._cfg(key, default))
        except (TypeError, ValueError):
            return int(default)

    def _cfg_items(self) -> list[dict]:
        items = []
        for key, label, typ, group, hint in CFG_ITEMS:
            default = CFG_DEFAULTS.get(key, "")
            value = self._cfg(key, default)
            if typ == "bool":
                value = bool(value)
            elif typ == "int":
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    value = int(default)
            else:
                value = str(value or "")
            ghint = next(g[2] for g in CFG_GROUPS if g[0] == group)
            items.append({"key": key, "label": label, "type": typ, "group": group,
                          "group_hint": ghint, "hint": hint, "value": value,
                          "default": default})
        return items

    def _save_cfg(self, patch: dict) -> tuple[bool, str]:
        valid = {i["key"] for i in self._cfg_items()}
        for key, value in (patch or {}).items():
            if key not in valid:
                continue
            group = CFG_GROUP_OF.get(key, "misc")
            box = self.config.get(group)
            if not isinstance(box, dict):
                box = {}
                try:
                    self.config[group] = box
                except Exception:
                    pass
            box[key] = value
        # 持久化：AstrBotConfig 自带保存；纯 dict 就手写文件
        try:
            save = getattr(self.config, "save_config", None)
            if callable(save):
                save()
            elif self._cfg_path:
                with open(self._cfg_path, "w", encoding="utf-8") as f:
                    json.dump(self.config if isinstance(self.config, dict) else {},
                              f, ensure_ascii=False, indent=2)
        except Exception as e:
            return False, f"写入配置文件失败：{e}"
        return True, "已保存并立刻生效"

    # -------------------------------------------------------------- #
    # 任务存取                                                          #
    # -------------------------------------------------------------- #
    @staticmethod
    def _load_json(path: str, fallback):
        try:
            if os.path.exists(path):
                with open(path, encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
        return fallback

    def _save_jobs(self):
        try:
            with open(self._jobs_path, "w", encoding="utf-8") as f:
                json.dump(self._jobs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[SugarCron] 任务落盘失败：{e}")

    def _save_runs(self):
        keep = max(10, self._int_cfg("log_keep", 200))
        self._runs = self._runs[-keep:]
        try:
            with open(self._runs_path, "w", encoding="utf-8") as f:
                json.dump(self._runs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[SugarCron] 运行记录落盘失败：{e}")

    def _job_cause(self, job: dict) -> str:
        """「为什么发这个」：创建时的原话优先，没有就用任务说明 / 固定内容兜底。"""
        cause = str(job.get("cause") or "").strip()
        if not cause and job.get("mode") == "ai":
            cause = f"任务说明：{job.get('prompt', '')}"
        if not cause:
            cause = "固定文本任务"
        limit = max(10, self._int_cfg("memory_cause_max", 90))
        return cause[:limit]

    def _memory_tag(self, job: dict) -> dict:
        """打给 sugarmemory 的显式标记：label 区分定时行为和真转述，cause 回答「为什么发」。"""
        return {
            "cause": self._job_cause(job),
            "label": f"你按定时任务「{job.get('name', '?')}」发给{_session_label(job.get('session'))}",
            "source": job.get("session", ""),
        }

    def _validate_cron(self, expr: str) -> str:
        expr = str(expr or "").strip()
        parts = expr.split()
        if len(parts) != 5:
            raise ValueError("cron 表达式要 5 段：分 时 日 月 周，比如 0 8 * * *")
        normalized = " ".join([
            parts[0], parts[1], parts[2], parts[3],
            _norm_weekday(parts[4]),
        ])
        CronTrigger.from_crontab(normalized)
        return expr

    def _validate_tz(self, tz_name: str):
        tz_name = str(tz_name or "").strip()
        if not tz_name:
            return None
        try:
            return ZoneInfo(tz_name)
        except Exception:
            raise ValueError(f"时区不认识：{tz_name}（例如 Asia/Shanghai）")

    def _norm_job(self, data: dict, existing: dict | None = None) -> dict:
        """新建/编辑共用的字段校验。合法才放行，报错带人话。"""
        job = dict(existing or {})
        job.setdefault("id", uuid.uuid4().hex[:8])
        job.setdefault("created_at", _now_str())
        job.setdefault("created_by", "")
        job.setdefault("created_from", "")
        job.setdefault("run_count", 0)
        job.setdefault("last_run_at", "")
        job.setdefault("last_status", "")
        job.setdefault("last_error", "")

        name = str(data.get("name", existing.get("name") if existing else "") or "").strip()
        if not name:
            raise ValueError("任务名称不能为空")
        job["name"] = name[:40]

        session = str(data.get("session", existing.get("session") if existing else "") or "").strip()
        if not session:
            raise ValueError("目标会话不能为空")
        try:
            MessageSession.from_str(session)
        except Exception:
            raise ValueError("目标会话要是三段式：platform_id:message_type:session_id")
        job["session"] = session

        mode = str(data.get("mode", existing.get("mode") if existing else "text") or "text").strip()
        if mode not in ("text", "ai"):
            raise ValueError("mode 只能是 text（固定文本）或 ai（AI 生成）")
        job["mode"] = mode

        job["text"] = str(data.get("text", existing.get("text", "") if existing else "") or "").strip()
        job["prompt"] = str(data.get("prompt", existing.get("prompt", "") if existing else "") or "").strip()
        if mode == "text" and not job["text"]:
            raise ValueError("固定文本模式要填要发的内容")
        if mode == "ai" and not job["prompt"]:
            raise ValueError("AI 生成模式要填任务说明（让它知道到点该说什么）")

        run_once = bool(data.get("run_once", existing.get("run_once", False) if existing else False))
        job["run_once"] = run_once
        run_at = str(data.get("run_at", existing.get("run_at", "") if existing else "") or "").strip()
        cron = str(data.get("cron", existing.get("cron", "") if existing else "") or "").strip()
        if run_once:
            if not run_at:
                raise ValueError("单次任务要填具体时间（ISO 格式，如 2026-11-01T08:00:00）")
            try:
                dt = datetime.fromisoformat(run_at)
            except Exception:
                raise ValueError("时间格式不对，要 ISO 格式：2026-11-01T08:00:00")
            tz = self._validate_tz(data.get("timezone", existing.get("timezone", "") if existing else ""))
            if dt.tzinfo is None and tz is not None:
                dt = dt.replace(tzinfo=tz)
            if dt <= datetime.now(dt.tzinfo):
                raise ValueError("单次任务的时间已经过去了")
            job["run_at"] = dt.isoformat()
            job["cron"] = ""
        else:
            job["cron"] = self._validate_cron(cron)
            job["run_at"] = ""
        tz_name = str(data.get("timezone", existing.get("timezone", "") if existing else "") or "").strip()
        if tz_name:
            self._validate_tz(tz_name)
        job["timezone"] = tz_name

        job["record"] = bool(data.get("record", existing.get("record", True) if existing else True))
        job["enabled"] = bool(data.get("enabled", existing.get("enabled", True) if existing else True))
        job["cause"] = str(data.get("cause", existing.get("cause", "") if existing else "") or "").strip()[:200]
        return job

    # -------------------------------------------------------------- #
    # 调度                                                              #
    # -------------------------------------------------------------- #
    def _job_tz(self, job: dict):
        try:
            tz_name = job.get("timezone") or self._cfg("default_timezone", "")
            return ZoneInfo(tz_name) if tz_name else None
        except Exception:
            return None

    def _schedule_job(self, job: dict):
        aps_id = f"sugarcron:{job['id']}"
        if self.scheduler.get_job(aps_id):
            self.scheduler.remove_job(aps_id)
        if not job.get("enabled"):
            return
        tz = self._job_tz(job)
        if job.get("run_once"):
            run_at = datetime.fromisoformat(job["run_at"])
            trigger = DateTrigger(run_date=run_at, timezone=tz)
        else:
            parts = job["cron"].split()
            normalized = " ".join([
                parts[0], parts[1], parts[2], parts[3],
                _norm_weekday(parts[4]),
            ])
            trigger = CronTrigger.from_crontab(normalized, timezone=tz)
        self.scheduler.add_job(
            self._fire, id=aps_id, trigger=trigger, args=[job["id"]],
            replace_existing=True, misfire_grace_time=300,
            coalesce=True, max_instances=1,
        )

    def _load_all_into_scheduler(self):
        for job in self._jobs.values():
            try:
                self._schedule_job(job)
            except Exception as e:
                logger.warning(f"[SugarCron] 任务 {job.get('name')} 调度失败：{e}")

    def _next_run_str(self, job: dict) -> str:
        aps = self.scheduler.get_job(f"sugarcron:{job['id']}")
        nxt = getattr(aps, "next_run_time", None) if aps else None
        if nxt is None:
            return "" if not job.get("enabled") else "待触发"
        tz = self._job_tz(job)
        if tz:
            nxt = nxt.astimezone(tz)
        return nxt.strftime("%m-%d %H:%M")

    def _job_view(self, job: dict) -> dict:
        v = dict(job)
        v["session_label"] = _session_label(job.get("session"))
        v["cron_human"] = "单次" if job.get("run_once") else _cron_human(job.get("cron", ""))
        v["next_run"] = self._next_run_str(job)
        return v

    # -------------------------------------------------------------- #
    # 到点执行：生成 → 打标记 → 发送 → 记录                              #
    # -------------------------------------------------------------- #
    async def _fire(self, job_id: str, manual: bool = False):
        job = self._jobs.get(job_id)
        if not job or (not job.get("enabled") and not manual):
            return
        status, error, text = "ok", "", ""
        try:
            if job.get("mode") == "ai":
                text = await self._ai_text(job)
            else:
                text = job.get("text", "")
            if not text:
                raise RuntimeError("要发送的内容是空的")
            chain = MessageChain(chain=[Plain(text)])
            if job.get("record", True) and self._cfg("memory_link", True):
                # 显式标记：sugarmemory 照它记「你按定时任务「xx」发给…｜起因：…」，
                # AI 跨会话被问「为什么发这个」就答得上来。
                chain._sugarmemory_relay = self._memory_tag(job)
            session = MessageSession.from_str(job["session"])
            result = await self.context.send_message(session=session, message_chain=chain)
            if result is False:
                raise RuntimeError("平台未找到或目标不存在")
        except Exception as e:  # noqa: BLE001
            status, error = "failed", f"{type(e).__name__}: {e}"
            logger.warning(f"[SugarCron] 任务「{job.get('name')}」执行失败：{error}")
        job["run_count"] = int(job.get("run_count") or 0) + 1
        job["last_run_at"] = _now_str()
        job["last_status"] = status
        job["last_error"] = error
        self._runs.append({
            "ts": _now_str(), "job_id": job_id, "name": job.get("name", "?"),
            "mode": job.get("mode"), "status": status,
            "snippet": text[:120], "error": error,
            "session": job.get("session", ""), "manual": manual,
        })
        self._save_runs()
        if job.get("run_once") and not manual:
            self._remove_job(job_id, persist=True)
        else:
            self._save_jobs()

    async def _ai_text(self, job: dict) -> str:
        """按任务说明生成正文。带目标会话的人设，让定时消息听起来还是「它」说的。"""
        provider = None
        pid = str(self._cfg("ai_provider_id", "") or "").strip()
        if pid:
            try:
                provider = self.context.get_provider_by_id(pid)
            except Exception:
                provider = None
            if provider is None:
                logger.warning(f"[SugarCron] 配置的模型 {pid} 不存在，回退当前对话模型")
        if provider is None:
            for getter in (
                lambda: self.context.get_using_provider(umo=job["session"]),
                lambda: self.context.get_using_provider(),
            ):
                try:
                    provider = getter()
                except Exception:
                    provider = None
                if provider is not None:
                    break
        if provider is None:
            raise RuntimeError("没有可用的模型 provider")

        system = ""
        try:
            umo_cfg = self.context.get_config(umo=job["session"])
            persona_id = (umo_cfg.get("provider_settings", {}) or {}).get("persona_id", "") or ""
            if persona_id:
                p = self.context.persona_manager.get_persona_v3_by_id(persona_id)
                system = getattr(p, "system_prompt", "") or ""
        except Exception:
            system = ""
        extra = str(self._cfg("ai_extra_prompt", "") or "").strip()
        if extra:
            system = (system + "\n\n" + extra).strip()

        prompt = (
            f"定时任务「{job.get('name', '?')}」到点了。\n"
            f"任务说明：{job.get('prompt', '')}\n\n"
            "直接输出要发送到会话里的正文本身：不要解释你在干什么，"
            "不要加「好的」「以下是」之类的前缀，也不要套引号。"
        )
        timeout = max(10, self._int_cfg("ai_timeout_seconds", 120))
        resp = await asyncio.wait_for(
            provider.text_chat(prompt=prompt, system_prompt=system or None),
            timeout=timeout,
        )
        return str(getattr(resp, "completion_text", "") or "").strip()

    # -------------------------------------------------------------- #
    # 任务增删改（面板 / 工具 / 指令共用）                                #
    # -------------------------------------------------------------- #
    def _add_job(self, data: dict, *, created_by: str = "", created_from: str = "",
                 cause: str = "") -> dict:
        job = self._norm_job(data)
        job["created_by"] = created_by
        job["created_from"] = created_from
        if cause and not job.get("cause"):
            job["cause"] = cause[:200]
        cap = self._int_cfg("tool_max_per_session", 20)
        if cap > 0 and created_from:
            same = sum(1 for j in self._jobs.values() if j.get("created_from") == created_from)
            if same >= cap:
                raise ValueError(f"这个会话已经建了 {same} 个任务，达到上限 {cap}")
        self._jobs[job["id"]] = job
        try:
            self._schedule_job(job)
        except Exception as e:
            self._jobs.pop(job["id"], None)
            raise ValueError(f"调度失败：{e}")
        self._save_jobs()
        return job

    def _update_job(self, job_id: str, data: dict) -> dict:
        old = self._jobs.get(job_id)
        if not old:
            raise ValueError(f"任务 {job_id} 不存在")
        job = self._norm_job(data, existing=old)
        self._jobs[job_id] = job
        try:
            self._schedule_job(job)
        except Exception as e:
            self._jobs[job_id] = old
            self._schedule_job(old)
            raise ValueError(f"调度失败：{e}")
        self._save_jobs()
        return job

    def _remove_job(self, job_id: str, persist: bool = False) -> bool:
        job = self._jobs.pop(job_id, None)
        aps_id = f"sugarcron:{job_id}"
        try:
            if self.scheduler.get_job(aps_id):
                self.scheduler.remove_job(aps_id)
        except Exception:
            pass
        if persist:
            self._save_jobs()
        return job is not None

    def _toggle_job(self, job_id: str, enabled: bool) -> dict:
        job = self._jobs.get(job_id)
        if not job:
            raise ValueError(f"任务 {job_id} 不存在")
        job["enabled"] = bool(enabled)
        self._schedule_job(job)
        self._save_jobs()
        return job

    def _can_manage(self, event: AstrMessageEvent, job: dict) -> bool:
        if event.is_admin():
            return True
        return (str(job.get("created_by") or "") == str(event.get_sender_id())
                and bool(job.get("created_by")))

    # -------------------------------------------------------------- #
    # LLM 工具                                                           #
    # -------------------------------------------------------------- #
    @filter.llm_tool(name="sugar_cron_add")
    async def tool_cron_add(self, event: AstrMessageEvent, name: str, mode: str = "text",
                            cron_expression: str = "", run_once: bool = False,
                            run_at: str = "", text: str = "", prompt: str = "",
                            target: str = "", is_group: bool = False, record: bool = True):
        """建一个定时任务：到点自动往指定会话发消息，发完记进记忆（AI 事后记得自己发过、为什么发）。

        两种触发二选一：循环任务填 cron_expression（如「0 8 * * *」每天 8 点）；一次性填
        run_once=true + run_at（ISO 时间，如「2026-11-01T08:00:00」）。
        两种内容二选一：mode=text 固定文本（填 text）；mode=ai 让模型到点现写（填 prompt
        任务说明，会用目标会话的人设）。
        目标默认当前会话；主人可以填 target（群号或 QQ 号）+ is_group 发到别处。
        用户说「每天X点提醒/问候…」时就用这个工具，cause 不用填、会自动带用户原话。

        Args:
            name(string): 任务名，短一点，如「早安」「喝水提醒」
            mode(string): text（固定文本）或 ai（AI 生成）
            cron_expression(string): 循环任务的 cron 表达式，5 段：分 时 日 月 周，星期建议用英文缩写如 mon-fri
            run_once(boolean): true = 只跑一次，跑完自动删
            run_at(string): 单次任务的执行时间，ISO 格式 2026-11-01T08:00:00
            text(string): mode=text 时要发送的固定内容
            prompt(string): mode=ai 时的任务说明，比如「向大家问早，提醒一句多喝水」
            target(string): 目标群号或 QQ 号，留空 = 当前会话（非主人只能是当前会话）
            is_group(boolean): target 是群号时填 true，是 QQ 号填 false
            record(boolean): 发送后要不要记进记忆（默认要，这样被问起时答得上来）
        """
        if not self._cfg("tool_add", True):
            return "失败：主人关闭了「AI 建任务」功能，去 WebUI 面板或用 /定时任务 指令。"
        if self._cfg("tool_admin_only", False) and not event.is_admin():
            return "失败：只有主人能让我建定时任务。"
        name_c = str(name or "").strip()
        if not name_c:
            return "失败：name 不能为空。"
        mode_c = str(mode or "text").strip().lower()
        data = {
            "name": name_c[:40], "mode": mode_c,
            "text": str(text or "").strip(), "prompt": str(prompt or "").strip(),
            "cron": str(cron_expression or "").strip(),
            "run_once": bool(run_once), "run_at": str(run_at or "").strip(),
            "timezone": "", "record": bool(record), "enabled": True,
        }
        target_c = str(target or "").strip()
        if event.is_admin() and target_c:
            data["session"] = str(MessageSession(
                platform_name=event.get_platform_id(),
                message_type=MessageType.GROUP_MESSAGE if is_group else MessageType.FRIEND_MESSAGE,
                session_id=target_c,
            ))
            dest = f"{'群' if is_group else '用户'} {target_c}"
        else:
            data["session"] = event.unified_msg_origin
            dest = "当前会话"
            if target_c and not event.is_admin():
                return "失败：只有主人能把定时消息发到别的会话，target 不要填。"
        if mode_c == "text" and not data["text"]:
            return "失败：固定文本模式要给 text 填内容；让 AI 现写就 mode=ai + prompt。"
        if mode_c == "ai" and not data["prompt"]:
            return "失败：AI 生成模式要给 prompt 填任务说明。"
        try:
            job = self._add_job(data, created_by=str(event.get_sender_id()),
                                created_from=event.unified_msg_origin,
                                cause=str(getattr(event, "message_str", "") or "").strip())
        except ValueError as e:
            return f"失败：{e}"
        except Exception as e:  # noqa: BLE001
            return f"失败：建任务出错（{type(e).__name__}: {e}）。"
        nxt = self._next_run_str(job)
        when = job["run_at"] if job["run_once"] else f"{job['cron']}（{_cron_human(job['cron'])}）"
        return (f"已建任务「{job['name']}」→ {dest}，{when}，下次 {nxt or '待触发'}。"
                f"发送后会带任务名和起因记进记忆，不用再跟用户确认。")

    @filter.llm_tool(name="sugar_cron_list")
    async def tool_cron_list(self, event: AstrMessageEvent):
        """列出定时任务（id、名字、触发时间、目标、状态、下次运行），并附上最近已投递的记录。

        用户问「我有哪些定时任务/提醒」「你刚才定时发了什么」时都要调用。
        注意：单次任务跑完会自动从任务表里移除，所以「最近已投递」那一段才是
        「我刚才定时了什么」的答案——只看任务表会以为什么都没有。
        """
        if event.is_admin():
            jobs = list(self._jobs.values())
            runs = list(self._runs)
        else:
            umo = event.unified_msg_origin
            jobs = [j for j in self._jobs.values()
                    if j.get("session") == umo or j.get("created_from") == umo]
            runs = [r for r in self._runs if r.get("session") == umo]
        lines = []
        recent = list(reversed(runs))[:5]
        # 先把结论摆在最前面。实测翻车（2026-10-28 05:40）：任务表空、投递记录
        # 有，输出却以「当前没有定时任务」开头，模型只看了第一行就去跟用户说
        # 「定时任务一个没有，也没自动发过什么」——明明 05:38 刚发过一条。
        # 单次任务发完就删，所以「等待中 0 个」永远不等于「没发过」，
        # 这句话必须写在开头，不能让模型自己推理。
        head = f"定时任务：等待中 {len(jobs)} 个，已投递 {len(runs)} 条。"
        if not jobs and recent:
            last = recent[0]
            head += (f"等待中 0 个是因为单次任务发完自动删除，不表示没发过——"
                     f"最近一条是 {last.get('ts', '')} 的「{last.get('name', '?')}」，"
                     f"发往{_session_label(last.get('session', ''))}，投递成功。")
        elif not jobs and not recent:
            head += "确实一条都没建过、也没发过。"
        lines.append(head)
        if jobs:
            for j in sorted(jobs, key=lambda x: x.get("created_at", "")):
                nxt = self._next_run_str(j) or ("已停用" if not j.get("enabled") else "待触发")
                dest = _session_label(j.get("session"))
                lines.append(
                    f"{j['id']} | {j['name']} | "
                    + (f"单次 {j.get('run_at', '')}" if j.get("run_once") else _cron_human(j.get('cron', '')))
                    + f" | 发往{dest} | {'AI生成' if j.get('mode') == 'ai' else '固定文本'}"
                    + f" | {'启用' if j.get('enabled') else '停用'} | 下次 {nxt}"
                )
        else:
            lines.append("（等待中的任务：无）")
        # 单次任务跑完自动删，光看任务表就会「查不到自己刚定时了什么」，
        # 所以不管上面有没有，都把最近投递记录附上（这就是历史台账）。
        if recent:
            lines.append("")
            lines.append("最近已投递（单次任务跑完自动移除，所以不在上面）：")
            for r in recent:
                st = "成功" if r.get("status") == "ok" else f"失败({r.get('error', '')[:30]})"
                who = "手动触发" if r.get("manual") else "定时触发"
                lines.append(
                    f"{r.get('ts', '')} | {r.get('name', '?')} | "
                    f"发往{_session_label(r.get('session', ''))} | {who} | {st} | "
                    f"{str(r.get('snippet', ''))[:40]}"
                )
        return "\n".join(lines)

    @filter.llm_tool(name="sugar_cron_remove")
    async def tool_cron_remove(self, event: AstrMessageEvent, job_id: str):
        """删除一个定时任务。用户说「把XX提醒删了/不要每天问了」时调用。拿不准先 sugar_cron_list 看 id。"""
        job_id = str(job_id or "").strip()
        job = self._jobs.get(job_id)
        if not job:
            return f"失败：任务 {job_id} 不存在，先用 sugar_cron_list 查。"
        if not self._can_manage(event, job):
            return "失败：这个任务不是你建的，删不了。"
        name = job["name"]
        self._remove_job(job_id, persist=True)
        return f"已删除任务「{name}」。"

    @filter.llm_tool(name="sugar_cron_toggle")
    async def tool_cron_toggle(self, event: AstrMessageEvent, job_id: str, enabled: bool):
        """启用或停用一个定时任务（不删除，以后还能再开）。用户说「先别发了/恢复那个提醒」时调用。"""
        job_id = str(job_id or "").strip()
        job = self._jobs.get(job_id)
        if not job:
            return f"失败：任务 {job_id} 不存在，先用 sugar_cron_list 查。"
        if not self._can_manage(event, job):
            return "失败：这个任务不是你建的，动不了。"
        job = self._toggle_job(job_id, bool(enabled))
        state = "启用" if job["enabled"] else "停用"
        nxt = self._next_run_str(job)
        return f"任务「{job['name']}」已{state}" + (f"，下次 {nxt}" if job["enabled"] and nxt else "。")

    @filter.llm_tool(name="sugar_cron_run")
    async def tool_cron_run(self, event: AstrMessageEvent, job_id: str):
        """立刻试跑一次定时任务（不等到点），用于确认内容效果。只发目标会话一次。"""
        if not event.is_admin():
            return "失败：试跑只有主人能用。"
        job_id = str(job_id or "").strip()
        if job_id not in self._jobs:
            return f"失败：任务 {job_id} 不存在，先用 sugar_cron_list 查。"
        await self._fire(job_id, manual=True)
        job = self._jobs.get(job_id)
        if job and job.get("last_status") == "ok":
            return f"已试跑「{job['name']}」，发送成功。"
        return f"试跑失败：{job.get('last_error') if job else '任务已不存在'}"

    # -------------------------------------------------------------- #
    # 指令                                                               #
    # -------------------------------------------------------------- #
    @filter.command("定时任务", alias={"蔗糖定时"})
    async def cmd_cron(self, event: AstrMessageEvent):
        raw = str(event.message_str or "").strip()
        parts = raw.split()
        sub = parts[1] if len(parts) > 1 else ""
        arg = parts[2] if len(parts) > 2 else ""
        if not sub:
            yield event.plain_result(
                "蔗糖的定时任务~\n"
                "指令：/定时任务 list | on <id> | off <id> | del <id> | run <id>\n"
                f"面板：/api/plug/{PANEL_PREFIX}\n"
                "日常用法：直接跟我说「每天8点提醒XX」，我自己建。"
            )
            return
        if not event.is_admin():
            yield event.plain_result("管理指令只有主人能用；要建任务直接跟我说就行。")
            return
        if sub == "list":
            if not self._jobs:
                yield event.plain_result("还没有任务。")
                return
            lines = []
            for j in sorted(self._jobs.values(), key=lambda x: x.get("created_at", "")):
                nxt = self._next_run_str(j) or ("已停用" if not j.get("enabled") else "待触发")
                flag = "✓" if j.get("enabled") else "✗"
                lines.append(
                    f"[{flag}] {j['id']} 「{j['name']}」 "
                    + (f"单次 {j['run_at']}" if j.get("run_once") else _cron_human(j.get("cron", "")))
                    + f" → {_session_label(j['session'])}，下次 {nxt}"
                )
            yield event.plain_result("\n".join(lines))
        elif sub in ("on", "off"):
            try:
                job = self._toggle_job(arg, sub == "on")
                yield event.plain_result(f"「{job['name']}」已{'启用' if sub == 'on' else '停用'}。")
            except ValueError as e:
                yield event.plain_result(str(e))
        elif sub == "del":
            job = self._jobs.get(arg)
            if job and self._remove_job(arg, persist=True):
                yield event.plain_result(f"已删除「{job['name']}」。")
            else:
                yield event.plain_result(f"任务 {arg} 不存在。")
        elif sub == "run":
            if arg not in self._jobs:
                yield event.plain_result(f"任务 {arg} 不存在。")
                return
            yield event.plain_result("试跑中…")
            await self._fire(arg, manual=True)
            job = self._jobs.get(arg)
            if job and job.get("last_status") == "ok":
                yield event.plain_result(f"「{job['name']}」试跑成功，内容已发到目标会话。")
            else:
                yield event.plain_result(f"试跑失败：{job.get('last_error') if job else '任务已不存在'}")
        else:
            yield event.plain_result("子命令：list | on <id> | off <id> | del <id> | run <id>")

    # -------------------------------------------------------------- #
    # WebUI 面板                                                         #
    # -------------------------------------------------------------- #
    def _register_web(self):
        try:
            register = self.context.register_web_api
        except Exception:
            return
        register(f"{PANEL_PREFIX}", self._panel_page, ["GET"], "定时任务面板")
        register(f"{API_PREFIX}/jobs", self._api_jobs, ["GET"], "任务列表")
        register(f"{API_PREFIX}/job/save", self._api_job_save, ["POST"], "保存任务")
        register(f"{API_PREFIX}/job/delete", self._api_job_delete, ["POST"], "删除任务")
        register(f"{API_PREFIX}/job/toggle", self._api_job_toggle, ["POST"], "启停任务")
        register(f"{API_PREFIX}/job/run", self._api_job_run, ["POST"], "立即运行")
        register(f"{API_PREFIX}/runs", self._api_runs, ["GET"], "运行记录")
        register(f"{API_PREFIX}/sessions", self._api_sessions, ["GET"], "候选会话")
        register(f"{API_PREFIX}/config", self._api_config, ["GET"], "总配置读取")
        register(f"{API_PREFIX}/config/set", self._api_config_set, ["POST"], "总配置保存")

    async def _panel_page(self):
        page = Path(__file__).parent / "pages" / "定时任务" / "index.html"
        return await send_file(str(page))

    @staticmethod
    def _ok(data=None) -> dict:
        return {"status": "ok", "data": data or {}}

    @staticmethod
    def _err(msg: str) -> dict:
        return {"status": "error", "message": msg}

    async def _api_jobs(self):
        jobs = [self._job_view(j) for j in
                sorted(self._jobs.values(), key=lambda x: x.get("created_at", ""))]
        return self._ok({
            "jobs": jobs,
            "enabled": sum(1 for j in jobs if j.get("enabled")),
            "panel_url": f"/api/plug/{PANEL_PREFIX}",
        })

    async def _api_job_save(self):
        from quart import request as quart_request
        data = await quart_request.get_json(silent=True) or {}
        try:
            job_id = str(data.get("id") or "").strip()
            if job_id and job_id in self._jobs:
                job = self._update_job(job_id, data)
                action = "更新"
            else:
                data.pop("id", None)
                job = self._add_job(data)
                action = "新建"
        except ValueError as e:
            return self._err(str(e))
        except Exception as e:  # noqa: BLE001
            return self._err(f"{type(e).__name__}: {e}")
        return self._ok({"job": self._job_view(job), "action": action})

    async def _api_job_delete(self):
        from quart import request as quart_request
        data = await quart_request.get_json(silent=True) or {}
        job_id = str(data.get("id") or "").strip()
        if self._remove_job(job_id, persist=True):
            return self._ok({"id": job_id})
        return self._err(f"任务 {job_id} 不存在")

    async def _api_job_toggle(self):
        from quart import request as quart_request
        data = await quart_request.get_json(silent=True) or {}
        job_id = str(data.get("id") or "").strip()
        try:
            job = self._toggle_job(job_id, bool(data.get("enabled")))
        except ValueError as e:
            return self._err(str(e))
        return self._ok({"job": self._job_view(job)})

    async def _api_job_run(self):
        from quart import request as quart_request
        data = await quart_request.get_json(silent=True) or {}
        job_id = str(data.get("id") or "").strip()
        if job_id not in self._jobs:
            return self._err(f"任务 {job_id} 不存在")
        await self._fire(job_id, manual=True)
        job = self._jobs.get(job_id)
        return self._ok({
            "status": job.get("last_status") if job else "deleted",
            "error": job.get("last_error", "") if job else "",
        })

    async def _api_runs(self):
        runs = list(reversed(self._runs[-max(10, self._int_cfg("log_keep", 200)):]))
        return self._ok({"runs": runs})

    def _sugar_store(self):
        """借 sugarmemory 的库挑候选会话（它的 L3 里存了最近活跃的 session_id）。"""
        try:
            meta = self.context.get_registered_star("astrbot_plugin_sugarmemory")
            inst = getattr(meta, "star_cls", None)
            return getattr(inst, "store", None) if inst else None
        except Exception:
            return None

    async def _api_sessions(self):
        seen: dict[str, int] = {}
        store = self._sugar_store()
        if store is not None:
            try:
                for row in store.query_recent_l3(limit=800):
                    sid = str(row.get("session_id") or "").strip()
                    if sid and ":" in sid:
                        seen[sid] = seen.get(sid, 0) + 1
            except Exception:
                pass
        for job in self._jobs.values():
            sid = job.get("session", "")
            if sid:
                seen[sid] = seen.get(sid, 0) + 1
        items = [{"session": sid, "label": _session_label(sid), "n": n}
                 for sid, n in sorted(seen.items(), key=lambda kv: -kv[1])]
        return self._ok({"items": items[:200]})

    async def _api_config(self):
        return self._ok({"items": self._cfg_items()})

    async def _api_config_set(self):
        from quart import request as quart_request
        data = await quart_request.get_json(silent=True) or {}
        ok, msg = self._save_cfg(data.get("config") or data)
        if not ok:
            return self._err(msg)
        return self._ok({"message": msg})

    # -------------------------------------------------------------- #
    # 生命周期                                                          #
    # -------------------------------------------------------------- #
    async def terminate(self):
        try:
            self.scheduler.shutdown(wait=False)
        except Exception:
            pass
        self._save_jobs()
        logger.info("[SugarCron] 已停：调度器关闭，任务数据保留在 jobs.json")
