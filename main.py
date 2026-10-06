"""astrbot_plugin_user_memory — 以用户为单位的分层记忆（写时整理 + 读时打分）

设计要点（详见设计文档）：
- 热层：消息只做「追加事件」(O(1))，空闲触发时用 LLM 整理成结构化 facts / narrative / 注入语境。
- 注入：on_llm_request 读取结构化 facts + 最近事件，**读时按打分规则选择**（无 LLM），
  再拼接 LLM 语境，塞进 extra_user_content_parts。不检索、不嵌入。
- 冷层：超期事件分块 + 嵌入存本地向量表；命中线索时按 user_id 过滤检索，可选重排。
- 全程不进请求关键路径：追加/整理/迁移都是轻量或后台。

嵌入 / 重排均复用 AstrBot 已配置的 provider（可在插件配置里选择）。
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import sqlite3
import threading
import time

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star

try:
    from astrbot.core.agent.message import TextPart
except Exception:  # pragma: no cover
    TextPart = None  # type: ignore

try:
    from astrbot.core.utils.astrbot_path import get_astrbot_plugin_data_path
except Exception:  # pragma: no cover
    get_astrbot_plugin_data_path = None  # type: ignore

# Web API 兼容：新版用 astrbot.api.web，旧版（如 4.24.5）回退到 quart。
try:
    from astrbot.api.web import error_response, json_response, request  # type: ignore

except Exception:  # pragma: no cover
    request = None  # type: ignore
    json_response = None  # type: ignore
    error_response = None  # type: ignore
    try:
        from quart import jsonify as _jsonify
        from quart import request as _qrequest

        class _QuartReqAdapter:
            """把 quart 的 request 适配成 .query / await .json() 的接口。"""

            def __init__(self, raw):
                self._raw = raw

            @property
            def query(self):
                return self._raw.args

            async def json(self, default=None):
                try:
                    data = await self._raw.get_json()
                except Exception:
                    data = None
                if data is None:
                    return default if default is not None else {}
                return data

            def __getattr__(self, name):
                return getattr(self._raw, name)

        request = _QuartReqAdapter(_qrequest)  # type: ignore

        def json_response(data=None, *args, **kwargs):  # type: ignore
            return _jsonify(data if data is not None else {})

        def error_response(message="error", status_code=400, *args, **kwargs):  # type: ignore
            resp = _jsonify({"status": "error", "message": message})
            try:
                resp.status_code = status_code
            except Exception:
                pass
            return resp

    except Exception:
        pass

PLUGIN_NAME = "astrbot_plugin_user_memory"
__version__ = "1.1.0"

DEFAULTS = {
    "enable": True,
    # 空闲触发
    "idle_threshold_private": 180,
    "idle_threshold_group": 900,
    "min_consolidate_interval": 600,
    "max_consolidate_per_hour": 6,
    # 记录
    "event_min_importance": 5,
    # 读时选择（注入上限 + 衰减）
    "inject_max_facts": 12,
    "inject_max_timeline": 8,
    "decay_days": 90,
    "decay_importance_floor": 50,
    "timeline_min_importance": 10,
    "inject_text_max_chars": 1200,
    # 打分权重
    "score_w_importance": 1.0,
    "score_w_freshness": 0.8,
    "score_w_tag": 0.6,
    "score_w_pinned": 0.5,
    # 冷层
    "cold_enable": True,
    "cold_migrate_days": 30,
    "cold_retrieve_top_k": 5,
    "cold_use_rerank": True,
    "cold_retrieve_cues": "还记得|上次|之前|以前|那天|当时|曾经|回忆|你忘了|记不记得",
    "chunk_chars": 400,
    # provider 选择（留空=默认/自动）
    "embedding_provider_id": "",
    "rerank_provider_id": "",
    "consolidate_provider_id": "",
    "consolidate_max_events": 40,
    # 即时记忆（用户明确要求“记住”时立刻写入）
    "remember_cues": "记住|记一下|记下|别忘|帮我记住",
    "remember_importance": 95,
    "debug": False,
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  ts INTEGER NOT NULL,
  scene TEXT NOT NULL,
  kind TEXT NOT NULL,
  content TEXT NOT NULL,
  importance INTEGER NOT NULL DEFAULT 30,
  ref_msg_id TEXT,
  migrated INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_id, id);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);

CREATE TABLE IF NOT EXISTS users(
  user_id TEXT PRIMARY KEY,
  aliases TEXT,
  facts TEXT,
  narrative TEXT,
  stats TEXT,
  relations TEXT,
  last_event_cursor INTEGER NOT NULL DEFAULT 0,
  last_consolidate_ts INTEGER NOT NULL DEFAULT 0,
  schema_version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS inject_cache(
  user_id TEXT PRIMARY KEY,
  text TEXT,
  updated_ts INTEGER NOT NULL DEFAULT 0,
  version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS mem_chunks(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  text TEXT NOT NULL,
  ts INTEGER NOT NULL,
  source_event_ids TEXT,
  embedding TEXT,
  migrated_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_chunks_user ON mem_chunks(user_id);

CREATE TABLE IF NOT EXISTS kv_state(k TEXT PRIMARY KEY, v TEXT);
"""

FENCE_RE = re.compile(r"^```[a-zA-Z]*\n?|\n?```$")


def _strip_fence(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("```"):
        s = FENCE_RE.sub("", s)
    return s.strip()


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))


def _cosine(a, b) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _tag_overlap(tags, msg: str) -> float:
    if not tags or not msg:
        return 0.0
    hits = 0.0
    for t in tags:
        t = str(t or "").strip()
        if not t:
            continue
        if t in msg:
            hits += 1.0
        elif len(t) >= 2:
            ts = set(t)
            if ts and len(ts & set(msg)) / len(ts) >= 0.6:
                hits += 0.5
    return min(1.0, hits / 3.0)


class _Store:
    """SQLite 存储；方法为同步，调用方用 asyncio.to_thread 包装。"""

    def __init__(self, db_path: str):
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        with self._lock:
            self.conn.executescript(SCHEMA)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.commit()

    def close(self):
        with self._lock:
            try:
                self.conn.close()
            except Exception:
                pass

    def append_event(self, user_id, scene, kind, content, importance, ref_msg_id=""):
        with self._lock:
            cur = self.conn.execute(
                "INSERT INTO events(user_id,ts,scene,kind,content,importance,ref_msg_id)"
                " VALUES(?,?,?,?,?,?,?)",
                (user_id, int(time.time()), scene, kind, content, importance, ref_msg_id),
            )
            self.conn.commit()
            return cur.lastrowid

    def ensure_user(self, user_id):
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO users(user_id,aliases,facts,narrative,stats,relations)"
                " VALUES(?,?,?,?,?,?)",
                (user_id, "[]", "[]", "", "{}", "{}"),
            )
            self.conn.commit()

    def get_user(self, user_id):
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    def new_events(self, user_id, cursor, limit):
        with self._lock:
            rows = self.conn.execute(
                "SELECT id,ts,scene,kind,content,importance FROM events"
                " WHERE user_id=? AND id>? ORDER BY id ASC LIMIT ?",
                (user_id, cursor, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def recent_events(self, user_id, limit):
        with self._lock:
            rows = self.conn.execute(
                "SELECT id,ts,scene,kind,content,importance FROM events"
                " WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def save_consolidation(self, user_id, facts, narrative, ctx_text, cursor):
        now = int(time.time())
        with self._lock:
            self.conn.execute(
                "UPDATE users SET facts=?, narrative=?, last_event_cursor=?,"
                " last_consolidate_ts=? WHERE user_id=?",
                (json.dumps(facts, ensure_ascii=False), narrative, cursor, now, user_id),
            )
            self.conn.execute(
                "INSERT INTO inject_cache(user_id,text,updated_ts,version) VALUES(?,?,?,1)"
                " ON CONFLICT(user_id) DO UPDATE SET text=excluded.text,"
                " updated_ts=excluded.updated_ts, version=version+1",
                (user_id, ctx_text, now),
            )
            self.conn.commit()

    def get_inject_text(self, user_id):
        with self._lock:
            row = self.conn.execute(
                "SELECT text FROM inject_cache WHERE user_id=?", (user_id,)
            ).fetchone()
            return row["text"] if row else None

    def stale_events(self, user_id, before_ts, limit):
        with self._lock:
            rows = self.conn.execute(
                "SELECT id,ts,content,importance FROM events"
                " WHERE user_id=? AND migrated=0 AND ts<? ORDER BY id ASC LIMIT ?",
                (user_id, before_ts, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def add_chunks(self, user_id, items):
        now = int(time.time())
        with self._lock:
            for text, ts, src, emb in items:
                self.conn.execute(
                    "INSERT INTO mem_chunks(user_id,text,ts,source_event_ids,embedding,migrated_at)"
                    " VALUES(?,?,?,?,?,?)",
                    (user_id, text, ts, json.dumps(src), json.dumps(emb), now),
                )
            ids = [i for _, _, src, _ in items for i in src]
            if ids:
                q = ",".join("?" for _ in ids)
                self.conn.execute(f"UPDATE events SET migrated=1 WHERE id IN ({q})", ids)
            self.conn.commit()

    def chunks_for_user(self, user_id):
        with self._lock:
            rows = self.conn.execute(
                "SELECT id,text,ts,embedding FROM mem_chunks WHERE user_id=?",
                (user_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def forget_user(self, user_id):
        with self._lock:
            self.conn.execute("DELETE FROM events WHERE user_id=?", (user_id,))
            self.conn.execute("DELETE FROM users WHERE user_id=?", (user_id,))
            self.conn.execute("DELETE FROM inject_cache WHERE user_id=?", (user_id,))
            self.conn.execute("DELETE FROM mem_chunks WHERE user_id=?", (user_id,))
            self.conn.commit()

    def add_fact(self, user_id, fact):
        with self._lock:
            row = self.conn.execute(
                "SELECT facts FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
            facts = []
            if row and row["facts"]:
                try:
                    facts = json.loads(row["facts"])
                except Exception:
                    facts = []
            val = str(fact.get("value", ""))
            facts = [f for f in facts if str(f.get("value", "")) != val]
            facts.append(fact)
            self.conn.execute(
                "UPDATE users SET facts=? WHERE user_id=?",
                (json.dumps(facts, ensure_ascii=False), user_id),
            )
            self.conn.commit()

    def list_users(self, limit):
        with self._lock:
            rows = self.conn.execute(
                "SELECT u.user_id AS user_id,"
                " (SELECT COUNT(*) FROM events WHERE user_id=u.user_id) AS events,"
                " (SELECT COUNT(*) FROM mem_chunks WHERE user_id=u.user_id) AS chunks,"
                " (SELECT MAX(ts) FROM events WHERE user_id=u.user_id) AS last_ts,"
                " (SELECT 1 FROM inject_cache WHERE user_id=u.user_id AND text IS NOT NULL"
                " AND text!='' LIMIT 1) AS has_text"
                " FROM (SELECT user_id FROM users UNION SELECT DISTINCT user_id FROM events) u"
                " ORDER BY last_ts DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def list_events(self, user_id, limit, offset=0):
        with self._lock:
            rows = self.conn.execute(
                "SELECT id, ts, scene, kind, content, importance, migrated FROM events"
                " WHERE user_id=? ORDER BY id DESC LIMIT ? OFFSET ?",
                (user_id, limit, offset),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_detail(self, user_id):
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
            ist = self.conn.execute(
                "SELECT text FROM inject_cache WHERE user_id=?", (user_id,)
            ).fetchone()
            ch = self.conn.execute(
                "SELECT COUNT(*) c FROM mem_chunks WHERE user_id=?", (user_id,)
            ).fetchone()["c"]
        user = dict(row) if row else {}
        try:
            facts = json.loads(user.get("facts") or "[]")
        except Exception:
            facts = []
        return {
            "user_id": user_id,
            "facts": facts,
            "narrative": user.get("narrative", ""),
            "inject_text": ist["text"] if ist else "",
            "last_consolidate_ts": user.get("last_consolidate_ts", 0),
            "chunks": ch,
        }

    def stats(self, user_id):
        with self._lock:
            ev = self.conn.execute(
                "SELECT COUNT(*) c FROM events WHERE user_id=?", (user_id,)
            ).fetchone()["c"]
            ch = self.conn.execute(
                "SELECT COUNT(*) c FROM mem_chunks WHERE user_id=?", (user_id,)
            ).fetchone()["c"]
            return {"events": ev, "chunks": ch}


class UserMemoryPlugin(Star):
    """以用户为单位的分层记忆插件。"""

    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self.config = config if config is not None else {}
        for k, v in DEFAULTS.items():
            try:
                self.config.setdefault(k, v)
            except Exception:
                pass
        self._store: _Store | None = None
        self._idle_tasks: dict[str, asyncio.Task] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._consolidate_times: dict[str, list[float]] = {}
        try:
            if request is not None:
                self.context.register_web_api(
                    f"/{PLUGIN_NAME}/users", self.api_users, ["GET"], "List users"
                )
                self.context.register_web_api(
                    f"/{PLUGIN_NAME}/user", self.api_user, ["GET"], "User detail"
                )
                self.context.register_web_api(
                    f"/{PLUGIN_NAME}/forget", self.api_forget, ["POST"], "Forget user"
                )
                self.context.register_web_api(
                    f"/{PLUGIN_NAME}/meta", self.api_meta, ["GET"], "Plugin meta"
                )
        except Exception as e:
            logger.warning(f"[用户记忆] 注册 Web API 失败: {e}")

    # ------------------------------------------------------------------ #
    # 基础设施
    # ------------------------------------------------------------------ #
    def cfg(self, key):
        try:
            v = self.config.get(key, DEFAULTS.get(key))
        except Exception:
            v = DEFAULTS.get(key)
        return DEFAULTS.get(key) if v is None else v

    def _get_store(self) -> _Store | None:
        if self._store is not None:
            return self._store
        try:
            base = (
                get_astrbot_plugin_data_path()
                if get_astrbot_plugin_data_path
                else os.path.join(os.getcwd(), "data", "plugin_data")
            )
            db_path = os.path.join(base, PLUGIN_NAME, "memory.db")
            self._store = _Store(db_path)
            logger.info(f"[用户记忆] 存储已就绪: {db_path}")
        except Exception as e:
            logger.error(f"[用户记忆] 初始化存储失败: {e}", exc_info=True)
            return None
        return self._store

    def _lock(self, user_id: str) -> asyncio.Lock:
        if user_id not in self._locks:
            self._locks[user_id] = asyncio.Lock()
        return self._locks[user_id]

    @staticmethod
    def _scene(event: AstrMessageEvent) -> str:
        gid = event.get_group_id()
        return f"group:{gid}" if gid else "private"

    # ------------------------------------------------------------------ #
    # 1) 记录消息
    # ------------------------------------------------------------------ #
    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_any_message(self, event: AstrMessageEvent):
        if not self.cfg("enable"):
            return
        try:
            if str(event.get_self_id()) == str(event.get_sender_id()):
                return
            store = self._get_store()
            if store is None:
                return
            text = (event.message_str or "").strip()
            if not text:
                return
            user_id = str(event.get_sender_id())
            scene = self._scene(event)
            importance = self._estimate_importance(text)
            if importance < _safe_int(self.cfg("event_min_importance"), 5):
                return
            await asyncio.to_thread(
                store.append_event,
                user_id,
                scene,
                "said",
                text[:1000],
                importance,
                str(getattr(event.message_obj, "message_id", "") or ""),
            )
            # 即时记忆：用户明确要求“记住”时立刻写入 pinned fact（无 LLM，下一轮即注入）
            try:
                if self._is_remember_request(text):
                    payload_text = self._extract_remember_payload(text)
                    if payload_text:
                        fact = {
                            "key": "牢记",
                            "value": payload_text[:500],
                            "importance": _safe_int(self.cfg("remember_importance"), 95),
                            "tags": [],
                            "pinned": True,
                            "updated_ts": int(time.time()),
                        }
                        await asyncio.to_thread(store.ensure_user, user_id)
                        await asyncio.to_thread(store.add_fact, user_id, fact)
                        if self.cfg("debug"):
                            logger.info(f"[用户记忆] 即时记忆已写入: {payload_text[:40]}")
                        try:
                            asyncio.create_task(self._consolidate(user_id, trigger="force"))
                        except RuntimeError:
                            pass
            except Exception as e:
                logger.warning(f"[用户记忆] 即时记忆失败: {e}")
            self._arm_idle_timer(user_id, scene)
        except Exception as e:
            logger.warning(f"[用户记忆] 记录消息失败: {e}")

    @staticmethod
    def _estimate_importance(text: str) -> int:
        n = len(text)
        score = 20 + min(30, n // 10)
        if re.search(r"我(叫|是|喜欢|讨厌|在|住|的生日|今天)|记住|帮我记得", text):
            score += 25
        if n <= 3:
            score = min(score, 10)
        return _clamp(score, 0, 100)

    def _is_remember_request(self, text: str) -> bool:
        cues = str(self.cfg("remember_cues") or "")
        if not cues:
            return False
        try:
            return bool(re.search(cues, text))
        except re.error:
            return False

    def _extract_remember_payload(self, text: str) -> str:
        cues = str(self.cfg("remember_cues") or "")
        try:
            m = re.search(cues, text)
        except re.error:
            m = None
        if not m:
            return ""
        return text[m.end():].lstrip("：:，,。. \t").strip()

    # ------------------------------------------------------------------ #
    # 2) 空闲触发整理
    # ------------------------------------------------------------------ #
    def _arm_idle_timer(self, user_id: str, scene: str):
        old = self._idle_tasks.get(user_id)
        if old and not old.done():
            old.cancel()
        threshold = (
            _safe_int(self.cfg("idle_threshold_group"), 900)
            if scene.startswith("group")
            else _safe_int(self.cfg("idle_threshold_private"), 180)
        )
        try:
            self._idle_tasks[user_id] = asyncio.create_task(
                self._idle_worker(user_id, threshold)
            )
        except RuntimeError:
            pass

    async def _idle_worker(self, user_id: str, threshold: int):
        try:
            await asyncio.sleep(max(5, threshold))
        except asyncio.CancelledError:
            return
        try:
            await self._consolidate(user_id, trigger="idle")
        except Exception as e:
            logger.warning(f"[用户记忆] 空闲整理失败({user_id}): {e}")

    def _allow_consolidate(self, user_id: str) -> bool:
        now = time.time()
        min_interval = _safe_int(self.cfg("min_consolidate_interval"), 600)
        hist = [t for t in self._consolidate_times.get(user_id, []) if now - t < 3600]
        if hist and now - hist[-1] < min_interval:
            return False
        if len(hist) >= _safe_int(self.cfg("max_consolidate_per_hour"), 6):
            return False
        hist.append(now)
        self._consolidate_times[user_id] = hist
        return True

    async def _consolidate(self, user_id: str, trigger="manual"):
        if not self.cfg("enable"):
            return
        store = self._get_store()
        if store is None:
            return
        if trigger == "idle" and not self._allow_consolidate(user_id):
            return

        async with self._lock(user_id):
            await asyncio.to_thread(store.ensure_user, user_id)
            user = await asyncio.to_thread(store.get_user, user_id)
            cursor = _safe_int(user.get("last_event_cursor"), 0) if user else 0
            max_events = _safe_int(self.cfg("consolidate_max_events"), 40)
            events = await asyncio.to_thread(store.new_events, user_id, cursor, max_events)
            if not events:
                return

            facts = self._load_facts(user)
            narrative = (user.get("narrative") or "") if user else ""
            result = await self._llm_consolidate(facts, narrative, events)
            if not result:
                return
            new_facts = self._normalize_facts(result.get("facts", facts))
            # 保留已有 pinned 事实，避免被 LLM 重写丢弃
            have = {str(f.get("value")) for f in new_facts}
            for pf in facts:
                if pf.get("pinned") and str(pf.get("value")) not in have:
                    new_facts.append(pf)
            new_narrative = result.get("narrative", narrative)
            ctx_text = (result.get("inject_text", "") or "")[
                : _safe_int(self.cfg("inject_text_max_chars"), 1200)
            ]
            await asyncio.to_thread(
                store.save_consolidation,
                user_id,
                new_facts,
                new_narrative,
                ctx_text,
                events[-1]["id"],
            )
            if self.cfg("cold_enable"):
                try:
                    await self._cold_migrate(user_id, store)
                except Exception as e:
                    logger.warning(f"[用户记忆] 冷层迁移失败({user_id}): {e}")
            logger.info(f"[用户记忆] 已整理用户 {user_id}（{len(events)} 事件, {trigger}）")

    @staticmethod
    def _load_facts(user) -> list:
        try:
            v = json.loads((user or {}).get("facts") or "[]")
            return v if isinstance(v, list) else []
        except Exception:
            return []

    def _normalize_facts(self, facts) -> list:
        now = int(time.time())
        out = []
        if not isinstance(facts, list):
            return out
        for f in facts:
            if not isinstance(f, dict):
                continue
            key = str(f.get("key", "")).strip()
            val = str(f.get("value", "")).strip()
            if not key and not val:
                continue
            tags = f.get("tags") or []
            if not isinstance(tags, list):
                tags = [str(tags)]
            out.append(
                {
                    "key": key,
                    "value": val,
                    "importance": _clamp(_safe_int(f.get("importance", 50), 50), 0, 100),
                    "tags": [str(t) for t in tags if str(t).strip()],
                    "pinned": bool(f.get("pinned", False)),
                    "updated_ts": _safe_int(f.get("updated_ts", now), now) or now,
                }
            )
        return out

    async def _llm_consolidate(self, facts, narrative, events):
        provider_id = str(self.cfg("consolidate_provider_id") or "").strip()
        if not provider_id:
            try:
                provider_id = await self.context.get_current_chat_provider_id(umo="")
            except Exception:
                provider_id = ""
        if not provider_id:
            return None

        lines = []
        for e in events:
            ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(e["ts"]))
            lines.append(f"- [{ts}][{e['scene']}] {e['content']}")
        events_text = "\n".join(lines)

        prompt = (
            "你是记忆整理助手。基于【已有画像】与【新增事件】，更新该用户的记忆。\n"
            "只输出 JSON，无多余文字：\n"
            '{"facts":[{"key":"","value":"","importance":0-100,"tags":[""],'
            '"pinned":false,"updated_ts":0}],"narrative":"","inject_text":""}\n'
            "要求：\n"
            "1. facts = 稳定事实（身份/偏好/关系/习惯）。去重、合并、冲突以新为准；\n"
            "   importance 表示长期重要度；tags 为检索关键词；updated_ts 为 Unix 秒（可留 0）。\n"
            "2. narrative = 最近发生了什么（≤200字）。\n"
            "3. inject_text = 可直接注入 prompt 的语境文本（≤400字，无 markdown 标题）。\n\n"
            f"【已有 facts】\n{json.dumps(facts, ensure_ascii=False)}\n\n"
            f"【已有 narrative】\n{narrative}\n\n"
            f"【新增事件】\n{events_text}\n"
        )
        try:
            resp = await asyncio.wait_for(
                self.context.llm_generate(chat_provider_id=provider_id, prompt=prompt),
                timeout=60,
            )
            data = json.loads(_strip_fence(getattr(resp, "completion_text", "") or ""))
            if isinstance(data, dict):
                return data
        except Exception as e:
            logger.warning(f"[用户记忆] LLM 整理失败: {e}")
        return None

    # ------------------------------------------------------------------ #
    # 3) 读时打分与选择（无 LLM）
    # ------------------------------------------------------------------ #
    def _score_fact(self, fact: dict, msg: str, now: float) -> float:
        decay_days = max(1.0, _safe_float(self.cfg("decay_days"), 90))
        imp = _clamp(_safe_int(fact.get("importance", 50), 50), 0, 100) / 100.0
        age_days = max(0.0, (now - _safe_int(fact.get("updated_ts", now), now)) / 86400.0)
        fresh = math.exp(-age_days / max(1.0, decay_days / 2.0))
        tag = _tag_overlap(fact.get("tags") or [], msg)
        pin = 1.0 if fact.get("pinned") else 0.0
        return (
            _safe_float(self.cfg("score_w_importance"), 1.0) * imp
            + _safe_float(self.cfg("score_w_freshness"), 0.8) * fresh
            + _safe_float(self.cfg("score_w_tag"), 0.6) * tag
            + _safe_float(self.cfg("score_w_pinned"), 0.5) * pin
        )

    def _drop_fact(self, fact: dict, now: float) -> bool:
        decay_days = max(1.0, _safe_float(self.cfg("decay_days"), 90))
        floor = _safe_int(self.cfg("decay_importance_floor"), 50)
        if fact.get("pinned"):
            return False
        age_days = max(0.0, (now - _safe_int(fact.get("updated_ts", now), now)) / 86400.0)
        imp = _clamp(_safe_int(fact.get("importance", 50), 50), 0, 100)
        return age_days > decay_days and imp < floor

    async def _build_injection(self, user_id: str, msg: str, store: _Store) -> str:
        user = await asyncio.to_thread(store.get_user, user_id)
        if not user:
            return ""
        now = time.time()
        max_facts = _safe_int(self.cfg("inject_max_facts"), 12)
        max_tl = _safe_int(self.cfg("inject_max_timeline"), 8)
        budget = _safe_int(self.cfg("inject_text_max_chars"), 1200)

        facts = [f for f in self._load_facts(user) if not self._drop_fact(f, now)]
        facts.sort(key=lambda f: self._score_fact(f, msg, now), reverse=True)
        facts = facts[:max_facts]

        tl_floor = _safe_int(self.cfg("timeline_min_importance"), 10)
        raw_events = await asyncio.to_thread(store.recent_events, user_id, max_tl * 3)
        recent = [e for e in raw_events if _safe_int(e.get("importance", 0)) >= tl_floor][
            :max_tl
        ]
        recent = list(reversed(recent))  # 时间正序

        ctx = ""
        try:
            ctx = await asyncio.to_thread(store.get_inject_text, user_id) or ""
        except Exception:
            ctx = ""

        sections = []
        if facts:
            lines = []
            for f in facts:
                kv = f"{f['key']}：{f['value']}" if f.get("key") else f["value"]
                lines.append(f"- {kv}")
            sections.append("【关于该用户】\n" + "\n".join(lines))
        if ctx:
            sections.append("【语境】\n" + ctx.strip())
        if recent:
            lines = []
            for e in recent:
                ts = time.strftime("%m-%d %H:%M", time.localtime(e["ts"]))
                lines.append(f"- [{ts}] {e['content']}")
            sections.append("【最近】\n" + "\n".join(lines))

        text = "\n\n".join(sections)
        if len(text) > budget:
            text = text[:budget].rstrip() + "…"
        return text

    # ------------------------------------------------------------------ #
    # 4) 冷层：迁移 + 按需检索（可重排）
    # ------------------------------------------------------------------ #
    def _embedding_provider(self):
        try:
            providers = self.context.get_all_embedding_providers()
        except Exception:
            providers = []
        if not providers:
            return None
        pid = str(self.cfg("embedding_provider_id") or "").strip()
        if pid:
            for p in providers:
                if str(getattr(p, "provider_config", {}).get("id", "")) == pid:
                    return p
        return providers[0]

    def _rerank_provider(self):
        try:
            insts = list(getattr(self.context.provider_manager, "rerank_provider_insts", []) or [])
        except Exception:
            insts = []
        if not insts:
            return None
        pid = str(self.cfg("rerank_provider_id") or "").strip()
        if pid:
            for p in insts:
                if str(getattr(p, "provider_config", {}).get("id", "")) == pid:
                    return p
        return insts[0]

    async def _embed(self, text: str):
        prov = self._embedding_provider()
        if prov is None:
            return None
        try:
            return await prov.get_embedding(text)
        except Exception as e:
            logger.warning(f"[用户记忆] 嵌入失败: {e}")
            return None

    async def _cold_migrate(self, user_id: str, store: _Store):
        days = _safe_int(self.cfg("cold_migrate_days"), 30)
        before = int(time.time()) - days * 86400
        rows = await asyncio.to_thread(store.stale_events, user_id, before, 50)
        if not rows:
            return
        limit = max(100, _safe_int(self.cfg("chunk_chars"), 400))
        items, buf, srcs, buf_ts = [], [], [], 0
        for r in rows:
            if not buf:
                buf_ts = r["ts"]
            buf.append(r["content"])
            srcs.append(r["id"])
            if sum(len(x) for x in buf) >= limit:
                emb = await self._embed("\n".join(buf))
                if emb:
                    items.append(("\n".join(buf), buf_ts, srcs, emb))
                buf, srcs = [], []
        if buf:
            emb = await self._embed("\n".join(buf))
            if emb:
                items.append(("\n".join(buf), buf_ts, srcs, emb))
        if items:
            await asyncio.to_thread(store.add_chunks, user_id, items)
            logger.info(f"[用户记忆] 冷层迁移 {len(items)} 块 (user={user_id})")

    async def _cold_retrieve(self, user_id: str, query: str) -> str:
        store = self._get_store()
        if store is None:
            return ""
        qemb = await self._embed(query)
        rows = await asyncio.to_thread(store.chunks_for_user, user_id)
        if not rows:
            return ""
        top_k = _safe_int(self.cfg("cold_retrieve_top_k"), 5)
        candidates = []  # (text, cosine)
        if qemb:
            for r in rows:
                try:
                    emb = json.loads(r.get("embedding") or "[]")
                except Exception:
                    continue
                candidates.append((r["text"], _cosine(qemb, emb)))
        if not candidates:
            return ""
        candidates.sort(key=lambda x: x[1], reverse=True)
        pool = [t for t, s in candidates[: max(top_k * 4, top_k)]]

        picked = []
        use_rerank = bool(self.cfg("cold_use_rerank"))
        rp = self._rerank_provider() if use_rerank else None
        if rp is not None:
            try:
                res = await rp.rerank(query, pool, top_n=top_k)
                idx_sorted = sorted(res, key=lambda r: r.relevance_score, reverse=True)
                for r in idx_sorted[:top_k]:
                    if 0 <= r.index < len(pool):
                        picked.append(pool[r.index])
            except Exception as e:
                logger.warning(f"[用户记忆] 重排失败，回退向量序: {e}")
        if not picked:
            picked = pool[:top_k]
        picked = [p for p in picked if p.strip()]
        if not picked:
            return ""
        return "【历史记忆片段】\n" + "\n---\n".join(picked)

    # ------------------------------------------------------------------ #
    # 5) 注入
    # ------------------------------------------------------------------ #
    @filter.on_llm_request()
    async def inject_memory(self, event: AstrMessageEvent, req: ProviderRequest):
        if not self.cfg("enable"):
            return
        store = self._get_store()
        if store is None:
            return
        try:
            user_id = str(event.get_sender_id())
            msg = event.message_str or ""
            text = await self._build_injection(user_id, msg, store)
            if self.cfg("cold_enable"):
                cues = str(self.cfg("cold_retrieve_cues") or "")
                try:
                    hit = bool(cues) and bool(re.search(cues, msg))
                except re.error:
                    hit = False
                if hit:
                    cold = await self._cold_retrieve(user_id, msg)
                    if cold:
                        text = (text + "\n\n" + cold).strip() if text else cold
            if not text:
                return
            budget = _safe_int(self.cfg("inject_text_max_chars"), 1200)
            extra = 0 if not self.cfg("cold_enable") else 1  # 冷层可略微超预算
            wrapped = f"<user_memory>\n{text[: budget + extra * 600]}\n</user_memory>"
            self._append_user_content(req, wrapped)
        except Exception as e:
            logger.warning(f"[用户记忆] 注入失败: {e}")

    @staticmethod
    def _append_user_content(req: ProviderRequest, text: str):
        parts = getattr(req, "extra_user_content_parts", None)
        if parts is None or TextPart is None:
            return
        try:
            part = TextPart(text=text)
            try:
                part = part.mark_as_temp()
            except Exception:
                pass
            parts.append(part)
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # 6) 命令
    # ------------------------------------------------------------------ #
    @filter.command("usermem", alias={"用户记忆"})
    async def usermem(self, event: AstrMessageEvent):
        """用户记忆：状态 / 立即整理 / 删除 / 查看可用模型。"""
        store = self._get_store()
        if store is None:
            yield event.plain_result("用户记忆：存储未就绪。")
            return
        user_id = str(event.get_sender_id())
        arg = (event.message_str or "").split()[-1].strip().lower()

        if arg in ("forget", "删除", "清除"):
            await asyncio.to_thread(store.forget_user, user_id)
            yield event.plain_result("已删除你的全部记忆。")
            return
        if arg in ("now", "整理"):
            await self._consolidate(user_id, trigger="manual")
            yield event.plain_result("已触发一次记忆整理。")
            return
        if arg in ("models", "模型"):
            yield event.plain_result(self._list_models())
            return

        st = await asyncio.to_thread(store.stats, user_id)
        cached = await asyncio.to_thread(store.get_inject_text, user_id)
        yield event.plain_result(
            f"【用户记忆 v{__version__}】\n"
            f"- 事件数：{st['events']}\n"
            f"- 冷层块数：{st['chunks']}\n"
            f"- 语境文本：{'有' if cached else '无'}\n"
            "指令：/usermem 整理、/usermem 删除、/usermem 模型"
        )

    def _list_models(self) -> str:
        lines = ["【可用模型（填入插件配置的 ID）】"]
        try:
            embs = self.context.get_all_embedding_providers()
        except Exception:
            embs = []
        lines.append("嵌入 provider：")
        if embs:
            for p in embs:
                pc = getattr(p, "provider_config", {}) or {}
                lines.append(f"  - id={pc.get('id','')} type={pc.get('type','')}")
        else:
            lines.append("  （无，请先在 AstrBot 配置嵌入模型）")
        try:
            rr = list(getattr(self.context.provider_manager, "rerank_provider_insts", []) or [])
        except Exception:
            rr = []
        lines.append("重排 provider：")
        if rr:
            for p in rr:
                pc = getattr(p, "provider_config", {}) or {}
                lines.append(f"  - id={pc.get('id','')} type={pc.get('type','')}")
        else:
            lines.append("  （无，可选配置）")
        return "\n".join(lines)

    # ------------------------------------------------------------------ #
    # 7) WebUI 记忆可视化 API
    # ------------------------------------------------------------------ #
    async def api_meta(self):
        return json_response({"plugin": PLUGIN_NAME, "version": __version__})

    async def api_users(self):
        store = self._get_store()
        if store is None:
            return error_response("存储未就绪")
        limit = request.query.get("limit", 200, type=int) or 200
        users = await asyncio.to_thread(store.list_users, max(1, min(limit, 500)))
        return json_response({"users": users})

    async def api_user(self):
        store = self._get_store()
        if store is None:
            return error_response("存储未就绪")
        uid = request.query.get("user_id")
        if not uid:
            return error_response("缺少 user_id")
        detail = await asyncio.to_thread(store.get_detail, uid)
        events = await asyncio.to_thread(store.list_events, uid, 200, 0)
        return json_response({"user": detail, "events": events})

    async def api_forget(self):
        store = self._get_store()
        if store is None:
            return error_response("存储未就绪")
        payload = await request.json(default={})
        uid = str((payload or {}).get("user_id") or "").strip()
        if not uid:
            return error_response("缺少 user_id")
        await asyncio.to_thread(store.forget_user, uid)
        return json_response({"ok": True})

    async def terminate(self):
        for t in list(self._idle_tasks.values()):
            try:
                t.cancel()
            except Exception:
                pass
        self._idle_tasks.clear()
        if self._store is not None:
            try:
                self._store.close()
            except Exception:
                pass
            self._store = None
        logger.info("[用户记忆] 已停用。")
