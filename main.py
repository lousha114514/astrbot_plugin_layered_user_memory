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
import concurrent.futures
import functools
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
    from astrbot.api.message_components import At, Plain, Reply
except Exception:  # pragma: no cover
    from astrbot.core.message.components import At, Plain, Reply  # type: ignore

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
__version__ = "1.8.0"

# 所有数据库操作串行到单一线程，且全局共享一把可重入锁（跨实例也串行）
_DB_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="um-db"
)
_DB_LOCK = threading.RLock()
_STORE_REGISTRY: dict = {}
_STORE_REGISTRY_LOCK = threading.Lock()

DEFAULTS = {
    "enable": True,
    # 空闲触发
    "idle_threshold_private": 180,
    "idle_threshold_group": 900,
    "min_consolidate_interval": 600,
    "max_consolidate_per_hour": 6,
    # 记录
    "event_min_importance": 5,
    # 内容长度/有效性校验
    "min_event_chars": 5,
    "strip_mentions": True,
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
    # 插件级管理员（可查看他人记忆）；留空则仅 AstrBot 全局管理员可用
    "admin_ids": [],
    # 超管（拥有普通管理员全部权限 + 可查看受保护用户 + 管理受保护名单）
    "superadmin_ids": [],
    # 受保护用户：普通管理员不可查看
    "protected_ids": [],
    # 启动时自动备份（VACUUM INTO）与保留天数
    "backup_on_start": True,
    "backup_keep_days": 7,
    # 关闭记忆记录的群（群号列表；也可用 /usermem group 关闭）
    "disabled_groups": [],
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
  migrated INTEGER NOT NULL DEFAULT 0,
  speaker_name TEXT DEFAULT '',
  is_bot INTEGER NOT NULL DEFAULT 0,
  addressed TEXT DEFAULT ''
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

CREATE TABLE IF NOT EXISTS blocklist(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  text TEXT NOT NULL,
  ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_block_user ON blocklist(user_id);

CREATE TABLE IF NOT EXISTS privacy(
  user_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL DEFAULT 'all',
  groups_off TEXT NOT NULL DEFAULT '[]'
);
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
        self.db_path = db_path
        self._lock = _DB_LOCK
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        with self._lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA busy_timeout=30000")
            self.conn.executescript(SCHEMA)
            self._migrate()
            self.conn.commit()

    def _migrate(self):
        """为旧库补齐新列（events 说话人/bot/对象）。"""
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(events)").fetchall()}
        for name, ddl in (
            ("speaker_name", "ALTER TABLE events ADD COLUMN speaker_name TEXT DEFAULT ''"),
            ("is_bot", "ALTER TABLE events ADD COLUMN is_bot INTEGER NOT NULL DEFAULT 0"),
            ("addressed", "ALTER TABLE events ADD COLUMN addressed TEXT DEFAULT ''"),
        ):
            if name not in cols:
                try:
                    self.conn.execute(ddl)
                except Exception:
                    pass

    def checkpoint(self):
        with self._lock:
            try:
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                self.conn.commit()
            except Exception:
                pass

    def integrity_ok(self) -> bool:
        with self._lock:
            try:
                row = self.conn.execute("PRAGMA quick_check").fetchone()
                return bool(row) and str(row[0]).lower() == "ok"
            except Exception:
                return False

    def backup_to(self, path: str) -> bool:
        with self._lock:
            try:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                safe = str(path).replace("'", "''")
                self.conn.execute(f"VACUUM INTO '{safe}'")
                return True
            except Exception:
                return False

    def export_all(self, user_id=None):
        with self._lock:
            def rows(sql, params=()):
                return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

            if user_id:
                uid = str(user_id)
                return {
                    "exported_at": int(time.time()),
                    "version": __version__,
                    "user_id": uid,
                    "users": rows("SELECT * FROM users WHERE user_id=?", (uid,)),
                    "events": rows("SELECT * FROM events WHERE user_id=?", (uid,)),
                    "inject_cache": rows("SELECT * FROM inject_cache WHERE user_id=?", (uid,)),
                    "mem_chunks": rows("SELECT * FROM mem_chunks WHERE user_id=?", (uid,)),
                    "blocklist": rows(
                        "SELECT user_id,text,ts FROM blocklist WHERE user_id=?", (uid,)
                    ),
                    "privacy": rows("SELECT * FROM privacy WHERE user_id=?", (uid,)),
                }
            return {
                "exported_at": int(time.time()),
                "version": __version__,
                "users": rows("SELECT * FROM users"),
                "events": rows("SELECT * FROM events"),
                "inject_cache": rows("SELECT * FROM inject_cache"),
                "mem_chunks": rows("SELECT * FROM mem_chunks"),
                "blocklist": rows("SELECT user_id,text,ts FROM blocklist"),
                "privacy": rows("SELECT * FROM privacy"),
            }

    def events_by_ids(self, user_id, ids):
        clean = [int(i) for i in (ids or []) if str(i).strip().isdigit()][:50]
        if not clean:
            return []
        q = ",".join("?" for _ in clean)
        with self._lock:
            rows = self.conn.execute(
                f"SELECT id,ts,scene,content,importance FROM events"
                f" WHERE user_id=? AND id IN ({q}) ORDER BY id DESC",
                [user_id, *clean],
            ).fetchall()
            return [dict(r) for r in rows]

    def import_all(self, data) -> int:
        n = 0
        with self._lock:
            for r in data.get("users", []) or []:
                self.conn.execute(
                    "INSERT OR REPLACE INTO users(user_id,aliases,facts,narrative,stats,relations,"
                    "last_event_cursor,last_consolidate_ts,schema_version) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        r.get("user_id"),
                        r.get("aliases", "[]"),
                        r.get("facts", "[]"),
                        r.get("narrative", ""),
                        r.get("stats", "{}"),
                        r.get("relations", "{}"),
                        _safe_int(r.get("last_event_cursor"), 0),
                        _safe_int(r.get("last_consolidate_ts"), 0),
                        _safe_int(r.get("schema_version"), 1),
                    ),
                )
                n += 1
            for r in data.get("inject_cache", []) or []:
                self.conn.execute(
                    "INSERT OR REPLACE INTO inject_cache(user_id,text,updated_ts,version) VALUES(?,?,?,?)",
                    (
                        r.get("user_id"),
                        r.get("text", ""),
                        _safe_int(r.get("updated_ts"), 0),
                        _safe_int(r.get("version"), 1),
                    ),
                )
                n += 1
            for r in data.get("events", []) or []:
                self.conn.execute(
                    "INSERT INTO events(user_id,ts,scene,kind,content,importance,ref_msg_id,migrated)"
                    " VALUES(?,?,?,?,?,?,?,?)",
                    (
                        r.get("user_id"),
                        _safe_int(r.get("ts"), 0),
                        r.get("scene", ""),
                        r.get("kind", "said"),
                        r.get("content", ""),
                        _safe_int(r.get("importance"), 30),
                        r.get("ref_msg_id", ""),
                        _safe_int(r.get("migrated"), 0),
                    ),
                )
                n += 1
            for r in data.get("mem_chunks", []) or []:
                self.conn.execute(
                    "INSERT INTO mem_chunks(user_id,text,ts,source_event_ids,embedding,migrated_at)"
                    " VALUES(?,?,?,?,?,?)",
                    (
                        r.get("user_id"),
                        r.get("text", ""),
                        _safe_int(r.get("ts"), 0),
                        r.get("source_event_ids", "[]"),
                        r.get("embedding", "[]"),
                        _safe_int(r.get("migrated_at"), 0),
                    ),
                )
                n += 1
            for r in data.get("blocklist", []) or []:
                self.conn.execute(
                    "INSERT INTO blocklist(user_id,text,ts) VALUES(?,?,?)",
                    (r.get("user_id"), r.get("text", ""), _safe_int(r.get("ts"), 0)),
                )
                n += 1
            for r in data.get("privacy", []) or []:
                self.conn.execute(
                    "INSERT OR REPLACE INTO privacy(user_id,mode,groups_off) VALUES(?,?,?)",
                    (r.get("user_id"), r.get("mode", "all"), r.get("groups_off", "[]")),
                )
                n += 1
            self.conn.commit()
        return n

    def close(self):
        with self._lock:
            try:
                self.conn.close()
            except Exception:
                pass

    def append_event(
        self,
        user_id,
        scene,
        kind,
        content,
        importance,
        ref_msg_id="",
        speaker_name="",
        is_bot=0,
        addressed="",
    ):
        with self._lock:
            cur = self.conn.execute(
                "INSERT INTO events(user_id,ts,scene,kind,content,importance,ref_msg_id,"
                "speaker_name,is_bot,addressed) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    user_id,
                    int(time.time()),
                    scene,
                    kind,
                    content,
                    importance,
                    ref_msg_id,
                    speaker_name,
                    int(is_bot),
                    addressed,
                ),
            )
            self.conn.commit()
            return cur.lastrowid

    def kv_set(self, k, v):
        with self._lock:
            self.conn.execute(
                "INSERT INTO kv_state(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (str(k), str(v)),
            )
            self.conn.commit()

    def kv_get(self, k, default=""):
        with self._lock:
            row = self.conn.execute("SELECT v FROM kv_state WHERE k=?", (str(k),)).fetchone()
            return row["v"] if row else default

    def context_events(self, scenes, ts_from, limit=60):
        """同场景、窗口内的其他发言（含机器人），用于整理时理解语境。"""
        if not scenes:
            return []
        q = ",".join("?" for _ in scenes)
        with self._lock:
            rows = self.conn.execute(
                f"SELECT id,ts,scene,content,speaker_name,is_bot,user_id FROM events"
                f" WHERE ts>=? AND scene IN ({q}) ORDER BY ts ASC LIMIT ?",
                [ts_from, *list(scenes), limit],
            ).fetchall()
            return [dict(r) for r in rows]

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
            self.conn.execute("DELETE FROM blocklist WHERE user_id=?", (user_id,))
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
                " FROM (SELECT user_id FROM users UNION SELECT DISTINCT user_id FROM events"
                " WHERE is_bot=0) u"
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
        if isinstance(facts, list):
            # 置顶优先展示，其次按重要度降序
            facts.sort(
                key=lambda f: (
                    0 if f.get("pinned") else 1,
                    -_safe_int(f.get("importance", 0), 0),
                )
            )
        return {
            "user_id": user_id,
            "facts": facts,
            "narrative": user.get("narrative", ""),
            "inject_text": ist["text"] if ist else "",
            "last_consolidate_ts": user.get("last_consolidate_ts", 0),
            "chunks": ch,
        }

    def update_fact(self, user_id, old_value, patch):
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
            changed = 0
            for f in facts:
                if str(f.get("value")) == str(old_value):
                    for k in ("key", "value", "importance", "tags", "pinned"):
                        if k in patch and patch[k] is not None:
                            f[k] = patch[k]
                    f["updated_ts"] = int(time.time())
                    changed += 1
            if changed:
                self.conn.execute(
                    "UPDATE users SET facts=? WHERE user_id=?",
                    (json.dumps(facts, ensure_ascii=False), user_id),
                )
                self.conn.commit()
            return changed

    def delete_fact(self, user_id, value):
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
            new = [f for f in facts if str(f.get("value")) != str(value)]
            removed = len(facts) - len(new)
            if removed:
                self.conn.execute(
                    "UPDATE users SET facts=? WHERE user_id=?",
                    (json.dumps(new, ensure_ascii=False), user_id),
                )
                self.conn.commit()
            return removed

    def set_inject_text(self, user_id, text):
        with self._lock:
            self.conn.execute(
                "INSERT INTO inject_cache(user_id,text,updated_ts,version) VALUES(?,?,?,1)"
                " ON CONFLICT(user_id) DO UPDATE SET text=excluded.text,"
                " updated_ts=excluded.updated_ts, version=version+1",
                (user_id, text, int(time.time())),
            )
            self.conn.commit()

    def delete_event(self, user_id, eid):
        with self._lock:
            cur = self.conn.execute(
                "DELETE FROM events WHERE user_id=? AND id=?", (user_id, int(eid))
            )
            self.conn.commit()
            return cur.rowcount

    def set_alias(self, user_id, name):
        name = (name or "").strip()
        if not name:
            return
        with self._lock:
            row = self.conn.execute(
                "SELECT aliases FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
            names = []
            if row and row["aliases"]:
                try:
                    names = json.loads(row["aliases"])
                except Exception:
                    names = []
            if not isinstance(names, list):
                names = []
            names = [n for n in names if n != name]
            names.append(name)
            names = names[-5:]
            self.conn.execute(
                "UPDATE users SET aliases=? WHERE user_id=?",
                (json.dumps(names, ensure_ascii=False), user_id),
            )
            self.conn.commit()

    def nickname(self, user_id):
        with self._lock:
            row = self.conn.execute(
                "SELECT aliases FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
        if not row or not row["aliases"]:
            return ""
        try:
            names = json.loads(row["aliases"])
        except Exception:
            return ""
        return names[-1] if isinstance(names, list) and names else ""

    def search_users(self, q, limit):
        like = f"%{q}%"
        out = {}
        with self._lock:
            for r in self.conn.execute(
                "SELECT e.user_id AS user_id, COUNT(*) AS events, MAX(e.ts) AS last_ts"
                " FROM events e WHERE e.content LIKE ? AND e.is_bot=0 GROUP BY e.user_id"
                " ORDER BY last_ts DESC LIMIT ?",
                (like, limit),
            ).fetchall():
                out[r["user_id"]] = {
                    "user_id": r["user_id"],
                    "events": r["events"],
                    "last_ts": r["last_ts"],
                    "matched": "event",
                }
            for r in self.conn.execute(
                "SELECT user_id FROM users WHERE facts LIKE ? OR narrative LIKE ? LIMIT ?",
                (like, like, limit),
            ).fetchall():
                d = out.setdefault(
                    r["user_id"],
                    {"user_id": r["user_id"], "events": 0, "last_ts": 0, "matched": "fact"},
                )
                d["matched"] = "fact"
            for uid, d in out.items():
                ev = self.conn.execute(
                    "SELECT COUNT(*) c, MAX(ts) t FROM events WHERE user_id=?", (uid,)
                ).fetchone()
                d["events"] = ev["c"]
                if ev["t"]:
                    d["last_ts"] = max(d["last_ts"] or 0, ev["t"])
                ch = self.conn.execute(
                    "SELECT COUNT(*) c FROM mem_chunks WHERE user_id=?", (uid,)
                ).fetchone()
                d["chunks"] = ch["c"]
        return list(out.values())[:limit]

    def add_block(self, user_id, text):
        text = str(text or "").strip()
        if not text:
            return
        with self._lock:
            self.conn.execute(
                "INSERT INTO blocklist(user_id,text,ts) VALUES(?,?,?)",
                (user_id, text, int(time.time())),
            )
            self.conn.commit()

    def list_blocks(self, user_id, limit=50):
        with self._lock:
            rows = self.conn.execute(
                "SELECT text FROM blocklist WHERE user_id=? ORDER BY id DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
            return [r["text"] for r in rows]

    def get_privacy(self, user_id):
        with self._lock:
            row = self.conn.execute(
                "SELECT mode,groups_off FROM privacy WHERE user_id=?", (str(user_id),)
            ).fetchone()
        if not row:
            return {"mode": "all", "groups_off": []}
        try:
            goff = json.loads(row["groups_off"] or "[]")
        except Exception:
            goff = []
        if not isinstance(goff, list):
            goff = []
        return {"mode": row["mode"] or "all", "groups_off": [str(g) for g in goff]}

    def set_privacy(self, user_id, mode, groups_off):
        with self._lock:
            self.conn.execute(
                "INSERT INTO privacy(user_id,mode,groups_off) VALUES(?,?,?)"
                " ON CONFLICT(user_id) DO UPDATE SET mode=excluded.mode, groups_off=excluded.groups_off",
                (str(user_id), str(mode), json.dumps([str(g) for g in (groups_off or [])])),
            )
            self.conn.commit()

    def stats(self, user_id):
        with self._lock:
            ev = self.conn.execute(
                "SELECT COUNT(*) c FROM events WHERE user_id=?", (user_id,)
            ).fetchone()["c"]
            ch = self.conn.execute(
                "SELECT COUNT(*) c FROM mem_chunks WHERE user_id=?", (user_id,)
            ).fetchone()["c"]
            return {"events": ev, "chunks": ch}


def _open_store(db_path: str) -> "_Store":
    """按路径返回单例 Store，避免热重载产生多个连接。"""
    with _STORE_REGISTRY_LOCK:
        st = _STORE_REGISTRY.get(db_path)
        if st is None:
            st = _Store(db_path)
            _STORE_REGISTRY[db_path] = st
        return st


def _quarantine_db(db_path: str, tag: str = "corrupt") -> str:
    """把损坏/待重建的库及其 -wal/-shm 改名存档，返回存档后缀时间戳。"""
    ts = time.strftime("%Y%m%d-%H%M%S")
    with _STORE_REGISTRY_LOCK:
        old = _STORE_REGISTRY.pop(db_path, None)
    if old is not None:
        try:
            old.conn.close()
        except Exception:
            pass
    for suffix in ("", "-wal", "-shm"):
        src = db_path + suffix
        if os.path.exists(src):
            try:
                os.replace(src, f"{src}.{tag}.{ts}")
            except Exception:
                pass
    return ts


def _open_store_safe(db_path: str) -> "_Store":
    """打开库；若因损坏在构造阶段就失败，则改名存档并重建空库。"""
    try:
        return _open_store(db_path)
    except sqlite3.DatabaseError as e:
        msg = str(e).lower()
        if "malformed" in msg or "corrupt" in msg or "not a database" in msg:
            ts = _quarantine_db(db_path, "corrupt")
            logger.error(f"[用户记忆] 数据库损坏，已改名存档 .corrupt.{ts} 并重建。")
            return _open_store(db_path)
        raise


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
        self._closing = False
        self._privacy_cache: dict = {}
        self._db_path: str | None = None
        self._db_dir: str | None = None
        self._last_backup_date: str | None = None
        self._store: _Store | None = None
        self._idle_tasks: dict[str, asyncio.Task] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._consolidate_times: dict[str, list[float]] = {}
        try:
            if request is not None:
                self._reg(
                    f"/{PLUGIN_NAME}/users", self.api_users, ["GET"], "List users"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/user", self.api_user, ["GET"], "User detail"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/forget", self.api_forget, ["POST"], "Forget user"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/meta", self.api_meta, ["GET"], "Plugin meta"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/fact/add", self.api_fact_add, ["POST"], "Add fact"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/fact/update", self.api_fact_update, ["POST"], "Update fact"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/fact/delete", self.api_fact_delete, ["POST"], "Delete fact"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/inject/save", self.api_inject_save, ["POST"], "Save inject text"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/event/delete", self.api_event_delete, ["POST"], "Delete event"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/search", self.api_search, ["GET"], "Search memory"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/providers", self.api_providers, ["GET"], "List providers"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/providers/save", self.api_providers_save, ["POST"], "Save providers"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/export", self.api_export, ["GET"], "Export memory"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/event/sources", self.api_event_sources, ["GET"], "Event sources"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/import", self.api_import, ["POST"], "Import memory"
                )
                self._reg(
                    f"/{PLUGIN_NAME}/rebuild", self.api_rebuild, ["POST"], "Rebuild database"
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
            self._db_dir = os.path.join(base, PLUGIN_NAME)
            self._db_path = os.path.join(self._db_dir, "memory.db")
            self._store = _open_store_safe(self._db_path)
            if not self._store.integrity_ok():
                logger.error("[用户记忆] 完整性检查失败，尝试自愈重建。")
                self._heal_db_sync()
            else:
                self._maybe_backup()
            self._store_error = ""
            logger.info(f"[用户记忆] 存储已就绪: {self._db_path}")
        except Exception as e:
            self._store_error = str(e)
            self._store = None
            logger.error(f"[用户记忆] 初始化存储失败: {e}", exc_info=True)
            return None
        return self._store

    def _storage_error(self) -> str:
        return f"存储不可用：{getattr(self, '_store_error', '') or '未初始化'}"

    async def _db(self, fn, *args):
        """在专用单线程中串行执行数据库操作；捕获损坏则自愈。"""
        if self._closing:
            raise RuntimeError("插件正在关闭")
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(_DB_EXECUTOR, functools.partial(fn, *args))
        except sqlite3.DatabaseError as e:
            msg = str(e).lower()
            if "malformed" in msg or "corrupt" in msg or "not a database" in msg:
                logger.error(f"[用户记忆] 数据库损坏：{e}；执行自愈。")
                await loop.run_in_executor(_DB_EXECUTOR, self._heal_db_sync)
            raise

    def _heal_db_sync(self):
        path = self._db_path
        if not path:
            return
        ts = time.strftime("%Y%m%d-%H%M%S")
        try:
            with _STORE_REGISTRY_LOCK:
                old = _STORE_REGISTRY.pop(path, None)
            if old is not None:
                try:
                    old.conn.close()
                except Exception:
                    pass
            for suffix in ("", "-wal", "-shm"):
                src = path + suffix
                if os.path.exists(src):
                    try:
                        os.replace(src, f"{src}.corrupt.{ts}")
                    except Exception:
                        pass
            self._store = _open_store(path)
            logger.error(f"[用户记忆] 已自愈重建数据库（旧库改名 .corrupt.{ts}）")
        except Exception as e:
            logger.error(f"[用户记忆] 自愈失败: {e}")

    def _rebuild_db_sync(self):
        path = self._db_path
        if not path:
            return
        ts = _quarantine_db(path, "bak")
        self._store = _open_store(path)
        logger.warning(f"[用户记忆] 已重建数据库（旧库改名 .bak.{ts}）")

    def _maybe_backup(self):
        if not self.cfg("backup_on_start") or not self._db_dir or self._store is None:
            return
        today = time.strftime("%Y%m%d")
        if self._last_backup_date == today:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._last_backup_date = today
        bdir = os.path.join(self._db_dir, "backups")
        path = os.path.join(bdir, f"memory-{today}.db")
        keep = max(1, _safe_int(self.cfg("backup_keep_days"), 7))
        store = self._store

        def _do():
            store.backup_to(path)
            try:
                files = sorted(
                    f
                    for f in os.listdir(bdir)
                    if f.startswith("memory-") and f.endswith(".db")
                )
                for f in files[:-keep]:
                    try:
                        os.remove(os.path.join(bdir, f))
                    except Exception:
                        pass
            except Exception:
                pass
            return True

        try:
            loop.create_task(self._db(_do))
        except Exception:
            pass

    def _reg(self, route, handler, methods, desc):
        async def _wrapped(*args, **kwargs):
            try:
                return await handler(*args, **kwargs)
            except Exception as e:
                logger.error(f"[用户记忆] API {route} 出错: {e}", exc_info=True)
                return error_response(f"插件内部错误：{e}", 500)

        try:
            self.context.register_web_api(route, _wrapped, methods, desc)
        except Exception as e:
            logger.warning(f"[用户记忆] 注册 Web API 失败 {route}: {e}")

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
            sid = str(event.get_sender_id())
            self_id = str(event.get_self_id())
            is_bot = sid == self_id
            store = self._get_store()
            if store is None:
                return
            user_id = sid
            nick = self._sender_name(event)
            text = self._clean_text(event.message_str or "")
            if not self._is_meaningful(text):
                return
            scene = self._scene(event)
            addressed = self._addressed(event)
            mid = str(getattr(event.message_obj, "message_id", "") or "")
            gid = event.get_group_id()
            if gid and self._is_group_disabled(str(gid)):
                return
            if is_bot:
                # 机器人自己的发言：只作为“对话上下文”记录，不为它建档
                await self._db(
                    store.append_event,
                    user_id,
                    scene,
                    "bot",
                    text[:1000],
                    30,
                    mid,
                    nick,
                    1,
                    addressed,
                )
                try:
                    await self._db(store.kv_set, "bot_id", self_id)
                    if nick:
                        await self._db(store.kv_set, "bot_name", nick)
                except Exception:
                    pass
                return
            if not await self._allow_record(user_id, gid):
                return
            await self._db(store.ensure_user, user_id)
            if nick:
                await self._db(store.set_alias, user_id, nick)
            importance = self._estimate_importance(text)
            if importance < _safe_int(self.cfg("event_min_importance"), 5):
                return
            await self._db(
                store.append_event,
                user_id,
                scene,
                "said",
                text[:1000],
                importance,
                mid,
                nick,
                0,
                addressed,
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
                        await self._db(store.ensure_user, user_id)
                        await self._db(store.add_fact, user_id, fact)
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

    _AT_RE = re.compile(r"@[^\s@()]{1,40}\(\s*\d{4,}\s*\)|\[CQ:[^\]]*\]|<@!?\d+>")

    @staticmethod
    def _addressed(event) -> str:
        """提取本条消息指向的对象（@某人 / 引用回复）。"""
        parts = []
        try:
            for comp in getattr(event.message_obj, "message", []) or []:
                if isinstance(comp, At):
                    q = getattr(comp, "qq", None)
                    if q:
                        parts.append(f"@{q}")
                elif isinstance(comp, Reply):
                    s = getattr(comp, "sender_id", None)
                    if s:
                        parts.append(f"reply:{s}")
        except Exception:
            pass
        return " ".join(parts)[:120]

    def _clean_text(self, text: str) -> str:
        """去除 @提及/CQ码 等噪音（避免他人身份泄漏进本人记忆）。"""
        t = text or ""
        if self.cfg("strip_mentions"):
            t = self._AT_RE.sub(" ", t)
        t = re.sub(r"\s+", " ", t).strip()
        return t

    def _is_meaningful(self, text: str) -> bool:
        """长度/内容校验：过滤过短、纯标点/表情/无意义片段。"""
        t = (text or "").strip()
        if len(t) < max(1, _safe_int(self.cfg("min_event_chars"), 5)):
            return False
        if not re.search(r"[0-9A-Za-z\u4e00-\u9fff]", t):
            return False
        return True

    @staticmethod
    def _sender_name(event) -> str:
        try:
            return (event.get_sender_name() or "").strip()
        except Exception:
            return ""

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
            await self._db(store.ensure_user, user_id)
            user = await self._db(store.get_user, user_id)
            cursor = _safe_int(user.get("last_event_cursor"), 0) if user else 0
            max_events = _safe_int(self.cfg("consolidate_max_events"), 40)
            events = await self._db(store.new_events, user_id, cursor, max_events)
            if not events:
                return

            facts = self._load_facts(user)
            narrative = (user.get("narrative") or "") if user else ""
            try:
                nickname = await self._db(store.nickname, user_id)
            except Exception:
                nickname = ""
            try:
                blocks = await self._db(store.list_blocks, user_id)
            except Exception:
                blocks = []
            try:
                bot_id = await self._db(store.kv_get, "bot_id", "")
                bot_name = await self._db(store.kv_get, "bot_name", "")
            except Exception:
                bot_id, bot_name = "", ""
            ctx_events = []
            try:
                scenes = sorted({e["scene"] for e in events})
                ts_from = (events[0]["ts"] - 600) if events else int(time.time()) - 3600
                ctx_events = await self._db(store.context_events, scenes, ts_from, 60)
            except Exception:
                ctx_events = []
            result = await self._llm_consolidate(
                facts,
                narrative,
                events,
                {
                    "user_id": user_id,
                    "nickname": nickname,
                    "blocks": blocks,
                    "bot_id": bot_id,
                    "bot_name": bot_name,
                    "ctx_events": ctx_events,
                },
            )
            if not result:
                return
            new_facts = self._normalize_facts(result.get("facts", facts))
            # 保留已有 pinned 事实，避免被 LLM 重写丢弃
            have = {str(f.get("value")) for f in new_facts}
            for pf in facts:
                if pf.get("pinned") and str(pf.get("value")) not in have:
                    new_facts.append(pf)
            # 身份锚定：本人 user_id / 昵称 始终在案
            new_facts = self._ensure_identity_facts(new_facts, user_id, nickname)
            # 幻觉复核：根据来源事件校验每一事实是否有依据
            valid_ids = {e["id"] for e in events}
            for f in new_facts:
                if str(f.get("key") or "") in ("QQ号", "昵称"):
                    f["verified"] = True
                    continue
                srcs = [s for s in (f.get("sources") or []) if s in valid_ids]
                f["sources"] = srcs
                f["verified"] = bool(srcs)
            new_narrative = result.get("narrative", narrative)
            ctx_text = (result.get("inject_text", "") or "")[
                : _safe_int(self.cfg("inject_text_max_chars"), 1200)
            ]
            await self._db(
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
            srcs = f.get("sources") or []
            if not isinstance(srcs, list):
                srcs = []
            srcs = [x for x in (_safe_int(s, -1) for s in srcs) if x > 0]
            out.append(
                {
                    "key": key,
                    "value": val,
                    "importance": _clamp(_safe_int(f.get("importance", 50), 50), 0, 100),
                    "tags": [str(t) for t in tags if str(t).strip()],
                    "pinned": bool(f.get("pinned", False)),
                    "sources": srcs,
                    "verified": bool(f.get("verified", True)),
                    "updated_ts": _safe_int(f.get("updated_ts", now), now) or now,
                }
            )
        return out

    def _ensure_identity_facts(self, facts, user_id, nickname):
        """确保本人 QQ号/昵称 始终作为事实存在（去除 LLM 可能写错的同类项）。"""
        now = int(time.time())
        out = [
            f
            for f in facts
            if str(f.get("key") or "").strip() not in ("QQ号", "QQ", "user_id", "昵称")
        ]
        out.append(
            {
                "key": "QQ号",
                "value": str(user_id),
                "importance": 100,
                "tags": [],
                "pinned": True,
                "sources": [],
                "verified": True,
                "updated_ts": now,
            }
        )
        if nickname:
            out.append(
                {
                    "key": "昵称",
                    "value": nickname,
                    "importance": 90,
                    "tags": [],
                    "pinned": True,
                    "sources": [],
                    "verified": True,
                    "updated_ts": now,
                }
            )
        return out

    async def _llm_consolidate(self, facts, narrative, events, ctx=None):
        ctx = ctx or {}
        provider_id = str(self.cfg("consolidate_provider_id") or "").strip()
        if not provider_id:
            try:
                provider_id = await self.context.get_current_chat_provider_id(umo="")
            except Exception:
                provider_id = ""
        if not provider_id:
            return None

        uid = str(ctx.get("user_id") or "")
        nickname = str(ctx.get("nickname") or "")
        bot_id = str(ctx.get("bot_id") or "")
        bot_name = str(ctx.get("bot_name") or "")
        blocks = ctx.get("blocks") or []
        ctx_events = ctx.get("ctx_events") or []

        def fmt(e):
            ts = time.strftime("%m-%d %H:%M", time.localtime(e["ts"]))
            eid = e.get("id", "")
            if e.get("is_bot"):
                who = e.get("speaker_name") or bot_name or e.get("user_id", "")
                tag = "机器人"
            else:
                who = e.get("speaker_name") or e.get("user_id", "")
                tag = f"用户{e.get('user_id','')}"
            return f"- [#{eid}][{ts}][{e.get('scene','')}] {who}({tag})：{e.get('content','')}"

        events_text = "\n".join(fmt(e) for e in events) or "（无）"
        ctx_text = "\n".join(fmt(e) for e in ctx_events) or "（无）"
        blocks_text = ""
        if blocks:
            blocks_text = (
                "【禁止重新添加（用户已删除，勿再写入）】\n"
                + "\n".join(f"- {b}" for b in blocks)
                + "\n\n"
            )

        prompt = (
            "你是用户记忆整理助手。只依据【角色】、【本人发言】与【对话上下文】更新【本人】的记忆。\n"
            "只输出 JSON，无多余文字：\n"
            '{"facts":[{"key":"","value":"","importance":0-100,"tags":[""],'
            '"pinned":false,"sources":[1,2],"updated_ts":0}],"narrative":"","inject_text":""}\n\n'
            "【角色（只能使用这些身份，不得编造）】\n"
            f"- 本人：昵称={nickname or '（未知）'}，QQ={uid}\n"
            f"- 本机器人：昵称={bot_name or '（未知）'}，QQ={bot_id or '（未知）'}（只有一个机器人，不存在“二号/三号”）\n"
            "对话中其他参与者以 昵称(QQ) 或 QQ 标注。\n\n"
            "硬性规则（必须遵守，违反即为错误）：\n"
            "1) 只记录【本人】的信息；不得为他人建档。\n"
            "2) 严禁编造人名、机器人编号或身份；只能用【角色】或文本中出现过的 QQ/昵称。\n"
            "3) 只依据给定文本；不确定就不要写（宁缺毋滥）。\n"
            "4) 本人身份一律以给定的 QQ/昵称为准。\n"
            "5) 关系类描述必须带明确对象 QQ（如“常与 1145140011 互动”）；没有明确对象就不写关系。\n"
            "6) 不要把同一个人拆成多个；机器人只有一个。\n"
            "7) 每条 fact 必须给出 sources（引用的【本人发言】的 #编号）；找不到依据就不要写这条。\n\n" + blocks_text +
            "【本人发言（待整理）】\n" + events_text + "\n\n"
            "【对话上下文（仅供理解语境，不要为其中人物建立记忆）】\n" + ctx_text + "\n\n"
            "【已有 facts】\n" + json.dumps(facts, ensure_ascii=False) + "\n\n"
            "【已有 narrative】\n" + narrative + "\n\n"
            "要求：\n"
            "1. facts = 本人稳定事实（身份/偏好/关系/习惯）。去重合并、冲突以新为准；importance 0-100；tags 关键词；updated_ts 可留 0。\n"
            "2. narrative = 本人最近发生了什么（≤200字）。\n"
            "3. inject_text = 可直接注入的语境（≤400字，无 markdown 标题）。\n"
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
        user = await self._db(store.get_user, user_id)
        if not user:
            return ""
        now = time.time()
        max_facts = _safe_int(self.cfg("inject_max_facts"), 12)
        max_tl = _safe_int(self.cfg("inject_max_timeline"), 8)
        budget = _safe_int(self.cfg("inject_text_max_chars"), 1200)

        inject_unverified = bool(self.cfg("inject_unverified"))
        facts = [
            f
            for f in self._load_facts(user)
            if not self._drop_fact(f, now)
            and (f.get("verified", True) or inject_unverified)
        ]
        facts.sort(key=lambda f: self._score_fact(f, msg, now), reverse=True)
        facts = facts[:max_facts]

        tl_floor = _safe_int(self.cfg("timeline_min_importance"), 10)
        raw_events = await self._db(store.recent_events, user_id, max_tl * 3)
        recent = [e for e in raw_events if _safe_int(e.get("importance", 0)) >= tl_floor][
            :max_tl
        ]
        recent = list(reversed(recent))  # 时间正序

        ctx = ""
        try:
            ctx = await self._db(store.get_inject_text, user_id) or ""
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
        rows = await self._db(store.stale_events, user_id, before, 50)
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
            await self._db(store.add_chunks, user_id, items)
            logger.info(f"[用户记忆] 冷层迁移 {len(items)} 块 (user={user_id})")

    async def _cold_retrieve(self, user_id: str, query: str) -> str:
        store = self._get_store()
        if store is None:
            return ""
        qemb = await self._embed(query)
        rows = await self._db(store.chunks_for_user, user_id)
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
        tokens = [
            t
            for t in re.sub(r"\s+", " ", (event.message_str or "").strip()).split(" ")
            if t
        ]
        if tokens and tokens[0].lower() in ("usermem", "用户记忆"):
            tokens = tokens[1:]
        arg = tokens[0].strip().lower() if tokens else ""

        if arg in ("forget", "删除", "清除"):
            await self._db(store.forget_user, user_id)
            yield event.plain_result("已删除你的全部记忆。")
            return
        if arg in ("now", "整理"):
            await self._consolidate(user_id, trigger="manual")
            yield event.plain_result("已触发一次记忆整理。")
            return
        if arg in ("models", "模型"):
            yield event.plain_result(self._list_models())
            return
        if arg in ("view", "查看"):
            target = tokens[1].strip() if len(tokens) >= 2 else user_id
            if not target:
                yield event.plain_result("用法：/usermem 查看 [user_id]")
                return
            if not self._can_view(event, target):
                yield event.plain_result("权限不足：无权查看该用户记忆。")
                return
            detail = await self._db(store.get_detail, target)
            events = await self._db(store.list_events, target, 10)
            yield event.plain_result(self._format_detail(target, detail, events))
            return
        if arg in ("list", "列表"):
            if not self._is_admin(event):
                yield event.plain_result("权限不足：仅管理员可查看用户列表。")
                return
            users = await self._db(store.list_users, 50)
            if not self._is_superadmin(event):
                users = [u for u in users if not self._is_protected(u["user_id"])]
            lines = ["【用户列表（最近活动）】"]
            for u in users:
                lines.append(
                    f"- {u['user_id']}  事件{u.get('events', 0)} 冷层{u.get('chunks', 0)}"
                )
            yield event.plain_result("\n".join(lines))
            return
        if arg in ("clear", "清除", "purge"):
            if not self._is_admin(event):
                yield event.plain_result("权限不足：仅管理员可清除用户记忆。")
                return
            if len(tokens) < 2:
                yield event.plain_result("用法：/usermem 清除 <user_id>")
                return
            target = tokens[1].strip()
            if self._is_protected(target) and not self._is_superadmin(event):
                yield event.plain_result("权限不足：该用户在受保护名单，仅超管可清除。")
                return
            try:
                await self._db(store.forget_user, target)
                yield event.plain_result(f"已清除用户 {target} 的全部记忆。")
            except Exception as e:
                yield event.plain_result(f"清除失败（可能数据库损坏）：{e}")
            return
        if arg in ("mode", "模式"):
            if len(tokens) < 2:
                yield event.plain_result(
                    "用法：/usermem mode 全局|关闭本群|关闭|开启本群 [user_id]"
                )
                return
            state = tokens[1].strip()
            target = user_id
            if len(tokens) >= 3:
                target = tokens[2].strip()
                if target != user_id:
                    if not self._is_admin(event):
                        yield event.plain_result("权限不足：仅管理员可设置他人。")
                        return
                    if self._is_protected(target) and not self._is_superadmin(event):
                        yield event.plain_result("权限不足：该用户在受保护名单，仅超管可设置。")
                        return
            gid = event.get_group_id()
            p = await self._get_privacy(target)
            mode = p.get("mode", "all")
            goff = list(p.get("groups_off") or [])
            if state in ("全局", "all", "global"):
                mode, goff = "all", []
            elif state in ("关闭", "none", "off"):
                mode, goff = "none", []
            elif state in ("关闭本群", "off_group"):
                if not gid:
                    yield event.plain_result("该指令需在群聊中使用。")
                    return
                mode = "all"
                if str(gid) not in goff:
                    goff.append(str(gid))
            elif state in ("开启本群", "on_group"):
                if not gid:
                    yield event.plain_result("该指令需在群聊中使用。")
                    return
                mode = "all"
                goff = [g for g in goff if g != str(gid)]
            else:
                yield event.plain_result(
                    "用法：/usermem mode 全局|关闭本群|关闭|开启本群 [user_id]"
                )
                return
            await self._db(store.set_privacy, target, mode, goff)
            self._privacy_cache.pop(target, None)
            yield event.plain_result(
                f"已更新 {target} 的记忆记录设置：{state}（mode={mode}，本群关闭={goff}）"
            )
            return
        if arg in ("group", "群"):
            if not self._is_superadmin(event):
                yield event.plain_result("权限不足：仅超管可设置本群是否记录。")
                return
            gid = event.get_group_id()
            if not gid:
                yield event.plain_result("该指令需在群聊中使用。")
                return
            sub = tokens[1].strip().lower() if len(tokens) >= 2 else ""
            ids = self._id_set(self.cfg("disabled_groups"))
            if sub in ("off", "关闭", "关"):
                ids.add(str(gid))
                self._set_config_list("disabled_groups", ids)
                yield event.plain_result(f"本群({gid})已关闭记忆记录。")
            elif sub in ("on", "开启", "开"):
                ids.discard(str(gid))
                self._set_config_list("disabled_groups", ids)
                yield event.plain_result(f"本群({gid})已开启记忆记录。")
            else:
                yield event.plain_result("用法：/usermem group 开启|关闭")
            return
        if arg in ("rebuild", "重建", "重建库"):
            if not self._is_superadmin(event):
                yield event.plain_result("权限不足：仅超管可重建数据库。")
                return
            try:
                await asyncio.get_running_loop().run_in_executor(
                    _DB_EXECUTOR, self._rebuild_db_sync
                )
                self._store = None
                self._store_error = ""
                self._get_store()
                yield event.plain_result("数据库已重建（旧库已改名为 .bak.<时间戳>）。")
            except Exception as e:
                yield event.plain_result(f"重建失败：{e}")
            return
        if arg in ("protect", "保护"):
            if not self._is_superadmin(event):
                yield event.plain_result("权限不足：仅超管可管理受保护名单。")
                return
            if len(tokens) < 2:
                yield event.plain_result("用法：/usermem 保护 <user_id>")
                return
            ids = self._id_set(self.cfg("protected_ids"))
            ids.add(tokens[1].strip())
            self._set_config_list("protected_ids", ids)
            yield event.plain_result(
                f"已将 {tokens[1].strip()} 加入受保护名单（普通管理员不可查看）。"
            )
            return
        if arg in ("unprotect", "取消保护"):
            if not self._is_superadmin(event):
                yield event.plain_result("权限不足：仅超管可管理受保护名单。")
                return
            if len(tokens) < 2:
                yield event.plain_result("用法：/usermem 取消保护 <user_id>")
                return
            ids = self._id_set(self.cfg("protected_ids"))
            ids.discard(tokens[1].strip())
            self._set_config_list("protected_ids", ids)
            yield event.plain_result(f"已将 {tokens[1].strip()} 移出受保护名单。")
            return
        if arg in ("protected", "保护列表"):
            if not self._is_superadmin(event):
                yield event.plain_result("权限不足：仅超管可查看受保护名单。")
                return
            ids = sorted(self._id_set(self.cfg("protected_ids")))
            yield event.plain_result(
                "【受保护用户】\n" + ("\n".join(f"- {i}" for i in ids) if ids else "（空）")
            )
            return

        st = await self._db(store.stats, user_id)
        cached = await self._db(store.get_inject_text, user_id)
        p = await self._get_privacy(user_id)
        yield event.plain_result(
            f"【用户记忆 v{__version__}】\n"
            f"- 事件数：{st['events']}\n"
            f"- 冷层块数：{st['chunks']}\n"
            f"- 语境文本：{'有' if cached else '无'}\n"
            f"- 记录设置：{p.get('mode')}（本群关闭={p.get('groups_off') or []}）\n"
            "指令：/usermem 查看、整理、删除、模型、mode、group；管理员：列表、查看 <id>、清除 <id>"
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
    @staticmethod
    def _id_set(v) -> set:
        if isinstance(v, str):
            return {x.strip() for x in v.replace("，", ",").split(",") if x.strip()}
        if isinstance(v, (list, tuple, set)):
            return {str(x).strip() for x in v if str(x).strip()}
        return set()

    @staticmethod
    def _sid(event) -> str:
        try:
            return str(event.get_sender_id())
        except Exception:
            return ""

    def _is_superadmin(self, event) -> bool:
        return self._sid(event) in self._id_set(self.cfg("superadmin_ids"))

    def _is_admin(self, event) -> bool:
        sid = self._sid(event)
        if sid in self._id_set(self.cfg("admin_ids")) or self._is_superadmin(event):
            return True
        try:
            if event.is_admin():
                return True
        except Exception:
            pass
        return False

    def _is_protected(self, user_id: str) -> bool:
        return str(user_id) in self._id_set(self.cfg("protected_ids"))

    def _can_view(self, event, target_id) -> bool:
        if str(target_id) == self._sid(event):
            return True
        if self._is_superadmin(event):
            return True
        if not self._is_admin(event):
            return False
        return not self._is_protected(target_id)

    def _is_group_disabled(self, gid) -> bool:
        return str(gid) in self._id_set(self.cfg("disabled_groups"))

    async def _get_privacy(self, uid):
        now = time.time()
        c = self._privacy_cache.get(uid)
        if c and now - c[0] < 30:
            return c[1]
        store = self._get_store()
        if store is None:
            return {"mode": "all", "groups_off": []}
        try:
            p = await self._db(store.get_privacy, uid)
        except Exception:
            p = {"mode": "all", "groups_off": []}
        self._privacy_cache[uid] = (now, p)
        return p

    async def _allow_record(self, user_id, gid) -> bool:
        p = await self._get_privacy(user_id)
        if p.get("mode") == "none":
            return False
        if gid and str(gid) in set(p.get("groups_off") or []):
            return False
        return True

    def _set_config_list(self, key, values):
        vals = sorted({str(v).strip() for v in values if str(v).strip()})
        try:
            self.config[key] = vals
        except Exception:
            pass
        self._save_config()
        return vals

    def _save_config(self) -> bool:
        try:
            save = getattr(self.config, "save_config", None)
            if callable(save):
                save()
                return True
        except Exception as e:
            logger.warning(f"[用户记忆] 保存配置失败: {e}")
        return False

    @staticmethod
    def _format_detail(uid, detail, events) -> str:
        lines = [f"【用户 {uid} 的记忆】"]
        facts = detail.get("facts") or []
        if facts:
            lines.append("事实：")
            for f in facts:
                kv = (
                    f"{f.get('key')}：{f.get('value')}"
                    if f.get("key")
                    else str(f.get("value"))
                )
                lines.append(
                    f"- {kv}  (imp={f.get('importance')}{' pinned' if f.get('pinned') else ''})"
                )
        if detail.get("inject_text"):
            lines.append("语境：")
            lines.append(str(detail["inject_text"]))
        if events:
            lines.append("最近事件：")
            for e in events[:10]:
                ts = time.strftime("%m-%d %H:%M", time.localtime(e["ts"]))
                lines.append(f"- [{ts}] {e['content']}")
        return "\n".join(lines)

    async def api_search(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        q = (request.query.get("q") or "").strip()
        limit = request.query.get("limit", 100, type=int) or 100
        if not q:
            users = await self._db(store.list_users, min(limit, 500))
        else:
            users = await self._db(store.search_users, q, min(limit, 500))
        for u in users:
            try:
                u["protected"] = self._is_protected(u.get("user_id"))
            except Exception:
                u["protected"] = False
        return json_response({"users": users})

    async def api_providers(self):
        embs, rrs = [], []
        try:
            for p in self.context.get_all_embedding_providers():
                pc = getattr(p, "provider_config", {}) or {}
                pid = str(pc.get("id", ""))
                embs.append({"id": pid, "label": f"{pid} ({pc.get('type', '')})"})
        except Exception:
            pass
        try:
            for p in getattr(self.context.provider_manager, "rerank_provider_insts", []) or []:
                pc = getattr(p, "provider_config", {}) or {}
                pid = str(pc.get("id", ""))
                rrs.append({"id": pid, "label": f"{pid} ({pc.get('type', '')})"})
        except Exception:
            pass
        return json_response(
            {
                "embeddings": embs,
                "reranks": rrs,
                "current": {
                    "embedding_provider_id": str(self.cfg("embedding_provider_id") or ""),
                    "rerank_provider_id": str(self.cfg("rerank_provider_id") or ""),
                },
            }
        )

    async def api_providers_save(self):
        p = await request.json(default={}) or {}
        if "embedding_provider_id" in p:
            self.config["embedding_provider_id"] = str(p.get("embedding_provider_id") or "")
        if "rerank_provider_id" in p:
            self.config["rerank_provider_id"] = str(p.get("rerank_provider_id") or "")
        self._save_config()
        return json_response({"ok": True})

    async def api_export(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        uid = (request.query.get("user_id") or "").strip()
        data = await self._db(store.export_all, uid or None)
        return json_response(data)

    async def api_event_sources(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        uid = (request.query.get("user_id") or "").strip()
        ids_raw = request.query.get("ids") or ""
        if not uid:
            return error_response("缺少 user_id")
        ids = [i for i in re.split(r"[,\s]+", ids_raw) if i.strip().isdigit()]
        rows = await self._db(store.events_by_ids, uid, ids)
        return json_response({"events": rows})

    async def api_import(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        p = await request.json(default={}) or {}
        data = p.get("data")
        if not isinstance(data, dict):
            return error_response("缺少 data")
        n = await self._db(store.import_all, data)
        return json_response({"ok": True, "imported": n})

    async def api_rebuild(self):
        try:
            await asyncio.get_running_loop().run_in_executor(
                _DB_EXECUTOR, self._rebuild_db_sync
            )
            self._store = None
            self._store_error = ""
            self._get_store()
            return json_response({"ok": True})
        except Exception as e:
            return error_response(f"重建失败：{e}", 500)

    async def api_meta(self):
        return json_response({"plugin": PLUGIN_NAME, "version": __version__})

    async def api_users(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        limit = request.query.get("limit", 200, type=int) or 200
        users = await self._db(store.list_users, max(1, min(limit, 500)))
        for u in users:
            try:
                u["protected"] = self._is_protected(u.get("user_id"))
            except Exception:
                u["protected"] = False
        return json_response({"users": users})

    async def api_user(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        uid = request.query.get("user_id")
        if not uid:
            return error_response("缺少 user_id")
        detail = await self._db(store.get_detail, uid)
        events = await self._db(store.list_events, uid, 200, 0)
        return json_response({"user": detail, "events": events})

    async def api_forget(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        payload = await request.json(default={})
        uid = str((payload or {}).get("user_id") or "").strip()
        if not uid:
            return error_response("缺少 user_id")
        await self._db(store.forget_user, uid)
        return json_response({"ok": True})

    @staticmethod
    def _split_tags(v):
        if isinstance(v, list):
            return [str(t).strip() for t in v if str(t).strip()]
        if v is None:
            return []
        return [t.strip() for t in str(v).replace("，", ",").split(",") if t.strip()]

    async def api_fact_add(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        p = await request.json(default={}) or {}
        uid = str(p.get("user_id") or "").strip()
        val = str(p.get("value") or "").strip()
        if not uid or not val:
            return error_response("缺少 user_id 或 value")
        await self._db(store.ensure_user, uid)
        fact = self._normalize_facts(
            [
                {
                    "key": p.get("key", ""),
                    "value": val,
                    "importance": _safe_int(p.get("importance", 60), 60),
                    "tags": self._split_tags(p.get("tags")),
                    "pinned": bool(p.get("pinned", False)),
                    "updated_ts": int(time.time()),
                }
            ]
        )
        if not fact:
            return error_response("事实不合法")
        await self._db(store.add_fact, uid, fact[0])
        return json_response({"ok": True})

    async def api_fact_update(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        p = await request.json(default={}) or {}
        uid = str(p.get("user_id") or "").strip()
        old_value = str(p.get("old_value") or "").strip()
        if not uid or not old_value:
            return error_response("缺少 user_id 或 old_value")
        patch = {}
        for k in ("key", "value"):
            if k in p:
                patch[k] = str(p.get(k) or "")
        if "importance" in p:
            patch["importance"] = _clamp(_safe_int(p.get("importance"), 50), 0, 100)
        if "tags" in p:
            patch["tags"] = self._split_tags(p.get("tags"))
        if "pinned" in p:
            patch["pinned"] = bool(p.get("pinned"))
        n = await self._db(store.update_fact, uid, old_value, patch)
        return json_response({"ok": bool(n), "changed": n})

    async def api_fact_delete(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        p = await request.json(default={}) or {}
        uid = str(p.get("user_id") or "").strip()
        val = str(p.get("value") or "").strip()
        if not uid or not val:
            return error_response("缺少 user_id 或 value")
        n = await self._db(store.delete_fact, uid, val)
        return json_response({"ok": bool(n), "removed": n})

    async def api_inject_save(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        p = await request.json(default={}) or {}
        uid = str(p.get("user_id") or "").strip()
        if not uid:
            return error_response("缺少 user_id")
        text = str(p.get("text") or "")
        await self._db(store.ensure_user, uid)
        await self._db(store.set_inject_text, uid, text)
        return json_response({"ok": True})

    async def api_event_delete(self):
        store = self._get_store()
        if store is None:
            return error_response(self._storage_error())
        p = await request.json(default={}) or {}
        uid = str(p.get("user_id") or "").strip()
        eid = p.get("id")
        if not uid or eid is None:
            return error_response("缺少 user_id 或 id")
        n = await self._db(store.delete_event, uid, eid)
        return json_response({"ok": bool(n)})

    @filter.llm_tool(name="edit_memory")
    async def edit_memory_tool(
        self,
        event: AstrMessageEvent,
        instruction: str,
        user_id: str = "",
        dry_run: bool = True,
    ):
        """修改或删除某个用户的长期记忆（仅管理员可用）。instruction 用自然语言描述要改什么，例如“把昵称改成阿强”“删掉关于猫的那条”。dry_run=True 时只返回将要执行的改动供确认；确认后再调用并设 dry_run=false。"""
        if not self._is_admin(event):
            return "权限不足：只有管理员可以修改记忆。"
        target = (user_id or "").strip() or self._sid(event)
        if not self._can_view(event, target):
            return "权限不足：无权修改该用户的记忆。"
        store = self._get_store()
        if store is None:
            return "存储未就绪。"
        detail = await self._db(store.get_detail, target)
        facts = detail.get("facts") or []
        inject_text = detail.get("inject_text") or ""
        ops = await self._parse_memory_edit(instruction, facts, inject_text)
        if not ops:
            return "无法解析这条指令，请说得更明确（或直接指定要删/改的内容）。"
        if dry_run:
            return "【待确认的改动】\n" + self._render_ops(ops) + "\n确认后再让我执行。"
        done = await self._apply_memory_ops(target, ops, facts, store)
        return "已执行：\n" + done

    async def _parse_memory_edit(self, instruction, facts, inject_text):
        provider_id = str(self.cfg("consolidate_provider_id") or "").strip()
        if not provider_id:
            try:
                provider_id = await self.context.get_current_chat_provider_id(umo="")
            except Exception:
                provider_id = ""
        if not provider_id:
            return []
        flist = [
            {
                "id": i,
                "key": f.get("key"),
                "value": f.get("value"),
                "importance": f.get("importance"),
                "pinned": f.get("pinned"),
            }
            for i, f in enumerate(facts)
        ]
        prompt = (
            "你是记忆编辑解析器。根据管理员的自然语言指令，输出要执行的操作。\n"
            '只输出 JSON：{"ops":[...]}，op 类型：\n'
            '- {"type":"delete_fact","fact_id":N}\n'
            '- {"type":"update_fact","fact_id":N,"key"?:str,"value"?:str,"importance"?:int}\n'
            '- {"type":"add_fact","key":str,"value":str,"importance"?:int}\n'
            '- {"type":"set_inject_text","text":str}\n'
            "fact_id 使用下面列表的 id；不确定就不要产生任何操作。\n\n"
            f"【现有事实】\n{json.dumps(flist, ensure_ascii=False)}\n\n"
            f"【现有语境】\n{inject_text}\n\n"
            f"【管理员指令】\n{instruction}\n"
        )
        try:
            resp = await asyncio.wait_for(
                self.context.llm_generate(chat_provider_id=provider_id, prompt=prompt),
                timeout=45,
            )
            data = json.loads(_strip_fence(getattr(resp, "completion_text", "") or ""))
            ops = data.get("ops") if isinstance(data, dict) else None
            return ops if isinstance(ops, list) else []
        except Exception as e:
            logger.warning(f"[用户记忆] 解析记忆编辑失败: {e}")
            return []

    @staticmethod
    def _render_ops(ops) -> str:
        lines = []
        for op in ops:
            t = op.get("type")
            if t == "delete_fact":
                lines.append(f"- 删除事实（id={op.get('fact_id')}）")
            elif t == "update_fact":
                lines.append(
                    f"- 修改事实 id={op.get('fact_id')} → {op.get('value') or op.get('key') or ''}"
                )
            elif t == "add_fact":
                lines.append(f"- 新增事实：{op.get('value')}")
            elif t == "set_inject_text":
                lines.append("- 替换语境文本")
            else:
                lines.append(f"- {t}")
        return "\n".join(lines) if lines else "（无操作）"

    async def _apply_memory_ops(self, user_id, ops, facts, store):
        lines = []
        for op in ops:
            t = op.get("type")
            fid = op.get("fact_id")
            if t == "delete_fact":
                val = None
                if isinstance(fid, int) and 0 <= fid < len(facts):
                    val = facts[fid].get("value")
                if val:
                    await self._db(store.delete_fact, user_id, val)
                    await self._db(store.add_block, user_id, val)
                    lines.append(f"已删除事实：{val}")
            elif t == "update_fact":
                old = None
                if isinstance(fid, int) and 0 <= fid < len(facts):
                    old = facts[fid].get("value")
                patch = {k: op[k] for k in ("key", "value", "importance") if k in op}
                if old and patch:
                    await self._db(store.update_fact, user_id, old, patch)
                    lines.append(f"已修改事实：{old} → {patch.get('value', old)}")
            elif t == "add_fact":
                v = str(op.get("value") or "").strip()
                if v:
                    fact = self._normalize_facts(
                        [
                            {
                                "key": op.get("key", ""),
                                "value": v,
                                "importance": _safe_int(op.get("importance", 70), 70),
                                "tags": [],
                                "pinned": bool(op.get("pinned", False)),
                                "updated_ts": int(time.time()),
                            }
                        ]
                    )
                    if fact:
                        await self._db(store.ensure_user, user_id)
                        await self._db(store.add_fact, user_id, fact[0])
                        lines.append(f"已新增事实：{v}")
            elif t == "set_inject_text":
                await self._db(store.ensure_user, user_id)
                await self._db(
                    store.set_inject_text, user_id, str(op.get("text") or "")
                )
                lines.append("已替换语境文本")
        return "\n".join(lines) if lines else "（无可执行操作）"

    async def terminate(self):
        self._closing = True
        for t in list(self._idle_tasks.values()):
            try:
                t.cancel()
            except Exception:
                pass
        self._idle_tasks.clear()
        # 不关闭共享连接（避免关闭与写并发导致损坏）；只做一次 checkpoint。
        try:
            st = self._store
            if st is not None:
                await asyncio.get_running_loop().run_in_executor(_DB_EXECUTOR, st.checkpoint)
        except Exception:
            pass
        logger.info("[用户记忆] 已停用（共享连接保留）。")
