const bridge = window.AstrBotPluginPage;
await bridge.ready();

const $ = (s) => document.querySelector(s);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const COLORS = ["#2f6df6", "#7c5cff", "#e05b8a", "#f0932b", "#1f9d63", "#1793ab"];
const color = (id) => COLORS[Number(String(id).replace(/\D/g, "")) % COLORS.length] || COLORS[0];
const fmt = (ts) => (ts ? new Date(ts * 1000).toLocaleString() : "-");
const hl = (s, q) => (q ? esc(s).replace(new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "ig"), (m) => `<mark>${m}</mark>`) : esc(s));
const ushort = (ts) => { if (!ts) return ""; const d = new Date(ts * 1000); const p = (n) => String(n).padStart(2, "0"); return `${p(d.getMonth()+1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}`; };

let users = [];
let cur = null;
let filter = "all";
let themeMode = 0; // 0 light 1 dark 2 system
let version = "";

function pick(res) { return res && res.data !== undefined ? res.data : res; }

async function apiGet(path, params) {
  return pick(await bridge.apiGet(path, params || {}));
}
async function apiPost(path, body) {
  try {
    return pick(await bridge.apiPost(path, body || {}));
  } catch (e) {
    toast("失败：" + e.message);
    throw e;
  }
}

function toast(msg) {
  const t = document.createElement("div");
  t.className = "toast";
  t.textContent = msg;
  $("#toasts").appendChild(t);
  requestAnimationFrame(() => t.classList.add("on"));
  setTimeout(() => { t.classList.remove("on"); setTimeout(() => t.remove(), 250); }, 1800);
}

function confirmModal(title, text, onYes) {
  $("#mTitle").textContent = title;
  $("#mText").textContent = text;
  $("#modalScrim").classList.add("on");
  $("#mNo").onclick = () => $("#modalScrim").classList.remove("on");
  $("#mYes").onclick = () => { $("#modalScrim").classList.remove("on"); onYes(); };
}

function applyTheme() {
  const light = themeMode === 0 || (themeMode === 2 && !matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.setAttribute("data-theme", light ? "light" : "dark");
  $("#theme").textContent = light ? "🌙" : "☀️";
}

// ---------------- users ----------------
async function loadUsers() {
  const q = $("#q").value.trim();
  const box = $("#users");
  box.innerHTML = '<div class="skel"></div><div class="skel"></div><div class="skel"></div>';
  try {
    const data = q ? await apiGet("search", { q, limit: 300 }) : await apiGet("users", { limit: 300 });
    users = (data && data.users) || [];
    renderUsers(q);
  } catch (e) {
    box.innerHTML = `<div class="empty">加载失败：${esc(e.message)}</div>`;
  }
}

function renderUsers(q) {
  q = (q != null ? q : $("#q").value).trim();
  const box = $("#users");
  const list = users.filter((u) => {
    if (filter === "cold" && !u.chunks) return false;
    if (filter === "protected" && !u.protected) return false;
    return true;
  });
  $("#ucount").textContent = list.length + " 人";
  if (!list.length) { box.innerHTML = '<div class="empty">无匹配用户</div>'; return; }
  box.innerHTML = list.map((u) => `
    <div class="user ${u.user_id === cur ? "active" : ""}" data-id="${esc(u.user_id)}">
      <div class="avatar" style="background:${color(u.user_id)}">${esc(String(u.user_id).slice(0, 1))}</div>
      <div class="u-main">
        <div class="u-name">#${hl(u.user_id, q)}</div>
        <div class="u-badges">
          <span class="badge">事件 <b>${u.events ?? 0}</b></span>
          ${u.chunks ? `<span class="badge">冷层 <b>${u.chunks}</b></span>` : ""}
          ${u.protected ? `<span class="badge" style="color:var(--gold);border-color:color-mix(in srgb,var(--gold) 45%,var(--border))">🔒 受保护</span>` : ""}
          ${u.matched ? `<span class="badge">命中${u.matched === "fact" ? "事实" : "事件"}</span>` : ""}
          ${u.last_ts ? `<span class="badge">${short(u.last_ts)}</span>` : ""}
        </div>
      </div>
      <div class="u-actions"><div class="mini danger" data-del="${esc(u.user_id)}">删除</div></div>
    </div>`).join("");
  box.querySelectorAll(".user").forEach((el) => (el.onclick = () => { cur = el.dataset.id; renderUsers(); loadDetail(); }));
  box.querySelectorAll("[data-del]").forEach((el) => (el.onclick = (ev) => {
    ev.stopPropagation();
    const id = el.dataset.del;
    confirmModal("删除该用户全部记忆", `将删除 ${id} 的事件/事实/语境/冷层记录，不可恢复。`, async () => {
      await apiPost("forget", { user_id: id });
      if (cur === id) cur = null;
      toast("已删除 " + id);
      await loadUsers();
      if (!cur) $("#detail").innerHTML = '<div class="empty">从左侧选择一个用户</div>';
    });
  }));
}
function short(ts) { return ushort(ts); }

// ---------------- detail ----------------
async function loadDetail() {
  const d = $("#detail");
  if (!cur) { d.innerHTML = '<div class="empty">从左侧选择一个用户</div>'; return; }
  d.innerHTML = '<div class="skel"></div><div class="skel" style="height:120px"></div><div class="skel" style="height:80px"></div>';
  try {
    const data = await apiGet("user", { user_id: cur });
    renderDetail((data && data.user) || {}, (data && data.events) || []);
  } catch (e) {
    d.innerHTML = `<div class="empty">加载失败：${esc(e.message)}</div>`;
  }
}

function factCard(uid, f, i) {
  return `<div class="card fact ${f.pinned ? "pinned" : ""}" data-i="${i}">
    <div class="k">${esc(f.key || "")}</div>
    <div class="v">${esc(f.value)}</div>
    <div class="bar" title="重要度 ${f.importance ?? 50}"><i style="width:${Math.max(0, Math.min(100, f.importance ?? 50))}%"></i></div>
    <div class="star" data-pin="${i}" title="置顶">${f.pinned ? "★" : "☆"}</div>
    <button class="mini" data-edit="${i}">编辑</button>
    <button class="mini danger" data-fdel="${i}">删除</button>
  </div>`;
}

function renderDetail(user, events) {
  const uid = user.user_id || cur;
  const facts = user.facts || [];
  const d = $("#detail");
  d.innerHTML = `
    <div class="ucard">
      <div class="avatar" style="background:${color(uid)}">${esc(String(uid).slice(0, 1))}</div>
      <div>
        <div class="name">#${esc(uid)}</div>
        <div class="sub">冷层块 ${user.chunks ?? 0} · 上次整理 ${fmt(user.last_consolidate_ts)}</div>
        <div class="pills">
          <span class="pill">事实 <b>${facts.length}</b></span>
          <span class="pill">事件 <b>${events.length}</b></span>
          <span class="pill">冷层 <b>${user.chunks ?? 0}</b></span>
        </div>
      </div>
      <div class="ucard-actions">
        <button class="btn sm" id="dExp">导出</button>
        <button class="btn sm danger" id="dDel">删除该用户</button>
      </div>
    </div>

    <div class="section">
      <h3>事实 <span class="line"></span> <button class="btn sm ghost" id="addFact">＋ 新增</button></h3>
      <div id="facts">${facts.map((f, i) => factCard(uid, f, i)).join("") || '<div class="empty">暂无事实</div>'}</div>
    </div>

    <div class="section">
      <h3>语境（inject_text） <span class="line"></span></h3>
      <div class="ctx">
        <textarea id="ctx">${esc(user.inject_text || "")}</textarea>
        <div class="ctx-foot">
          <span class="counter"><span id="cc">${(user.inject_text || "").length}</span> 字</span>
          <span><span class="saved" id="saved"></span> <button class="btn sm primary" id="saveCtx">保存语境</button></span>
        </div>
      </div>
    </div>

    <div class="section">
      <h3>最近事件 <span class="line"></span></h3>
      <div class="timeline">
        ${events.length ? events.map((e) => `
          <div class="tl-item" data-eid="${e.id}">
            <div class="tl-time">${esc(short(e.ts))}<span class="tl-scene">${esc(e.scene || "")}</span>
              <span class="mini danger" data-edel="${e.id}" style="margin-left:6px">删除</span></div>
            <div class="tl-text">${esc(e.content)}</div>
          </div>`).join("") : '<div class="empty">暂无事件</div>'}
      </div>
    </div>`;

  d.querySelector("#dExp").onclick = async () => {
    try {
      const data = await apiGet("export");
      const blob = new Blob([JSON.stringify(data)], { type: "application/json" });
      const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = `memory_${uid}.json`; a.click(); URL.revokeObjectURL(a.href);
      toast("已导出");
    } catch (e) {}
  };
  d.querySelector("#dDel").onclick = () => confirmModal("删除该用户全部记忆", `确认删除 ${uid} 的所有记忆？`, async () => {
    await apiPost("forget", { user_id: uid });
    cur = null; toast("已删除"); await loadUsers(); $("#detail").innerHTML = '<div class="empty">从左侧选择一个用户</div>';
  });
  d.querySelector("#addFact").onclick = () => {
    const box = d.querySelector("#facts");
    if (box.querySelector("#newFact")) return;
    const el = document.createElement("div");
    el.className = "card"; el.id = "newFact";
    el.innerHTML = `<div class="editgrid">
      <input class="k" placeholder="键（可空）"/>
      <input class="v" placeholder="值（必填）"/>
      <input class="imp" type="number" min="0" max="100" value="60"/>
      <input class="tags" placeholder="标签(逗号分隔)"/>
      <label style="color:var(--muted);font-size:12px"><input type="checkbox" class="pin" checked/> 置顶</label>
    </div>
    <div style="display:flex;gap:6px;margin-top:6px"><button class="mini" id="nSave">保存</button><button class="mini" id="nCancel">取消</button></div>`;
    box.prepend(el);
    el.querySelector("#nCancel").onclick = () => el.remove();
    el.querySelector("#nSave").onclick = async () => {
      const v = el.querySelector(".v").value.trim();
      if (!v) return toast("值不能为空");
      await apiPost("fact/add", {
        user_id: uid, key: el.querySelector(".k").value, value: v,
        importance: Number(el.querySelector(".imp").value), tags: el.querySelector(".tags").value,
        pinned: el.querySelector(".pin").checked,
      });
      toast("已新增事实"); loadDetail();
    };
  };
  d.querySelectorAll("[data-pin]").forEach((el) => (el.onclick = async () => {
    const f = facts[+el.dataset.pin];
    await apiPost("fact/update", { user_id: uid, old_value: f.value, pinned: !f.pinned });
    loadDetail();
  }));
  d.querySelectorAll("[data-edit]").forEach((el) => (el.onclick = () => {
    const i = +el.dataset.edit; const f = facts[i];
    const card = d.querySelector(`.fact[data-i="${i}"]`);
    card.innerHTML = `<div class="editgrid">
      <input class="k" value="${esc(f.key || "")}"/>
      <input class="v" value="${esc(f.value)}"/>
      <input class="imp" type="number" min="0" max="100" value="${f.importance ?? 60}"/>
      <input class="tags" value="${esc((f.tags || []).join(","))}"/>
      <label style="color:var(--muted);font-size:12px"><input type="checkbox" class="pin" ${f.pinned ? "checked" : ""}/> 置顶</label>
    </div>
    <div style="display:flex;gap:6px;margin-top:6px"><button class="mini" data-ok>保存</button><button class="mini" data-no>取消</button></div>`;
    card.querySelector("[data-no]").onclick = loadDetail;
    card.querySelector("[data-ok]").onclick = async () => {
      await apiPost("fact/update", {
        user_id: uid, old_value: f.value,
        key: card.querySelector(".k").value, value: card.querySelector(".v").value,
        importance: Number(card.querySelector(".imp").value), tags: card.querySelector(".tags").value,
        pinned: card.querySelector(".pin").checked,
      });
      toast("已保存"); loadDetail();
    };
  }));
  d.querySelectorAll("[data-fdel]").forEach((el) => (el.onclick = () => {
    const f = facts[+el.dataset.fdel];
    confirmModal("删除事实", `确认删除：${f.value}？`, async () => {
      await apiPost("fact/delete", { user_id: uid, value: f.value });
      toast("已删除事实"); loadDetail();
    });
  }));
  const ta = d.querySelector("#ctx");
  ta.oninput = () => { d.querySelector("#cc").textContent = ta.value.length; d.querySelector("#saved").textContent = ""; };
  d.querySelector("#saveCtx").onclick = async () => {
    await apiPost("inject/save", { user_id: uid, text: ta.value });
    d.querySelector("#saved").textContent = "已保存 ✓"; toast("已保存语境");
  };
  d.querySelectorAll("[data-edel]").forEach((el) => (el.onclick = () => {
    confirmModal("删除事件", "确认删除这条事件？", async () => {
      await apiPost("event/delete", { user_id: uid, id: Number(el.dataset.edel) });
      toast("已删除事件"); loadDetail();
    });
  }));
}

// ---------------- settings ----------------
async function loadProviders() {
  try {
    const data = await apiGet("providers");
    const embs = (data && data.embeddings) || [];
    const rrs = (data && data.reranks) || [];
    const cur2 = (data && data.current) || {};
    const opt = (arr, sel) => `<option value="">（默认/自动）</option>` + arr.map((o) => `<option value="${esc(o.id)}" ${o.id === sel ? "selected" : ""}>${esc(o.label)}</option>`).join("");
    $("#sel-emb").innerHTML = opt(embs, cur2.embedding_provider_id);
    $("#sel-rr").innerHTML = opt(rrs, cur2.rerank_provider_id);
  } catch (e) {}
}

// ---------------- events ----------------
$("#q").addEventListener("input", () => { clearTimeout(window._t); window._t = setTimeout(loadUsers, 300); });
$("#chips").querySelectorAll(".chip").forEach((ch) => (ch.onclick = () => {
  $("#chips").querySelectorAll(".chip").forEach((x) => x.classList.remove("on"));
  ch.classList.add("on"); filter = ch.dataset.f; renderUsers();
}));
$("#theme").onclick = () => { themeMode = (themeMode + 1) % 3; applyTheme(); toast(["浅色", "深色", "跟随系统"][themeMode]); };
$("#refresh").onclick = () => { loadUsers(); if (cur) loadDetail(); loadProviders(); toast("已刷新"); };
$("#settings").onclick = () => { $("#drawer").classList.add("on"); $("#scrim").classList.add("on"); loadProviders(); };
$("#closeDrawer").onclick = $("#scrim").onclick = () => { $("#drawer").classList.remove("on"); $("#scrim").classList.remove("on"); };
$("#save-prov").onclick = async () => {
  await apiPost("providers/save", { embedding_provider_id: $("#sel-emb").value, rerank_provider_id: $("#sel-rr").value });
  toast("已保存模型设置");
};
$("#exp").onclick = async () => {
  try {
    const data = await apiGet("export");
    const blob = new Blob([JSON.stringify(data)], { type: "application/json" });
    const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = "user_memory_export.json"; a.click(); URL.revokeObjectURL(a.href);
    toast("已导出");
  } catch (e) {}
};
$("#imp").onchange = async (ev) => {
  const f = ev.target.files[0]; if (!f) return;
  let data; try { data = JSON.parse(await f.text()); } catch { return toast("JSON 解析失败"); }
  await apiPost("import", { data });
  toast("已导入"); await loadUsers();
};
$("#rebuild").onclick = () => confirmModal("重建数据库", "会清空当前库并新建空库（旧库改名为 .bak.<时间戳>）。建议先导出。", async () => {
  try {
    const data = await apiGet("export");
    const blob = new Blob([JSON.stringify(data)], { type: "application/json" });
    const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = "user_memory_before_rebuild.json"; a.click(); URL.revokeObjectURL(a.href);
  } catch (e) {}
  await apiPost("rebuild", {});
  toast("数据库已重建"); cur = null; await loadUsers(); $("#detail").innerHTML = '<div class="empty">从左侧选择一个用户</div>';
});

try {
  const meta = await apiGet("meta");
  version = (meta && meta.version) || "";
  if (version) $("#ver").textContent = "v" + version;
} catch {}
try { themeMode = bridge.getContext() && bridge.getContext().isDark ? 1 : 0; } catch {}
applyTheme();
await loadUsers();
loadProviders();
