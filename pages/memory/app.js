const bridge = window.AstrBotPluginPage;
await bridge.ready();

const usersEl = document.getElementById("users");
const detailEl = document.getElementById("detail");
const searchEl = document.getElementById("search");

let allUsers = [];
let currentUid = null;
let version = "";

function pick(res) {
  return res && res.data !== undefined ? res.data : res;
}
function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
  );
}
function fmt(ts) {
  if (!ts) return "-";
  try {
    return new Date(ts * 1000).toLocaleString();
  } catch {
    return String(ts);
  }
}
function toast(msg) {
  let t = document.getElementById("toast");
  if (!t) {
    t = document.createElement("div");
    t.id = "toast";
    document.body.appendChild(t);
  }
  t.textContent = msg;
  t.className = "show";
  clearTimeout(t._tid);
  t._tid = setTimeout(() => (t.className = ""), 1800);
}

async function api(path, body) {
  try {
    return pick(await bridge.apiPost(path, body));
  } catch (e) {
    toast("失败：" + e.message);
    throw e;
  }
}

async function loadUsers() {
  const kw = searchEl.value.trim();
  usersEl.innerHTML = '<li class="muted">加载中…</li>';
  try {
    const data = kw
      ? pick(await bridge.apiGet("search", { q: kw, limit: 300 }))
      : pick(await bridge.apiGet("users", { limit: 300 }));
    allUsers = (data && data.users) || [];
    renderUsers(kw);
  } catch (e) {
    usersEl.innerHTML = `<li class="muted">加载失败：${esc(e.message)}</li>`;
  }
}

function renderUsers(kw) {
  kw = (kw != null ? kw : searchEl.value).trim();
  const list = kw ? allUsers.filter((u) => String(u.user_id).includes(kw)) : allUsers;
  usersEl.innerHTML = "";
  if (!list.length) {
    usersEl.innerHTML = '<li class="muted">无匹配</li>';
    return;
  }
  for (const u of list) {
    const li = document.createElement("li");
    li.dataset.uid = u.user_id;
    if (u.user_id === currentUid) li.classList.add("active");
    const tag = u.matched === "fact" ? " · 命中事实" : u.matched === "event" ? " · 命中事件" : "";
    const head = document.createElement("div");
    head.className = "uhead";
    const name = document.createElement("span");
    name.textContent = u.user_id;
    const del = document.createElement("button");
    del.className = "mini-danger";
    del.textContent = "删除";
    del.onclick = (ev) => {
      ev.stopPropagation();
      if (del.dataset.confirm !== "1") {
        del.dataset.confirm = "1";
        del.textContent = "确认删除?";
        return;
      }
      deleteUser(u.user_id);
    };
    head.append(name, del);
    const sub2 = document.createElement("div");
    sub2.className = "sub";
    sub2.textContent = `事件 ${u.events ?? 0} · 冷层 ${u.chunks ?? 0}${tag} · ${fmt(u.last_ts)}`;
    li.append(head, sub2);
    li.onclick = () => loadDetail(u.user_id);
    usersEl.appendChild(li);
  }
}

async function deleteUser(uid) {
  try {
    await api("forget", { user_id: uid });
    toast("已删除 " + uid + " 的记忆");
    if (currentUid === uid) {
      currentUid = null;
      detailEl.innerHTML = '<div class="muted">已删除</div>';
    }
    loadUsers();
  } catch (e) {}
}

async function loadDetail(uid) {
  currentUid = uid;
  for (const c of usersEl.children) c.classList.toggle("active", c.dataset.uid === uid);
  detailEl.innerHTML = '<div class="muted">加载中…</div>';
  try {
    const data = pick(await bridge.apiGet("user", { user_id: uid }));
    renderDetail(uid, (data && data.user) || {}, (data && data.events) || []);
  } catch (e) {
    detailEl.innerHTML = `<div class="muted">加载失败：${esc(e.message)}</div>`;
  }
}

function factRow(uid, f) {
  const row = document.createElement("div");
  row.className = "fact";
  const label = `${f.key ? esc(f.key) + "：" : ""}${esc(f.value)}<span class="tag">imp=${f.importance ?? "-"}${f.pinned ? " · pinned" : ""}</span>`;
  row.innerHTML = `<div class="fact-text">${label}</div>`;
  const btns = document.createElement("div");
  btns.className = "row-btns";
  const bEdit = document.createElement("button");
  bEdit.textContent = "编辑";
  bEdit.onclick = () => editFactForm(row, uid, f);
  const bDel = document.createElement("button");
  bDel.textContent = "删除";
  bDel.onclick = () => confirmInline(row, "确认删除该事实？", () => delFact(uid, f.value));
  btns.append(bEdit, bDel);
  row.appendChild(btns);
  return row;
}

function editFactForm(row, uid, f) {
  row.innerHTML = `
    <div class="edit-grid">
      <input class="k" placeholder="键（可空）" value="${esc(f.key || "")}" />
      <input class="v" placeholder="值" value="${esc(f.value || "")}" />
      <input class="imp" type="number" min="0" max="100" value="${f.importance ?? 60}" />
      <input class="tags" placeholder="标签（逗号分隔）" value="${esc((f.tags || []).join(","))}" />
      <label class="pin"><input type="checkbox" ${f.pinned ? "checked" : ""}/> pinned</label>
    </div>`;
  const btns = document.createElement("div");
  btns.className = "row-btns";
  const bSave = document.createElement("button");
  bSave.textContent = "保存";
  bSave.onclick = async () => {
    const patch = {
      user_id: uid,
      old_value: f.value,
      key: row.querySelector(".k").value,
      value: row.querySelector(".v").value,
      importance: Number(row.querySelector(".imp").value),
      tags: row.querySelector(".tags").value,
      pinned: row.querySelector(".pin input").checked,
    };
    await api("fact/update", patch);
    toast("已保存");
    loadDetail(uid);
  };
  const bCancel = document.createElement("button");
  bCancel.textContent = "取消";
  bCancel.onclick = () => loadDetail(uid);
  btns.append(bSave, bCancel);
  row.appendChild(btns);
}

function confirmInline(container, text, onYes) {
  container.innerHTML = "";
  const span = document.createElement("div");
  span.className = "fact-text";
  span.textContent = text;
  const btns = document.createElement("div");
  btns.className = "row-btns";
  const y = document.createElement("button");
  y.textContent = "确认";
  y.className = "danger";
  y.onclick = onYes;
  const n = document.createElement("button");
  n.textContent = "取消";
  n.onclick = () => loadDetail(currentUid);
  btns.append(y, n);
  container.append(span, btns);
}

async function delFact(uid, value) {
  await api("fact/delete", { user_id: uid, value });
  toast("已删除事实");
  loadDetail(uid);
}

function addFactForm(uid) {
  const row = document.createElement("div");
  row.className = "fact";
  row.innerHTML = `
    <div class="edit-grid">
      <input class="k" placeholder="键（可空）" />
      <input class="v" placeholder="值（必填）" />
      <input class="imp" type="number" min="0" max="100" value="60" />
      <input class="tags" placeholder="标签（逗号分隔）" />
      <label class="pin"><input type="checkbox" checked/> pinned</label>
    </div>`;
  const btns = document.createElement("div");
  btns.className = "row-btns";
  const bSave = document.createElement("button");
  bSave.textContent = "保存";
  bSave.onclick = async () => {
    const v = row.querySelector(".v").value.trim();
    if (!v) {
      toast("值不能为空");
      return;
    }
    await api("fact/add", {
      user_id: uid,
      key: row.querySelector(".k").value,
      value: v,
      importance: Number(row.querySelector(".imp").value),
      tags: row.querySelector(".tags").value,
      pinned: row.querySelector(".pin input").checked,
    });
    toast("已新增事实");
    loadDetail(uid);
  };
  const bCancel = document.createElement("button");
  bCancel.textContent = "取消";
  bCancel.onclick = () => loadDetail(uid);
  btns.append(bSave, bCancel);
  row.appendChild(btns);
  return row;
}

function renderDetail(uid, user, events) {
  detailEl.innerHTML = "";
  const head = document.createElement("div");
  head.innerHTML = `<div class="uid">用户 ${esc(uid)}</div>
    <div class="muted">冷层块 ${user.chunks ?? 0} · 上次整理 ${fmt(user.last_consolidate_ts)}</div>`;
  const delUserBtn = document.createElement("button");
  delUserBtn.textContent = "删除该用户全部记忆";
  delUserBtn.className = "danger";
  delUserBtn.onclick = () => confirmInline(head, "确认删除该用户的全部记忆？不可恢复。", async () => {
    await api("forget", { user_id: uid });
    toast("已删除该用户记忆");
    currentUid = null;
    loadUsers();
    detailEl.innerHTML = '<div class="muted">已删除</div>';
  });
  head.appendChild(delUserBtn);
  detailEl.appendChild(head);

  const h3f = document.createElement("h3");
  h3f.textContent = "事实";
  detailEl.appendChild(h3f);
  const factsBox = document.createElement("div");
  const facts = user.facts || [];
  facts.forEach((f) => factsBox.appendChild(factRow(uid, f)));
  const addBtn = document.createElement("button");
  addBtn.textContent = "＋ 新增事实";
  addBtn.onclick = () => factsBox.appendChild(addFactForm(uid));
  detailEl.append(factsBox, addBtn);

  const h3c = document.createElement("h3");
  h3c.textContent = "语境（inject_text）";
  detailEl.appendChild(h3c);
  const ta = document.createElement("textarea");
  ta.rows = 4;
  ta.value = user.inject_text || "";
  const saveCtx = document.createElement("button");
  saveCtx.textContent = "保存语境";
  saveCtx.onclick = async () => {
    await api("inject/save", { user_id: uid, text: ta.value });
    toast("已保存语境");
  };
  detailEl.append(ta, saveCtx);

  const h3e = document.createElement("h3");
  h3e.textContent = "最近事件";
  detailEl.appendChild(h3e);
  if (!events.length) {
    const d = document.createElement("div");
    d.className = "muted";
    d.textContent = "无";
    detailEl.appendChild(d);
  } else {
    const table = document.createElement("table");
    table.innerHTML =
      "<thead><tr><th>时间</th><th>场景</th><th>内容</th><th>重要度</th><th>迁移</th><th></th></tr></thead>";
    const tb = document.createElement("tbody");
    for (const e of events) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${fmt(e.ts)}</td><td>${esc(e.scene)}</td><td>${esc(e.content)}</td><td>${e.importance ?? ""}</td><td>${e.migrated ? "✓" : ""}</td>`;
      const td = document.createElement("td");
      const bd = document.createElement("button");
      bd.textContent = "删除";
      bd.onclick = () => confirmInline(td, "删除？", async () => {
        await api("event/delete", { user_id: uid, id: e.id });
        toast("已删除事件");
        loadDetail(uid);
      });
      td.appendChild(bd);
      tr.appendChild(td);
      tb.appendChild(tr);
    }
    table.appendChild(tb);
    detailEl.appendChild(table);
  }
}

document.getElementById("refresh").onclick = () => {
  loadUsers();
  renderSettings();
  if (currentUid) loadDetail(currentUid);
};

// 主题切换（蓝白 <-> 深色）
const themeBtn = document.getElementById("theme");
function applyTheme(dark) {
  document.documentElement.setAttribute("data-theme", dark ? "dark" : "light");
  if (themeBtn) themeBtn.textContent = dark ? "☀️" : "🌙";
}
let isDark = false;
try {
  isDark = !!(bridge.getContext() && bridge.getContext().isDark);
} catch {}
applyTheme(isDark);
if (themeBtn) {
  themeBtn.onclick = () => {
    isDark = !isDark;
    applyTheme(isDark);
  };
}
searchEl.addEventListener("input", () => {
  clearTimeout(searchEl._t);
  searchEl._t = setTimeout(loadUsers, 300);
});

async function renderSettings() {
  const box = document.getElementById("settings");
  if (!box) return;
  try {
    const data = pick(await bridge.apiGet("providers"));
    const embs = (data && data.embeddings) || [];
    const rrs = (data && data.reranks) || [];
    const cur = (data && data.current) || {};
    const opt = (o, sel) =>
      `<option value="${esc(o.id)}" ${o.id === sel ? "selected" : ""}>${esc(o.label)}</option>`;
    box.innerHTML = `
      <h2>模型设置</h2>
      <label>嵌入模型</label>
      <select id="sel-emb"><option value="">（默认/自动）</option>${embs.map((o) => opt(o, cur.embedding_provider_id)).join("")}</select>
      <label>重排模型</label>
      <select id="sel-rr"><option value="">（默认/自动）</option>${rrs.map((o) => opt(o, cur.rerank_provider_id)).join("")}</select>
      <button id="save-prov">保存模型设置</button>
      <h2>备份与迁移</h2>
      <button id="exp">导出记忆(JSON)</button>
      <label class="file-label">导入记忆(JSON)<input id="imp" type="file" accept=".json,application/json" /></label>
      <button id="rebuild" class="danger">重建数据库</button>`;
    box.querySelector("#save-prov").onclick = async () => {
      await api("providers/save", {
        embedding_provider_id: box.querySelector("#sel-emb").value,
        rerank_provider_id: box.querySelector("#sel-rr").value,
      });
      toast("已保存模型设置");
    };
    box.querySelector("#exp").onclick = async () => {
      try {
        const data = pick(await bridge.apiGet("export"));
        const blob = new Blob([JSON.stringify(data)], { type: "application/json" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = "user_memory_export.json";
        a.click();
        URL.revokeObjectURL(a.href);
        toast("已导出");
      } catch (e) {}
    };
    box.querySelector("#imp").onchange = async (ev) => {
      const f = ev.target.files[0];
      if (!f) return;
      const text = await f.text();
      let data;
      try {
        data = JSON.parse(text);
      } catch {
        toast("JSON 解析失败");
        return;
      }
      await api("import", { data });
      toast("已导入");
      loadUsers();
    };
    box.querySelector("#rebuild").onclick = async () => {
      const btn = box.querySelector("#rebuild");
      if (btn.dataset.confirm !== "1") {
        btn.dataset.confirm = "1";
        btn.textContent = "确认重建？(会先自动导出)";
        return;
      }
      try {
        const data = pick(await bridge.apiGet("export"));
        const blob = new Blob([JSON.stringify(data)], { type: "application/json" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = "user_memory_before_rebuild.json";
        a.click();
        URL.revokeObjectURL(a.href);
      } catch (e) {}
      await api("rebuild", {});
      toast("数据库已重建");
      currentUid = null;
      detailEl.innerHTML = '<div class="muted">已重建</div>';
      loadUsers();
    };
  } catch (e) {
    box.innerHTML = '<h2>模型设置</h2><div class="muted">加载失败</div>';
  }
}
try {
  const meta = pick(await bridge.apiGet("meta"));
  if (meta && meta.version) {
    version = meta.version;
    const h = document.querySelector("header h1");
    if (h) h.textContent = `用户记忆 v${version}`;
  }
} catch {}
await loadUsers();
renderSettings();
