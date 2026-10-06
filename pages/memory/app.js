const bridge = window.AstrBotPluginPage;
await bridge.ready();

const usersEl = document.getElementById("users");
const detailEl = document.getElementById("detail");
const searchEl = document.getElementById("search");

let allUsers = [];

function pick(res) {
  return res && res.data !== undefined ? res.data : res;
}
function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c],
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

async function loadUsers() {
  usersEl.innerHTML = '<li class="muted">加载中…</li>';
  try {
    const data = pick(await bridge.apiGet("users", { limit: 300 }));
    allUsers = (data && data.users) || [];
    renderUsers();
  } catch (e) {
    usersEl.innerHTML = `<li class="muted">加载失败：${esc(e.message)}</li>`;
  }
}

function renderUsers() {
  const kw = searchEl.value.trim();
  const list = kw ? allUsers.filter((u) => String(u.user_id).includes(kw)) : allUsers;
  usersEl.innerHTML = "";
  if (!list.length) {
    usersEl.innerHTML = '<li class="muted">暂无记忆</li>';
    return;
  }
  for (const u of list) {
    const li = document.createElement("li");
    li.dataset.uid = u.user_id;
    li.innerHTML = `${esc(u.user_id)}<div class="sub">事件 ${u.events ?? 0} · 冷层 ${u.chunks ?? 0} · ${fmt(u.last_ts)}</div>`;
    li.onclick = () => loadDetail(u.user_id);
    usersEl.appendChild(li);
  }
}

async function loadDetail(uid) {
  for (const c of usersEl.children) c.classList.toggle("active", c.dataset.uid === uid);
  detailEl.innerHTML = '<div class="muted">加载中…</div>';
  try {
    const data = pick(await bridge.apiGet("user", { user_id: uid }));
    const user = (data && data.user) || {};
    const events = (data && data.events) || [];
    const facts = user.facts || [];
    let html = `<div class="uid">用户 ${esc(uid)}</div>`;
    html += `<div class="muted">冷层块 ${user.chunks ?? 0} · 上次整理 ${fmt(user.last_consolidate_ts)}</div>`;
    html += "<h3>事实</h3>";
    html += facts.length
      ? "<ul>" +
        facts
          .map(
            (f) =>
              `<li>${esc(f.key ? f.key + "：" : "")}${esc(f.value)}<span class="tag">imp=${f.importance ?? "-"}${f.pinned ? " · pinned" : ""}</span></li>`,
          )
          .join("") +
        "</ul>"
      : '<div class="muted">无</div>';
    html += "<h3>语境（inject_text）</h3><pre>" + esc(user.inject_text || "（无）") + "</pre>";
    html += "<h3>最近事件</h3>";
    html += events.length
      ? "<table><thead><tr><th>时间</th><th>场景</th><th>内容</th><th>重要度</th><th>迁移</th></tr></thead><tbody>" +
        events
          .map(
            (e) =>
              `<tr><td>${fmt(e.ts)}</td><td>${esc(e.scene)}</td><td>${esc(e.content)}</td><td>${e.importance ?? ""}</td><td>${e.migrated ? "✓" : ""}</td></tr>`,
          )
          .join("") +
        "</tbody></table>"
      : '<div class="muted">无</div>';
    detailEl.innerHTML = html;
  } catch (e) {
    detailEl.innerHTML = `<div class="muted">加载失败：${esc(e.message)}</div>`;
  }
}

document.getElementById("refresh").onclick = loadUsers;
searchEl.addEventListener("input", renderUsers);
try {
  const meta = pick(await bridge.apiGet("meta"));
  if (meta && meta.version) {
    const h = document.querySelector("header h1");
    if (h) h.textContent = `用户记忆 v${meta.version}`;
  }
} catch {}
await loadUsers();
