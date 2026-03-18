import { $ } from "./lib/dom.js?v=20260307_01";
import { apiFetch, apiJson, safeJson } from "./lib/http.js?v=20260307_01";
import { bindUserMenu } from "./lib/userMenu.js?v=20260312_02";
import { loadCurrentUser, logoutAndRedirect, isAdminRole } from "./lib/session.js?v=20260307_01";

const SettingsPage = (window.NIMSettings && typeof window.NIMSettings === "object") ? window.NIMSettings : {};
let ME = null;

const API = {
  me: "/api/me",
  meSettings: "/api/me/settings",
  deleteMe: "/api/me",
  logout: "/api/auth/logout",
  pwLink: "/api/auth/password_link",
  bookmarkLists: "/api/bookmarks/lists",
  bookmarkList: (id) => `/api/bookmarks/lists/${id}`,
};

// Shared DOM / HTTP helpers are imported from ./lib/*.js

function setStatus(msg, cls){
  const el = $("settingsStatus");
  if(!el) return;
  el.textContent = msg || "";
  el.className = "small" + (cls ? (" " + cls) : "");
}

async function doLogout(){
  await logoutAndRedirect(API.logout);
}

async function loadMe(){
  return await loadCurrentUser({
    endpoint: API.me,
    onLoaded: (me) => {
      ME = me;
      const sw = !!Number(me?.share_works || 0);
      const sb = !!Number(me?.share_bookmarks || 0);
      setToggle($("toggleShareWorks"), sw);
      setToggle($("toggleShareBookmarks"), sb);
      renderAccountDelete(me);
      bindUserMenu({
        logoutEndpoint: API.logout,
        passwordLinkEndpoint: API.pwLink,
        showAdmin: isAdminRole(String(me?.role || "user")),
        showMaintenance: isAdminRole(String(me?.role || "user")),
      });
    },
  });
}


function setToggle(btn, on){
  if(!btn) return;
  btn.classList.toggle("on", !!on);
  btn.classList.toggle("off", !on);
  btn.textContent = on ? "ON" : "OFF";
}

function renderAccountDelete(me){
  const btn = $("deleteMyAccountBtn");
  const note = $("accountDeleteNote");
  if(!btn || !note) return;

  const isMaster = String(me?.role || "") === "master";
  btn.disabled = isMaster;
  note.textContent = isMaster
    ? "master アカウントは削除できません。"
    : "作品、ブックマーク、作者登録、共有ブックマーク登録、関連データも削除します。";
}

function confirmMyAccountDelete(username){
  const name = String(username || "").trim();
  if(!confirm(`アカウント「${name}」を削除します。\n作品、ブックマーク、作者登録、共有ブックマーク登録、関連データも削除します。元に戻せません。`)) return false;
  return confirm("本当に削除しますか？");
}


async function updateMySettings(patch){
  const body = JSON.stringify(patch || {});
  // Some environments/proxies may reject PATCH. Try PATCH first, then fall back to POST.
  try{
    const r = await apiFetch(API.meSettings, {
      method: "PATCH",
      headers: {"Content-Type":"application/json"},
      body,
    });
    return await apiJson(r);
  }catch(_e){
    const r = await apiFetch(API.meSettings, {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body,
    });
    return await apiJson(r);
  }
}

function bindShareToggles(){
  const btnW = $("toggleShareWorks");
  const btnB = $("toggleShareBookmarks");

  const bind = (btn, fn) => {
    if(!btn) return;
    // Prefer onclick as the most compatible path, but keep addEventListener as well.
    btn.onclick = fn;
  };

  bind(btnW, async (e) => {
    e.preventDefault();
    const prev = btnW.classList.contains("on");
    const on = !prev;
    // optimistic UI
    setToggle(btnW, on);
    btnW.disabled = true;
    try{
      setStatus("更新中…", null);
      const j = await updateMySettings({ share_works: on ? 1 : 0 });
      setToggle(btnW, !!Number(j?.share_works || 0));
      setStatus("更新しました", "ok");
    }catch(_e){
      setToggle(btnW, prev);
      setStatus("更新に失敗しました", "error");
    }finally{
      btnW.disabled = false;
    }
  });

  bind(btnB, async (e) => {
    e.preventDefault();
    const prev = btnB.classList.contains("on");
    const on = !prev;
    setToggle(btnB, on);
    btnB.disabled = true;
    try{
      setStatus("更新中…", null);
      const j = await updateMySettings({ share_bookmarks: on ? 1 : 0 });
      setToggle(btnB, !!Number(j?.share_bookmarks || 0));
      setStatus("更新しました", "ok");
    }catch(_e){
      setToggle(btnB, prev);
      setStatus("更新に失敗しました", "error");
    }finally{
      btnB.disabled = false;
    }
  });
}

function renderLists(data){
  const wrap = $("bmManageList");
  if(!wrap) return;
  wrap.innerHTML = "";

  const lists = (data && data.lists) ? data.lists : [];

  lists.forEach((l) => {
    const row = document.createElement("div");
    row.className = "bmManageRow";

    const left = document.createElement("div");
    left.className = "bmManageLeft";

    const nm = document.createElement("div");
    nm.className = "bmManageName";
    nm.textContent = String(l.name || "");

    const meta = document.createElement("div");
    meta.className = "bmManageMeta";
    const cnt = Number(l.count || 0);
    meta.textContent = `${cnt.toLocaleString()} 件` + (Number(l.is_default || 0) ? "  (default)" : "");

    left.appendChild(nm);
    left.appendChild(meta);

    const btns = document.createElement("div");
    btns.className = "bmManageBtns";

    const renameBtn = document.createElement("button");
    renameBtn.className = "ghostBtn";
    renameBtn.textContent = "名前変更";
    renameBtn.addEventListener("click", async () => {
      const cur = String(l.name || "");
      const nxt = prompt("リスト名", cur);
      if(nxt === null) return;
      const name = String(nxt || "").trim();
      if(!name) return;
      try{
        setStatus("更新中…", null);
        const r = await apiFetch(API.bookmarkList(l.id), {
          method: "PATCH",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({ name }),
        });
        const j = await apiJson(r);
        renderLists(j);
        setStatus("更新しました", "ok");
      }catch(_e){
        setStatus("更新に失敗しました", "error");
      }
    });

    const delBtn = document.createElement("button");
    delBtn.className = "dangerBtn";
    delBtn.textContent = "削除";
    delBtn.disabled = !!Number(l.is_default || 0);
    delBtn.addEventListener("click", async () => {
      if(Number(l.is_default || 0)){
        alert("default は削除できません");
        return;
      }
      const nm = String(l.name || "");
      if(!confirm(`リスト「${nm}」を削除します。元に戻せません。よろしいですか？`)) return;
      if(!confirm("本当に削除しますか？")) return;
      try{
        setStatus("削除中…", null);
        const r = await apiFetch(API.bookmarkList(l.id), { method: "DELETE" });
        const j = await apiJson(r);
        renderLists(j);
        setStatus("削除しました", "ok");
      }catch(_e){
        setStatus("削除に失敗しました", "error");
      }
    });

    btns.appendChild(renameBtn);
    btns.appendChild(delBtn);

    row.appendChild(left);
    row.appendChild(btns);
    wrap.appendChild(row);
  });
}

async function refreshLists(){
  const r = await apiFetch(API.bookmarkLists);
  const j = await apiJson(r);
  renderLists(j);
}

async function deleteMyAccount(){
  const btn = $("deleteMyAccountBtn");
  if(!btn || btn.disabled) return;
  if(!confirmMyAccountDelete(ME?.username)) return;

  btn.disabled = true;
  try{
    setStatus("削除中…", null);
    const r = await apiFetch(API.deleteMe, { method: "DELETE" });
    if(!r.ok){
      const j = await safeJson(r);
      throw new Error(j?.detail || "削除に失敗しました");
    }
    await logoutAndRedirect(API.logout);
  }catch(e){
    renderAccountDelete(ME);
    setStatus(e?.message || "削除に失敗しました", "error");
  }
}

function bindCreate(){
  const btn = $("createListBtn");
  btn?.addEventListener("click", async (e) => {
    e.preventDefault();
    const input = $("newListName");
    const name = String(input?.value || "").trim();
    if(!name) return;
    try{
      setStatus("作成中…", null);
      const r = await apiFetch(API.bookmarkLists, {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ name }),
      });
      const j = await apiJson(r);
      if(input) input.value = "";
      renderLists(j);
      setStatus("作成しました", "ok");
    }catch(_e){
      setStatus("作成に失敗しました", "error");
    }
  });
}

async function boot(){
  if(SettingsPage._booted) return;
  SettingsPage._booted = true;
  bindCreate();
  bindShareToggles();
  $("deleteMyAccountBtn")?.addEventListener("click", deleteMyAccount);
  try{
    await loadMe();
  }catch(_e){
    return;
  }
  try{
    await refreshLists();
  }catch(_e){
    setStatus("読み込みに失敗しました", "error");
  }
}

// Expose minimal hooks (used by potential inline fallbacks).
SettingsPage.boot = boot;
window.NIMSettings = SettingsPage;

// Run as soon as possible.
if(document.readyState === "loading"){
  document.addEventListener("DOMContentLoaded", boot);
}else{
  boot();
}
