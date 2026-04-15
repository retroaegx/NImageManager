import { $ } from "./lib/dom.js";
import { apiFetch, apiJson, safeJson } from "./lib/http.js";
import { bindUserMenu } from "./lib/userMenu.js";
import { loadCurrentUser, logoutAndRedirect, isAdminRole } from "./lib/session.js";
import { t, setLocalePreference } from "./lib/i18n.js";


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
      const langSel = $("uiLanguage");
      if(langSel) langSel.value = String(me?.ui_language || "auto");
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
  btn.textContent = on ? t("common.on") : t("common.off");
}

function renderAccountDelete(me){
  const btn = $("deleteMyAccountBtn");
  const note = $("accountDeleteNote");
  if(!btn || !note) return;

  const isMaster = String(me?.role || "") === "master";
  btn.disabled = isMaster;
  note.textContent = isMaster
    ? t("settings.account.delete.master_blocked")
    : t("settings.account.delete.note");
}

function confirmMyAccountDelete(username){
  const name = String(username || "").trim();
  if(!confirm(t("admin.account.delete.first_confirm", { name }))) return false;
  return confirm(t("common.confirm_delete"));
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
      setStatus(t("status.updating"), null);
      const j = await updateMySettings({ share_works: on ? 1 : 0 });
      setToggle(btnW, !!Number(j?.share_works || 0));
      setStatus(t("status.updated"), "ok");
    }catch(_e){
      setToggle(btnW, prev);
      setStatus(t("status.update_failed"), "error");
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
      setStatus(t("status.updating"), null);
      const j = await updateMySettings({ share_bookmarks: on ? 1 : 0 });
      setToggle(btnB, !!Number(j?.share_bookmarks || 0));
      setStatus(t("status.updated"), "ok");
    }catch(_e){
      setToggle(btnB, prev);
      setStatus(t("status.update_failed"), "error");
    }finally{
      btnB.disabled = false;
    }
  });

  const langSel = $("uiLanguage");
  if(langSel){
    langSel.addEventListener("change", async () => {
      const nextLang = String(langSel.value || "auto");
      const prevLang = String(ME?.ui_language || "auto");
      langSel.disabled = true;
      try{
        setStatus(t("status.updating"), null);
        const j = await updateMySettings({ ui_language: nextLang });
        ME = { ...(ME || {}), ...(j || {}), ui_language: String(j?.ui_language || nextLang) };
        await setLocalePreference(String(ME.ui_language || nextLang), { apply: true });
        setStatus(t("status.updated"), "ok");
      }catch(_e){
        langSel.value = prevLang;
        setStatus(t("status.update_failed"), "error");
      }finally{
        langSel.disabled = false;
      }
    });
  }
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
    meta.textContent = t("count.items", { count: cnt.toLocaleString() }) + (Number(l.is_default || 0) ? `  ${t("label.default")}` : "");

    left.appendChild(nm);
    left.appendChild(meta);

    const btns = document.createElement("div");
    btns.className = "bmManageBtns";

    const renameBtn = document.createElement("button");
    renameBtn.className = "ghostBtn";
    renameBtn.textContent = t("settings.list.rename");
    renameBtn.addEventListener("click", async () => {
      const cur = String(l.name || "");
      const nxt = prompt(t("settings.list.name"), cur);
      if(nxt === null) return;
      const name = String(nxt || "").trim();
      if(!name) return;
      try{
        setStatus(t("status.updating"), null);
        const r = await apiFetch(API.bookmarkList(l.id), {
          method: "PATCH",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({ name }),
        });
        const j = await apiJson(r);
        renderLists(j);
        setStatus(t("status.updated"), "ok");
      }catch(_e){
        setStatus(t("status.update_failed"), "error");
      }
    });

    const delBtn = document.createElement("button");
    delBtn.className = "dangerBtn";
    delBtn.textContent = t("common.delete");
    delBtn.disabled = !!Number(l.is_default || 0);
    delBtn.addEventListener("click", async () => {
      if(Number(l.is_default || 0)){
        alert(t("settings.list.default_delete_blocked"));
        return;
      }
      const nm = String(l.name || "");
      if(!confirm(t("settings.list.delete.confirm", { name: nm }))) return;
      if(!confirm(t("common.confirm_delete"))) return;
      try{
        setStatus(t("status.deleting"), null);
        const r = await apiFetch(API.bookmarkList(l.id), { method: "DELETE" });
        const j = await apiJson(r);
        renderLists(j);
        setStatus(t("status.deleted"), "ok");
      }catch(_e){
        setStatus(t("status.delete_failed"), "error");
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
    setStatus(t("status.deleting"), null);
    const r = await apiFetch(API.deleteMe, { method: "DELETE" });
    if(!r.ok){
      const j = await safeJson(r);
      throw new Error(j?.detail || t("status.delete_failed"));
    }
    await logoutAndRedirect(API.logout);
  }catch(e){
    renderAccountDelete(ME);
    setStatus(e?.message || t("status.delete_failed"), "error");
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
      setStatus(t("status.creating"), null);
      const r = await apiFetch(API.bookmarkLists, {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ name }),
      });
      const j = await apiJson(r);
      if(input) input.value = "";
      renderLists(j);
      setStatus(t("status.created"), "ok");
    }catch(_e){
      setStatus(t("status.create_failed"), "error");
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
    setStatus(t("status.load_failed"), "error");
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
