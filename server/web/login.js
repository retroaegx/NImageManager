import { initI18n, t } from "./lib/i18n.js";

const API = {
  login: "/api/auth/login",
  status: "/api/auth/setup_status",
  embedSession: "/api/ext/session",
};

const EMBED_MESSAGE_TYPES = {
  authRequired: "NIM_EMBED_AUTH_REQUIRED",
  ready: "NIM_EMBED_READY",
};

const EMBED_AUTH_NAV_FLAG = "nim_embed_auth_nav_attempted_v2";

function $(id){ return document.getElementById(id); }

function isEmbedded(){
  try { return window.self !== window.top; } catch (_error) { return true; }
}

function absoluteUrl(path){ return new URL(path, location.origin).toString(); }
function postParentMessage(type, extra={}){ if(isEmbedded()) window.parent.postMessage({ type, ...extra }, "*"); }
function getEmbedNavAttempted(){ try { return window.sessionStorage.getItem(EMBED_AUTH_NAV_FLAG) === "1"; } catch { return false; } }
function setEmbedNavAttempted(value){ try { value ? window.sessionStorage.setItem(EMBED_AUTH_NAV_FLAG, "1") : window.sessionStorage.removeItem(EMBED_AUTH_NAV_FLAG); } catch {} }
async function hasStorageAccessSafe(){ if(typeof document.hasStorageAccess !== "function") return null; try { return await document.hasStorageAccess(); } catch { return null; } }
async function requestStorageAccessSafe(){ if(typeof document.requestStorageAccess !== "function") throw new Error(t("login.embed.storage_not_supported")); return await document.requestStorageAccess(); }
function setEmbedMessage(message, kind="info"){ const node = $("embedLoginText"); if(node){ node.textContent = message || ""; node.dataset.kind = kind; } }

async function fetchEmbedSession(){
  try {
    const response = await fetch(API.embedSession, { method: "GET", credentials: "include", cache: "no-store", headers: { Accept: "application/json" } });
    const data = await response.json().catch(() => ({}));
    return { ok: Boolean(response.ok && data && data.ok), response, data };
  } catch (error) {
    return { ok: false, response: null, data: {}, error };
  }
}

function navigateEmbeddedHome(message){
  setEmbedNavAttempted(true);
  setEmbedMessage(message, "success");
  window.setTimeout(() => location.replace(absoluteUrl("/")), 80);
}

async function activateStorageAccessAndRefresh({
  successMessage,
  pendingMessage = t("login.embed.rechecking"),
  failureMessage = t("login.embed.enable_failed"),
}={}){
  await requestStorageAccessSafe();
  setEmbedMessage(pendingMessage, "info");
  const ready = await refreshEmbeddedLoginState({ redirectOnSuccess: true, tryAutoActivate: false });
  if(ready) return true;
  const hasStorageAccess = await hasStorageAccessSafe();
  if(hasStorageAccess === true){
    navigateEmbeddedHome(successMessage || t("login.embed.enabled_and_reloading"));
    return true;
  }
  setEmbedMessage(failureMessage, "error");
  return false;
}

async function tryActivateGrantedStorageAccess(){
  if(typeof document.requestStorageAccess !== "function") return false;
  const hasStorageAccess = await hasStorageAccessSafe();
  if(hasStorageAccess === true) return false;
  try {
    return await activateStorageAccessAndRefresh({
      successMessage: t("login.embed.enabled_and_reloading"),
      pendingMessage: t("login.embed.applying_storage_access"),
      failureMessage: t("login.embed.auto_enable_failed"),
    });
  } catch { return false; }
}

async function refreshEmbeddedLoginState({ redirectOnSuccess=false, tryAutoActivate=false }={}){
  if(!isEmbedded()) return false;
  if(tryAutoActivate){
    const activated = await tryActivateGrantedStorageAccess();
    if(activated) return false;
  }
  const [session, hasStorageAccess] = await Promise.all([fetchEmbedSession(), hasStorageAccessSafe()]);
  if(session.ok){
    setEmbedNavAttempted(false);
    postParentMessage(EMBED_MESSAGE_TYPES.ready, { origin: location.origin, url: absoluteUrl("/") });
    if(redirectOnSuccess || location.pathname.endsWith("/login.html") || location.pathname === "/login.html") location.replace("/");
    return true;
  }
  if(hasStorageAccess === true && !getEmbedNavAttempted()){
    navigateEmbeddedHome(t("login.embed.storage_access_enabled"));
    return false;
  }
  const loginUrl = absoluteUrl("/login.html");
  postParentMessage(EMBED_MESSAGE_TYPES.authRequired, { loginUrl, origin: location.origin, storageApiSupported: typeof document.requestStorageAccess === "function", hasStorageAccess });
  const messageParts = [t("login.embed.use_top_level_login")];
  if(typeof document.requestStorageAccess === "function") messageParts.push(t("login.embed.grant_access_after_login"));
  else messageParts.push(t("login.embed.storage_api_missing"));
  if(hasStorageAccess === true) messageParts.push(t("login.embed.cookie_hint"));
  setEmbedMessage(messageParts.join(" "), "info");
  return false;
}

async function doLogin(){
  const username = $("loginUser").value.trim();
  const password = $("loginPass").value;
  $("loginErr").textContent = "";
  if(!username || !password){ $("loginErr").textContent = t("common.required"); return; }
  try{
    const r = await fetch(API.login, { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({username, password}), credentials: "include" });
    if(!r.ok){ $("loginErr").textContent = t("login.failed"); return; }
    await r.text().catch(() => "");
    location.href = "/";
  }catch{ $("loginErr").textContent = t("common.connection_failed"); }
}

function setupFirstTimeRedirect(){
  fetch(API.status, { credentials: "include" }).then((r) => r.json()).then((j) => { if(j && j.needs_setup) location.replace("/setup.html"); }).catch(() => {});
}

async function initEmbeddedLogin(){
  if(!isEmbedded()) return;
  $("embedLoginBox")?.classList.remove("hidden");
  $("loginFormArea")?.classList.add("hidden");
  $("embedOpenTopLogin")?.addEventListener("click", () => window.open(absoluteUrl("/login.html"), "_blank", "noopener"));
  $("embedGrantAccess")?.addEventListener("click", async () => {
    try { await activateStorageAccessAndRefresh(); } catch (error) { setEmbedMessage(error?.message || t("common.failed"), "error"); }
  });
  $("embedRetry")?.addEventListener("click", () => refreshEmbeddedLoginState({ redirectOnSuccess: true, tryAutoActivate: true }));
  await refreshEmbeddedLoginState({ redirectOnSuccess: false, tryAutoActivate: true });
}

async function init(){
  await initI18n(globalThis.__NIM_BOOTSTRAP__?.user?.ui_language || "auto");
  setupFirstTimeRedirect();
  $("loginBtn")?.addEventListener("click", doLogin);
  $("loginPass")?.addEventListener("keydown", (e) => { if(e.key === "Enter") doLogin(); });
  $("loginUser")?.focus();
  await initEmbeddedLogin();
}

init();
