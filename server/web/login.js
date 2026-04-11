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
  try {
    return window.self !== window.top;
  } catch (_error) {
    return true;
  }
}

function absoluteUrl(path){
  return new URL(path, location.origin).toString();
}

function postParentMessage(type, extra={}){
  if(!isEmbedded()) return;
  try {
    window.parent.postMessage({ type, ...extra }, "*");
  } catch (_error) {}
}

function getEmbedNavAttempted(){
  try {
    return window.sessionStorage.getItem(EMBED_AUTH_NAV_FLAG) === "1";
  } catch (_error) {
    return false;
  }
}

function setEmbedNavAttempted(value){
  try {
    if(value) window.sessionStorage.setItem(EMBED_AUTH_NAV_FLAG, "1");
    else window.sessionStorage.removeItem(EMBED_AUTH_NAV_FLAG);
  } catch (_error) {}
}

async function hasStorageAccessSafe(){
  if(typeof document.hasStorageAccess !== "function") return null;
  try {
    return await document.hasStorageAccess();
  } catch (_error) {
    return null;
  }
}

async function requestStorageAccessSafe(){
  if(typeof document.requestStorageAccess !== "function"){
    throw new Error("このブラウザでは埋め込みストレージ許可に対応していません");
  }
  return await document.requestStorageAccess();
}

function setEmbedMessage(message, kind="info"){
  const node = $("embedLoginText");
  if(!node) return;
  node.textContent = message || "";
  node.dataset.kind = kind;
}

async function fetchEmbedSession(){
  try {
    const response = await fetch(API.embedSession, {
      method: "GET",
      credentials: "include",
      cache: "no-store",
      headers: { "Accept": "application/json" },
    });
    const data = await response.json().catch(() => ({}));
    return {
      ok: Boolean(response.ok && data && data.ok),
      response,
      data,
    };
  } catch (error) {
    return {
      ok: false,
      response: null,
      data: {},
      error,
    };
  }
}

function navigateEmbeddedHome(message){
  setEmbedNavAttempted(true);
  setEmbedMessage(message, "success");
  window.setTimeout(() => {
    location.replace(absoluteUrl("/"));
  }, 80);
}

async function activateStorageAccessAndRefresh({
  successMessage,
  pendingMessage = "状態を再確認しています…",
  failureMessage = "ストレージアクセスは許可されましたが、埋め込みセッションを有効化できませんでした。別タブ側でログイン完了後に再確認してください。",
} = {}){
  await requestStorageAccessSafe();
  setEmbedMessage(pendingMessage, "info");
  const ready = await refreshEmbeddedLoginState({ redirectOnSuccess: true, tryAutoActivate: false });
  if(ready) return true;
  const hasStorageAccess = await hasStorageAccessSafe();
  if(hasStorageAccess === true){
    navigateEmbeddedHome(successMessage || "ストレージアクセスを有効化しました。認証付きで開き直しています…");
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
      successMessage: "ストレージアクセスを有効化しました。認証付きで開き直しています…",
      pendingMessage: "ストレージアクセス権を反映しています…",
      failureMessage: "ストレージアクセスの自動有効化に失敗しました。ログイン後に『再確認』を押してください。",
    });
  } catch (_error) {
    return false;
  }
}

async function refreshEmbeddedLoginState({ redirectOnSuccess=false, tryAutoActivate=false }={}){
  if(!isEmbedded()) return false;

  if(tryAutoActivate){
    const activated = await tryActivateGrantedStorageAccess();
    if(activated) return false;
  }

  const [session, hasStorageAccess] = await Promise.all([
    fetchEmbedSession(),
    hasStorageAccessSafe(),
  ]);

  if(session.ok){
    setEmbedNavAttempted(false);
    postParentMessage(EMBED_MESSAGE_TYPES.ready, {
      origin: location.origin,
      url: absoluteUrl("/"),
    });
    if(redirectOnSuccess || location.pathname.endsWith("/login.html") || location.pathname === "/login.html"){
      location.replace("/");
    }
    return true;
  }

  if(hasStorageAccess === true && !getEmbedNavAttempted()){
    navigateEmbeddedHome("ストレージアクセスは有効です。認証付きで開き直しています…");
    return false;
  }

  const loginUrl = absoluteUrl("/login.html");
  postParentMessage(EMBED_MESSAGE_TYPES.authRequired, {
    loginUrl,
    origin: location.origin,
    storageApiSupported: typeof document.requestStorageAccess === "function",
    hasStorageAccess,
  });

  const messageParts = [
    "この埋め込み内では直接ログインせず、別タブの NIM で先にログインしてください。",
  ];

  if(typeof document.requestStorageAccess === "function"){
    messageParts.push("ログイン後に『アクセスを許可して再読込』を押すと、このオーバーレイでもセッションを使えるようになります。");
  } else {
    messageParts.push("このブラウザでは埋め込みストレージ許可 API が使えないため、別タブでログイン後に『再確認』を押してください。");
  }

  if(hasStorageAccess === true){
    messageParts.push("ストレージアクセスは既に許可されています。別タブでログイン済みなのにこの表示のままなら、NIM 側の Cookie がサードパーティ送信可能になっていない可能性があります。");
  }

  setEmbedMessage(messageParts.join(" "), "info");
  return false;
}

async function doLogin(){
  const username = $("loginUser").value.trim();
  const password = $("loginPass").value;
  $("loginErr").textContent = "";
  if(!username || !password){
    $("loginErr").textContent = "入力してください";
    return;
  }
  try{
    const r = await fetch(API.login, {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({username, password}),
      credentials: "include",
    });
    if(!r.ok){
      $("loginErr").textContent = "ログインに失敗しました";
      return;
    }
    await r.text().catch(() => "");
    location.href = "/";
  }catch(_error){
    $("loginErr").textContent = "接続できません";
  }
}

function setupFirstTimeRedirect(){
  fetch(API.status, { credentials: "include" })
    .then((r) => r.ok ? r.json() : null)
    .then((j) => {
      if(j && j.needs_setup){
        location.replace("/setup.html");
      }
    })
    .catch(() => {});
}

function setupEmbeddedMode(){
  if(!isEmbedded()) return;

  document.body.classList.add("loginEmbedded");
  $("loginHint").textContent = "オーバーレイ内ではログイン画面の代わりに、別タブで認証してから戻します。";
  $("loginFormArea").classList.add("hidden");
  $("embedLoginBox").classList.remove("hidden");

  $("embedOpenTopLogin").addEventListener("click", () => {
    const url = new URL("/login.html", location.origin);
    url.searchParams.set("overlay_login", "1");
    window.open(url.toString(), "_blank", "noopener,noreferrer");
    setEmbedMessage("別タブで NIM を開きました。ログイン後にこの画面へ戻り、『アクセスを許可して再読込』または『再確認』を押してください。", "info");
  });

  $("embedGrantAccess").addEventListener("click", async () => {
    try {
      await activateStorageAccessAndRefresh({
        successMessage: "ストレージアクセスを許可しました。認証付きで開き直しています…",
      });
    } catch (error) {
      setEmbedMessage(String(error?.message || error || "アクセス許可に失敗しました"), "error");
    }
  });

  $("embedRetry").addEventListener("click", () => {
    setEmbedMessage("状態を再確認しています…", "info");
    refreshEmbeddedLoginState({ redirectOnSuccess: true, tryAutoActivate: true }).catch(() => {
      setEmbedMessage("状態確認に失敗しました。", "error");
    });
  });

  refreshEmbeddedLoginState({ tryAutoActivate: true }).catch(() => {
    setEmbedMessage("埋め込みセッションの確認に失敗しました。", "error");
  });
}

function init(){
  setupFirstTimeRedirect();

  const url = new URL(location.href);
  const presetUser = (url.searchParams.get("username") || "").trim();
  if(presetUser) $("loginUser").value = presetUser;

  $("loginBtn").addEventListener("click", doLogin);
  $("loginPass").addEventListener("keydown", (e)=>{ if(e.key === "Enter") doLogin(); });

  if(isEmbedded()){
    setupEmbeddedMode();
    return;
  }

  if(presetUser) $("loginPass").focus();
  else $("loginUser").focus();
}

init();
