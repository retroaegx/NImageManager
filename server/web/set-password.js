import { initI18n, t } from "./lib/i18n.js";

const API = {
  info: (token) => `/api/auth/password_tokens/info?token=${encodeURIComponent(token)}`,
  consume: "/api/auth/password_tokens/consume",
};

function $(id){ return document.getElementById(id); }
function getToken(){ return (new URL(location.href).searchParams.get("token") || "").trim(); }

async function loadInfo(token){
  $("pwErr").textContent = "";
  if(!token){ $("pwErr").textContent = t("password.token_missing"); $("pwBtn").disabled = true; return; }
  try{
    const r = await fetch(API.info(token), { credentials: "include" });
    if(!r.ok){ $("pwErr").textContent = t("password.invalid_url"); $("pwBtn").disabled = true; return; }
    const j = await r.json();
    $("pwUser").value = j.username || "";
    $("pwTitle").textContent = j.kind === "setup" ? t("password.title_setup") : t("password.title_change");
    if(j.status && j.status !== "ok"){
      if(j.status === "expired") $("pwErr").textContent = t("password.expired");
      if(j.status === "used") $("pwErr").textContent = t("password.used");
      $("pwBtn").disabled = true;
    }
  }catch{ $("pwErr").textContent = t("common.connection_failed"); $("pwBtn").disabled = true; }
}

async function submit(token){
  const password = $("pwPass").value;
  const password2 = $("pwPass2").value;
  $("pwErr").textContent = "";
  if(!password || !password2){ $("pwErr").textContent = t("common.required"); return; }
  if(password !== password2){ $("pwErr").textContent = t("common.confirmation_mismatch"); return; }
  try{
    const r = await fetch(API.consume, { method: "POST", headers: {"Content-Type":"application/json"}, body: JSON.stringify({ token, password, password2 }), credentials: "include" });
    if(!r.ok){
      let msg = t("common.failed");
      try{ const j = await r.json(); msg = j.detail || msg; }catch{}
      $("pwErr").textContent = msg;
      return;
    }
    $("pwOk").style.display = "block";
    $("pwBtn").disabled = true;
    const username = ($("pwUser")?.value || "").trim();
    const next = new URL("/login.html", location.origin);
    if(username) next.searchParams.set("username", username);
    setTimeout(() => location.replace(next.pathname + next.search), 400);
  }catch{ $("pwErr").textContent = t("common.connection_failed"); }
}

async function init(){
  await initI18n("auto");
  const token = getToken();
  await loadInfo(token);
  $("pwBtn").addEventListener("click", () => submit(token));
  $("pwPass2").addEventListener("keydown", (e)=>{ if(e.key === "Enter") submit(token); });
  $("pwPass").focus();
}

init();
