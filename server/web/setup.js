import { initI18n, t } from "./lib/i18n.js";

const API = {
  status: "/api/auth/setup_status",
  setup: "/api/auth/setup_master",
};

function $(id){ return document.getElementById(id); }

async function checkStatus(){
  try{
    const r = await fetch(API.status, { credentials: "include" });
    const j = await r.json();
    if(j && j.needs_setup === false){
      location.replace("/login.html");
      return false;
    }
    return true;
  }catch{ return true; }
}

async function doSetup(){
  const username = $("setupUser").value.trim();
  const password = $("setupPass").value;
  const password2 = $("setupPass2").value;
  $("setupErr").textContent = "";
  if(!username || !password || !password2){ $("setupErr").textContent = t("common.required"); return; }
  if(password !== password2){ $("setupErr").textContent = t("common.confirmation_mismatch"); return; }
  try{
    const r = await fetch(API.setup, { method: "POST", headers: {"Content-Type":"application/json"}, credentials: "include", body: JSON.stringify({ username, password, password2 }) });
    if(r.status === 409){ location.replace("/login.html"); return; }
    if(!r.ok){
      let msg = t("setup.failed");
      try{ const j = await r.json(); msg = j.detail || msg; }catch{}
      $("setupErr").textContent = msg;
      return;
    }
    location.replace("/");
  }catch{ $("setupErr").textContent = t("common.connection_failed"); }
}

async function init(){
  await initI18n("auto");
  await checkStatus();
  $("setupBtn").addEventListener("click", doSetup);
  $("setupPass2").addEventListener("keydown", (e)=>{ if(e.key === "Enter") doSetup(); });
  $("setupUser").focus();
}

init();
