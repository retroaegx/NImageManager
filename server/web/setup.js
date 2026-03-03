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
  }catch(_e){
    // If API is unreachable, stay here and show errors on submit.
    return true;
  }
}

async function doSetup(){
  const username = $("setupUser").value.trim();
  const password = $("setupPass").value;
  const password2 = $("setupPass2").value;
  $("setupErr").textContent = "";

  if(!username || !password || !password2){
    $("setupErr").textContent = "未入力";
    return;
  }
  if(password !== password2){
    $("setupErr").textContent = "確認が一致しません";
    return;
  }

  try{
    const r = await fetch(API.setup, {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      credentials: "include",
      body: JSON.stringify({ username, password, password2 }),
    });
    if(r.status === 409){
      location.replace("/login.html");
      return;
    }
    if(!r.ok){
      let msg = "作成に失敗しました";
      try{ const j = await r.json(); msg = j.detail || msg; }catch(_e){}
      $("setupErr").textContent = msg;
      return;
    }
    location.replace("/");
  }catch(_e){
    $("setupErr").textContent = "接続できません";
  }
}

async function init(){
  await checkStatus();
  $("setupBtn").addEventListener("click", doSetup);
  $("setupPass2").addEventListener("keydown", (e)=>{ if(e.key === "Enter") doSetup(); });
  $("setupUser").focus();
}

init();
