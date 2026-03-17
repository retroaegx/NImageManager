const API = {
  info: (t) => `/api/auth/password_tokens/info?token=${encodeURIComponent(t)}`,
  consume: "/api/auth/password_tokens/consume",
};

function $(id){ return document.getElementById(id); }

function getToken(){
  const u = new URL(location.href);
  return (u.searchParams.get("token") || "").trim();
}

async function loadInfo(token){
  $("pwErr").textContent = "";
  if(!token){
    $("pwErr").textContent = "tokenがありません";
    $("pwBtn").disabled = true;
    return;
  }
  try{
    const r = await fetch(API.info(token), { credentials: "include" });
    if(!r.ok){
      $("pwErr").textContent = "URLが無効です";
      $("pwBtn").disabled = true;
      return;
    }
    const j = await r.json();
    $("pwUser").value = j.username || "";
    if(j.kind === "setup"){
      $("pwTitle").textContent = "初回パスワードを設定";
    }else{
      $("pwTitle").textContent = "パスワードを変更";
    }
    if(j.status && j.status !== "ok"){
      if(j.status === "expired") $("pwErr").textContent = "期限切れです";
      if(j.status === "used") $("pwErr").textContent = "使用済みです";
      $("pwBtn").disabled = true;
    }
  }catch(_e){
    $("pwErr").textContent = "接続できません";
    $("pwBtn").disabled = true;
  }
}

async function submit(token){
  const password = $("pwPass").value;
  const password2 = $("pwPass2").value;
  $("pwErr").textContent = "";
  if(!password || !password2){
    $("pwErr").textContent = "未入力";
    return;
  }
  if(password !== password2){
    $("pwErr").textContent = "確認が一致しません";
    return;
  }
  try{
    const r = await fetch(API.consume, {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ token, password, password2 }),
      credentials: "include",
    });
    if(!r.ok){
      let msg = "失敗";
      try{ const j = await r.json(); msg = j.detail || msg; }catch(_e){}
      $("pwErr").textContent = msg;
      return;
    }
    $("pwOk").style.display = "block";
    $("pwBtn").disabled = true;
    const username = ($("pwUser")?.value || "").trim();
    const next = new URL("/login.html", location.origin);
    if(username) next.searchParams.set("username", username);
    setTimeout(() => {
      location.replace(next.pathname + next.search);
    }, 400);
  }catch(_e){
    $("pwErr").textContent = "接続できません";
  }
}

async function init(){
  const token = getToken();
  await loadInfo(token);
  $("pwBtn").addEventListener("click", () => submit(token));
  $("pwPass2").addEventListener("keydown", (e)=>{ if(e.key === "Enter") submit(token); });
  $("pwPass").focus();
}

init();
