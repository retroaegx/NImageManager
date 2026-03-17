const API = {
  login: "/api/auth/login",
  status: "/api/auth/setup_status",
};

function $(id){ return document.getElementById(id); }

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
    // Cookie auth is the primary session path.
    // The UI no longer persists API tokens in localStorage.
    await r.text().catch(() => "");
    location.href = "/";
  }catch(e){
    $("loginErr").textContent = "接続できません";
  }
}

function init(){
  // First-time setup redirect (defensive; server also redirects).
  fetch(API.status, { credentials: "include" })
    .then(r => r.ok ? r.json() : null)
    .then(j => {
      if(j && j.needs_setup){
        location.replace("/setup.html");
      }
    })
    .catch(_e => {});

  const url = new URL(location.href);
  const presetUser = (url.searchParams.get("username") || "").trim();
  if(presetUser) $("loginUser").value = presetUser;

  $("loginBtn").addEventListener("click", doLogin);
  $("loginPass").addEventListener("keydown", (e)=>{ if(e.key==="Enter") doLogin(); });
  if(presetUser) $("loginPass").focus();
  else $("loginUser").focus();
}
init();
