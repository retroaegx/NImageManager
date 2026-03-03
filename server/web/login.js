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
    // API may respond with non-JSON on misconfig; avoid crashing.
    const text = await r.text();
    try{
      const data = JSON.parse(text || "{}");
      if(data && data.token){
        localStorage.setItem("token", data.token);
      }
    }catch(_e){
      // Cookie auth is the primary path; token storage is optional.
    }
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

  $("loginBtn").addEventListener("click", doLogin);
  $("loginPass").addEventListener("keydown", (e)=>{ if(e.key==="Enter") doLogin(); });
  $("loginUser").focus();
}
init();
