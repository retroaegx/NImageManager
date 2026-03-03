const API = {
  me: "/api/me",
  logout: "/api/auth/logout",
  listUsers: "/api/admin/users",
  createUser: "/api/admin/users",
  updateUser: (id) => `/api/admin/users/${id}`,
  deleteUser: (id) => `/api/admin/users/${id}`,
  issuePwLink: (id) => `/api/admin/users/${id}/password_link`,
  selfPwLink: "/api/auth/password_link",
};

function $(id){ return document.getElementById(id); }

async function apiFetch(url, opts={}){
  const o = { credentials: "include", ...opts };
  o.headers = o.headers || {};
  if(o.body && !o.headers["Content-Type"]) o.headers["Content-Type"] = "application/json";
  const res = await fetch(url, o);
  if(res.status === 401){
    location.replace("/login.html");
    throw new Error("unauthorized");
  }
  if(res.status === 403){
    location.replace("/");
    throw new Error("forbidden");
  }
  return res;
}

async function apiJson(res){
  const text = await res.text();
  if(!res.ok){
    const head = (text || "").slice(0, 140);
    throw new Error(`${res.status} ${head}`);
  }
  if(!text) return null;
  try{ return JSON.parse(text); }
  catch(_e){ throw new Error(`bad json: ${(text||"").slice(0,140)}`); }
}

async function safeJson(res){
  try{
    const t = await res.text();
    if(!t) return {};
    return JSON.parse(t) || {};
  }catch(_e){
    return {};
  }
}

function isAdminRole(role){
  return role === "admin" || role === "master";
}

let ME = null;

async function loadMe(){
  const res = await apiFetch(API.me);
  const me = await apiJson(res);
  if(!isAdminRole(me.role)){
    location.replace("/");
    return null;
  }
  ME = me;
  $("meLabel").textContent = `${me.username} (${me.role})`;

  // Only master can create admin users.
  if(me.role !== "master"){
    const sel = $("newUserRole");
    if(sel){
      // Remove admin option.
      [...sel.querySelectorAll("option")].forEach(o => {
        if(o.value === "admin") o.remove();
      });
      sel.value = "user";
    }
  }

  return me;
}

async function doLogout(){
  try{ await fetch(API.logout, { method: "POST", credentials: "include" }); }catch(_e){}
  location.replace("/login.html");
}

function bindUserMenu(){
  const hamburger = $("hamburger");
  const userMenu = $("userMenu");
  const menuAdmin = $("menuAdmin");
  const menuMaintenance = $("menuMaintenance");
  const menuPwLink = $("menuPwLink");
  const menuLogout = $("menuLogout");

  const closeMenu = () => userMenu?.classList.add("hidden");
  const toggleMenu = () => userMenu?.classList.toggle("hidden");

  hamburger?.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    toggleMenu();
  });
  menuAdmin?.addEventListener("click", () => location.assign("/admin.html"));
  menuMaintenance?.addEventListener("click", () => location.assign("/maintenance.html"));
  menuPwLink?.addEventListener("click", async () => {
    try{
      const r = await apiFetch(API.selfPwLink, { method: "POST" });
      const j = await apiJson(r);
      if(j && j.reset_url) location.assign(j.reset_url);
    }catch(_e){
      alert("URL発行に失敗しました");
    }
  });
  menuLogout?.addEventListener("click", doLogout);
  document.addEventListener("click", () => closeMenu());
}

function showIssuedUrl(url){
  const box = $("issuedBox");
  const input = $("issuedUrl");
  if(box) box.style.display = "block";
  if(input) input.value = url || "";
}

async function copyIssued(){
  const v = $("issuedUrl")?.value || "";
  if(!v) return;
  try{
    await navigator.clipboard.writeText(v);
  }catch(_e){
    // fallback
    const i = $("issuedUrl");
    i?.focus();
    i?.select();
    try{ document.execCommand("copy"); }catch(_e2){}
  }
}

async function loadUsers(){
  const res = await apiFetch(API.listUsers);
  const users = await apiJson(res);
  renderUsers(users);
}

function renderUsers(users){
  const tb = $("userTbody");
  tb.innerHTML = "";
  const me = ME;

  (users || []).forEach(u => {
    const tr = document.createElement("tr");

    // username
    const tdName = document.createElement("td");
    tdName.textContent = u.username;

    // role
    const tdRole = document.createElement("td");
    if(u.role === "master"){
      tdRole.textContent = "master";
      tdRole.className = "small";
    }else{
      const sel = document.createElement("select");
      sel.className = "miniSelect";
      sel.innerHTML = `
        <option value="user">user</option>
        <option value="admin">admin</option>
      `;
      sel.value = u.role;
      const canChangeRole = (me && me.role === "master");
      sel.disabled = !canChangeRole;
      if(!canChangeRole){
        // Avoid giving the impression that role changes are possible.
        sel.classList.add("disabled");
      }
      sel.addEventListener("change", async () => {
        await updateUser(u.id, { role: sel.value });
        await loadUsers();
      });
      tdRole.appendChild(sel);
    }

    // status
    const tdStatus = document.createElement("td");
    if(u.must_set_password){
      tdStatus.textContent = "未設定";
    }else{
      tdStatus.textContent = "OK";
    }
    tdStatus.className = "small";

    // disabled
    const tdDis = document.createElement("td");
    const chk = document.createElement("input");
    chk.type = "checkbox";
    chk.checked = !!u.disabled;
    const isSelf = me && Number(me.id) === Number(u.id);
    const isTargetMaster = u.role === "master";
    const targetIsAdmin = u.role === "admin";
    const canToggle = !isTargetMaster && !(targetIsAdmin && me.role !== "master" && !isSelf);
    chk.disabled = !canToggle || (isSelf && chk.checked);
    chk.addEventListener("change", async () => {
      await updateUser(u.id, { disabled: chk.checked ? 1 : 0 });
      await loadUsers();
    });
    tdDis.appendChild(chk);

    // created
    const tdCreated = document.createElement("td");
    tdCreated.textContent = (u.created_at || "").replace("T"," ").replace("Z","");

    // actions
    const tdAct = document.createElement("td");
    tdAct.style.whiteSpace = "nowrap";

    const btnUrl = document.createElement("button");
    btnUrl.className = "ghostBtn smallBtn";
    btnUrl.textContent = "URL発行";
    // Allow self always. For others, enforce admin/master rules on server; disable in UI for clarity.
    const canIssueUrl = isSelf || isAdminRole(me.role);
    btnUrl.disabled = !canIssueUrl || (u.role === "master" && !isSelf);
    btnUrl.addEventListener("click", async () => {
      const r = await apiFetch(API.issuePwLink(u.id), { method: "POST" });
      if(!r.ok){
        const j = await safeJson(r);
        alert(j.detail || "発行に失敗しました");
        return;
      }
      const j = await apiJson(r);
      showIssuedUrl(j.reset_url);
    });

    const btnDel = document.createElement("button");
    btnDel.className = "ghostBtn smallBtn";
    btnDel.textContent = "削除";
    const canDelete = !isSelf && u.role !== "master" && (u.role === "user" || (u.role === "admin" && me.role === "master"));
    btnDel.disabled = !canDelete;
    btnDel.addEventListener("click", async () => {
      if(!confirm(`削除しますか？\n${u.username}`)) return;
      const r = await apiFetch(API.deleteUser(u.id), { method: "DELETE" });
      if(!r.ok){
        const j = await safeJson(r);
        alert(j.detail || "削除に失敗しました");
        return;
      }
      await loadUsers();
    });

    tdAct.appendChild(btnUrl);
    tdAct.appendChild(document.createTextNode(" "));
    tdAct.appendChild(btnDel);

    tr.appendChild(tdName);
    tr.appendChild(tdRole);
    tr.appendChild(tdStatus);
    tr.appendChild(tdDis);
    tr.appendChild(tdCreated);
    tr.appendChild(tdAct);
    tb.appendChild(tr);
  });
}

async function updateUser(id, patch){
  const res = await apiFetch(API.updateUser(id), {
    method: "POST",
    body: JSON.stringify(patch),
  });
  if(!res.ok){
    const j = await safeJson(res);
    alert(j.detail || "更新に失敗しました");
  }
}

async function createUser(){
  $("createErr").textContent = "";
  const username = $("newUserName").value.trim();
  const role = $("newUserRole").value;
  if(!username){
    $("createErr").textContent = "未入力";
    return;
  }
  const res = await apiFetch(API.createUser, {
    method: "POST",
    body: JSON.stringify({ username, role }),
  });
  if(res.status === 409){
    $("createErr").textContent = "既に存在";
    return;
  }
  if(!res.ok){
    const j = await safeJson(res);
    $("createErr").textContent = j.detail || "失敗";
    return;
  }
  const j = await apiJson(res);
  showIssuedUrl(j.setup_url);
  $("newUserName").value = "";
  await loadUsers();
}

function bindUI(){
  bindUserMenu();
  $("navToUpload")?.addEventListener("click", () => location.assign("/?view=upload"));
  $("navToPreview")?.addEventListener("click", () => location.assign("/?view=preview"));
  $("createUserBtn")?.addEventListener("click", createUser);
  $("copyIssuedBtn")?.addEventListener("click", copyIssued);
}

async function boot(){
  bindUI();
  const me = await loadMe();
  if(!me) return;
  await loadUsers();
}

boot();
