import { $ } from "./lib/dom.js?v=20260307_01";
import { apiFetch, apiJson, safeJson } from "./lib/http.js?v=20260307_01";
import { bindUserMenu } from "./lib/userMenu.js?v=20260312_02";
import { loadCurrentUser, logoutAndRedirect, isAdminRole } from "./lib/session.js?v=20260307_01";

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

let ME = null;

function confirmAccountDelete(username){
  const name = String(username || "").trim();
  if(!confirm(`アカウント「${name}」を削除します。\n作品、ブックマーク、作者登録、共有ブックマーク登録、関連データも削除します。元に戻せません。`)) return false;
  return confirm("本当に削除しますか？");
}

async function loadMe(){
  const me = await loadCurrentUser({
    endpoint: API.me,
    requireAdmin: true,
    onLoaded: (user) => {
      ME = user;
      bindUserMenu({
        logoutEndpoint: API.logout,
        passwordLinkEndpoint: API.selfPwLink,
        showAdmin: true,
        showMaintenance: true,
      });
      if(user?.role !== "master"){
        const sel = $("newUserRole");
        if(sel){
          [...sel.querySelectorAll("option")].forEach(o => {
            if(o.value === "admin") o.remove();
          });
          sel.value = "user";
        }
      }
    },
  });
  ME = me;
  return me;
}


async function doLogout(){
  await logoutAndRedirect(API.logout);
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
