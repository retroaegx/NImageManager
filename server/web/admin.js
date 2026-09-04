import { $ } from "./lib/dom.js";
import { apiFetch, apiJson, safeJson } from "./lib/http.js";
import { bindUserMenu } from "./lib/userMenu.js";
import { loadCurrentUser, logoutAndRedirect, isAdminRole } from "./lib/session.js";
import { t } from "./lib/i18n.js?v=1.0.6-i18n1";


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
const GIB = 1024 * 1024 * 1024;

function fmtBytes(value){
  const n = Math.max(0, Number(value || 0));
  if(n >= GIB) return `${(n / GIB).toFixed(n >= 10 * GIB ? 1 : 2).replace(/\.0+$/, "")} GiB`;
  if(n >= 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1).replace(/\.0$/, "")} MiB`;
  if(n >= 1024) return `${(n / 1024).toFixed(1).replace(/\.0$/, "")} KiB`;
  return `${Math.round(n)} B`;
}

function quotaInputValue(bytes){
  if(bytes === null || bytes === undefined) return "";
  const value = Number(bytes || 0) / GIB;
  return String(Number(value.toFixed(3)));
}

function confirmAccountDelete(username){
  const name = String(username || "").trim();
  if(!confirm(t("admin.account.delete.first_confirm", { name }))) return false;
  return confirm(t("common.confirm_delete"));
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
      tdStatus.textContent = t("common.unset");
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

    // upload quota
    const tdQuota = document.createElement("td");
    tdQuota.className = "adminQuotaCell";
    if(u.role !== "user"){
      tdQuota.textContent = t("admin.quota.role_unlimited");
      tdQuota.classList.add("small");
    }else{
      const editor = document.createElement("div");
      editor.className = "adminQuotaEditor";
      const input = document.createElement("input");
      input.className = "adminQuotaInput";
      input.type = "number";
      input.min = "0";
      input.step = "0.1";
      input.value = quotaInputValue(u.upload_limit_override_bytes);
      input.placeholder = t("admin.quota.default_short");
      input.title = t("admin.quota.input_help");
      const unit = document.createElement("span");
      unit.className = "adminQuotaUnit";
      unit.textContent = "GiB";
      const save = document.createElement("button");
      save.className = "ghostBtn smallBtn";
      save.textContent = t("common.save_action");
      save.disabled = true;
      input.addEventListener("input", () => { save.disabled = false; });
      save.addEventListener("click", async () => {
        const raw = input.value.trim();
        const gib = raw === "" ? null : Number(raw);
        if(gib !== null && (!Number.isFinite(gib) || gib < 0)){
          alert(t("admin.quota.invalid"));
          return;
        }
        const bytes = gib === null ? null : Math.round(gib * GIB);
        if(await updateUser(u.id, { upload_limit_bytes: bytes })) await loadUsers();
      });
      editor.append(input, unit, save);
      const usage = document.createElement("div");
      usage.className = "adminQuotaUsage small";
      usage.textContent = u.upload_quota_limited
        ? t("admin.quota.usage", { used: fmtBytes(u.upload_used_bytes), limit: fmtBytes(u.upload_limit_bytes) })
        : t("admin.quota.usage_unlimited", { used: fmtBytes(u.upload_used_bytes) });
      tdQuota.append(editor, usage);
    }

    // created
    const tdCreated = document.createElement("td");
    tdCreated.textContent = (u.created_at || "").replace("T"," ").replace("Z","");

    // actions
    const tdAct = document.createElement("td");
    tdAct.style.whiteSpace = "nowrap";

    const btnUrl = document.createElement("button");
    btnUrl.className = "ghostBtn smallBtn";
    btnUrl.textContent = t("admin.issue_url");
    // Allow self always. For others, enforce admin/master rules on server; disable in UI for clarity.
    const canIssueUrl = isSelf || isAdminRole(me.role);
    btnUrl.disabled = !canIssueUrl || (u.role === "master" && !isSelf);
    btnUrl.addEventListener("click", async () => {
      const r = await apiFetch(API.issuePwLink(u.id), { method: "POST" });
      if(!r.ok){
        const j = await safeJson(r);
        alert(j.detail || t("admin.issue_url_failed"));
        return;
      }
      const j = await apiJson(r);
      showIssuedUrl(j.reset_url);
    });

    const btnDel = document.createElement("button");
    btnDel.className = "ghostBtn smallBtn";
    btnDel.textContent = t("common.delete");
    const canDelete = !isSelf && u.role !== "master" && (u.role === "user" || (u.role === "admin" && me.role === "master"));
    btnDel.disabled = !canDelete;
    btnDel.addEventListener("click", async () => {
      if(!confirm(t("admin.account.delete.first_confirm", { name: u.username }))) return;
      const r = await apiFetch(API.deleteUser(u.id), { method: "DELETE" });
      if(!r.ok){
        const j = await safeJson(r);
        alert(j.detail || t("status.delete_failed"));
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
    tr.appendChild(tdQuota);
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
    alert(j.detail || t("status.update_failed"));
    return false;
  }
  return true;
}

async function createUser(){
  $("createErr").textContent = "";
  const username = $("newUserName").value.trim();
  const role = $("newUserRole").value;
  if(!username){
    $("createErr").textContent = t("common.required");
    return;
  }
  const res = await apiFetch(API.createUser, {
    method: "POST",
    body: JSON.stringify({ username, role }),
  });
  if(res.status === 409){
    $("createErr").textContent = t("common.already_exists");
    return;
  }
  if(!res.ok){
    const j = await safeJson(res);
    $("createErr").textContent = j.detail || t("common.failed");
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
