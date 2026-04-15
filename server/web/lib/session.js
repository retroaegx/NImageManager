import { $ } from "./dom.js";
import { apiFetch, apiJson } from "./http.js";
import { t, setLocalePreference } from "./i18n.js";

export function isAdminRole(role){
  return role === "admin" || role === "master";
}

function roleLabel(role){
  if(role === "master") return t("role.master");
  if(role === "admin") return t("role.admin");
  return t("role.user");
}

function readBootstrapUser(){
  try{
    const user = globalThis.__NIM_BOOTSTRAP__?.user;
    return user && typeof user === "object" ? user : null;
  }catch(_e){
    return null;
  }
}

function applyUserToUi(me, labelId="meLabel"){
  const label = $(labelId);
  if(label && me?.username){
    label.textContent = `${me.username} (${roleLabel(String(me.role || "user"))})`;
  }
}

export async function loadCurrentUser({ endpoint="/api/me", requireAdmin=false, labelId="meLabel", onLoaded=null, forceNetwork=false }={}){
  let me = null;
  if(!forceNetwork){
    me = readBootstrapUser();
  }
  if(!me){
    const res = await apiFetch(endpoint);
    me = await apiJson(res);
  }
  await setLocalePreference(String(me?.ui_language || "auto"), { apply: true });
  if(requireAdmin && !isAdminRole(me?.role)){
    location.replace("/");
    return null;
  }
  applyUserToUi(me, labelId);
  if(typeof onLoaded === "function") onLoaded(me);
  return me;
}

export async function logoutAndRedirect(logoutEndpoint="/api/auth/logout", redirectTo="/login.html"){
  try{
    await fetch(logoutEndpoint, { method: "POST", credentials: "include" });
  }catch(_e){}
  location.replace(redirectTo);
}
