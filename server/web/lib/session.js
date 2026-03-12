import { $ } from "./dom.js";
import { apiFetch, apiJson } from "./http.js";

export function isAdminRole(role){
  return role === "admin" || role === "master";
}

export async function loadCurrentUser({ endpoint="/api/me", requireAdmin=false, labelId="meLabel", onLoaded=null }={}){
  const res = await apiFetch(endpoint);
  const me = await apiJson(res);
  if(requireAdmin && !isAdminRole(me?.role)){
    location.replace("/");
    return null;
  }
  const label = $(labelId);
  if(label && me?.username){
    label.textContent = `${me.username} (${me.role || "user"})`;
  }
  if(typeof onLoaded === "function") onLoaded(me);
  return me;
}

export async function logoutAndRedirect(logoutEndpoint="/api/auth/logout", redirectTo="/login.html"){
  try{
    await fetch(logoutEndpoint, { method: "POST", credentials: "include" });
  }catch(_e){}
  location.replace(redirectTo);
}
