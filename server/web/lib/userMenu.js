import { $ } from "./dom.js";
import { apiFetch, apiJson } from "./http.js";
import { logoutAndRedirect } from "./session.js";
import { t, formatDateTime } from "./i18n.js";

function formatUtc(ts){
  if(!ts) return "";
  try{
    const d = new Date(ts);
    if(Number.isNaN(d.getTime())) return String(ts);
    return formatDateTime(d, {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });
  }catch(_e){
    return String(ts);
  }
}

function buildUpdateNotice(state){
  const right = document.querySelector(".right");
  if(!right) return;
  const existing = right.querySelector(".updateNotice");
  if(existing) existing.remove();

  const latest = String(state?.latest_version || "").trim() || t("common.unknown");
  const current = String(state?.current_version || "").trim() || t("common.unknown");
  const releaseUrl = String(state?.release_url || state?.release_page_url || "").trim();
  const releaseName = String(state?.release_name || latest || t("common.latest")).trim();
  const checkedAt = formatUtc(state?.checked_at);

  const wrap = document.createElement("div");
  wrap.className = "updateNotice";

  const badge = document.createElement("button");
  badge.type = "button";
  badge.className = "updateBadge";
  badge.setAttribute("aria-label", t("user_menu.update.badge_aria"));
  badge.title = t("user_menu.update.badge_title", { latest });
  badge.textContent = "!";

  const popup = document.createElement("div");
  popup.className = "updatePopup hidden";

  const title = document.createElement("div");
  title.className = "updatePopupTitle";
  title.textContent = t("user_menu.update.popup_title");

  const body = document.createElement("div");
  body.className = "updatePopupBody";
  body.textContent = t("user_menu.update.popup_body", { latest, releaseName });

  const meta = document.createElement("div");
  meta.className = "updatePopupMeta";
  meta.textContent = checkedAt
    ? t("user_menu.update.popup_meta_checked", { current, latest, checkedAt })
    : t("user_menu.update.popup_meta", { current, latest });

  popup.appendChild(title);
  popup.appendChild(body);
  popup.appendChild(meta);

  if(releaseUrl){
    const link = document.createElement("a");
    link.className = "updatePopupLink";
    link.href = releaseUrl;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.textContent = t("user_menu.update.open_github");
    link.title = releaseUrl;
    popup.appendChild(link);
  }

  let hideTimer = 0;
  const openPopup = () => {
    if(hideTimer){
      clearTimeout(hideTimer);
      hideTimer = 0;
    }
    popup.classList.remove("hidden");
    badge.setAttribute("aria-expanded", "true");
  };
  const closePopup = () => {
    popup.classList.add("hidden");
    badge.setAttribute("aria-expanded", "false");
  };
  const queueClose = () => {
    if(hideTimer) clearTimeout(hideTimer);
    hideTimer = window.setTimeout(closePopup, 120);
  };

  badge.addEventListener("mouseenter", openPopup);
  badge.addEventListener("focus", openPopup);
  badge.addEventListener("mouseleave", queueClose);
  badge.addEventListener("blur", queueClose);
  badge.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    if(popup.classList.contains("hidden")) openPopup();
    else closePopup();
  });
  popup.addEventListener("mouseenter", openPopup);
  popup.addEventListener("mouseleave", queueClose);
  popup.addEventListener("click", (e) => e.stopPropagation());
  wrap.addEventListener("click", (e) => e.stopPropagation());

  wrap.appendChild(badge);
  wrap.appendChild(popup);
  right.insertBefore(wrap, $("hamburger") || right.firstChild);
  document.addEventListener("click", closePopup, { capture: true });
}

async function loadUpdateNotice(){
  try{
    const r = await apiFetch("/api/app/update_status");
    const state = await apiJson(r);
    if(!state?.visible) return;
    buildUpdateNotice(state);
  }catch(_e){
    // Update indicator is optional.
  }
}

export function bindUserMenu({
  logoutEndpoint="/api/auth/logout",
  passwordLinkEndpoint="/api/auth/password_link",
  showAdmin=false,
  showMaintenance=false,
}={}){
  const hamburger = $("hamburger");
  const userMenu = $("userMenu");
  const menuAdmin = $("menuAdmin");
  const menuMaintenance = $("menuMaintenance");
  const menuSettings = $("menuSettings");
  const menuPwLink = $("menuPwLink");
  const menuLogout = $("menuLogout");

  if(menuAdmin) menuAdmin.classList.toggle("hidden", !showAdmin);
  if(menuMaintenance) menuMaintenance.classList.toggle("hidden", !showMaintenance);

  const closeMenu = () => userMenu?.classList.add("hidden");
  const toggleMenu = () => userMenu?.classList.toggle("hidden");

  hamburger?.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    toggleMenu();
  });
  menuAdmin?.addEventListener("click", () => location.assign("/admin.html"));
  menuMaintenance?.addEventListener("click", () => location.assign("/maintenance.html"));
  menuSettings?.addEventListener("click", () => location.assign("/settings.html"));
  menuPwLink?.addEventListener("click", async () => {
    try{
      const r = await apiFetch(passwordLinkEndpoint, { method: "POST" });
      const j = await apiJson(r);
      if(j && j.reset_url) location.assign(j.reset_url);
    }catch(_e){
      alert(t("user_menu.password_link_failed"));
    }
  });
  menuLogout?.addEventListener("click", () => logoutAndRedirect(logoutEndpoint));
  document.addEventListener("click", () => closeMenu());

  if(showAdmin && showMaintenance){
    loadUpdateNotice();
  }
}
