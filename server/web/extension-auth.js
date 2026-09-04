import { initI18n, t } from "./lib/i18n.js?v=1.0.6-i18n1";

const $ = (id) => document.getElementById(id);
const requestId = new URLSearchParams(location.search).get("request_id") || "";

function showMessage(message, kind = "info") {
  const node = $("extensionAuthMessage");
  node.textContent = message;
  node.className = `authMessage ${kind === "error" ? "err" : kind === "success" ? "ok" : ""}`;
}

function loginUrl() {
  const next = `${location.pathname}${location.search}`;
  return `/login.html?next=${encodeURIComponent(next)}`;
}

async function readJson(response) {
  return await response.json().catch(() => ({}));
}

async function submitDecision(path) {
  $("approveExtensionAuth").disabled = true;
  $("denyExtensionAuth").disabled = true;
  const response = await fetch(path, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ request_id: requestId }),
  });
  const data = await readJson(response);
  if (!response.ok) throw new Error(t("extension_auth.failed"));
  return data;
}

async function approve() {
  try {
    await submitDecision("/api/ext/device/approve");
    $("extensionAuthActions").classList.add("hidden");
    showMessage(t("extension_auth.approved"), "success");
  } catch (error) {
    $("approveExtensionAuth").disabled = false;
    $("denyExtensionAuth").disabled = false;
    showMessage(error?.message || t("extension_auth.failed"), "error");
  }
}

async function deny() {
  try {
    await submitDecision("/api/ext/device/deny");
    $("extensionAuthActions").classList.add("hidden");
    showMessage(t("extension_auth.denied"));
  } catch (error) {
    $("approveExtensionAuth").disabled = false;
    $("denyExtensionAuth").disabled = false;
    showMessage(error?.message || t("extension_auth.failed"), "error");
  }
}

async function init() {
  await initI18n(globalThis.__NIM_BOOTSTRAP__?.user?.ui_language || "auto").catch(() => {});
  if (!requestId) {
    showMessage(t("extension_auth.invalid"), "error");
    return;
  }
  const infoResponse = await fetch(`/api/ext/device/info?request_id=${encodeURIComponent(requestId)}`, {
    credentials: "include",
    cache: "no-store",
  });
  const info = await readJson(infoResponse);
  if (!infoResponse.ok || info.status !== "pending") {
    showMessage(t("extension_auth.invalid"), "error");
    return;
  }
  const user = globalThis.__NIM_BOOTSTRAP__?.user;
  if (!user) {
    location.replace(loginUrl());
    return;
  }
  $("extensionAuthClient").textContent = info.client_name || "NIM Transfer";
  $("extensionAuthUser").textContent = user.username || "";
  $("extensionAuthDetails").classList.remove("hidden");
  $("extensionAuthActions").classList.remove("hidden");
  showMessage(t("extension_auth.confirm"));
}

$("approveExtensionAuth")?.addEventListener("click", approve);
$("denyExtensionAuth")?.addEventListener("click", deny);
init().catch(() => showMessage(t("extension_auth.failed"), "error"));
