import { initI18n, t } from "./lib/i18n.js?v=1.0.6-i18n1";

const message = document.getElementById("verifyMessage");
const loginLink = document.getElementById("verifyLoginLink");
const token = new URL(location.href).searchParams.get("token") || "";

async function verify(){
  if(!token){ message.textContent = t("verify_email.token_missing"); return; }
  try{
    const response = await fetch("/api/auth/verify-email", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({token}),
      credentials: "include",
    });
    if(!response.ok){
      message.textContent = response.status === 410 ? t("verify_email.expired") : t("verify_email.invalid");
      return;
    }
    message.textContent = t("verify_email.success");
    message.classList.add("ok");
    loginLink.classList.remove("hidden");
  }catch{
    message.textContent = t("common.connection_failed");
  }
}
async function init(){
  try{ await initI18n("auto"); }catch{}
  await verify();
}

init();
