import { initI18n, t } from "./lib/i18n.js";

const API = {
  login: "/api/auth/login", register: "/api/auth/register", resend: "/api/auth/resend-verification", forgot: "/api/auth/forgot-password",
  providers: "/api/auth/providers", google: "/api/auth/google", status: "/api/auth/setup_status",
  embedSession: "/api/ext/session",
};
const EMBED_MESSAGE_TYPES = { authRequired: "NIM_EMBED_AUTH_REQUIRED", ready: "NIM_EMBED_READY" };
const EMBED_AUTH_NAV_FLAG = "nim_embed_auth_nav_attempted_v2";

let providers = { google_enabled:false, registration_enabled:false, terms_version:"", privacy_version:"" };
let agreementAccepted = false;
let agreementStep = 0;
let pendingGoogleCredential = "";
let googleRegistrationActive = false;

function $(id){ return document.getElementById(id); }
function isEmbedded(){ try { return window.self !== window.top; } catch { return true; } }
function loginDestination(){
  const raw=new URLSearchParams(location.search).get("next")||"";
  if(!raw.startsWith("/")||raw.startsWith("//"))return "/";
  try{const url=new URL(raw,location.origin);return url.origin===location.origin?`${url.pathname}${url.search}${url.hash}`:"/";}catch{return "/";}
}
function finishLogin(){location.href=loginDestination();}
function absoluteUrl(path){ return new URL(path, location.origin).toString(); }
function postParentMessage(type, extra={}){ if(isEmbedded()) window.parent.postMessage({ type, ...extra }, "*"); }
function getEmbedNavAttempted(){ try { return sessionStorage.getItem(EMBED_AUTH_NAV_FLAG) === "1"; } catch { return false; } }
function setEmbedNavAttempted(value){ try { value ? sessionStorage.setItem(EMBED_AUTH_NAV_FLAG,"1") : sessionStorage.removeItem(EMBED_AUTH_NAV_FLAG); } catch {} }
async function hasStorageAccessSafe(){ if(typeof document.hasStorageAccess !== "function") return null; try { return await document.hasStorageAccess(); } catch { return null; } }
async function requestStorageAccessSafe(){ if(typeof document.requestStorageAccess !== "function") throw new Error(t("login.embed.storage_not_supported")); return await document.requestStorageAccess(); }
function setEmbedMessage(message, kind="info"){ const node=$("embedLoginText"); if(node){ node.textContent=message||""; node.dataset.kind=kind; } }

async function fetchEmbedSession(){
  try{
    const response=await fetch(API.embedSession,{method:"GET",credentials:"include",cache:"no-store",headers:{Accept:"application/json"}});
    const data=await response.json().catch(()=>({}));
    return {ok:Boolean(response.ok&&data?.ok),response,data};
  }catch(error){ return {ok:false,response:null,data:{},error}; }
}
function navigateEmbeddedHome(message){ setEmbedNavAttempted(true); setEmbedMessage(message,"success"); setTimeout(()=>location.replace(absoluteUrl("/")),80); }
async function activateStorageAccessAndRefresh({successMessage,pendingMessage=t("login.embed.rechecking"),failureMessage=t("login.embed.enable_failed")}={}){
  await requestStorageAccessSafe(); setEmbedMessage(pendingMessage,"info");
  const ready=await refreshEmbeddedLoginState({redirectOnSuccess:true,tryAutoActivate:false});
  if(ready) return true;
  if(await hasStorageAccessSafe()===true){ navigateEmbeddedHome(successMessage||t("login.embed.enabled_and_reloading")); return true; }
  setEmbedMessage(failureMessage,"error"); return false;
}
async function tryActivateGrantedStorageAccess(){
  if(typeof document.requestStorageAccess!=="function"||await hasStorageAccessSafe()===true) return false;
  try{return await activateStorageAccessAndRefresh({successMessage:t("login.embed.enabled_and_reloading"),pendingMessage:t("login.embed.applying_storage_access"),failureMessage:t("login.embed.auto_enable_failed")});}catch{return false;}
}
async function refreshEmbeddedLoginState({redirectOnSuccess=false,tryAutoActivate=false}={}){
  if(!isEmbedded()) return false;
  if(tryAutoActivate&&await tryActivateGrantedStorageAccess()) return false;
  const [session,hasStorageAccess]=await Promise.all([fetchEmbedSession(),hasStorageAccessSafe()]);
  if(session.ok){
    setEmbedNavAttempted(false); postParentMessage(EMBED_MESSAGE_TYPES.ready,{origin:location.origin,url:absoluteUrl("/")});
    if(redirectOnSuccess||location.pathname.endsWith("/login.html")) location.replace("/");
    return true;
  }
  if(hasStorageAccess===true&&!getEmbedNavAttempted()){ navigateEmbeddedHome(t("login.embed.storage_access_enabled")); return false; }
  postParentMessage(EMBED_MESSAGE_TYPES.authRequired,{loginUrl:absoluteUrl("/login.html"),origin:location.origin,storageApiSupported:typeof document.requestStorageAccess==="function",hasStorageAccess});
  const parts=[t("login.embed.use_top_level_login")];
  parts.push(typeof document.requestStorageAccess==="function"?t("login.embed.grant_access_after_login"):t("login.embed.storage_api_missing"));
  if(hasStorageAccess===true) parts.push(t("login.embed.cookie_hint"));
  setEmbedMessage(parts.join(" "),"info"); return false;
}

async function readJson(response){ return await response.json().catch(()=>({})); }
function showMessage(id,message,kind="error"){
  const el=$(id); if(!el) return;
  el.textContent=message||""; el.classList.toggle("ok",kind==="success"); el.classList.toggle("err",kind!=="success"&&Boolean(message));
}
function showPanel(name){
  const login=name==="login", register=name==="register", forgot=name==="forgot";
  $("loginPanel")?.classList.toggle("hidden",!login); $("registerPanel")?.classList.toggle("hidden",!register); $("forgotPanel")?.classList.toggle("hidden",!forgot);
  $("showLoginBtn")?.classList.toggle("active",login); $("showRegisterBtn")?.classList.toggle("active",register);
  $("showLoginBtn")?.setAttribute("aria-selected",String(login)); $("showRegisterBtn")?.setAttribute("aria-selected",String(register));
  $("authHeading").textContent=t(forgot?"password.reset_title":register?"register.heading":"login.heading");
  (forgot?$("forgotEmail"):register?(googleRegistrationActive?$("googleRegisterUser"):$("registerUser")):$("loginUser"))?.focus();
}
function setGoogleRegistrationMode(active,email=""){
  googleRegistrationActive=Boolean(active);
  $("emailRegisterFields")?.classList.toggle("hidden",googleRegistrationActive);
  $("googleRegisterFields")?.classList.toggle("hidden",!googleRegistrationActive);
  $("registerBtn")?.classList.toggle("hidden",googleRegistrationActive);
  $("completeGoogleRegisterBtn")?.classList.toggle("hidden",!googleRegistrationActive);
  $("resendVerificationBtn")?.classList.toggle("hidden",googleRegistrationActive);
  $("cancelGoogleRegisterBtn")?.classList.toggle("hidden",!googleRegistrationActive);
  if($("googleRegisterEmail")) $("googleRegisterEmail").textContent=String(email||"");
  updateAgreementState();
}
function resetGoogleRegistration(){
  pendingGoogleCredential="";
  setGoogleRegistrationMode(false);
  showMessage("registerErr","");
}
function openRegisterPanel(){resetGoogleRegistration();showPanel("register");}
async function doLogin(){
  const username=$("loginUser").value.trim(), password=$("loginPass").value; showMessage("loginErr","");
  if(!username||!password){ showMessage("loginErr",t("common.required")); return; }
  try{
    const response=await fetch(API.login,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({username,password}),credentials:"include"});
    if(!response.ok){ showMessage("loginErr",t("login.failed")); return; }
    finishLogin();
  }catch{ showMessage("loginErr",t("common.connection_failed")); }
}

function updateAgreementState(){
  const state=$("agreementState");
  if(state){ state.textContent=t(agreementAccepted?"agreement.accepted":"agreement.required"); state.classList.toggle("ok",agreementAccepted); }
  if($("registerBtn")) $("registerBtn").disabled=!agreementAccepted||!providers.registration_enabled;
  if($("completeGoogleRegisterBtn")) $("completeGoogleRegisterBtn").disabled=!agreementAccepted||!pendingGoogleCredential;
  $("googleRegisterArea")?.classList.toggle("hidden",googleRegistrationActive||!providers.google_enabled);
}
async function doRegister(){
  const payload={username:$("registerUser").value.trim(),email:$("registerEmail").value.trim(),password:$("registerPass").value,password2:$("registerPass2").value,accepted:agreementAccepted,terms_version:providers.terms_version,privacy_version:providers.privacy_version};
  showMessage("registerErr","");
  if(!payload.username||!payload.email||!payload.password||!payload.password2){ showMessage("registerErr",t("common.required")); return; }
  if(payload.password!==payload.password2){ showMessage("registerErr",t("common.confirmation_mismatch")); return; }
  try{
    const response=await fetch(API.register,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload),credentials:"include"});
    const data=await readJson(response);
    if(!response.ok){ showMessage("registerErr",data?.detail||t("register.failed")); return; }
    showMessage("registerErr",t("register.check_email"),"success"); $("registerBtn").disabled=true;
  }catch{ showMessage("registerErr",t("common.connection_failed")); }
}
async function sendPasswordReset(){
  const email=$("forgotEmail").value.trim(); showMessage("forgotMessage","");
  if(!email){ showMessage("forgotMessage",t("common.required")); return; }
  try{
    await fetch(API.forgot,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({email}),credentials:"include"});
    showMessage("forgotMessage",t("password.reset_sent"),"success");
  }catch{ showMessage("forgotMessage",t("common.connection_failed")); }
}
async function resendVerification(){
  const email=$("registerEmail").value.trim();showMessage("registerErr","");
  if(!email){showMessage("registerErr",t("common.required"));return;}
  try{
    await fetch(API.resend,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({email}),credentials:"include"});
    showMessage("registerErr",t("register.resent"),"success");
  }catch{showMessage("registerErr",t("common.connection_failed"));}
}

function agreementDocumentScrolledToEnd(){
  try{ const doc=$("agreementFrame").contentDocument,root=doc.scrollingElement||doc.documentElement; return root.scrollTop+root.clientHeight>=root.scrollHeight-8; }catch{return false;}
}
function bindAgreementFrameScroll(){
  try{
    const doc=$("agreementFrame").contentDocument,root=doc.scrollingElement||doc.documentElement;
    const update=()=>{if(agreementDocumentScrolledToEnd()) $("agreementNextBtn").disabled=false;};
    doc.addEventListener("scroll",update,{passive:true}); root.addEventListener?.("scroll",update,{passive:true}); setTimeout(update,50);
  }catch{}
}
function loadAgreementStep(step){
  agreementStep=step; const terms=step===0,frame=$("agreementFrame"),next=$("agreementNextBtn");
  $("agreementTitle").textContent=t(terms?"agreement.terms_title":"agreement.privacy_title"); $("agreementProgress").textContent=t(terms?"agreement.step_terms":"agreement.step_privacy");
  next.textContent=t(terms?"agreement.next_privacy":"agreement.accept"); next.disabled=true; frame.title=$("agreementTitle").textContent; frame.src=terms?"/terms.html?embed=1":"/privacy.html?embed=1";
}
function openAgreement(){ $("agreementModal").classList.remove("hidden"); loadAgreementStep(0); }
function closeAgreement(){ $("agreementModal").classList.add("hidden"); }
function advanceAgreement(){ if($("agreementNextBtn").disabled)return; if(agreementStep===0){loadAgreementStep(1);return;} agreementAccepted=true;closeAgreement();updateAgreementState(); }

function loadGoogleScript(){
  if(globalThis.google?.accounts?.id)return Promise.resolve();
  return new Promise((resolve,reject)=>{const script=document.createElement("script");script.src="https://accounts.google.com/gsi/client";script.async=true;script.defer=true;script.onload=resolve;script.onerror=reject;document.head.appendChild(script);});
}
async function handleGoogleCredential(credential){
  const messageId=$("registerPanel")?.classList.contains("hidden")?"loginErr":"registerErr";
  try{
    const response=await fetch(API.google,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({credential}),credentials:"include"}); const data=await readJson(response);
    if(response.ok){finishLogin();return;}
    if(response.status===428&&data?.detail==="google registration required"){
      pendingGoogleCredential=credential;setGoogleRegistrationMode(true,data?.email||"");showPanel("register");return;
    }
    showMessage(messageId,data?.detail||t("login.google_failed"));
  }catch{showMessage(messageId,t("common.connection_failed"));}
}
async function completeGoogleRegistration(){
  const payload={credential:pendingGoogleCredential,username:$("googleRegisterUser").value.trim(),accepted:agreementAccepted,terms_version:providers.terms_version,privacy_version:providers.privacy_version};
  showMessage("registerErr","");
  if(!payload.credential){resetGoogleRegistration();showMessage("registerErr",t("register.google_session_expired"));return;}
  if(!payload.username){showMessage("registerErr",t("common.required"));return;}
  try{
    const response=await fetch(API.google,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload),credentials:"include"});const data=await readJson(response);
    if(response.ok){pendingGoogleCredential="";finishLogin();return;}
    if(response.status===401){resetGoogleRegistration();showMessage("registerErr",t("register.google_session_expired"));return;}
    showMessage("registerErr",data?.detail||t("register.failed"));
  }catch{showMessage("registerErr",t("common.connection_failed"));}
}
async function setupGoogle(){
  if(!providers.google_enabled||!providers.google_client_id)return;
  try{
    await loadGoogleScript(); google.accounts.id.initialize({client_id:providers.google_client_id,nonce:providers.google_nonce,callback:r=>handleGoogleCredential(r.credential)});
    for(const [id,mode] of [["googleLoginButton","login"],["googleRegisterButton","register"]]){
      const node=$(id);if(!node)continue;
      google.accounts.id.renderButton(node,{theme:"outline",size:"large",shape:"rectangular",width:320,text:mode==="login"?"signin_with":"signup_with"});
    }
    $("googleLoginArea")?.classList.remove("hidden");updateAgreementState();
  }catch{showMessage("loginErr",t("login.google_load_failed"));}
}
async function loadProviders(){
  try{const response=await fetch(API.providers,{credentials:"include",cache:"no-store"});if(response.ok)providers={...providers,...await response.json()};}catch{}
  if(!providers.registration_enabled&&!providers.google_enabled){$("showRegisterBtn").disabled=true;$("showRegisterBtn").title=t("register.unavailable");}
  updateAgreementState();await setupGoogle();
}

function setupFirstTimeRedirect(){fetch(API.status,{credentials:"include"}).then(r=>r.json()).then(j=>{if(j?.needs_setup)location.replace("/setup.html");}).catch(()=>{});}
async function initEmbeddedLogin(){
  if(!isEmbedded())return false;document.body.classList.add("loginEmbedded");$("embedLoginBox")?.classList.remove("hidden");$("loginFormArea")?.classList.add("hidden");
  $("embedOpenTopLogin")?.addEventListener("click",()=>window.open(absoluteUrl("/login.html"),"_blank","noopener"));
  $("embedGrantAccess")?.addEventListener("click",async()=>{try{await activateStorageAccessAndRefresh();}catch(error){setEmbedMessage(error?.message||t("common.failed"),"error");}});
  $("embedRetry")?.addEventListener("click",()=>refreshEmbeddedLoginState({redirectOnSuccess:true,tryAutoActivate:true}));await refreshEmbeddedLoginState({redirectOnSuccess:false,tryAutoActivate:true});return true;
}
async function init(){
  $("showLoginBtn")?.addEventListener("click",()=>{resetGoogleRegistration();showPanel("login");});$("showRegisterBtn")?.addEventListener("click",openRegisterPanel);$("forgotPasswordBtn")?.addEventListener("click",()=>showPanel("forgot"));$("backToLoginBtn")?.addEventListener("click",()=>showPanel("login"));
  $("loginBtn")?.addEventListener("click",doLogin);$("loginPass")?.addEventListener("keydown",e=>{if(e.key==="Enter")doLogin();});$("registerBtn")?.addEventListener("click",doRegister);$("resendVerificationBtn")?.addEventListener("click",resendVerification);$("sendResetBtn")?.addEventListener("click",sendPasswordReset);
  $("completeGoogleRegisterBtn")?.addEventListener("click",completeGoogleRegistration);$("cancelGoogleRegisterBtn")?.addEventListener("click",resetGoogleRegistration);
  $("openAgreementBtn")?.addEventListener("click",openAgreement);$("closeAgreementBtn")?.addEventListener("click",closeAgreement);$("agreementNextBtn")?.addEventListener("click",advanceAgreement);$("agreementFrame")?.addEventListener("load",bindAgreementFrameScroll);
  $("agreementModal")?.addEventListener("click",e=>{if(e.target===$("agreementModal"))closeAgreement();});document.addEventListener("keydown",e=>{if(e.key==="Escape"&&!$("agreementModal")?.classList.contains("hidden"))closeAgreement();});
  showPanel("login");setupFirstTimeRedirect();if(await initEmbeddedLogin())return;
  try{await initI18n(globalThis.__NIM_BOOTSTRAP__?.user?.ui_language||"auto");}catch{}
  await loadProviders();showPanel("login");
}
init().catch(()=>showMessage("loginErr",t("common.connection_failed")));
