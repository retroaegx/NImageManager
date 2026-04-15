import { initI18n, applyTranslations } from "./i18n.js";

function readBootstrapPreference(){
  try{
    return globalThis.__NIM_BOOTSTRAP__?.user?.ui_language || "auto";
  }catch(_e){
    return "auto";
  }
}

async function start(){
  await initI18n(readBootstrapPreference());
  applyTranslations(document);
}

if(document.readyState === "loading"){
  document.addEventListener("DOMContentLoaded", start, { once: true });
}else{
  start();
}
