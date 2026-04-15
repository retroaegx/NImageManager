const SUPPORTED_LOCALES = new Set(["ja", "en"]);
const SUPPORTED_PREFERENCES = new Set(["auto", "ja", "en"]);

let currentLocale = "ja";
let currentPreference = "auto";
let currentCatalog = Object.create(null);
let initialized = false;
let initPromise = null;

function normalizeLocale(locale){
  const value = String(locale || "").trim().toLowerCase();
  if(value.startsWith("en")) return "en";
  if(value.startsWith("ja")) return "ja";
  return "ja";
}

function normalizePreference(value){
  const normalized = String(value || "auto").trim().toLowerCase();
  return SUPPORTED_PREFERENCES.has(normalized) ? normalized : "auto";
}

function readQueryLocale(){
  try{
    const value = new URL(window.location.href).searchParams.get("lang");
    return value ? normalizeLocale(value) : null;
  }catch(_e){
    return null;
  }
}

function detectLocale(){
  const query = readQueryLocale();
  if(query) return query;
  const langs = Array.isArray(navigator.languages) && navigator.languages.length
    ? navigator.languages
    : [navigator.language || navigator.userLanguage || "ja"];
  for(const lang of langs){
    const locale = normalizeLocale(lang);
    if(SUPPORTED_LOCALES.has(locale)) return locale;
  }
  return "ja";
}

function resolveLocale(preference){
  const forced = readQueryLocale();
  if(forced) return forced;
  const normalized = normalizePreference(preference);
  return normalized === "auto" ? detectLocale() : normalizeLocale(normalized);
}

async function loadCatalog(locale){
  const response = await fetch(`/i18n/${locale}.json`, { cache: "no-store" });
  if(!response.ok) throw new Error(`locale load failed: ${locale}`);
  const payload = await response.json();
  if(!payload || typeof payload !== "object" || Array.isArray(payload)){
    throw new Error(`invalid locale payload: ${locale}`);
  }
  return payload;
}

function interpolate(template, params){
  let out = String(template ?? "");
  const values = params || {};
  for(const [key, value] of Object.entries(values)){
    out = out.replaceAll(`{${key}}`, String(value ?? ""));
  }
  return out;
}

export function t(key, params){
  const message = Object.prototype.hasOwnProperty.call(currentCatalog, key)
    ? currentCatalog[key]
    : key;
  return interpolate(message, params);
}

export function getLocale(){
  return currentLocale;
}

export function getPreference(){
  return currentPreference;
}

export async function setLocale(locale){
  const normalized = normalizeLocale(locale);
  if(initialized && normalized === currentLocale) return currentLocale;
  currentCatalog = await loadCatalog(normalized);
  currentLocale = normalized;
  initialized = true;
  if(typeof document !== "undefined"){
    document.documentElement.lang = normalized;
  }
  applyTranslations(document);
  return currentLocale;
}

export async function setLocalePreference(preference, { apply=true }={}){
  currentPreference = normalizePreference(preference);
  try{ localStorage.removeItem("nim.ui_language"); }catch(_e){}
  if(!apply) return currentPreference;
  await setLocale(resolveLocale(currentPreference));
  return currentPreference;
}

function setNodeText(el, value){
  if(!(el instanceof Element)) return;
  const textNode = Array.from(el.childNodes).find((node) => node.nodeType === Node.TEXT_NODE);
  if(textNode){
    textNode.textContent = value;
    return;
  }
  el.textContent = value;
}

function applyElement(el){
  if(!(el instanceof Element)) return;
  if(el.hasAttribute("data-i18n")) setNodeText(el, t(el.getAttribute("data-i18n")));
  if(el.hasAttribute("data-i18n-html")) el.innerHTML = t(el.getAttribute("data-i18n-html"));
  if(el.hasAttribute("data-i18n-title")) el.setAttribute("title", t(el.getAttribute("data-i18n-title")));
  if(el.hasAttribute("data-i18n-placeholder")) el.setAttribute("placeholder", t(el.getAttribute("data-i18n-placeholder")));
  if(el.hasAttribute("data-i18n-aria-label")) el.setAttribute("aria-label", t(el.getAttribute("data-i18n-aria-label")));
}

export function applyTranslations(root=document){
  if(!root || !root.querySelectorAll) return;
  if(root instanceof Element) applyElement(root);
  root.querySelectorAll("[data-i18n],[data-i18n-html],[data-i18n-title],[data-i18n-placeholder],[data-i18n-aria-label]").forEach(applyElement);
}

export async function initI18n(preference="auto"){
  if(initPromise) return initPromise;
  initPromise = (async () => {
    currentPreference = normalizePreference(preference);
    await setLocale(resolveLocale(currentPreference));
    return currentLocale;
  })();
  try{
    return await initPromise;
  }finally{
    initPromise = null;
  }
}

export function formatDateTime(value, options){
  const date = value instanceof Date ? value : new Date(value);
  if(Number.isNaN(date.getTime())) return String(value ?? "");
  return new Intl.DateTimeFormat(currentLocale === "en" ? "en-US" : "ja-JP", options || {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

export function formatNumber(value){
  return new Intl.NumberFormat(currentLocale === "en" ? "en-US" : "ja-JP").format(Number(value || 0));
}

const api = {
  initI18n,
  setLocale,
  setLocalePreference,
  getLocale,
  getPreference,
  detectLocale,
  applyTranslations,
  formatDateTime,
  formatNumber,
  t,
};

globalThis.NIMI18n = api;

export default api;
