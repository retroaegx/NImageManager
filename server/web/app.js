import { $, escapeHtml } from "./lib/dom.js?v=20260307_01";
import { apiFetch, apiJson } from "./lib/http.js?v=20260307_01";
import { bindUserMenu } from "./lib/userMenu.js?v=20260312_02";
import { loadCurrentUser, logoutAndRedirect } from "./lib/session.js?v=20260307_01";
import { buildPageQuery as buildPageQueryShared, buildPageQueryCore as buildPageQueryCoreShared, buildScrollQuery as buildScrollQueryShared } from "./lib/galleryQuery.js?v=20260307_01";
import { joinKeep, joinPlain, promptTextForCopyKeep, promptTextForCopyPlain } from "./lib/prompt.js?v=20260321_02";

const API = {
  login: "/api/auth/login",
  logout: "/api/auth/logout",
  pwLink: "/api/auth/password_link",
  me: "/api/me",
  upload: "/api/upload",
  uploadBatchInit: "/api/upload_batch/init",
  uploadBatchAppend: (id) => `/api/upload_batch/${id}/append`,
  uploadBatchFinish: (id) => `/api/upload_batch/${id}/finish`,
  uploadZip: "/api/upload_zip",
  uploadZipChunkInit: "/api/upload_zip_chunk/init",
  uploadZipChunkAppend: "/api/upload_zip_chunk/append",
  uploadZipChunkFinish: "/api/upload_zip_chunk/finish",
  uploadZipStatus: (id) => `/api/upload_zip/${id}`,
  uploadZipCancel: (id) => `/api/upload_zip/${id}/cancel`,
  list: "/api/images",               // cursor-based (legacy)
  pageList: "/api/images_page",      // page-based (UI)
  scrollList: "/api/images_scroll",  // cursor-based (UI)
  detail: (id) => `/api/images/${id}/detail`,
  detailsBatch: "/api/images/details",
  debugPerf: "/api/debug/perf",
  favorite: (id) => `/api/images/${id}/favorite`, // compat
  bookmarkLists: "/api/bookmarks/lists",
  bookmarkSidebar: "/api/bookmarks/sidebar",
  bookmarkList: (id) => `/api/bookmarks/lists/${id}`,
  bookmarkImage: (id) => `/api/bookmarks/images/${id}`,
  bookmarkDefault: (id) => `/api/bookmarks/images/${id}/default`,
  bookmarkClear: (id) => `/api/bookmarks/images/${id}/clear`,
  bookmarkBulkStatus: "/api/bookmarks/bulk/status",
  bookmarkBulkApply: "/api/bookmarks/bulk/apply",
  bulkDelete: "/api/images/bulk_delete",
  creators: "/api/creators/list",
  software: "/api/stats/software",
  dayCounts: (month) => `/api/stats/day_counts?month=${encodeURIComponent(month)}`,
  monthCounts: (year) => `/api/stats/month_counts?year=${encodeURIComponent(year)}`,
  yearCounts: `/api/stats/year_counts`,
  suggest: (q) => `/api/tags/suggest?q=${encodeURIComponent(q)}&limit=24`,
  creatorListAdd: "/api/creators/list",
  creatorListDel: (id) => `/api/creators/list/${id}`,
  bookmarkSubAdd: "/api/bookmarks/subscriptions",
  bookmarkSubDel: (id) => `/api/bookmarks/subscriptions/${id}`,
  userSuggest: (kind, q) => `/api/users/suggest?kind=${encodeURIComponent(kind||'')}&q=${encodeURIComponent(q||'')}&limit=20`,
};

function currentBookmarkContextListId(){
  const lid = Number(state?.preview?.bm_list_id || 0);
  return lid > 0 ? lid : null;
}

function withBookmarkContext(url, bmListId = currentBookmarkContextListId()){
  const lid = Number(bmListId || 0);
  if(!url || !(lid > 0)) return url;
  return `${url}${String(url).includes("?") ? "&" : "?"}bm_list_id=${encodeURIComponent(String(lid))}`;
}

function detailCacheKey(id, bmListId = currentBookmarkContextListId()){
  const iid = Number(id || 0);
  const lid = Number(bmListId || 0) || 0;
  return `${iid}:${lid}`;
}

function xhrPostBinary(url, body, contentType, onProgress){
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", url, true);
    xhr.withCredentials = true;
    if(contentType) xhr.setRequestHeader("Content-Type", contentType);
    if(xhr.upload && onProgress){
      xhr.upload.onprogress = (e) => {
        try{
          const loaded = Number(e.loaded || 0);
          const total = e.lengthComputable ? Number(e.total || 0) : 0;
          onProgress(loaded, total);
        }catch(_e){}
      };
    }
    xhr.onerror = () => reject(new TypeError("Failed to fetch"));
    xhr.onload = () => {
      if(xhr.status === 401){
        location.replace("/login.html");
        reject(new Error("unauthorized"));
        return;
      }
      if(xhr.status === 403){
        const target = (typeof window !== "undefined" && window.location && window.location.pathname !== "/") ? "/" : null;
        if(target) location.replace(target);
        reject(new Error("forbidden"));
        return;
      }
      const text = xhr.responseText || "";
      if(xhr.status < 200 || xhr.status >= 300){
        reject(new Error(`${xhr.status} ${(text||"").slice(0,140)}`));
        return;
      }
      if(!text){ resolve(null); return; }
      try{ resolve(JSON.parse(text)); }
      catch(_e){ reject(new Error(`bad json: ${(text||"").slice(0,140)}`)); }
    };
    xhr.send(body);
  });
}
const state = {
  user: null,
  uploadQueue: [],
  uploadStop: false,
  uploadZipJob: null,
  uploadSummary: {
    creators: new Map(),
    software: new Map(),
    tags: new Map(),
  },
  uploadBookmark: {
    enabled: false,
    list_id: 0,
  },
  perfEnabled: false,
  preview: {
    items: [],
    tags: [],
		tags_not: [],
    dedup_only: 0,
    creator: "",
    software: "",
    date_from: "",
    date_to: "",
    bm_any: 0,
    bm_list_id: 0,
    sort: (localStorage.getItem("gallery_sort") || "newest"),
    view: (localStorage.getItem("gallery_view") || "grid"),
    cursor: null,
    done: false,
    // scroll mode fetch size (mobile-first). PC uses page-based (limit=16).
    limit: 30,
    loading: false,
    mode: "scroll", // "scroll" (mobile) / "page" (PC)
    page: 1,
    total_pages: 0,
    total_count: 0,
    got_total: false,
    bulk: {
      selected: new Set(),
      deselected: new Set(),
      all: false,
      deleting: false,
    },
  },
  bookmarks: {
    lists: [],
    any_count: 0,
    others: [],
    expanded: new Set(),
  },
  calendar: {
    month: new Date(),
    counts: new Map(),           // day counts (ymd -> n)
    monthCounts: new Map(),      // month counts (ym -> n) for the loaded year
    yearCounts: new Map(),       // year counts (yyyy -> n)
    loadedYear: "",
    from: "",
    to: "",
    dragging: false,
    drag_anchor: "",
    press_key: "",
    press_x: 0,
    press_y: 0,
    click_anchor: "",
    click_ts: 0,
    keep_anchor: false,
    // touch drag (mobile Safari fallback)
    touch_key: "",
    touch_x: 0,
    touch_y: 0,
    _scrollLock: null,
    picker: "",             // '' | 'year' | 'month'
  },
  facets: {
    creators: [],
    softwares: [],
    creatorSet: new Set(),
  },
  selectedTileId: null,
};

let scrollObserver = null;
let _autoFillBurst = 0;
let _queuedLoadMore = false;
let _drainActive = false;
let _drainPending = false;
let _bottomBindingsInstalled = false;

// Very small client-side caches to reduce perceived latency.
// (Do not persist to disk; keep it purely in-memory.)
const _pageCache = new Map(); // key -> { promise, data, ts }
let _scrollPrefetch = null;   // { key, promise, data, ts }
const PREVIEW_CACHE_TTL_MS = 15000;
const _overlayWarmSet = new Set();
const _thumbWarmSet = new Set();
const _thumbWarmImages = new Set();
let _thumbPreferredFormat = "webp";
let _thumbPreferredFormatReady = null;
let _suspendPreviewWarmup = false;
const _previewRequestControllers = new Set();
let _previewSearchSeq = 0;

function _registerPreviewRequestController(controller){
  if(!controller) return controller;
  _previewRequestControllers.add(controller);
  return controller;
}

function _releasePreviewRequestController(controller){
  if(!controller) return;
  _previewRequestControllers.delete(controller);
}

function _abortPreviewRequests(){
  for(const controller of Array.from(_previewRequestControllers)){
    try{ controller.abort(); }catch(_e){}
  }
  _previewRequestControllers.clear();
}

function _isAbortError(err){
  const name = String(err?.name || "");
  const msg = String(err?.message || "");
  return name === "AbortError" || /aborted|aborterror/i.test(msg);
}

function _cancelThumbWarmers(){
  _thumbWarmImages.forEach((img) => {
    try{
      img.onload = null;
      img.onerror = null;
      img.src = "";
    }catch(_e){}
  });
  _thumbWarmImages.clear();
}

function _abortGridImageLoads(){
  const grid = $("grid");
  if(!grid) return;
  grid.querySelectorAll("img").forEach((img) => {
    try{
      img.loading = "eager";
      img.removeAttribute("srcset");
      img.removeAttribute("sizes");
      img.src = "";
    }catch(_e){}
  });
}

function _thumbWebpUrl(it){
  if(typeof it === "string") return String(it || "");
  return String(it?.thumb || "");
}

function _thumbAvifUrl(it){
  const webpUrl = _thumbWebpUrl(it);
  if(!webpUrl) return "";
  if(!/\/thumbs\/grid\/webp\//i.test(webpUrl) || !/\.webp(?:[?#].*)?$/i.test(webpUrl)) return "";
  return webpUrl
    .replace(/\/thumbs\/grid\/webp\//i, "/thumbs/grid/avif/")
    .replace(/\.webp((?:\?|#).*)?$/i, ".avif$1");
}

const _AVIF_DETECT_DATA_URI = "data:image/avif;base64,AAAAIGZ0eXBhdmlmAAAAAGF2aWZtaWYxbWlhZk1BMUIAAADybWV0YQAAAAAAAAAoaGRscgAAAAAAAAAAcGljdAAAAAAAAAAAAAAAAGxpYmF2aWYAAAAADnBpdG0AAAAAAAEAAAAeaWxvYwAAAABEAAABAAEAAAABAAABGgAAAB0AAAAoaWluZgAAAAAAAQAAABppbmZlAgAAAAABAABhdjAxQ29sb3IAAAAAamlwcnAAAABLaXBjbwAAABRpc3BlAAAAAAAAAAIAAAACAAAAEHBpeGkAAAAAAwgICAAAAAxhdjFDgQ0MAAAAABNjb2xybmNseAACAAIAAYAAAAAXaXBtYQAAAAAAAAABAAEEAQKDBAAAACVtZGF0EgAKCBgANogQEAwgMg8f8D///8WfhwB8+ErK42A=";

function _detectThumbPreferredFormatAsync(){
  return new Promise((resolve) => {
    try{
      const img = new Image();
      let done = false;
      const finish = (fmt) => {
        if(done) return;
        done = true;
        resolve(fmt);
      };
      img.onload = () => finish((img.width > 0 && img.height > 0) ? "avif" : "webp");
      img.onerror = () => finish("webp");
      img.src = _AVIF_DETECT_DATA_URI;
    }catch(_e){
      resolve("webp");
    }
  });
}

async function ensureThumbPreferredFormat(){
  if(_thumbPreferredFormatReady) return _thumbPreferredFormatReady;
  _thumbPreferredFormatReady = _detectThumbPreferredFormatAsync().then((fmt) => {
    _thumbPreferredFormat = (fmt === "avif") ? "avif" : "webp";
    try{ document.documentElement.dataset.thumbFormat = _thumbPreferredFormat; }catch(_e){}
    return _thumbPreferredFormat;
  }).catch(() => {
    _thumbPreferredFormat = "webp";
    try{ document.documentElement.dataset.thumbFormat = _thumbPreferredFormat; }catch(_e){}
    return _thumbPreferredFormat;
  });
  return await _thumbPreferredFormatReady;
}

function _thumbDisplayUrl(it){
  const webpUrl = _thumbWebpUrl(it);
  if(!webpUrl) return "";
  if(_thumbPreferredFormat === "avif"){
    const avifUrl = _thumbAvifUrl(webpUrl);
    if(avifUrl) return avifUrl;
  }
  return webpUrl;
}

function _thumbImgHtml(it){
  const src = _thumbDisplayUrl(it);
  return `<img loading="lazy" decoding="async" src="${escapeHtml(src)}" alt="">`;
}


function invalidatePreviewCaches(){
  // Used when the underlying dataset changes (e.g. bulk delete) but the search key stays the same.
  _pageCache.clear();
  _scrollPrefetch = null;
  _overlayWarmSet.clear();
  _thumbWarmSet.clear();
  _abortPreviewRequests();
  _cancelThumbWarmers();
  _abortGridImageLoads();
  // Detail cache can contain deleted ids; keep it simple.
  _detailCache.clear();
  _detailInFlight.clear();
}
function _isPreviewCacheFresh(entry){
  return !!entry && (Date.now() - Number(entry.ts || 0)) <= PREVIEW_CACHE_TTL_MS;
}

function _getFreshPageCache(key){
  const entry = _pageCache.get(key);
  if(!_isPreviewCacheFresh(entry)){
    if(entry) _pageCache.delete(key);
    return null;
  }
  return entry;
}

function _getFreshScrollPrefetch(key){
  if(!_scrollPrefetch || _scrollPrefetch.key !== key) return null;
  if(!_isPreviewCacheFresh(_scrollPrefetch)){
    _scrollPrefetch = null;
    return null;
  }
  return _scrollPrefetch;
}

let _previewSearchKey = "";

const BP_MOBILE = 1024; // unified breakpoint (matches styles.css @media (max-width: 1024px))

function isMobile(){
  return (window.innerWidth || 0) <= BP_MOBILE;
}

function isDesktop(){
  return !isMobile();
}


let _activeView = "preview";
let _previewInited = false;

function isPreviewVisible(){
  const panel = $("viewPreview");
  return _activeView === "preview" && !!panel && !panel.classList.contains("hidden");
}

function setView(active, opts={}){
  const { pushState=true } = opts;
  const prev = _activeView;
  _activeView = active;

  const elUpload = $("viewUpload");
  const elPreview = $("viewPreview");

  // Mobile: slide-in animation (one panel visible at a time).
  if(isMobile() && prev !== active){
    const target = (active === "preview") ? elPreview : elUpload;
    const other = (active === "preview") ? elUpload : elPreview;

    other.classList.add("hidden");
    target.classList.remove("hidden");

    // reset any previous transition classes
    target.classList.remove("panelSlide", "fromLeft", "fromRight", "enter");
    // preview is left, upload is right
    target.classList.add("panelSlide", (active === "upload") ? "fromRight" : "fromLeft");
    requestAnimationFrame(() => {
      target.classList.add("enter");
      // cleanup
      setTimeout(() => target.classList.remove("panelSlide", "fromLeft", "fromRight", "enter"), 260);
    });
  }else{
    elUpload.classList.toggle("hidden", active !== "upload");
    elPreview.classList.toggle("hidden", active !== "preview");
  }

  $("viewUpload").setAttribute("aria-hidden", active !== "upload" ? "true" : "false");
  $("viewPreview").setAttribute("aria-hidden", active !== "preview" ? "true" : "false");

  $("navUpload").classList.toggle("active", active === "upload");
  $("navPreview").classList.toggle("active", active === "preview");

  const appEl = $("app");
  appEl?.classList.toggle("view-preview", active === "preview");
  appEl?.classList.toggle("view-upload", active === "upload");

  if(active !== "preview") _abortPreviewRequests();
  if(active !== "preview" && scrollObserver){
    try{ scrollObserver.disconnect(); }catch(_e){}
    scrollObserver = null;
  }else if(active === "preview" && state.preview.mode === "scroll"){
    ensureScrollObserver();
  }

  if(pushState){
    try{ history.pushState(null, "", `#${active}`); }catch(_e){}
  }
}

function bindMobileSwipe(){
  const main = document.querySelector("main.main");
  if(!main) return;
  let sx = 0, sy = 0;
  let tracking = false;
  let decided = false;
  let dx = 0, dy = 0;

  const shouldIgnore = () => {
    // Do not swipe-switch while a modal overlay is open.
    const ov = $("overlay");
    if(ov && ov.classList.contains("open")) return true;
    const fo = $("filterOverlay");
    if(fo && !fo.classList.contains("hidden")) return true;
    return false;
  };

  main.addEventListener("touchstart", (e) => {
    if(!isMobile()) return;
    if(shouldIgnore()) return;
    if(!(e.touches && e.touches.length === 1)) return;
    tracking = true;
    decided = false;
    dx = 0; dy = 0;
    sx = e.touches[0].clientX;
    sy = e.touches[0].clientY;
  }, { passive: true });

  main.addEventListener("touchmove", (e) => {
    if(!tracking) return;
    if(!(e.touches && e.touches.length === 1)) return;
    dx = e.touches[0].clientX - sx;
    dy = e.touches[0].clientY - sy;
    if(!decided){
      if(Math.abs(dx) > 16 && Math.abs(dx) > (Math.abs(dy) + 8)){
        decided = true;
      }else if(Math.abs(dy) > 18){
        // vertical scroll; abort swipe.
        tracking = false;
        return;
      }
    }
    if(decided){
      // prevent accidental horizontal page pan
      e.preventDefault();
    }
  }, { passive: false });

  main.addEventListener("touchend", () => {
    if(!tracking) return;
    tracking = false;
    if(!decided) return;
    if(Math.abs(dx) < 70) return;

    // preview is left, upload is right
    if(dx < 0 && _activeView === "preview"){
      // swipe left -> upload
      setView("upload");
    }else if(dx > 0 && _activeView === "upload"){
      // swipe right -> preview
      setView("preview");
      if(!_previewInited) initPreview().catch(()=>{});
    }
  }, { passive: true });
}

async function loadMe(){
  const me = await loadCurrentUser({
    endpoint: API.me,
    onLoaded: (user) => {
      state.user = user;
      state.perfEnabled = !!user?.perf_enabled;
      const isAdmin = user?.role === "admin" || user?.role === "master";
      bindUserMenu({
        logoutEndpoint: API.logout,
        passwordLinkEndpoint: API.pwLink,
        showAdmin: isAdmin,
        showMaintenance: isAdmin,
      });
    },
  });
  state.user = me;
  state.perfEnabled = !!me?.perf_enabled;
  return me;
}

async function doLogout(){
  await logoutAndRedirect(API.logout);
}

function fmtMtime(m){
  if(!m) return "";
  return m.replace("T"," ").replace("+00:00","Z");
}

function fmtShort(m){
  if(!m) return "";
  const [d, t] = m.split("T");
  if(!d || !t) return m;
  const mmdd = d.slice(5).replace("-", "/");
  const hhmm = t.slice(0,5);
  return `${mmdd} ${hhmm}`;
}

/* =====================
   Upload (sequential)
   ===================== */

function resetUploadSummary(){
  state.uploadSummary.creators = new Map();
  state.uploadSummary.software = new Map();
  state.uploadSummary.tags = new Map();
}

function bump(map, key, n=1){
  if(!key) return;
  map.set(key, (map.get(key) || 0) + n);
}

function applyUploadSummary(detail){
  if(!detail) return;
  bump(state.uploadSummary.software, detail.software || "(unknown)", 1);
}

function renderUploadSummary(){
  const box = $("uploadSummary");
  if(!box) return;

  const sortMap = (m) => Array.from(m.entries()).sort((a,b)=> (b[1]-a[1]) || String(a[0]).localeCompare(String(b[0])));
  const softwares = sortMap(state.uploadSummary.software);

  const renderList = (title, items) => {
    const wrap = document.createElement("div");
    wrap.className = "uploadSumBlock";
    wrap.innerHTML = `<div class="uploadSumHead">${escapeHtml(title)}</div>`;
    const list = document.createElement("div");
    list.className = "uploadSumList";
    if(!items.length){
      const s = document.createElement("div");
      s.className = "small";
      s.textContent = "(none)";
      list.appendChild(s);
    }else{
      items.forEach(([k,v]) => {
        const row = document.createElement("div");
        row.className = "uploadSumItem";
        row.innerHTML = `<div class="name">${escapeHtml(k)}</div><div class="cnt">${Number(v||0)}</div>`;
        list.appendChild(row);
      });
    }
    wrap.appendChild(list);
    return wrap;
  };

  box.innerHTML = "";
  box.appendChild(renderList("ソフト", softwares));
}

function _defaultUploadBookmarkListId(){
  const lists = Array.isArray(state.bookmarks.lists) ? state.bookmarks.lists : [];
  const def = lists.find((x) => Number(x?.is_default || 0) === 1) || lists[0] || null;
  return def ? Number(def.id || 0) : 0;
}

function syncUploadBookmarkControls(){
  const toggle = $("uploadBookmarkToggle");
  const sel = $("uploadBookmarkList");
  if(!toggle || !sel) return;

  const lists = Array.isArray(state.bookmarks.lists) ? state.bookmarks.lists : [];
  const selected = Number(state.uploadBookmark.list_id || 0);
  const defaultId = _defaultUploadBookmarkListId();
  const nextSelected = lists.some((x) => Number(x?.id || 0) === selected) ? selected : defaultId;
  state.uploadBookmark.list_id = nextSelected;

  sel.innerHTML = "";
  if(!lists.length){
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "ブックマークなし";
    sel.appendChild(opt);
    state.uploadBookmark.enabled = false;
  }else{
    lists.forEach((item) => {
      const opt = document.createElement("option");
      const id = Number(item?.id || 0);
      opt.value = String(id);
      opt.textContent = Number(item?.is_default || 0) === 1 ? `${String(item?.name || "")}（既定）` : String(item?.name || "");
      if(id === Number(state.uploadBookmark.list_id || 0)) opt.selected = true;
      sel.appendChild(opt);
    });
  }

  const hasLists = lists.length > 0;
  const enabled = !!state.uploadBookmark.enabled && hasLists;
  toggle.classList.toggle("on", enabled);
  toggle.classList.toggle("off", !enabled);
  toggle.textContent = enabled ? "ON" : "OFF";
  toggle.disabled = !hasLists;
  toggle.title = hasLists ? "アップロード完了時にブックマークへ追加" : "ブックマークリストがありません";
  sel.disabled = !enabled;
  sel.classList.toggle("hidden", !enabled);
  if(enabled && Number(state.uploadBookmark.list_id || 0) > 0){
    sel.value = String(state.uploadBookmark.list_id || 0);
  }
}

function getUploadBookmarkConfig(){
  const lists = Array.isArray(state.bookmarks.lists) ? state.bookmarks.lists : [];
  if(!state.uploadBookmark.enabled || !lists.length){
    return { bookmark_enabled: 0, bookmark_list_id: null };
  }
  const sel = $("uploadBookmarkList");
  const fallbackId = _defaultUploadBookmarkListId();
  const selectedId = Number(sel?.value || state.uploadBookmark.list_id || fallbackId || 0);
  state.uploadBookmark.list_id = selectedId > 0 ? selectedId : fallbackId;
  return {
    bookmark_enabled: 1,
    bookmark_list_id: Number(state.uploadBookmark.list_id || fallbackId || 0) || null,
  };
}

function pickTopTags(detail, limit=6){
  return [];
}

const UPLOAD_PARALLEL = 4;
let _uploadThumbObserver = null;

function _ensureUploadThumbObserver(){
  if(_uploadThumbObserver) return _uploadThumbObserver;
  _uploadThumbObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if(!entry.isIntersecting) return;
      const img = entry.target;
      _uploadThumbObserver.unobserve(img);
      const src = img.dataset.src || "";
      if(!src) return;
      img.addEventListener("load", () => {
        img.classList.remove("isHidden");
        const ph = img._uploadPlaceholder;
        if(ph) ph.remove();
      }, { once: true });
      img.addEventListener("error", () => {
        const ph = img._uploadPlaceholder;
        if(ph) ph.classList.remove("hidden");
      }, { once: true });
      img.src = src;
    });
  }, { root: $("uploadListWrap"), rootMargin: "240px 0px" });
  return _uploadThumbObserver;
}

function uploadProgressUpdate(){
  const total = (state.uploadQueue || []).length;
  let ok = 0, ng = 0, dup = 0, doing = 0, staged = 0;
  (state.uploadQueue || []).forEach(it => {
    if(!it) return;
    if(it.state === "完了") ok++;
    else if(it.state === "失敗") ng++;
    else if(it.state === "重複") dup++;
    else if(it.state === "アップロード中") doing++;
    else if(it.state === "受信済み") staged++;
  });
  const job = state.uploadZipJob;
  const isDirectCollecting = !!(job && job.source_kind === "direct" && (job.status === "collecting" || job.status === "queued"));
  const done = ok + ng + dup;
  let text = "";
  let pct = 0;
  if(total){
    if(isDirectCollecting){
      const sent = Math.min(total, staged + ng);
      text = `受信 ${sent}/${total}（受信済み ${staged} / 失敗 ${ng}${doing ? ` / 送信中 ${doing}` : ""}）`;
      pct = Math.min(100, Math.max(0, (sent / total) * 100));
    }else{
      text = `完了 ${done}/${total}（成功 ${ok} / 重複 ${dup} / 失敗 ${ng}${doing ? ` / 送信中 ${doing}` : ""}${staged ? ` / 受信済み ${staged}` : ""}）`;
      pct = Math.min(100, Math.max(0, (done / total) * 100));
    }
  }
  const elText = $("uploadProgressText");
  if(elText) elText.textContent = text;
  const fill = $("uploadProgressBar");
  if(fill) fill.style.width = `${pct.toFixed(1)}%`;
}

function _createUploadListItemElement(it){
  const div = document.createElement("div");
  div.className = "uploadItem";

  const left = document.createElement("div");
  left.className = "uploadLeft";

  const thumbWrap = document.createElement("div");
  thumbWrap.className = "uploadThumbWrap";
  const ph = document.createElement("div");
  ph.className = "uploadThumb ph";
  thumbWrap.appendChild(ph);

  if(it.previewUrl){
    const img = document.createElement("img");
    img.className = "uploadThumb isHidden";
    img.alt = "";
    img.loading = "lazy";
    img.decoding = "async";
    img.dataset.src = it.previewUrl;
    img._uploadPlaceholder = ph;
    thumbWrap.appendChild(img);
    _ensureUploadThumbObserver().observe(img);
  }

  const info = document.createElement("div");
  info.className = "uploadInfo";
  const name = document.createElement("div");
  name.className = "uploadName";
  name.textContent = it.file?.name || it.name || "";
  const chips = document.createElement("div");
  chips.className = "uploadChips";
  info.appendChild(name);
  info.appendChild(chips);

  left.appendChild(thumbWrap);
  left.appendChild(info);

  const right = document.createElement("div");
  right.className = "uploadRight";
  const st = document.createElement("div");
  st.className = "uploadState";
  st.textContent = it.state || "待機";
  right.appendChild(st);

  div.appendChild(left);
  div.appendChild(right);

  it._el = div;
  it._elState = st;
  it._elChips = chips;

  return div;
}

function uploadListInit(){
  const wrap = $("uploadList");
  if(!wrap) return;
  wrap.innerHTML = "";
  const frag = document.createDocumentFragment();
  (state.uploadQueue || []).forEach(it => frag.appendChild(_createUploadListItemElement(it)));
  wrap.appendChild(frag);
}

function uploadListAppendItems(items){
  const wrap = $("uploadList");
  if(!wrap) return;
  const frag = document.createDocumentFragment();
  (items || []).forEach(it => frag.appendChild(_createUploadListItemElement(it)));
  wrap.appendChild(frag);
}

function uploadListUpdateItem(it){
  if(!it || !it._elState) return;
  const st = it.state || "";
  it._elState.textContent = st;
  it._elState.classList.toggle("ok", st === "完了");
  it._elState.classList.toggle("ng", st === "失敗");
  it._elState.classList.toggle("dup", st === "重複");

  if(it._elChips){
    const chips = [];
    if(it.detail?.software) chips.push(`<span class="uploadChip">${escapeHtml(it.detail.software)}</span>`);
    it._elChips.innerHTML = chips.join("");
  }
}

async function _runUploadPool(items, concurrency, worker){
  const list = Array.from(items || []);
  const n = Math.max(1, Math.min(Number(concurrency || 1), list.length || 1));
  let cursor = 0;
  const runOne = async () => {
    while(true){
      if(state.uploadStop) return;
      const idx = cursor;
      cursor += 1;
      if(idx >= list.length) return;
      await worker(list[idx], idx);
    }
  };
  await Promise.all(Array.from({ length: n }, () => runOne()));
}

function _applyUploadJobDetails(items){
  try{
    const seen = state._zipSeen || new Set();
    (items || []).forEach(it => {
      const iid = Number(it.image_id || 0);
      if(!iid || seen.has(iid)) return;
      seen.add(iid);
      if(it.detail) applyUploadSummary(it.detail);
    });
    state._zipSeen = seen;
    renderUploadSummary();
  }catch(_e){}
}

function _renderUploadJobStatus(jobLabel, job){
  const jobBox = $("uploadZipJob");
  const fill = $("uploadProgressBar");
  const elText = $("uploadProgressText");
  if(!job) return;
  const doneAll = Number(job.done || 0) + Number(job.failed || 0) + Number(job.dup || 0);
  const total = Number(job.total || 0);
  const label = job.source_kind === "direct" ? "画像" : "zip";
  const name = escapeHtml(jobLabel || job.filename || "");
  if(jobBox){
    const progText = (total > 0)
      ? `${doneAll}/${total}（成功 ${job.done || 0} / 重複 ${job.dup || 0} / 失敗 ${job.failed || 0}）`
      : `スキャン中…（成功 ${job.done || 0} / 重複 ${job.dup || 0} / 失敗 ${job.failed || 0}）`;
    jobBox.classList.remove("hidden");
    jobBox.innerHTML = `
      <div class="row"><b>${label}</b><span class="mut">${name}</span></div>
      <div class="row"><span>進捗</span><span>${progText}</span></div>
    `;
  }
  if(fill){
    const pct = total > 0 ? Math.min(100, Math.max(0, (doneAll / total) * 100)) : 2;
    fill.style.width = `${pct.toFixed(1)}%`;
  }
  if(elText){
    elText.textContent = total > 0 ? `${label} ${doneAll}/${total}` : `${label} スキャン中…`;
  }
}

async function _pollActiveUploadJob(jobLabel){
  const poll = async () => {
    if(state.uploadStop){
      _zipPollTimer = null;
      return;
    }
    try{
      const j = state.uploadZipJob;
      if(!j || !j.id) return;
      const isDirect = String(j.source_kind || "") === "direct";
      const limit = isDirect ? Math.max(300, (state.uploadQueue || []).length + 50) : 300;
      let after = isDirect ? 0 : Number(state.uploadZipLastSeq || 0);

      while(true){
        const r = await apiFetch(`${API.uploadZipStatus(j.id)}?after_seq=${encodeURIComponent(String(after))}&limit=${encodeURIComponent(String(limit))}`);
        const st = await apiJson(r);

        j.total = Number(st.total || j.total || 0);
        j.done = Number(st.done || 0);
        j.failed = Number(st.failed || 0);
        j.dup = Number(st.dup || 0);
        j.status = String(st.status || "running");
        j.source_kind = String(st.source_kind || j.source_kind || "zip");
        j.filename = String(st.filename || jobLabel || "");

        const items = (st.items || []).map(x => ({
          seq: Number(x.seq || 0),
          name: x.filename || "",
          previewUrl: "",
          state: x.state || "",
          detail: x.detail || null,
          image_id: x.image_id || null,
        })).filter(x => x.seq > 0);

        if(isDirect){
          const bySeq = new Map((state.uploadQueue || []).map(it => [Number(it.seq || 0), it]));
          const extras = [];
          items.forEach(it => {
            const cur = bySeq.get(it.seq);
            if(cur){
              cur.state = it.state || cur.state;
              cur.detail = it.detail || cur.detail || null;
              cur.image_id = it.image_id || cur.image_id || null;
              uploadListUpdateItem(cur);
            }else{
              extras.push(it);
            }
          });
          if(extras.length){
            state.uploadQueue = (state.uploadQueue || []).concat(extras);
            uploadListAppendItems(extras);
            extras.forEach(it => uploadListUpdateItem(it));
          }
          _applyUploadJobDetails(items);
        }else if(items.length){
          state.uploadQueue = (state.uploadQueue || []).concat(items);
          uploadListAppendItems(items);
          items.forEach(it => uploadListUpdateItem(it));
          _applyUploadJobDetails(items);
        }

        const latest = Number(st.latest_seq || 0);
        if(!isDirect && latest > after) after = latest;
        if(isDirect || items.length < limit) break;
      }

      state.uploadZipLastSeq = isDirect ? 0 : after;
      _renderUploadJobStatus(jobLabel, j);

      if(j.status === "done" || j.status === "error" || j.status === "cancelled"){
        _zipPollTimer = null;
        await refreshStatsAndPreviewAfterChange();
        return;
      }
    }catch(_e){}
    _zipPollTimer = setTimeout(poll, 500);
  };
  if(_zipPollTimer) clearTimeout(_zipPollTimer);
  _zipPollTimer = setTimeout(poll, 200);
}

async function startUploadFiles(files){
  const imgs = (files || []).filter(f => f && f.type && f.type.startsWith("image/"));
  if(!imgs.length) return;

  try{
    (state.uploadQueue || []).forEach(it => {
      if(it && it.previewUrl) URL.revokeObjectURL(it.previewUrl);
    });
  }catch(_e){}

  state.uploadStop = false;
  state.uploadZipJob = null;
  state._zipSeen = new Set();
  state.uploadZipLastSeq = 0;
  const jobBox = $("uploadZipJob");
  if(jobBox){
    jobBox.classList.remove("hidden");
    jobBox.innerHTML = `<div class="row"><b>画像</b><span class="mut">アップロードを準備中…</span></div>`;
  }

  state.uploadQueue = imgs.map((f, idx) => ({
    seq: idx + 1,
    file: f,
    name: f.name || "",
    state: "待機",
    previewUrl: (() => {
      try{ return URL.createObjectURL(f); }catch(_e){ return ""; }
    })(),
    detail: null,
    image_id: null,
  }));
  resetUploadSummary();
  renderUploadSummary();
  uploadListInit();
  uploadProgressUpdate();

  let jobId = 0;
  try{
    const bookmarkConfig = getUploadBookmarkConfig();
    const initRes = await apiFetch(API.uploadBatchInit, {
      method: "POST",
      body: JSON.stringify({ total: state.uploadQueue.length, ...bookmarkConfig }),
    });
    const initData = await apiJson(initRes);
    jobId = Number(initData?.job_id || 0);
    if(!jobId) throw new Error("batch init failed");
    state.uploadZipJob = { id: jobId, total: Number(initData?.total || state.uploadQueue.length), done: 0, failed: 0, dup: 0, status: "collecting", source_kind: "direct", filename: "画像アップロード" };
  }catch(_e){
    if(jobBox) jobBox.innerHTML = `<div class="row"><b>画像</b><span class="mut">開始に失敗</span></div>`;
    return;
  }

  await _runUploadPool(state.uploadQueue, UPLOAD_PARALLEL, async (it) => {
    if(state.uploadStop) return;
    it.state = "アップロード中";
    uploadListUpdateItem(it);
    uploadProgressUpdate();
    try{
      const qs = new URLSearchParams();
      qs.set("seq", String(it.seq || 0));
      qs.set("filename", String(it.file?.name || "upload"));
      qs.set("last_modified_ms", String(it.file?.lastModified || ""));
      await xhrPostBinary(
        `${API.uploadBatchAppend(jobId)}?${qs.toString()}`,
        it.file,
        String((it.file && it.file.type) || "application/octet-stream"),
      );
      it.state = "受信済み";
    }catch(_e){
      it.state = "失敗";
    }
    uploadListUpdateItem(it);
    uploadProgressUpdate();
  });

  if(state.uploadStop) return;

  const uploadedCount = (state.uploadQueue || []).filter(it => it && it.state === "受信済み").length;
  if(uploadedCount <= 0){
    if(jobBox) jobBox.innerHTML = `<div class="row"><b>画像</b><span class="mut">受信できたファイルがありません</span></div>`;
    return;
  }

  try{
    const finRes = await apiFetch(API.uploadBatchFinish(jobId), { method: "POST" });
    const fin = await apiJson(finRes);
    if(state.uploadZipJob){
      state.uploadZipJob.total = Number(fin?.total || uploadedCount);
      state.uploadZipJob.status = String(fin?.status || "queued");
    }
    if(jobBox){
      jobBox.innerHTML = `<div class="row"><b>画像</b><span class="mut">受信完了・サーバー処理を開始…</span></div>`;
    }
    await _pollActiveUploadJob("画像アップロード");
  }catch(_e){
    if(jobBox) jobBox.innerHTML = `<div class="row"><b>画像</b><span class="mut">処理開始に失敗</span></div>`;
  }
}

async function startUpload(){
  const files = Array.from($("fileInput").files || []);
  if(!files.length) return;

  const zip = files.find(f =>
    (f && (f.type === "application/zip" || String(f.name||"").toLowerCase().endsWith(".zip")))
  );
  if(zip){
    await startZipUpload(zip);
    return;
  }
  await startUploadFiles(files);
}

let _zipPollTimer = null;
async function startZipUpload(zipFile){
  if(!zipFile) return;
  state.uploadStop = false;
  state._zipSeen = new Set();
  state.uploadZipLastSeq = 0;
  state.uploadQueue = [];
  uploadListInit();
  resetUploadSummary();
  renderUploadSummary();
  uploadProgressUpdate();

  const jobBox = $("uploadZipJob");
  if(jobBox){
    jobBox.classList.remove("hidden");
    jobBox.innerHTML = `<div class="row"><b>zip処理を開始…</b><span class="mut">${escapeHtml(zipFile.name)}</span></div>`;
  }

  const setSendProgress = (loaded, total) => {
    const elText = $("uploadProgressText");
    const fill = $("uploadProgressBar");
    const t = Number(total || zipFile.size || 0);
    const l = Math.min(t || 0, Math.max(0, Number(loaded || 0)));
    const pct = t ? (l / t) * 100 : 0;
    if(fill) fill.style.width = `${pct.toFixed(1)}%`;
    if(elText) elText.textContent = t ? `zip送信 ${pct.toFixed(1)}%（${fmtBytes(l)}/${fmtBytes(t)}）` : "zip送信";
    if(jobBox){
      jobBox.innerHTML = `<div class="row"><b>zip送信中…</b><span class="mut">${escapeHtml(zipFile.name)}</span></div><div class="mut">${t ? `${pct.toFixed(1)}%（${fmtBytes(l)}/${fmtBytes(t)}）` : ""}</div>`;
    }
  };

  const doZipUploadChunked = async () => {
    const total = Number(zipFile.size || 0);
    const bookmarkConfig = getUploadBookmarkConfig();
    const initQs = new URLSearchParams();
    initQs.set("filename", zipFile.name);
    initQs.set("total_bytes", String(total));
    initQs.set("bookmark_enabled", String(Number(bookmarkConfig.bookmark_enabled || 0)));
    if(bookmarkConfig.bookmark_list_id){
      initQs.set("bookmark_list_id", String(bookmarkConfig.bookmark_list_id));
    }
    const initRes = await apiFetch(`${API.uploadZipChunkInit}?${initQs.toString()}`, { method: "POST" });
    const init = await apiJson(initRes);
    const token = init?.token;
    if(!token) throw new Error("chunk init failed");

    const chunkSize = 1572864;
    let offset = 0;
    while(offset < total){
      if(state.uploadStop) throw new Error("cancelled");
      const end = Math.min(total, offset + chunkSize);
      const blob = zipFile.slice(offset, end);
      const qs = new URLSearchParams();
      qs.set("token", token);
      qs.set("offset", String(offset));
      await xhrPostBinary(
        `${API.uploadZipChunkAppend}?${qs.toString()}`,
        blob,
        "application/octet-stream",
        (l, _t) => {
          setSendProgress(offset + Number(l || 0), total);
        },
      );
      offset = end;
      setSendProgress(offset, total);
    }

    const finQs = new URLSearchParams();
    finQs.set("token", token);
    const finRes = await apiFetch(`${API.uploadZipChunkFinish}?${finQs.toString()}`, { method: "POST" });
    return await apiJson(finRes);
  };

  let data = null;
  try{
    data = await doZipUploadChunked();
  }catch(e){
    const msg = String(e && (e.message || e) || "");
    if(state.uploadStop && /cancelled/i.test(msg)){
      if(jobBox) jobBox.innerHTML = `<div class="row"><b>zip</b><span class="mut">キャンセル</span></div>`;
      const elText = $("uploadProgressText");
      if(elText) elText.textContent = "キャンセル";
      return;
    }
    throw e;
  }

  if(jobBox){
    jobBox.innerHTML = `<div class="row"><b>zip送信完了</b><span class="mut">サーバ処理を開始…</span></div>`;
  }
  const jobId = data?.job_id;
  if(!jobId){
    if(jobBox) jobBox.innerHTML = `<div class="row"><b>zip</b><span class="mut">失敗</span></div>`;
    return;
  }
  state.uploadZipJob = { id: jobId, total: Number(data.total||0), done:0, failed:0, dup:0, status:"queued", source_kind: "zip", filename: zipFile.name };
  await _pollActiveUploadJob(zipFile.name);
}

function stopUpload(){
  state.uploadStop = true;
  try{ if(_zipPollTimer) clearTimeout(_zipPollTimer); }catch(_e){}
  _zipPollTimer = null;
  const j = state.uploadZipJob;
  if(j && j.id){
    apiFetch(API.uploadZipCancel(j.id), { method: "POST" }).catch(()=>{});
  }
  const elText = $("uploadProgressText");
  if(elText) elText.textContent = "キャンセル";
}

function bindDropZone(){
  const dz = $("dropZone");
  if(!dz) return;

  $("startUploadBtn").addEventListener("click", () => $("fileInput").click());
  $("startFolderBtn")?.addEventListener("click", () => $("folderInput").click());
  $("startZipBtn")?.addEventListener("click", () => $("zipInput").click());

  $("fileInput").addEventListener("change", async () => {
    await startUpload();
  });

  $("folderInput")?.addEventListener("change", async () => {
    const files = Array.from($("folderInput").files || []);
    await startUploadFiles(files);
  });

  $("zipInput")?.addEventListener("change", async () => {
    const f = ($("zipInput").files || [])[0];
    if(!f) return;
    await startZipUpload(f);
  });

  dz.addEventListener("dragover", (e) => {
    e.preventDefault();
    dz.classList.add("drag");
  });
  dz.addEventListener("dragleave", () => dz.classList.remove("drag"));
  dz.addEventListener("drop", async (e) => {
    e.preventDefault();
    dz.classList.remove("drag");
    const dropped = Array.from(e.dataTransfer?.files || []).filter(Boolean);
    const zip = dropped.find(f => (f.type === "application/zip") || (String(f.name||"").toLowerCase().endsWith(".zip")));
    if(zip){
      await startZipUpload(zip);
      return;
    }
    const files = dropped.filter(f => f && f.type && f.type.startsWith("image/"));
    if(!files.length) return;
    await startUploadFiles(files);
  });
}

/* =====================
   Facets / lists
   ===================== */

async function refreshFacets(){
  try{
    const [cRes, sRes, bRes] = await Promise.all([
      apiFetch(API.creators),
      apiFetch(API.software),
      apiFetch(API.bookmarkSidebar),
    ]);
    const creators = await apiJson(cRes);
    const softwares = await apiJson(sRes);
    const bms = await apiJson(bRes);

    const creators2 = (Array.isArray(creators) ? creators : [])
      .map(x => ({
        id: Number(x.id || 0),
        creator: _normFacetValue(x.creator),
        count: Number(x.count || 0),
        is_self: Number(x.is_self || 0),
      }))
      .filter(x => !_isPseudoAllItem(x.creator));

    const softwares2 = (Array.isArray(softwares) ? softwares : [])
      .map(x => ({ software: _normFacetValue(x.software), count: Number(x.count || 0) }))
      .filter(x => !_isPseudoAllItem(x.software));

    state.facets.creators = creators2;
    state.facets.softwares = softwares2;
    state.facets.creatorSet = new Set(creators2.map(x => x.creator));

    fillSelect($("filterCreator"), creators2.map(x => x.creator));
    fillSelect($("filterSoftware"), softwares2.map(x => x.software));

    renderCreatorList(creators2);
    renderSoftwareList(softwares2);

    const mine = (bms && bms.mine) ? bms.mine : {};
    state.bookmarks.lists = (mine && mine.lists) ? mine.lists : [];
    state.bookmarks.any_count = Number((mine && mine.any_count) || 0);
    state.bookmarks.others = (bms && bms.others) ? bms.others : [];
    renderBookmarkList();
    syncUploadBookmarkControls();
  }catch(e){}
}


async function refreshStatsAndPreviewAfterChange(){
  // Used when the underlying dataset changes (e.g. upload / bulk delete / zip completion).
  // Important: paging caches must be invalidated even when filter key stays the same.
  _suspendPreviewWarmup = true;
  _abortPreviewRequests();
  invalidatePreviewCaches();
  resetGrid();
  try{
    await refreshFacets();
    await search(1);
    await loadYearCounts();
    await loadYearMonthCounts(state.calendar.month.getFullYear());
    await loadMonthCounts(state.calendar.month);
    renderCalendar();
  }finally{
    _suspendPreviewWarmup = false;
  }
}

function fillSelect(sel, items){
  const cur = sel.value;
  sel.innerHTML = '<option value="">(未選択)</option>';
  items.forEach(v => {
    const o = document.createElement("option");
    o.value = v;
    o.textContent = v;
    sel.appendChild(o);
  });
  if(items.includes(cur)) sel.value = cur;
}

function _normFacetValue(v){
  return String(v || "").trim();
}

function _isPseudoAllItem(v){
  const s = _normFacetValue(v);
  if(!s) return true;
  if(s === "すべて" || s === "全て") return true;
  if(s.toLowerCase() === "all") return true;
  if(s === "(未選択)") return true;
  return false;
}

function renderCreatorList(creators){
  const wrap = $("creatorList");
  if(!wrap) return;
  state.facets.creators = creators || [];
  wrap.innerHTML = "";

  const cur = getCreatorFilter();

  (creators || []).slice(0, 60).forEach((x) => {
    const val = _normFacetValue(x.creator);
    if(_isPseudoAllItem(val)) return;

    const div = document.createElement("div");
    div.className = "sideItem" + ((!!cur && (val === cur)) ? " active" : "");
    div.dataset.value = val;

    const left = document.createElement("div");
    left.className = "nameRow";
    const name = document.createElement("div");
    name.className = "name";
    name.textContent = val;
    left.appendChild(name);

    const right = document.createElement("div");
    right.style.display = "flex";
    right.style.alignItems = "center";
    right.style.gap = "8px";

    const cnt = document.createElement("div");
    cnt.className = "cnt";
    cnt.textContent = Number(x.count||0).toLocaleString();
    right.appendChild(cnt);

    const isSelf = !!Number(x.is_self || 0);
    const id = Number(x.id || 0);
    if(!isSelf && id){
      const del = document.createElement("button");
      del.className = "delBtn";
      del.textContent = "✕";
      del.title = "削除";
      del.addEventListener("click", async (e) => {
        e.preventDefault();
        e.stopPropagation();
        if(!confirm(`作者登録「${val}」を削除しますか？`)) return;
        try{
          await apiFetch(API.creatorListDel(id), { method: "DELETE" });
          if(getCreatorFilter() === val){
            setCreatorFilter("");
          }
          await refreshStatsAndPreviewAfterChange();
        }catch(_e){
          alert("削除に失敗しました");
        }
      });
      right.appendChild(del);
    }

    div.appendChild(left);
    div.appendChild(right);

    div.addEventListener("click", async () => {
      const isActive = div.classList.contains("active");
      const next = isActive ? "" : val;
      setCreatorFilter(next);
      await search();
    });

    wrap.appendChild(div);
  });

  syncCreatorListActive();
  syncCreatorClearBtn();
}

function renderSoftwareList(softwares){
  const wrap = $("softwareList");
  if(!wrap) return;
  state.facets.softwares = softwares || [];
  wrap.innerHTML = "";

  const cur = getSoftwareFilter();

  (softwares || []).slice(0, 40).forEach((x) => {
    const val = _normFacetValue(x.software);
    if(_isPseudoAllItem(val)) return;

    const div = document.createElement("div");
    div.className = "sideItem" + ((!!cur && (val === cur)) ? " active" : "");
    div.dataset.value = val;
    div.innerHTML = `<div class="name">${escapeHtml(val)}</div><div class="cnt">${Number(x.count||0).toLocaleString()}</div>`;

    div.addEventListener("click", async () => {
      const isActive = div.classList.contains("active");
      const next = isActive ? "" : val;
      setSoftwareFilter(next);
      await search();
    });

    wrap.appendChild(div);
  });

  syncSoftwareListActive();
  syncSoftwareClearBtn();
}

async function refreshBookmarkLists(){
  try{
    const r = await apiFetch(API.bookmarkSidebar);
    const j = await apiJson(r);
    const mine = (j && j.mine) ? j.mine : {};
    state.bookmarks.lists = (mine && mine.lists) ? mine.lists : [];
    state.bookmarks.any_count = Number((mine && mine.any_count) || 0);
    state.bookmarks.others = (j && j.others) ? j.others : [];
    renderBookmarkList();
    syncUploadBookmarkControls();
  }catch(_e){}
}

function renderBookmarkList(){
  const wrap = $("bookmarkList");
  if(!wrap) return;
  wrap.innerHTML = "";

  const mine = state.bookmarks.lists || [];
  const anyCount = Number(state.bookmarks.any_count || 0);
  const others = state.bookmarks.others || [];

  const mkItem = (parent, label, cntText, active, onClick) => {
    const div = document.createElement("div");
    div.className = "sideItem" + (active ? " active" : "");
    div.innerHTML = `<div class="name">${escapeHtml(label)}</div><div class="cnt">${cntText}</div>`;
    div.addEventListener("click", onClick);
    parent.appendChild(div);
    return div;
  };

  const allActive = !!state.preview.bm_any && !state.preview.bm_list_id;
  mkItem(wrap, "すべてのブックマーク", anyCount ? anyCount.toLocaleString() : "0", allActive, async () => {
    if(allActive){
      state.preview.bm_any = 0;
      state.preview.bm_list_id = 0;
    }else{
      state.preview.bm_any = 1;
      state.preview.bm_list_id = 0;
    }
    renderBookmarkList();
    await search();
  });

  mine.forEach((l) => {
    const id = Number(l.id || 0);
    if(!id) return;
    const nm = String(l.name || "");
    const cnt = Number(l.count || 0);
    const active = (!state.preview.bm_any) && (Number(state.preview.bm_list_id || 0) === id);
    mkItem(wrap, nm, cnt.toLocaleString(), active, async () => {
      if(active){
        state.preview.bm_any = 0;
        state.preview.bm_list_id = 0;
      }else{
        state.preview.bm_any = 0;
        state.preview.bm_list_id = id;
      }
      renderBookmarkList();
      await search();
    });
  });

  // Subscribed creators' bookmark lists (collapsed).
  const activeListId = Number(state.preview.bm_list_id || 0);
  (others || []).forEach((g) => {
    const cid = Number(g.creator_id || 0);
    const cname = String(g.creator || "");
    const lists = Array.isArray(g.lists) ? g.lists : [];
    if(!cid || !cname) return;

    const activeInside = !!lists.find(x => Number(x.id || 0) === activeListId);
    const expanded = activeInside || state.bookmarks.expanded.has(cid);

    const group = document.createElement("div");
    group.className = "sideGroup";

    const head = document.createElement("div");
    head.className = "sideGroupHead";

    const title = document.createElement("div");
    title.className = "sideGroupTitle";
    title.innerHTML = `<span class="arrow">${expanded ? "▾" : "▸"}</span><span class="name">${escapeHtml(cname)}</span>`;
    head.appendChild(title);

    const delBtn = document.createElement("button");
    delBtn.className = "delBtn";
    delBtn.textContent = "✕";
    delBtn.title = "削除";
    delBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      if(!confirm(`共有ブックマーク登録「${cname}」を削除しますか？`)) return;
      try{
        await apiFetch(API.bookmarkSubDel(cid), { method: "DELETE" });
        // If current filter points to a removed creator's list, clear it.
        if(activeInside){
          state.preview.bm_any = 0;
          state.preview.bm_list_id = 0;
        }
        await refreshBookmarkLists();
        await search();
      }catch(_e){
        alert("削除に失敗しました");
      }
    });
    head.appendChild(delBtn);

    head.addEventListener("click", () => {
      if(expanded){
        state.bookmarks.expanded.delete(cid);
      }else{
        state.bookmarks.expanded.add(cid);
      }
      renderBookmarkList();
    });

    const body = document.createElement("div");
    body.className = "sideGroupBody" + (expanded ? "" : " hidden");

    lists.forEach((l) => {
      const id = Number(l.id || 0);
      if(!id) return;
      const nm = String(l.name || "");
      const cnt = Number(l.count || 0);
      const active = (!state.preview.bm_any) && (Number(state.preview.bm_list_id || 0) === id);
      mkItem(body, nm, cnt.toLocaleString(), active, async (e) => {
        e.preventDefault();
        state.preview.bm_any = 0;
        state.preview.bm_list_id = active ? 0 : id;
        renderBookmarkList();
        await search();
      });
    });

    group.appendChild(head);
    group.appendChild(body);
    wrap.appendChild(group);
  });

  syncBookmarkClearBtn();
}

function isBookmarkFilterActive(){
  return !!(Number(state.preview.bm_any || 0) || Number(state.preview.bm_list_id || 0));
}

function syncBookmarkClearBtn(){
  const btn = $("bookmarkClearBtn");
  if(!btn) return;
  const active = isBookmarkFilterActive();
  btn.disabled = !active;
  btn.classList.toggle("active", active);
}

async function clearBookmarkFilter(){
  state.preview.bm_any = 0;
  state.preview.bm_list_id = 0;
  renderBookmarkList();
  await search();
}


function getCreatorFilter(){
  return _normFacetValue($("filterCreator")?.value || state.preview.creator);
}

function getSoftwareFilter(){
  return _normFacetValue($("filterSoftware")?.value || state.preview.software);
}

function setCreatorFilter(value){
  const v = _normFacetValue(value);
  if($("filterCreator")) $("filterCreator").value = v;
  state.preview.creator = v;
  syncCreatorListActive();
  syncCreatorClearBtn();
}

function setSoftwareFilter(value){
  const v = _normFacetValue(value);
  if($("filterSoftware")) $("filterSoftware").value = v;
  state.preview.software = v;
  syncSoftwareListActive();
  syncSoftwareClearBtn();
}

function isCreatorFilterActive(){
  return !!getCreatorFilter();
}

function isSoftwareFilterActive(){
  return !!getSoftwareFilter();
}

function syncCreatorListActive(){
  const wrap = $("creatorList");
  if(!wrap) return;
  const cur = getCreatorFilter();
  wrap.querySelectorAll(".sideItem").forEach((div) => {
    const v = _normFacetValue(div.dataset.value);
    div.classList.toggle("active", !!cur && v === cur);
  });
}

function syncSoftwareListActive(){
  const wrap = $("softwareList");
  if(!wrap) return;
  const cur = getSoftwareFilter();
  wrap.querySelectorAll(".sideItem").forEach((div) => {
    const v = _normFacetValue(div.dataset.value);
    div.classList.toggle("active", !!cur && v === cur);
  });
}

function syncCreatorClearBtn(){
  const btn = $("creatorClearBtn");
  if(!btn) return;
  const active = isCreatorFilterActive();
  btn.disabled = !active;
  btn.classList.toggle("active", active);
}

function syncSoftwareClearBtn(){
  const btn = $("softwareClearBtn");
  if(!btn) return;
  const active = isSoftwareFilterActive();
  btn.disabled = !active;
  btn.classList.toggle("active", active);
}

async function clearCreatorFilter(){
  setCreatorFilter("");
  await search();
}

async function clearSoftwareFilter(){
  setSoftwareFilter("");
  await search();
}

/* =====================
   Preview search / paging
   ===================== */

function buildPreviewSearchKey(){
  return [
    state.preview.creator || "",
    state.preview.software || "",
    (state.preview.tags || []).join(","),
    (state.preview.tags_not || []).join(","),
    state.preview.date_from || "",
    state.preview.date_to || "",
    String(state.preview.dedup_only || 0),
    String(state.preview.bm_any || 0),
    String(state.preview.bm_list_id || 0),
    String(state.preview.sort || "newest"),
  ].join("|");
}

function resetPreviewCachesIfNeeded(){
  const k = buildPreviewSearchKey();
  if(k === _previewSearchKey) return false;
  _previewSearchKey = k;
  _pageCache.clear();
  _scrollPrefetch = null;
  _overlayWarmSet.clear();
  _thumbWarmSet.clear();
  _cancelThumbWarmers();
  return true;
}

function warmThumbUrls(urls, maxN){
  if(_suspendPreviewWarmup) return;
  const n = Math.min(maxN || 0, urls.length);
  for(let i=0;i<n;i++){
    const rawUrl = urls[i];
    const u = _thumbDisplayUrl(rawUrl);
    if(!u || _thumbWarmSet.has(u)) continue;
    _thumbWarmSet.add(u);
    const img = new Image();
    img.decoding = "async";
    img.loading = "eager";
    img.onload = () => { _thumbWarmImages.delete(img); };
    img.onerror = () => { _thumbWarmImages.delete(img); };
    _thumbWarmImages.add(img);
    img.src = u;
  }
}

function roundPerfMs(v){
  const n = Number(v);
  if(!Number.isFinite(n)) return null;
  return Math.round(n * 1000) / 1000;
}

function currentPreviewPage(){
  const n = Number(state?.preview?.page || 0);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function currentPreviewMode(){
  return isDesktop() ? "desktop" : "mobile";
}

function newClientTraceId(){
  try{
    if(window.crypto && typeof window.crypto.randomUUID === "function") return window.crypto.randomUUID();
  }catch(_e){}
  return `nim-${Date.now().toString(36)}-${Math.random().toString(36).slice(2,10)}`;
}

function isPerfClientEnabled(){
  return false;
}

function buildPerfRequestHeaders({ traceId=null, source="", page=null, mode="" }={}){
  if(!isPerfClientEnabled()) return {};
  const headers = {};
  if(traceId) headers["X-NIM-Client-Trace-Id"] = String(traceId);
  if(source) headers["X-NIM-Detail-Source"] = String(source);
  if(page !== null && page !== undefined && page !== "") headers["X-NIM-Detail-Page"] = String(page);
  if(mode) headers["X-NIM-Detail-Mode"] = String(mode);
  return headers;
}

function sendPerfLog(payload){
  if(!isPerfClientEnabled()) return;
  try{
    const body = JSON.stringify(payload || {});
    if(navigator.sendBeacon){
      const blob = new Blob([body], { type: "application/json" });
      navigator.sendBeacon(API.debugPerf, blob);
      return;
    }
    fetch(API.debugPerf, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body,
      keepalive: true,
    }).catch(()=>{});
  }catch(_e){}
}

function warmOverlayDerivatives(items){
  return;
}

// Detail prefetch cache: we want the detail text to be instant when opening the overlay.
const _detailCache = new Map();      // `${id}:${bm_list_id||0}` -> detail json
const _detailInFlight = new Map();   // `${id}:${bm_list_id||0}` -> Promise
let _detailRenderToken = 0;

async function fetchDetailsBatchCached(ids, opts={}){
  const source = String(opts?.source || "prefetch");
  const page = Number.isFinite(Number(opts?.page)) && Number(opts?.page) > 0 ? Number(opts?.page) : currentPreviewPage();
  const mode = String(opts?.mode || currentPreviewMode());
  const traceId = isPerfClientEnabled() ? String(opts?.traceId || newClientTraceId()) : "";
  const bmListId = Number(opts?.bm_list_id || currentBookmarkContextListId() || 0) || 0;
  const uniqueIds = [];
  for(const rawId of (ids || [])){
    const iid = Number(rawId || 0);
    const key = detailCacheKey(iid, bmListId);
    if(!iid || uniqueIds.includes(iid)) continue;
    if(_detailCache.has(key) || _detailInFlight.has(key)) continue;
    uniqueIds.push(iid);
  }
  if(!uniqueIds.length) return {};

  const started = performance.now();
  let resolveAll;
  let rejectAll;
  const batchPromise = new Promise((resolve, reject) => {
    resolveAll = resolve;
    rejectAll = reject;
  });
  const perIdPromises = new Map();
  for(const iid of uniqueIds){
    const itemPromise = batchPromise.then((items) => {
      if(!items || !items[iid]) throw new Error(`detail missing: ${iid}`);
      return items[iid];
    });
    perIdPromises.set(iid, itemPromise);
    _detailInFlight.set(detailCacheKey(iid, bmListId), itemPromise);
  }

  const controller = _registerPreviewRequestController(new AbortController());
  try{
    const res = await apiFetch(API.detailsBatch, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        ...buildPerfRequestHeaders({ traceId, source, page, mode }),
      },
      body: JSON.stringify({ ids: uniqueIds, bm_list_id: bmListId || null }),
    });
    const fetchedAt = performance.now();
    const data = await apiJson(res);
    const parsedAt = performance.now();
    const rawItems = data?.items || {};
    const itemMap = {};
    for(const iid of uniqueIds){
      const item = rawItems[String(iid)] || rawItems[iid] || null;
      if(item) itemMap[iid] = item;
    }
    for(const [iid, item] of Object.entries(itemMap)){
      _detailCache.set(detailCacheKey(Number(iid), bmListId), item);
    }
    resolveAll(itemMap);
    sendPerfLog({
      event: "detail_batch_fetch_complete",
      trace_id: traceId,
      page,
      mode,
      source,
      fetch_ms: roundPerfMs(fetchedAt - started),
      parse_ms: roundPerfMs(parsedAt - fetchedAt),
      total_ms: roundPerfMs(parsedAt - started),
      note: `count=${uniqueIds.length}`,
    });
    return itemMap;
  }catch(err){
    rejectAll(err);
    const failedAt = performance.now();
    sendPerfLog({
      event: "detail_batch_fetch_error",
      trace_id: traceId,
      page,
      mode,
      source,
      total_ms: roundPerfMs(failedAt - started),
      note: String(err?.message || err || "error").slice(0, 240),
    });
    throw err;
  }finally{
    _releasePreviewRequestController(controller);
    for(const iid of uniqueIds){
      _detailInFlight.delete(detailCacheKey(iid, bmListId));
    }
  }
}

async function fetchDetailCached(id, opts={}){
  const iid = Number(id||0);
  if(!iid) throw new Error("bad id");
  const source = String(opts?.source || "overlay_open");
  const page = Number.isFinite(Number(opts?.page)) && Number(opts?.page) > 0 ? Number(opts?.page) : currentPreviewPage();
  const mode = String(opts?.mode || currentPreviewMode());
  const traceId = isPerfClientEnabled() ? String(opts?.traceId || newClientTraceId()) : "";
  const bmListId = Number(opts?.bm_list_id || currentBookmarkContextListId() || 0) || 0;
  const cacheKey = detailCacheKey(iid, bmListId);
  if(_detailCache.has(cacheKey)){
    sendPerfLog({
      event: "detail_cache_hit",
      trace_id: traceId,
      image_id: iid,
      page,
      mode,
      source,
      total_ms: 0,
      note: "memory_cache",
    });
    return _detailCache.get(cacheKey);
  }
  if(_detailInFlight.has(cacheKey)){
    sendPerfLog({
      event: "detail_join_inflight",
      trace_id: traceId,
      image_id: iid,
      page,
      mode,
      source,
      note: "join_existing_request",
    });
    return await _detailInFlight.get(cacheKey);
  }
  const started = performance.now();
  const p = (async () => {
    try{
      const res = await apiFetch(withBookmarkContext(API.detail(iid), bmListId), {
        headers: buildPerfRequestHeaders({ traceId, source, page, mode }),
      });
      const fetchedAt = performance.now();
      const data = await apiJson(res);
      const parsedAt = performance.now();
      sendPerfLog({
        event: "detail_fetch_complete",
        trace_id: traceId,
        image_id: iid,
        page,
        mode,
        source,
        fetch_ms: roundPerfMs(fetchedAt - started),
        parse_ms: roundPerfMs(parsedAt - fetchedAt),
        total_ms: roundPerfMs(parsedAt - started),
      });
      return data;
    }catch(err){
      const failedAt = performance.now();
      sendPerfLog({
        event: "detail_fetch_error",
        trace_id: traceId,
        image_id: iid,
        page,
        mode,
        source,
        total_ms: roundPerfMs(failedAt - started),
        note: String(err?.message || err || "error").slice(0, 240),
      });
      throw err;
    }
  })();
  _detailInFlight.set(cacheKey, p);
  try{
    const d = await p;
    _detailCache.set(cacheKey, d);
    return d;
  }finally{
    _detailInFlight.delete(cacheKey);
  }
}

function prefetchDetails(items){
  if(!items || !items.length) return;
  const maxN = isDesktop() ? 16 : 30;
  const ids = [];
  const seen = new Set();
  for(const it of items){
    if(ids.length >= maxN) break;
    const id = Number(it?.id||0);
    if(!id || seen.has(id)) continue;
    seen.add(id);
    const key = detailCacheKey(id);
    if(_detailCache.has(key) || _detailInFlight.has(key)) continue;
    ids.push(id);
  }
  if(!ids.length) return;
  fetchDetailsBatchCached(ids, { source: "prefetch", page: currentPreviewPage(), mode: currentPreviewMode() }).catch(()=>{});
}

function pageCacheKey(page){
  // Key WITHOUT include_total so prefetch and normal navigation share cache.
  return buildPageQueryCore(page);
}

async function fetchPageCached(page, includeTotal){
  const key = pageCacheKey(page);
  const hit = _getFreshPageCache(key);
  if(hit?.data) return hit.data;
  if(hit?.promise) return await hit.promise;

  const qs = key + `&include_total=${includeTotal ? 1 : 0}`;
  const controller = _registerPreviewRequestController(new AbortController());
  const p = (async () => {
    try{
      const res = await apiFetch(`${API.pageList}?${qs}`, { signal: controller.signal });
      return await apiJson(res);
    }finally{
      _releasePreviewRequestController(controller);
    }
  })();
  _pageCache.set(key, { promise: p, data: null, ts: Date.now() });
  try{
    const data = await p;
    const e = _pageCache.get(key);
    if(e){ e.data = data; e.promise = null; e.ts = Date.now(); }
    return data;
  }catch(e){
    _pageCache.delete(key);
    throw e;
  }
}

function prefetchPage(page){
  if(_suspendPreviewWarmup) return;
  const seq = _previewSearchSeq;
  const p = Math.max(1, Number(page || 1));
  const total = Number(state.preview.total_pages || 0);
  if(total && p > total) return;
  const key = pageCacheKey(p);
  const hit = _getFreshPageCache(key);
  if(hit?.data || hit?.promise) return;

  // Prefetch without COUNT to keep DB light.
  fetchPageCached(p, 0).then((data) => {
    if(seq !== _previewSearchSeq) return;
    const items = data?.items || [];
    warmThumbUrls(items.map(x=>x.thumb), 16);
    warmOverlayDerivatives(items);
    prefetchDetails(items);
  }).catch((err) => {
    if(_isAbortError(err)) return;
  });
}

function getScrollMetrics(){
  const doc = document.documentElement;
  const body = document.body;
  const root = document.scrollingElement || doc || body;
  const top = Number(root?.scrollTop || window.pageYOffset || 0);
  const viewport = Number(window.innerHeight || doc?.clientHeight || 0);
  const height = Math.max(
    Number(root?.scrollHeight || 0),
    Number(doc?.scrollHeight || 0),
    Number(body?.scrollHeight || 0),
    Number(root?.offsetHeight || 0),
    Number(doc?.offsetHeight || 0),
    Number(body?.offsetHeight || 0)
  );
  return { top, viewport, height, remaining: Math.max(0, height - (top + viewport)) };
}

function shouldDrainMore(){
  if(!isPreviewVisible()) return false;
  if(state.preview.mode !== "scroll" || state.preview.done) return false;
  const threshold = Math.max(240, Math.round((window.innerHeight || 0) * 1.2));
  return getScrollMetrics().remaining <= threshold;
}

function queueLoadMore(){
  if(!isPreviewVisible()) return;
  if(state.preview.mode !== "scroll" || state.preview.done) return;
  _drainPending = true;
  if(state.preview.loading){
    _queuedLoadMore = true;
    return;
  }
  queueMicrotask(() => { drainScrollLoads().catch(()=>{}); });
}

async function drainScrollLoads(){
  if(_drainActive) return;
  _drainActive = true;
  try{
    let guard = 0;
    while(guard < 4 && state.preview.mode === "scroll" && !state.preview.done){
      if(state.preview.loading){
        _queuedLoadMore = true;
        break;
      }
      if(!_drainPending && !shouldDrainMore()) break;
      _drainPending = false;
      const seq = _previewSearchSeq;
      await loadMore(seq);
      if(seq !== _previewSearchSeq) break;
      guard += 1;
      await new Promise((resolve) => requestAnimationFrame(() => resolve()));
      if(!_queuedLoadMore && !_drainPending && !shouldDrainMore()) break;
    }
  }finally{
    _drainActive = false;
    if((_queuedLoadMore || _drainPending || shouldDrainMore()) && !state.preview.loading && !state.preview.done){
      queueMicrotask(() => { drainScrollLoads().catch(()=>{}); });
    }
  }
}

function ensureBottomTrigger(){
  if(_bottomBindingsInstalled) return;
  _bottomBindingsInstalled = true;
  const onMaybeNeedMore = () => {
    if(!isPreviewVisible()) return;
    if(shouldDrainMore()) queueLoadMore();
  };
  window.addEventListener("scroll", onMaybeNeedMore, { passive: true });
  window.addEventListener("touchmove", onMaybeNeedMore, { passive: true });
  window.addEventListener("touchend", onMaybeNeedMore, { passive: true });
  window.addEventListener("resize", onMaybeNeedMore, { passive: true });
}

function ensureScrollObserver(){
  if(state.preview.mode !== "scroll") return;
  if(scrollObserver) return;
  const sentinel = $("scrollSentinel");
  if(!sentinel) return;
  scrollObserver = new IntersectionObserver(async (entries) => {
    if(!isPreviewVisible()) return;
    const e = entries[0];
    if(!e || !e.isIntersecting) return;
    await loadMore(_previewSearchSeq);
  }, { root: null, rootMargin: "1200px 0px", threshold: 0.01 });
  scrollObserver.observe(sentinel);
}

function setPagingUI(){
  const sentinel = $("scrollSentinel");
  const pager = $("pager");
  if(sentinel) sentinel.classList.toggle("hidden", state.preview.mode === "page");
  if(pager) pager.classList.toggle("hidden", state.preview.mode !== "page");
  if(state.preview.mode === "page" && scrollObserver){
    try{ scrollObserver.disconnect(); }catch(_e){}
    scrollObserver = null;
  }
}

function buildScrollQuery(cursor, includeTotal=1){
  return buildScrollQueryShared(state.preview, { cursor, includeTotal });
}

function buildPageQueryCore(page){
  return buildPageQueryCoreShared(state.preview, page);
}

function buildPageQuery(page, includeTotal=1){
  return buildPageQueryShared(state.preview, page, { includeTotal });
}

async function search(page=1){
  const seq = ++_previewSearchSeq;
  // Keep state in sync with UI controls (some actions modify selects directly).
  state.preview.creator = getCreatorFilter();
  state.preview.software = getSoftwareFilter();
  state.preview.mode = isDesktop() ? "page" : "scroll";
  // scroll mode: fetch in smaller chunks to reduce initial load on mobile.
  // (page mode uses a fixed limit=16 in buildPageQuery)
  if(state.preview.mode === "scroll") state.preview.limit = 30;
  const keyChanged = resetPreviewCachesIfNeeded();
  setPagingUI();
  resetGrid();
  _resetBulkSelection();

  if(state.preview.mode === "page") _abortPreviewRequests();

  if(state.preview.mode === "page"){
    state.preview.page = Math.max(1, Number(page || 1));
    if(keyChanged){
      state.preview.total_pages = 0;
      state.preview.total_count = 0;
    }
    try{
      const includeTotal = keyChanged || !state.preview.total_pages;
      // Start prefetch of next pages as early as possible (no COUNT).
      queueMicrotask(() => {
        prefetchPage(state.preview.page + 1);
        prefetchPage(state.preview.page + 2);
      });

      const data = await fetchPageCached(state.preview.page, includeTotal ? 1 : 0);
      if(seq !== _previewSearchSeq) return;

      state.preview.items = data.items || [];
      state.preview.page = Number(data.page || state.preview.page);
      if(typeof data.total_pages !== "undefined" && data.total_pages !== null) state.preview.total_pages = Number(data.total_pages || 0);
      if(typeof data.total_count !== "undefined" && data.total_count !== null) state.preview.total_count = Number(data.total_count || 0);
      if(data.sort) state.preview.sort = data.sort;      if(typeof data.bm_any !== "undefined") state.preview.bm_any = Number(data.bm_any || 0);
      if(typeof data.bm_list_id !== "undefined") state.preview.bm_list_id = Number(data.bm_list_id || 0);

      appendTiles(state.preview.items);
      _updateBulkActions();
      // Warm up detail preview cache and image decode path.
      warmOverlayDerivatives(state.preview.items);
      warmThumbUrls(state.preview.items.map(x=>x.thumb), 16);
      prefetchDetails(state.preview.items);
      updateGalleryTitle();
      syncGalleryControls();
      renderPager();

      // After render, prefetch next pages again (safe if already in-cache).
      prefetchPage(state.preview.page + 1);
      prefetchPage(state.preview.page + 2);
    }catch(_e){
      if(seq !== _previewSearchSeq || _isAbortError(_e)) return;
      state.preview.items = [];
      updateGalleryTitle();
      syncGalleryControls();
      renderPager();
    }
    return;
  }

  // scroll mode (mobile)
  state.preview.items = [];
  state.preview.cursor = null;
  state.preview.done = false;
  if(keyChanged) state.preview.total_count = 0;
  state.preview.got_total = false;
  _autoFillBurst = 0;
  _queuedLoadMore = false;
  _drainPending = false;
  updateGalleryTitle();
  syncGalleryControls();
  ensureScrollObserver();
  ensureBottomTrigger();
  await loadMore(seq);
  if(seq === _previewSearchSeq && !state.preview.done) queueLoadMore();
}

async function loadMore(seq=_previewSearchSeq){
  if(seq !== _previewSearchSeq) return;
  if(!isPreviewVisible()) return;
  if(state.preview.mode !== "scroll") return;
  if(state.preview.done) return;
  if(state.preview.loading){
    _queuedLoadMore = true;
    _drainPending = true;
    return;
  }
  state.preview.loading = true;
  let appended = 0;
  try{
    const includeTotal = !state.preview.got_total;
    const qs = buildScrollQuery(state.preview.cursor, includeTotal ? 1 : 0);

    // Consume prefetched page if available.
    let data = null;
    const prefetched = _getFreshScrollPrefetch(qs);
    if(prefetched){
      if(prefetched.data){
        data = prefetched.data;
      }else if(prefetched.promise){
        data = await prefetched.promise;
      }
      _scrollPrefetch = null;
    }
    if(!data){
      const controller = _registerPreviewRequestController(new AbortController());
      try{
        const res = await apiFetch(`${API.scrollList}?${qs}`, { signal: controller.signal });
        data = await apiJson(res);
      }finally{
        _releasePreviewRequestController(controller);
      }
    }

    if(seq !== _previewSearchSeq) return;
    const items = data.items || [];

    if(data.sort) state.preview.sort = data.sort;      if(typeof data.bm_any !== "undefined") state.preview.bm_any = Number(data.bm_any || 0);
      if(typeof data.bm_list_id !== "undefined") state.preview.bm_list_id = Number(data.bm_list_id || 0);
    if(typeof data.total_count !== "undefined" && data.total_count !== null){
      state.preview.total_count = Number(data.total_count || 0);
      state.preview.got_total = true;
    }

    if(!items.length){
      state.preview.done = true;
      updateGalleryTitle();
      return;
    }

    state.preview.items = state.preview.items.concat(items);
    state.preview.cursor = data.next_cursor || null;
    if(!state.preview.cursor) state.preview.done = true;

    appended = items.length;

    appendTiles(items);
    _updateBulkActions();
    warmThumbUrls(items.map(x => x.thumb), state.preview.limit);
    warmOverlayDerivatives(items);
    prefetchDetails(items);
    updateGalleryTitle();

    // Prefetch next cursor chunk (no COUNT).
    if(state.preview.cursor){
      const nextQs = buildScrollQuery(state.preview.cursor, 0);
      if(!_getFreshScrollPrefetch(nextQs)){
        const p = (async () => {
          const controller = _registerPreviewRequestController(new AbortController());
          try{
            const r = await apiFetch(`${API.scrollList}?${nextQs}`, { signal: controller.signal });
            const d = await apiJson(r);
            const prefItems = d?.items || [];
            if(prefItems.length) warmThumbUrls(prefItems.map(x => x.thumb), state.preview.limit);
            return d;
          }finally{
            _releasePreviewRequestController(controller);
          }
        })();
        _scrollPrefetch = { key: nextQs, promise: p, data: null, ts: Date.now() };
        p.then(d => { if(_scrollPrefetch && _scrollPrefetch.key === nextQs){ _scrollPrefetch.data = d; _scrollPrefetch.promise = null; _scrollPrefetch.ts = Date.now(); } }).catch(()=>{ if(_scrollPrefetch && _scrollPrefetch.key === nextQs) _scrollPrefetch = null; });
      }
    }
  } finally {
    state.preview.loading = false;

    if(_queuedLoadMore && !state.preview.done){
      _queuedLoadMore = false;
      _drainPending = true;
    }

    // IntersectionObserver may not fire again if the sentinel remains visible.
    // Proactively keep loading while the bottom stays near the viewport.
    if(!state.preview.done && appended > 0){
      const sentinel = $("scrollSentinel");
      let near = shouldDrainMore();
      if(sentinel){
        const r = sentinel.getBoundingClientRect();
        near = near || (r.top < (window.innerHeight + 800));
      }
      if(near && _autoFillBurst < 8){
        _autoFillBurst += 1;
        _drainPending = true;
      }else if(!near){
        _autoFillBurst = 0;
      }
    }

    if((_drainPending || shouldDrainMore()) && !state.preview.done){
      queueMicrotask(() => { drainScrollLoads().catch(()=>{}); });
    }
  }
}

function syncGalleryControls(){
  const sortSel = $("sortBy");
  if(sortSel) sortSel.value = state.preview.sort || "newest";


  setGalleryView(state.preview.view || "grid", true);

  // Keep bookmark list highlight in sync.
  try{ renderBookmarkList(); }catch(_e){}

}

function setGalleryView(mode, silent=false){
  const m = (mode === "list") ? "list" : "grid";
  state.preview.view = m;
  if(!silent) localStorage.setItem("gallery_view", m);
  const grid = $("grid");
  if(grid) grid.classList.toggle("list", m === "list");
  $("viewGrid")?.classList.toggle("active", m === "grid");
  $("viewList")?.classList.toggle("active", m === "list");
}

function updateGalleryTitle(){
  const t = $("galleryTitle");
  const hint = $("galleryHint");
  if(state.preview.mode === "page"){
    const total = Number(state.preview.total_count || 0);
    if(t) t.textContent = `ギャラリー (${total.toLocaleString()})`;
    if(hint) hint.textContent = "";
    return;
  }

  const total = Number(state.preview.total_count || 0);
  const loaded = state.preview.items.length || 0;
  if(t) t.textContent = `ギャラリー (${(total || loaded).toLocaleString()})`;
  if(hint) hint.textContent = "";
}

function resetGrid(){
  _abortGridImageLoads();
  const grid = $("grid");
  if(grid) grid.innerHTML = "";
}

function _canBulkSelectItem(it){
  if(!state.user) return false;
  const role = String(state.user.role || "user");
  if(role === "user"){
    return String(it.creator || "") === String(state.user.username || "");
  }
  return true;
}

function _isTileSelected(it){
  const b = state.preview.bulk;
  if(!b) return false;
  const id = Number(it.id);
  if(!Number.isFinite(id)) return false;
  if(b.all){
    if(!_canBulkSelectItem(it)) return false;
    return !b.deselected.has(id);
  }
  return b.selected.has(id);
}

function _setTileSelected(it, checked){
  const b = state.preview.bulk;
  const id = Number(it.id);
  if(!Number.isFinite(id)) return;
  if(!_canBulkSelectItem(it)) return;

  if(b.all){
    if(checked) b.deselected.delete(id);
    else b.deselected.add(id);
  }else{
    if(checked) b.selected.add(id);
    else b.selected.delete(id);
  }
}

function _refreshTileChecks(){
  document.querySelectorAll(".tile").forEach(tile => {
    const id = Number(tile.dataset.id || 0);
    if(!id) return;
    const it = state.preview.items.find(x => Number(x.id) === id);
    if(!it) return;
    const chk = tile.querySelector("input.tileChk");
    if(chk){
      chk.checked = _isTileSelected(it);
    }
    tile.classList.toggle("selected", _isTileSelected(it));
  });
}

function _hasBulkSelection(){
  const b = state.preview.bulk;
  return !!(b && (b.all || (b.selected && b.selected.size > 0)));
}

function _currentBulkSelectionPayload(){
  const b = state.preview.bulk;
  if(!b) return null;
  const bmListId = Number(state.preview.bm_list_id || 0) || null;
  if(b.all){
    return {
      mode: "query",
      query: _currentFilterQuery(),
      exclude_ids: Array.from(b.deselected || []),
      bm_list_id: bmListId,
    };
  }
  const ids = Array.from(b.selected || []);
  if(!ids.length) return null;
  return {
    mode: "ids",
    ids,
    exclude_ids: [],
    bm_list_id: bmListId,
  };
}

function _bulkBookmarkStatus(msg, kind){
  const el = $("bulkBmOverlayStatus");
  if(!el) return;
  el.textContent = msg || "";
  el.className = "small" + (kind ? (" " + kind) : "");
}

function _bulkStatus(msg, kind){
  const el = $("bulkStatus");
  if(!el) return;
  el.textContent = msg || "";
  el.classList.remove("error","ok");
  if(kind) el.classList.add(kind);
}

function _updateBulkActions(){
  const b = state.preview.bulk;
  const btnDel = $("bulkDeleteBtn");
  const btnBm = $("bulkBookmarkBtn");
  const btnPage = $("pageSelectBtn");
  const btnAll = $("allSelectBtn");

  if(!b){
    btnDel && (btnDel.disabled = true);
    btnBm && (btnBm.disabled = true);
    return;
  }

  const hasSel = _hasBulkSelection();
  if(btnBm){
    btnBm.disabled = !hasSel;
  }
  if(btnDel){
    btnDel.disabled = !hasSel || !!b.deleting;
    if(b.all){
      const total = Number(state.preview.total_count || 0);
      const excl = b.deselected.size;
      const hint = total ? `削除 (${Math.max(0, total - excl)})` : "削除 (全対象)";
      btnDel.textContent = hint;
    }else{
      btnDel.textContent = `削除 (${b.selected.size})`;
    }
  }

  if(btnAll){
    btnAll.classList.toggle("active", !!b.all);
    btnAll.disabled = !!b.deleting;
  }
  if(btnPage){
    btnPage.disabled = !!b.deleting;
  }

  // Visual hint: if user can't select others, and current creator filter isn't self, show note.
  if(state.user && String(state.user.role||"user")==="user"){
    const c = String(state.preview.creator||"");
    if(c && c !== String(state.user.username||"")){
      _bulkStatus("自分の画像のみ選択/削除できます", "error");
    }else if(!b.deleting){
      _bulkStatus("", null);
    }
  }else if(!b.deleting){
    _bulkStatus("", null);
  }
}

function _resetBulkSelection(){
  const b = state.preview.bulk;
  b.selected = new Set();
  b.deselected = new Set();
  b.all = false;
  b.deleting = false;
  closeBulkBookmarkOverlay();
  _refreshTileChecks();
  _updateBulkActions();
}

function _togglePageSelect(){
  const b = state.preview.bulk;
  const items = state.preview.items || [];
  const deletable = items.filter(_canBulkSelectItem);
  if(!deletable.length){
    _updateBulkActions();
    return;
  }

  // Determine if all deletable items on the current page are selected.
  const allSelected = deletable.every(it => _isTileSelected(it));
  const next = !allSelected;
  deletable.forEach(it => _setTileSelected(it, next));

  _refreshTileChecks();
  _updateBulkActions();
}

function _toggleAllSelect(){
  const b = state.preview.bulk;
  if(b.all){
    b.all = false;
    b.deselected = new Set();
    b.selected = new Set();
  }else{
    b.all = true;
    b.deselected = new Set();
    b.selected = new Set();
  }
  _refreshTileChecks();
  _updateBulkActions();
}

function _currentFilterQuery(){
  return {
    creator: state.preview.creator || "",
    software: state.preview.software || "",
    tags: (state.preview.tags || []).slice(),
    tags_not: (state.preview.tags_not || []).slice(),
    date_from: state.preview.date_from || "",
    date_to: state.preview.date_to || "",
    dedup_only: Number(state.preview.dedup_only || 0) ? 1 : 0,
    bm_any: Number(state.preview.bm_any || 0) ? 1 : 0,
    bm_list_id: Number(state.preview.bm_list_id || 0) ? Number(state.preview.bm_list_id || 0) : null,
    // backward-compat
    fav_only: 0,
  };
}

async function _bulkDelete(){
  const b = state.preview.bulk;
  if(!b || b.deleting) return;

  const payload = b.all
    ? { mode: "query", query: _currentFilterQuery(), exclude_ids: Array.from(b.deselected || []) }
    : { mode: "ids", ids: Array.from(b.selected || []) };

  const countHint = b.all
    ? (() => {
        const total = Number(state.preview.total_count || 0);
        const excl = b.deselected.size;
        return total ? String(Math.max(0, total - excl)) : "全対象";
      })()
    : String(b.selected.size);

  if(!confirm(`選択した画像(${countHint})を削除します。元に戻せません。実行しますか？`)) return;

  b.deleting = true;
  _bulkStatus("削除中…", null);
  _updateBulkActions();

  try{
    const res = await apiFetch(API.bulkDelete, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const j = await apiJson(res);
    const del = Number(j && j.deleted || 0);
    _bulkStatus(`削除完了: ${del.toLocaleString()}件`, "ok");

    // Reset selection and refresh UI lists/counts.
    _resetBulkSelection();
    await refreshStatsAndPreviewAfterChange();
  }catch(_e){
    _bulkStatus("削除に失敗しました", "error");
    b.deleting = false;
    _updateBulkActions();
  }
}

function makeTile(it){
  const div = document.createElement("div");
  div.className = "tile";
  div.dataset.id = it.id;

  const badges = [];
  // Thumbnail: do not show dedup marker (dup).
  if(it.is_nsfw) badges.push('<span class="badge nsfw">NSFW</span>');

  const canSel = _canBulkSelectItem(it);
  const checked = _isTileSelected(it);

  div.innerHTML = `
    <div class="tileImg">
      ${_thumbImgHtml(it)}
      <div class="tileBadges">
        <div class="tileBadgesLeft">
          ${badges.join("")}
        </div>
        <div class="tileBadgesRight">
          <label class="tileCheck" title="${canSel ? "選択" : "自分の画像のみ"}">
            <input type="checkbox" class="tileChk" ${checked ? "checked" : ""} ${canSel ? "" : "disabled"}>
            <span class="tileChkBox"></span>
          </label>
          <button class="favBtn ${it.favorite ? "on" : ""}" title="ブックマーク" aria-label="ブックマーク">${it.favorite ? "★" : "☆"}</button>
        </div>
      </div>
      <div class="tileOverlay">
        <div>
          <div class="creator">${escapeHtml(it.creator || "")}</div>
          <div class="meta">${escapeHtml(fmtShort(it.mtime))}</div>
        </div>
        <div class="meta">#${it.id}</div>
      </div>
    </div>
  `;

  div.classList.toggle("selected", checked);

  const lab = div.querySelector(".tileCheck");
  if(lab){
    lab.addEventListener("click", (e) => { e.stopPropagation(); });
  }

  const chk = div.querySelector("input.tileChk");
  if(chk){
    chk.addEventListener("click", (e) => {
      e.stopPropagation();
    });
    chk.addEventListener("change", (e) => {
      e.stopPropagation();
      _setTileSelected(it, !!chk.checked);
      div.classList.toggle("selected", _isTileSelected(it));
      _updateBulkActions();
    });
  }

  const favBtn = div.querySelector(".favBtn");
  if(favBtn){
    favBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      await onBookmarkButtonClick(it.id);
    });
  }

  div.addEventListener("click", () => openDetail(it.id));
  return div;
}

function appendTiles(items){
  const grid = $("grid");
  if(!grid) return;
  items.forEach((it) => grid.appendChild(makeTile(it)));
}


function buildDetailCompactMeta(d){
  const soft = String(d?.software || '-').trim() || '-';
  const sampler = String(d?.sampler || '-').trim() || '-';
  const potion = typeof d?.uses_potion === 'boolean' ? (d.uses_potion ? '〇' : '×') : '-';
  const precise = typeof d?.uses_precise_reference === 'boolean' ? (d.uses_precise_reference ? '〇' : '×') : '-';
  return `ソフト ${soft} / ポーション ${potion} / 精密参照 ${precise} / サンプラー ${sampler}`;
}

function renderPager(){
  const pager = $("pager");
  if(!pager) return;
  pager.innerHTML = "";

  const page = state.preview.page;
  const total = state.preview.total_pages || 0;

  const mkBtn = (label, p, disabled=false, active=false) => {
    const b = document.createElement("button");
    b.className = "pageBtn" + (active ? " active" : "");
    b.textContent = label;
    b.disabled = !!disabled;
    b.addEventListener("click", async () => await search(p));
    return b;
  };

  const mkDots = () => {
    const b = document.createElement("button");
    b.className = "pageBtn dots";
    b.textContent = "…";
    b.disabled = true;
    return b;
  };

  pager.appendChild(mkBtn("◀", Math.max(1, page-1), page<=1));

  // Keep the pager width stable.
  // For large totals, always render 9 items between prev/next:
  //   - near start: 1..7 … last
  //   - middle: 1 … (p-2..p+2) … last
  //   - near end: 1 … (last-6..last)
  if(!total){
    // total unknown: keep a small centered window.
    const windowSize = 7;
    let start = Math.max(1, page - Math.floor(windowSize/2));
    let end = start + windowSize - 1;
    if((end - start + 1) < windowSize) start = Math.max(1, end - windowSize + 1);
    for(let p=start; p<=end; p++) pager.appendChild(mkBtn(String(p), p, false, p===page));
  }else if(total <= 9){
    for(let p=1; p<=total; p++) pager.appendChild(mkBtn(String(p), p, false, p===page));
  }else{
    if(page <= 4){
      for(let p=1; p<=7; p++) pager.appendChild(mkBtn(String(p), p, false, p===page));
      pager.appendChild(mkDots());
      pager.appendChild(mkBtn(String(total), total, false, page===total));
    }else if(page >= total - 3){
      pager.appendChild(mkBtn("1", 1, false, page===1));
      pager.appendChild(mkDots());
      for(let p=Math.max(1, total-6); p<=total; p++) pager.appendChild(mkBtn(String(p), p, false, p===page));
    }else{
      pager.appendChild(mkBtn("1", 1, false, page===1));
      pager.appendChild(mkDots());
      for(let p=page-2; p<=page+2; p++) pager.appendChild(mkBtn(String(p), p, false, p===page));
      pager.appendChild(mkDots());
      pager.appendChild(mkBtn(String(total), total, false, page===total));
    }
  }

  pager.appendChild(mkBtn("▶", total ? Math.min(total, page+1) : page+1, total ? page>=total : false));
}

/* =====================
   Detail overlay
   ===================== */

let currentDetail = null;

function setOverlayImageLoading(thumbUrl){
  const box = $("overlayImageBox");
  if(box){
    box.classList.add("loading");
    box.classList.remove("loaded");
  }
  const th = $("overlayThumb");
  if(th){
    th.style.backgroundImage = thumbUrl ? `url("${thumbUrl}")` : "";
  }
  const ld = $("overlayImgLoading");
  if(ld) ld.classList.remove("hidden");
}

function setOverlayImageLoaded(){
  const box = $("overlayImageBox");
  if(box){
    box.classList.remove("loading");
    box.classList.add("loaded");
  }
  const ld = $("overlayImgLoading");
  if(ld) ld.classList.add("hidden");
}

function resetOverlayImage(){
  const box = $("overlayImageBox");
  if(box){
    box.classList.remove("loading");
    box.classList.remove("loaded");
  }
  const th = $("overlayThumb");
  if(th) th.style.backgroundImage = "";
  const ld = $("overlayImgLoading");
  if(ld) ld.classList.add("hidden");
  const overlayImg = $("overlayImg");
  if(overlayImg){
    overlayImg.onload = null;
    overlayImg.onerror = null;
  }
}

// Prevent background (grid) from scrolling while the detail overlay is open.
function updateViewportHeightVar(){
  const vv = window.visualViewport;
  const height = vv && Number.isFinite(vv.height) ? vv.height : window.innerHeight;
  if(!Number.isFinite(height) || height <= 0) return;
  document.documentElement.style.setProperty("--vvh", `${height * 0.01}px`);
}

let _scrollLockY = 0;
function lockBodyScroll(){
  if(document.body.classList.contains("modalOpen")) return;
  _scrollLockY = window.scrollY || document.documentElement.scrollTop || 0;
  document.documentElement.classList.add("modalOpen");
  document.body.classList.add("modalOpen");
}

function unlockBodyScroll(){
  if(!document.body.classList.contains("modalOpen")) return;
  document.documentElement.classList.remove("modalOpen");
  document.body.classList.remove("modalOpen");
  window.scrollTo(0, _scrollLockY);
}

function setSelectedTile(id){
  // clear
  document.querySelectorAll(".tile.selected").forEach(el => el.classList.remove("selected"));
  state.selectedTileId = id;
  const tile = document.querySelector(`.tile[data-id="${id}"]`);
  if(tile) tile.classList.add("selected");
}

function copyText(t){
  navigator.clipboard.writeText(t);
}


function ensureDetailPromptUI(d){
  if(!d || d._ui_prompt_ready) return;

  const rawEntries = Array.isArray(d.character_entries) ? d.character_entries : [];
  d._ui_char_entries = rawEntries.map((e, i) => ({
    name: String(e?.name || `不明${i+1}`),
    pos: String(e?.pos || ""),
    neg: String(e?.neg || ""),
  }));

  d._ui_main_neg_combined = String(d.main_negative_combined_raw || d.prompt_negative_raw || "").trim();
  d._ui_uc_tags = Array.isArray(d.uc_tags) ? d.uc_tags : [];

  d._ui_prompt_ready = true;
}

function makePromptSection(title, key){
  const sec = document.createElement("div");
  sec.className = "section copyOnly";
  const head = document.createElement("div");
  head.className = "secHead";
  head.innerHTML = `
    <h3>${escapeHtml(title)}</h3>
    <div class="secBtns">
      <button class="mini" title="括弧/数値強調を保持したままコピーします" data-copyall="${escapeHtml(key)}_keep">全コピー</button>
      <button class="mini" title="括弧/数値強調を外してコピーします" data-copyall="${escapeHtml(key)}_plain">全コピー(強調解除)</button>
    </div>
  `;
  sec.appendChild(head);
  return sec;
}

function renderPromptSections(d){
  const wrap = $("promptSections");
  if(!wrap) return;
  if(!d){
    wrap.innerHTML = "";
    return;
  }

  ensureDetailPromptUI(d);
  wrap.innerHTML = "";

  // Copy-only frames (no per-tag buttons)
  // Naming: positive prompt = pc, negative prompt = uc
  wrap.appendChild(makePromptSection("メインpc", "promptMain"));
  wrap.appendChild(makePromptSection("メインuc", "negMain"));

  const entries = d._ui_char_entries || [];
  entries.forEach((e, i) => {
    const nm = (e && e.name) ? String(e.name) : `不明${i+1}`;
    wrap.appendChild(makePromptSection(`${nm}のpc`, `char${i}`));
    wrap.appendChild(makePromptSection(`${nm}のuc`, `char${i}Neg`));
  });
}

function makeTagButton(tag, onClick){
  const b = document.createElement("button");
  const label = String(tag || "");
  const len = label.length;
  let cls = "tagBtn";
  if(len >= 28) cls += " tagBtn-xlong";
  else if(len >= 18) cls += " tagBtn-long";
  b.className = cls;
  b.textContent = label;
  b.addEventListener("click", onClick);
  return b;
}

const DETAIL_TAG_VIEW_STORAGE_KEY = "nim.detail.tagViewMode";
const DETAIL_EMPHASIS_COPY_STORAGE_KEY = "nim.detail.emphasisCopy";

function getDetailTagViewMode(){
  try{
    const raw = String(localStorage.getItem(DETAIL_TAG_VIEW_STORAGE_KEY) || "B").toUpperCase();
    return raw === "T" ? "T" : "B";
  }catch(_e){
    return "B";
  }
}

function setDetailTagViewMode(mode){
  const next = String(mode || "B").toUpperCase() === "T" ? "T" : "B";
  try{ localStorage.setItem(DETAIL_TAG_VIEW_STORAGE_KEY, next); }catch(_e){}
  return next;
}

function isDetailEmphasisCopyEnabled(){
  try{
    return localStorage.getItem(DETAIL_EMPHASIS_COPY_STORAGE_KEY) === "1";
  }catch(_e){
    return false;
  }
}

function setDetailEmphasisCopyEnabled(enabled){
  const next = !!enabled;
  try{ localStorage.setItem(DETAIL_EMPHASIS_COPY_STORAGE_KEY, next ? "1" : "0"); }catch(_e){}
  return next;
}

function getDisplayTextForTags(arr, keepEmphasis){
  return keepEmphasis ? joinKeep(arr) : joinPlain(arr);
}

function getSingleTagCopyText(tagObj, keepEmphasis){
  const base = keepEmphasis
    ? (tagObj?.raw_one || "")
    : (tagObj?.text || "");
  const text = String(base || "").trim();
  return text ? `${text}, ` : "";
}

function syncDetailToggleUi(){
  const mode = getDetailTagViewMode();
  const keepEmphasis = isDetailEmphasisCopyEnabled();

  const viewToggle = $("detailTagViewToggle");
  const viewValue = $("detailTagViewToggleValue");
  if(viewToggle){
    const isText = mode === "T";
    viewToggle.classList.toggle("is-on", isText);
    viewToggle.setAttribute("aria-pressed", isText ? "true" : "false");
  }
  if(viewValue) viewValue.textContent = mode;

  const emphasisToggle = $("detailEmphasisToggle");
  const emphasisValue = $("detailEmphasisToggleValue");
  if(emphasisToggle){
    emphasisToggle.classList.toggle("is-on", keepEmphasis);
    emphasisToggle.setAttribute("aria-pressed", keepEmphasis ? "true" : "false");
  }
  if(emphasisValue) emphasisValue.textContent = keepEmphasis ? "ON" : "OFF";
}

function makeTagTextBox(value){
  const area = document.createElement("textarea");
  area.className = "tagTextBox";
  area.readOnly = true;
  area.spellcheck = false;
  area.rows = 1;
  area.value = String(value || "");
  const isEmpty = !area.value.trim();
  area.classList.toggle("is-empty", isEmpty);
  if(isEmpty){
    area.placeholder = "(none)";
    area.style.height = "36px";
  }
  return area;
}

function renderDetailTagSections(d){
  const secArtist = $("secArtist");
  const secQuality = $("secQuality");
  const secCharacter = $("secCharacter");
  const secOther = $("secOther");
  const secNegative = $("secNegative");
  const sections = [secArtist, secQuality, secCharacter, secOther, secNegative].filter(Boolean);
  sections.forEach((box) => { box.innerHTML = ""; });

  const mode = getDetailTagViewMode();
  const keepEmphasis = isDetailEmphasisCopyEnabled();
  const renderBox = (box, arr) => {
    if(!box) return;
    if(mode === "T"){
      box.appendChild(makeTagTextBox(getDisplayTextForTags(arr, keepEmphasis)));
      return;
    }
    if(!arr.length){
      const empty = document.createElement("span");
      empty.className = "small";
      empty.textContent = "(none)";
      box.appendChild(empty);
      return;
    }
    arr.forEach((t) => {
      const label = t?.text || "";
      if(!label) return;
      box.appendChild(makeTagButton(label, () => copyText(getSingleTagCopyText(t, keepEmphasis))));
    });
  };

  renderBox(secArtist, d?.tags?.artist || []);
  renderBox(secQuality, d?.tags?.quality || []);
  renderBox(secCharacter, d?.tags?.character || []);
  renderBox(secOther, d?.tags?.other || []);

  ensureDetailPromptUI(d);
  renderBox(secNegative, Array.isArray(d?._ui_uc_tags) ? d._ui_uc_tags : []);
}

function bindDetailToggleControls(){
  $("detailTagViewToggle")?.addEventListener("click", (e) => {
    e.preventDefault();
    setDetailTagViewMode(getDetailTagViewMode() === "T" ? "B" : "T");
    syncDetailToggleUi();
    if(currentDetail) renderDetailTagSections(currentDetail);
  });

  $("detailEmphasisToggle")?.addEventListener("click", (e) => {
    e.preventDefault();
    setDetailEmphasisCopyEnabled(!isDetailEmphasisCopyEnabled());
    syncDetailToggleUi();
    if(currentDetail && getDetailTagViewMode() === "T") renderDetailTagSections(currentDetail);
  });
}

function fmtBytes(n){
  const x = Number(n||0);
  if(!x) return "";
  const kb = x / 1024;
  if(kb < 1024) return `${kb.toFixed(1)}KB`;
  const mb = kb / 1024;
  if(mb < 1024) return `${mb.toFixed(2)}MB`;
  const gb = mb / 1024;
  return `${gb.toFixed(2)}GB`;
}

async function onBookmarkButtonClick(id){
  const iid = Number(id || 0);
  if(!iid) return;

  // Determine current state from caches.
  let curFav = 0;
  if(currentDetail && Number(currentDetail.id) === iid){
    curFav = Number(currentDetail.favorite || 0);
  }else{
    const it = (state.preview.items || []).find(x => Number(x.id) === iid);
    curFav = Number(it?.favorite || 0);
  }

  if(!curFav){
    await addBookmarkDefault(iid);
    return;
  }
  await openBookmarkOverlay(iid);
}

function _applyFavoriteState(id, fav){
  const iid = Number(id || 0);
  const v = Number(fav || 0) ? 1 : 0;

  // list cache
  state.preview.items.forEach(it => {
    if(Number(it.id) === iid) it.favorite = v;
  });

  // detail cache
  if(currentDetail && Number(currentDetail.id) === iid){
    currentDetail.favorite = v;
    syncDetailFavorite();
  }

  // tile UI
  const tile = document.querySelector(`.tile[data-id="${iid}"]`);
  const btn = tile?.querySelector(".favBtn");
  if(btn){
    btn.textContent = v ? "★" : "☆";
    btn.classList.toggle("on", !!v);
  }
}

async function addBookmarkDefault(id){
  try{
    const res = await apiFetch(withBookmarkContext(API.bookmarkDefault(id)), { method: "POST" });
    const data = await apiJson(res);
    _applyFavoriteState(id, Number(data.favorite || 0));
    await refreshBookmarkLists();

    // If current view depends on bookmark state, refresh.
    if(state.preview.sort === "favorite" || state.preview.bm_any || state.preview.bm_list_id){
      await search();
    }
  }catch(_e){}
}

let _bmOverlayImageId = 0;
let _bmSaveTimer = null;
let _bulkBmState = null;

function _bmSetStatus(msg, cls){
  const el = $("bmOverlayStatus");
  if(!el) return;
  el.textContent = msg || "";
  el.className = "small" + (cls ? (" " + cls) : "");
}

function _bmOpen(){
  const ov = $("bmOverlay");
  if(!ov) return;
  ov.classList.remove("hidden");
  ov.setAttribute("aria-hidden", "false");
  _bmSetStatus("", null);
}

function closeBookmarkOverlay(){
  const ov = $("bmOverlay");
  if(!ov) return;
  ov.classList.add("hidden");
  ov.setAttribute("aria-hidden", "true");
  _bmOverlayImageId = 0;
  if(_bmSaveTimer){
    clearTimeout(_bmSaveTimer);
    _bmSaveTimer = null;
  }
}


/* =====================
   User picker modal (creator list / bookmark subscriptions)
   ===================== */

let _userPickTimer = null;

function closeUserPick(){
  const m = $("userPickModal");
  if(!m) return;
  m.classList.add("hidden");
  m.setAttribute("aria-hidden", "true");
}

async function _loadUserPick(kind, q){
  const res = await apiFetch(API.userSuggest(kind, q));
  const data = await apiJson(res);
  const items = (data && data.items) ? data.items : [];
  return items;
}

function _renderUserPick(items){
  const m = $("userPickModal");
  const list = $("userPickList");
  const hint = $("userPickHint");
  if(!m || !list) return;
  const kind = String(m.dataset.kind || "creators");
  list.innerHTML = "";

  if(hint) hint.textContent = items.length ? "" : "候補なし";

  items.forEach((u) => {
    const id = Number(u.id || 0);
    const name = String(u.username || "");
    if(!id || !name) return;
    const sw = !!Number(u.share_works || 0);
    const sb = !!Number(u.share_bookmarks || 0);
    const inCreator = !!Number(u.in_creator_list || 0);
    const inBm = !!Number(u.in_bookmark_subs || 0);

    // Safety: backend also filters, but keep the UI consistent.
    if(kind === "creators" && !sw) return;
    if(kind === "bookmarks" && (!sw || !sb)) return;

    const row = document.createElement("div");
    row.className = "userPickRow";
    const av = escapeHtml((name.trim()[0] || "?").toUpperCase());
    row.innerHTML = `
      <div class="uLeft">
        <div class="uAv" aria-hidden="true">${av}</div>
        <div class="uMain">
          <div class="uName">${escapeHtml(name)}</div>
          <div class="uMeta">${kind === "bookmarks" ? "ブックマーク共有ON" : "作品共有ON"}</div>
        </div>
      </div>
      <div class="uBtns"></div>
    `;

    const btns = row.querySelector(".uBtns");
    const addBtn = document.createElement("button");
    addBtn.className = "addBtn";
    // Use a widely supported glyph (some fonts miss "⊕").
    addBtn.textContent = "＋";
    addBtn.title = "追加";
    addBtn.setAttribute("aria-label", "追加");
    addBtn.disabled = (kind === "creators") ? inCreator : inBm;

    addBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      try{
        if(kind === "creators"){
          await apiFetch(API.creatorListAdd, {
            method: "POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify({ user_id: id }),
          });
          addBtn.disabled = true;
          await refreshStatsAndPreviewAfterChange();
        }else{
          await apiFetch(API.bookmarkSubAdd, {
            method: "POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify({ user_id: id }),
          });
          addBtn.disabled = true;
          await refreshBookmarkLists();
        }
      }catch(_e){}
    });

    btns?.appendChild(addBtn);
    list.appendChild(row);
  });
}

async function openUserPick(kind){
  const m = $("userPickModal");
  const q = $("userPickQuery");
  const title = $("userPickTitle");
  const sub = $("userPickSub");
  if(!m) return;
  const k = (kind === "bookmarks") ? "bookmarks" : "creators";
  m.dataset.kind = k;
  if(title) title.textContent = (k === "creators") ? "作者を追加" : "ブックマーク作者を追加";
  if(sub) sub.textContent = (k === "creators")
    ? "作品共有ONのユーザーが候補に出ます。"
    : "作品共有ON かつ ブックマーク共有ON のユーザーが候補に出ます。";
  if(q) q.value = "";

  m.classList.remove("hidden");
  m.setAttribute("aria-hidden", "false");

  try{
    const items = await _loadUserPick(k, "");
    _renderUserPick(items);
  }catch(_e){
    _renderUserPick([]);
  }
}

function bindUserPick(){
  $("userPickBg")?.addEventListener("click", closeUserPick);
  $("userPickClose")?.addEventListener("click", closeUserPick);

  const doSearch = async () => {
    const m = $("userPickModal");
    const q = $("userPickQuery");
    if(!m) return;
    const kind = String(m.dataset.kind || "creators");
    const txt = String(q?.value || "");
    try{
      const items = await _loadUserPick(kind, txt);
      _renderUserPick(items);
    }catch(_e){
      _renderUserPick([]);
    }
  };

  $("userPickSearchBtn")?.addEventListener("click", (e) => {
    e.preventDefault();
    doSearch();
  });

  const q = $("userPickQuery");
  q?.addEventListener("input", () => {
    const m = $("userPickModal");
    if(!m) return;
    const kind = String(m.dataset.kind || "creators");
    const txt = String(q.value || "");
    if(_userPickTimer) clearTimeout(_userPickTimer);
    _userPickTimer = setTimeout(async () => {
      try{
        const items = await _loadUserPick(kind, txt);
        _renderUserPick(items);
      }catch(_e){
        _renderUserPick([]);
      }
    }, 220);
  });

  q?.addEventListener("keydown", (e) => {
    if(e.key === "Enter"){
      e.preventDefault();
      doSearch();
    }
  });
}

async function addCreatorFromDetail(){
  const btn = $("addCreatorFromDetailBtn");
  if(!btn) return;
  const name = String(currentDetail?.creator || "").trim();
  if(!name) return;
  try{
    btn.disabled = true;
    await apiFetch(API.creatorListAdd, {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ username: name }),
    });
    await refreshStatsAndPreviewAfterChange();
  }catch(_e){
    btn.disabled = false;
  }
}

function syncAddCreatorFromDetailBtn(){
  const btn = $("addCreatorFromDetailBtn");
  if(!btn) return;
  const name = String(currentDetail?.creator || "").trim();
  if(!name){
    btn.disabled = true;
    return;
  }
  const selfName = String(state.user?.username || "").trim();
  if(selfName && name === selfName){
    btn.disabled = true;
    return;
  }
  const set = state.facets?.creatorSet;
  btn.disabled = !!(set && set.has(name));
}

function closeBulkBookmarkOverlay(){
  _bulkBmState = null;
  const el = $("bulkBmOverlay");
  if(el) el.classList.add("hidden");
  if(el) el.setAttribute("aria-hidden", "true");
  _bulkBookmarkStatus("", null);
}

async function _createBookmarkListInteractive(){
  const nm0 = prompt("新しいリスト名", "");
  if(nm0 === null) return null;
  const nm = String(nm0 || "").trim();
  if(!nm) return null;
  const res = await apiFetch(API.bookmarkLists, {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({ name: nm }),
  });
  const data = await apiJson(res);
  await refreshBookmarkLists();
  return data;
}

async function _renameBookmarkListInteractive(listId, currentName){
  const cur = String(currentName || "");
  const nxt = prompt("リスト名", cur);
  if(nxt === null) return null;
  const nm = String(nxt || "").trim();
  if(!nm || nm === cur) return null;
  const res = await apiFetch(API.bookmarkList(listId), {
    method: "PATCH",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({ name: nm }),
  });
  const data = await apiJson(res);
  await refreshBookmarkLists();
  return data;
}

function _syncBulkBookmarkListsMeta(serverLists){
  const st = _bulkBmState;
  if(!st || !Array.isArray(st.lists) || !Array.isArray(serverLists)) return;
  const prev = new Map();
  st.lists.forEach((item) => {
    const id = Number(item?.id || 0);
    if(id > 0) prev.set(id, item);
  });
  st.lists = serverLists
    .map((list) => {
      const id = Number(list?.id || 0);
      if(!(id > 0)) return null;
      const existing = prev.get(id);
      if(existing){
        existing.name = String(list?.name || "");
        existing.is_default = Number(list?.is_default || 0);
        return existing;
      }
      return {
        id,
        name: String(list?.name || ""),
        is_default: Number(list?.is_default || 0),
        initialState: "none",
        editState: null,
      };
    })
    .filter(Boolean);
  _renderBulkBookmarkLists();
  _updateBulkBookmarkSaveButton();
}

function _bulkBookmarkInitialState(item){
  return String(item?.initialState || "none");
}

function _bulkBookmarkResolvedState(item){
  return String(item?.editState || _bulkBookmarkInitialState(item));
}

function _bulkBookmarkHasChange(item){
  return !!(item && item.editState);
}

function _bulkBookmarkStateGlyph(state){
  const key = String(state || "none");
  if(key === "all") return "☑";
  if(key === "some") return "▣";
  return "□";
}

function _cycleBulkBookmarkEditState(item){
  if(!item) return;
  const initial = _bulkBookmarkInitialState(item);
  const edit = item.editState ? String(item.editState) : "";

  if(initial === "some"){
    if(!edit) item.editState = "all";
    else if(edit === "all") item.editState = "none";
    else item.editState = null;
    return;
  }

  const opposite = initial === "all" ? "none" : "all";
  item.editState = edit === opposite ? null : opposite;
}

function _bulkBookmarkRowVisuals(row, stateCtl, item){
  if(!row || !stateCtl || !item) return;
  const state = _bulkBookmarkResolvedState(item);
  const glyph = _bulkBookmarkStateGlyph(state);
  stateCtl.textContent = glyph;
  stateCtl.dataset.state = state;
  stateCtl.setAttribute("aria-checked", state === "some" ? "mixed" : (state === "all" ? "true" : "false"));
  stateCtl.setAttribute("title", state === "all" ? "全件登録" : (state === "some" ? "一部登録" : "未登録"));
  row.dataset.state = state;
  row.classList.toggle("mixed", state === "some");
  row.classList.toggle("all", state === "all");
  row.classList.toggle("none", state === "none");
  row.classList.toggle("changed", _bulkBookmarkHasChange(item));
}

function _renderBulkBookmarkLists(){
  const box = $("bulkBmOverlayLists");
  if(!box) return;
  box.innerHTML = "";
  const st = _bulkBmState;
  if(!st || !Array.isArray(st.lists) || !st.lists.length){
    box.innerHTML = `<div class="small">(リストがありません)</div>`;
    return;
  }
  st.lists.forEach((item) => {
    const row = document.createElement("div");
    row.className = "bmListRow bulkBmListRow";
    row.dataset.listId = String(item.id || 0);
    row.setAttribute("role", "button");
    row.setAttribute("tabindex", "0");

    const main = document.createElement("div");
    main.className = "bulkBmMain";

    const stateCtl = document.createElement("span");
    stateCtl.className = "bulkBmStateCtl";
    stateCtl.setAttribute("aria-hidden", "true");

    const textWrap = document.createElement("div");
    textWrap.className = "bulkBmText";

    const name = document.createElement("div");
    name.className = "bmListName";
    name.textContent = String(item.name || "");
    textWrap.appendChild(name);

    if(Number(item.is_default || 0)){
      const badge = document.createElement("span");
      badge.className = "bmListBadge";
      badge.textContent = "default";
      textWrap.appendChild(badge);
    }

    main.appendChild(stateCtl);
    main.appendChild(textWrap);

    const btns = document.createElement("div");
    btns.className = "bmListBtns bulkBmListBtns";

    const renameBtn = document.createElement("button");
    renameBtn.type = "button";
    renameBtn.className = "mini ghostBtn";
    renameBtn.textContent = "✎";
    renameBtn.title = "名前変更";
    renameBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      try{
        const data = await _renameBookmarkListInteractive(item.id, item.name);
        if(data?.lists) _syncBulkBookmarkListsMeta(data.lists);
      }catch(_e){
        alert("名前変更に失敗しました");
      }
    });
    btns.appendChild(renameBtn);

    const applyState = () => {
      _bulkBookmarkRowVisuals(row, stateCtl, item);
    };
    applyState();

    const toggleRow = (e) => {
      e.preventDefault();
      _cycleBulkBookmarkEditState(item);
      applyState();
      _updateBulkBookmarkSaveButton();
    };
    row.addEventListener("click", toggleRow);
    row.addEventListener("keydown", (e) => {
      if(e.key === "Enter" || e.key === " ") toggleRow(e);
    });

    row.appendChild(main);
    row.appendChild(btns);
    box.appendChild(row);
  });
}

function _updateBulkBookmarkSaveButton(){
  const btn = $("bulkBmSaveBtn");
  if(!btn) return;
  const st = _bulkBmState;
  const changed = !!(st && Array.isArray(st.lists) && st.lists.some((item) => _bulkBookmarkHasChange(item)));
  btn.disabled = !(changed && st && !st.saving);
}

async function openBulkBookmarkOverlay(){
  const payload = _currentBulkSelectionPayload();
  if(!payload) return;

  const overlay = $("bulkBmOverlay");
  if(!overlay) return;
  overlay.classList.remove("hidden");
  overlay.setAttribute("aria-hidden", "false");
  const hint = $("bulkBmOverlayHint");
  if(hint) hint.textContent = "読み込み中…";
  const box = $("bulkBmOverlayLists");
  if(box) box.innerHTML = `<div class="small">(loading…)</div>`;
  _bulkBookmarkStatus("", null);

  try{
    const res = await apiFetch(withBookmarkContext(API.bookmarkBulkStatus), {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify(payload),
    });
    const data = await apiJson(res);
    _bulkBmState = {
      selection: payload,
      selectedCount: Number(data?.selected_count || 0),
      lists: Array.isArray(data?.lists)
        ? data.lists.map((item) => ({
            id: Number(item?.id || 0),
            name: String(item?.name || ""),
            is_default: Number(item?.is_default || 0),
            initialState: String(item?.state || "none"),
            editState: null,
          }))
        : [],
      saving: false,
    };
    if(hint){
      const n = Number(_bulkBmState.selectedCount || 0);
      hint.textContent = n > 0 ? `選択: ${n.toLocaleString()}件` : "選択対象がありません";
    }
    _renderBulkBookmarkLists();
    _updateBulkBookmarkSaveButton();
  }catch(_e){
    if(hint) hint.textContent = "読み込みに失敗しました";
    if(box) box.innerHTML = "";
    _bulkBookmarkStatus("読み込みに失敗しました", "error");
  }
}

async function saveBulkBookmarkOverlay(){
  const st = _bulkBmState;
  if(!st || st.saving) return;
  const addListIds = [];
  const removeListIds = [];
  (st.lists || []).forEach((item) => {
    const edit = item?.editState ? String(item.editState) : "";
    if(!edit) return;
    if(edit === "all") addListIds.push(Number(item.id || 0));
    else if(edit === "none") removeListIds.push(Number(item.id || 0));
  });
  if(!addListIds.length && !removeListIds.length){
    closeBulkBookmarkOverlay();
    return;
  }

  st.saving = true;
  _bulkBookmarkStatus("保存中…", null);
  _updateBulkBookmarkSaveButton();
  try{
    await apiFetch(withBookmarkContext(API.bookmarkBulkApply), {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({
        ...(st.selection || {}),
        add_list_ids: addListIds,
        remove_list_ids: removeListIds,
      }),
    });
    closeBulkBookmarkOverlay();
    await refreshBookmarkLists();
    await search();
  }catch(_e){
    st.saving = false;
    _bulkBookmarkStatus("保存に失敗しました", "error");
    _updateBulkBookmarkSaveButton();
  }
}

async function openBookmarkOverlay(imageId){
  const iid = Number(imageId || 0);
  if(!iid) return;
  _bmOverlayImageId = iid;

  _bmOpen();
  const hint = $("bmOverlayHint");
  if(hint) hint.textContent = `#${iid}`;

  const box = $("bmOverlayLists");
  if(box) box.innerHTML = `<div class="small">(loading…)</div>`;

  try{
    const res = await apiFetch(withBookmarkContext(API.bookmarkImage(iid)));
    const data = await apiJson(res);
    renderBookmarkOverlayLists(data);
  }catch(_e){
    if(box) box.innerHTML = `<div class="small">読み込みに失敗しました</div>`;
  }
}

function _bmCheckedListIds(){
  const box = $("bmOverlayLists");
  if(!box) return [];
  const ids = [];
  box.querySelectorAll("input[type=checkbox][data-list-id]").forEach((el) => {
    if(el.checked) ids.push(Number(el.dataset.listId || 0));
  });
  return ids.filter(x => x > 0);
}

function _bmScheduleSave(){
  if(_bmSaveTimer){
    clearTimeout(_bmSaveTimer);
    _bmSaveTimer = null;
  }
  _bmSaveTimer = setTimeout(async () => {
    const iid = Number(_bmOverlayImageId || 0);
    if(!iid) return;
    const ids = _bmCheckedListIds();
    _bmSetStatus("保存中…", null);
    try{
      const res = await apiFetch(withBookmarkContext(API.bookmarkImage(iid)), {
        method: "PUT",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ list_ids: ids }),
      });
      const j = await apiJson(res);
      _applyFavoriteState(iid, Number(j.favorite || 0));
      _bmSetStatus("保存しました", "ok");
      await refreshBookmarkLists();
      if(state.preview.sort === "favorite" || state.preview.bm_any || state.preview.bm_list_id){
        await search();
      }
    }catch(_e){
      _bmSetStatus("保存に失敗しました", "error");
    }
  }, 240);
}

function renderBookmarkOverlayLists(data){
  const box = $("bmOverlayLists");
  if(!box) return;
  box.innerHTML = "";

  const lists = (data && data.lists) ? data.lists : [];
  if(!lists.length){
    box.innerHTML = `<div class="small">(リストがありません)</div>`;
    return;
  }

  lists.forEach((l) => {
    const row = document.createElement("div");
    row.className = "bmListRow";

    const left = document.createElement("div");
    left.className = "bmListLeft";

    const chk = document.createElement("input");
    chk.type = "checkbox";
    chk.checked = !!Number(l.checked || 0);
    chk.dataset.listId = String(l.id || 0);
    chk.addEventListener("change", () => _bmScheduleSave());

    const name = document.createElement("div");
    name.className = "bmListName";
    name.textContent = String(l.name || "");

    left.appendChild(chk);
    left.appendChild(name);

    if(Number(l.is_default || 0)){
      const badge = document.createElement("span");
      badge.className = "bmListBadge";
      badge.textContent = "default";
      left.appendChild(badge);
    }

    const btns = document.createElement("div");
    btns.className = "bmListBtns";

    const renameBtn = document.createElement("button");
    renameBtn.className = "mini ghostBtn";
    renameBtn.textContent = "✎";
    renameBtn.title = "名前変更";
    renameBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      const cur = String(l.name || "");
      const nxt = prompt("リスト名", cur);
      if(nxt === null) return;
      const nm = String(nxt || "").trim();
      if(!nm) return;
      try{
        await apiFetch(API.bookmarkList(l.id), {
          method: "PATCH",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({ name: nm }),
        });
        await refreshBookmarkLists();
        await openBookmarkOverlay(_bmOverlayImageId);
      }catch(_e){
        alert("名前変更に失敗しました");
      }
    });

    const delBtn = document.createElement("button");
    delBtn.className = "mini dangerBtn";
    delBtn.textContent = "🗑";
    delBtn.title = "削除";
    delBtn.disabled = !!Number(l.is_default || 0);
    delBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      if(Number(l.is_default || 0)){
        alert("default は削除できません");
        return;
      }
      const nm = String(l.name || "");
      if(!confirm(`リスト「${nm}」を削除します。元に戻せません。よろしいですか？`)) return;
      if(!confirm("本当に削除しますか？")) return;
      try{
        await apiFetch(API.bookmarkList(l.id), { method: "DELETE" });
        await refreshBookmarkLists();
        await openBookmarkOverlay(_bmOverlayImageId);
      }catch(_e){
        alert("削除に失敗しました");
      }
    });

    btns.appendChild(renameBtn);
    btns.appendChild(delBtn);

    row.appendChild(left);
    row.appendChild(btns);
    box.appendChild(row);
  });
}

function syncDetailFavorite(){
  if(!currentDetail) return;
  const fav = Number(currentDetail.favorite || 0);
  const apply = (btn) => {
    if(!btn) return;
    const icon = btn.querySelector(".icon") || btn;
    icon.textContent = fav ? "★" : "☆";
    btn.classList.toggle("active", !!fav);
  };
  apply($("favDetailBtn"));
  apply($("favDetailBtnFixed"));
}

function renderDetailLoading(id){
  const iid = Number(id);
  const it = (state.preview.items || []).find(x => Number(x.id) === iid) || {};

  // Use the grid thumb as a stretched placeholder until the overlay image is loaded.
  const thumbEl = document.querySelector(`.tile[data-id="${iid}"] img`);
  const thumbUrl = thumbEl ? (thumbEl.currentSrc || thumbEl.src) : (it.thumb || "");
  setOverlayImageLoading(thumbUrl);

  const overlayImg = $("overlayImg");
  if(overlayImg) overlayImg.src = ""; // image can be later

  const titleText = it.filename || `#${iid}`;
  const dTitle = $("dTitle");
  if(dTitle){
    dTitle.classList.remove("expanded");
    dTitle.textContent = titleText;
  }
  const dSub = $("dSub");
  if(dSub) dSub.textContent = `${it.w||""}x${it.h||""}  ${fmtMtime(it.mtime)}`.trim();
  const dMeta = $("dMeta");
  if(dMeta) dMeta.textContent = `dedup:${it.dedup_flag||0}  model:${it.software||""}`;

  const dCompactMeta = $("dCompactMeta");
  if(dCompactMeta) dCompactMeta.textContent = buildDetailCompactMeta(it);
  const dCreator = $("dCreator");
  if(dCreator) dCreator.textContent = it.creator || "";

  // Disable until detail arrives (avoid using stale currentDetail).
  const addBtn = $("addCreatorFromDetailBtn");
  if(addBtn) addBtn.disabled = true;

  // Favorite icon (from list) until detail arrives.
  const fav = Number(it.favorite || 0);
  const applyFav = (btn) => {
    if(!btn) return;
    const icon = btn.querySelector('.icon') || btn;
    icon.textContent = fav ? '★' : '☆';
    btn.classList.toggle('active', !!fav);
  };
  applyFav($("favDetailBtn"));
  applyFav($("favDetailBtnFixed"));

  const mobile = isMobile();
  const imgOverlayInfo = $("imgOverlayInfo");
  if(imgOverlayInfo){
    if(mobile){
      const t = $("ioTitle"); if(t) t.textContent = titleText;
      const s = $("ioSub"); if(s) s.textContent = `${it.w||""}x${it.h||""}  ${fmtMtime(it.mtime)}`.trim();
      const m = $("ioMeta"); if(m){
        const src = it.software || "";
        const by = it.creator ? `by ${it.creator}` : "";
        m.textContent = (src && by) ? `${src}  ${by}` : (src || by);
      }
      imgOverlayInfo.classList.remove("hidden");
    }else{
      imgOverlayInfo.classList.add("hidden");
    }
  }

  const overlay = $("overlay");
  overlay?.classList.toggle("mobileDetail", mobile);

  const fixedBtns = $("overlayFixedBtns");
  if(fixedBtns) fixedBtns.classList.toggle("hidden", !mobile);

  // links disabled until detail arrives
  const dlFile = $("dlFile");
  if(dlFile) dlFile.href = "#";
  const dlFileDesktop = $("dlFileDesktop");
  if(dlFileDesktop) dlFileDesktop.href = "#";
  const vf = $("viewFull");
  if(vf){ vf.href = "#"; vf.classList.add("hidden"); }
  const vfDesktop = $("viewFullDesktop");
  if(vfDesktop){ vfDesktop.href = "#"; vfDesktop.classList.add("hidden"); }
  const dlMeta = $("dlMeta");
  if(dlMeta) dlMeta.href = "#";

  const secArtist = $("secArtist");
  const secQuality = $("secQuality");
  const secCharacter = $("secCharacter");
  const secOther = $("secOther");
  const secNegative = $("secNegative");
  const setLoading = (box) => {
    if(!box) return;
    box.innerHTML = "";
    const s = document.createElement("span");
    s.className = "small";
    s.textContent = "(loading…)";
    box.appendChild(s);
  };
  setLoading(secArtist);
  setLoading(secQuality);
  setLoading(secCharacter);
  setLoading(secOther);
  setLoading(secNegative);

  const promptWrap = $("promptSections");
  if(promptWrap){
    promptWrap.innerHTML = "";
    const s = document.createElement("span");
    s.className = "small";
    s.textContent = "(loading…)";
    promptWrap.appendChild(s);
  }

  const metaPre = $("metaPre");
  if(metaPre) metaPre.textContent = "loading…";
}

function renderDetailFull(d){
  // This is the previous openDetail rendering body, factored out.
  syncDetailFavorite();

  const overlayImg = $("overlayImg");
  // image is allowed to be later: keep thumb+spinner until onload fires
  if(overlayImg){
    const tok = String(_detailRenderToken);
    overlayImg.dataset.token = tok;
    overlayImg.onload = () => {
      if(overlayImg.dataset.token !== tok) return;
      setOverlayImageLoaded();
    };
    overlayImg.onerror = () => {
      if(overlayImg.dataset.token !== tok) return;
      // keep the stretched thumb, but stop the spinner
      const ld = $("overlayImgLoading");
      if(ld) ld.classList.add("hidden");
    };

    requestAnimationFrame(() => {
      // trigger load after text is already rendered
      overlayImg.src = d.overlay;
    });
  }
  const dTitle = $("dTitle");
  if(dTitle){
    dTitle.classList.remove("expanded");
    dTitle.textContent = d.filename || `#${d.id}`;
  }
  const dSub = $("dSub");
  if(dSub) dSub.textContent = `${d.w}x${d.h}  ${fmtMtime(d.mtime)}`;
  const nsfw = d.is_nsfw ? "  NSFW" : "";
  const fav = d.favorite ? "  ★" : "";
  const dMeta = $("dMeta");
  if(dMeta) dMeta.textContent = `${fmtBytes(d.file_bytes)}  dedup:${d.dedup_flag}  model:${d.model||""}${nsfw}${fav}`;

  const dCompactMeta = $("dCompactMeta");
  if(dCompactMeta) dCompactMeta.textContent = buildDetailCompactMeta(d);
  const dCreator = $("dCreator");
  if(dCreator) dCreator.textContent = d.creator || "";

  syncAddCreatorFromDetailBtn();

  // Mobile: show compact info as an overlay on the image (no blur)
  const mobile = isMobile();
  const imgOverlayInfo = $("imgOverlayInfo");
  if(imgOverlayInfo){
    if(mobile){
      const t = $("ioTitle"); if(t) t.textContent = d.filename || `#${d.id}`;
      const s = $("ioSub"); if(s) s.textContent = `${d.w}x${d.h}  ${fmtMtime(d.mtime)}`;
      const m = $("ioMeta"); if(m){
        const src = d.software || "";
        const by = d.creator ? `by ${d.creator}` : "";
        m.textContent = (src && by) ? `${src}  ${by}` : (src || by);
      }
      const u = $("ioUsage"); if(u){
        const potion = typeof d.uses_potion === "boolean" ? (d.uses_potion ? "〇" : "×") : "-";
        const precise = typeof d.uses_precise_reference === "boolean" ? (d.uses_precise_reference ? "〇" : "×") : "-";
        const sampler = d.sampler || "-";
        u.textContent = `ポーション ${potion} / 精密参照 ${precise} / サンプラー ${sampler}`;
      }
      imgOverlayInfo.classList.remove("hidden");
    }else{
      imgOverlayInfo.classList.add("hidden");
    }
  }

  // Mobile: keep favorite/close buttons fixed on top-right
  const overlay = $("overlay");
  overlay?.classList.toggle("mobileDetail", mobile);

  const fixedBtns = $("overlayFixedBtns");
  if(fixedBtns) fixedBtns.classList.toggle("hidden", !mobile);

  const dlFile = $("dlFile");
  if(dlFile) dlFile.href = d.download_file;
  const dlFileDesktop = $("dlFileDesktop");
  if(dlFileDesktop) dlFileDesktop.href = d.download_file;
  const viewHref = d.view_full || d.download_file;
  const vf = $("viewFull");
  if(vf){
    vf.href = viewHref;
    vf.classList.toggle("hidden", !viewHref);
  }
  const vfDesktop = $("viewFullDesktop");
  if(vfDesktop){
    vfDesktop.href = viewHref;
    vfDesktop.classList.toggle("hidden", !viewHref);
  }
  const dlMeta = $("dlMeta");
  if(dlMeta) dlMeta.href = d.download_meta;


  syncDetailToggleUi();
  renderDetailTagSections(d);

  // Copy-only prompt frames requested by UI spec
  renderPromptSections(d);

  let params = null;
  if(d.params_json){
    try{ params = JSON.parse(d.params_json); }
    catch(_e){ params = { _raw: String(d.params_json) }; }
  }
  const meta = {
    software: d.software,
    model: d.model,
    uses_potion: !!d.uses_potion,
    uses_precise_reference: !!d.uses_precise_reference,
    sampler: d.sampler || null,
    prompt_positive_raw: d.prompt_positive_raw,
    prompt_negative_raw: d.prompt_negative_raw,
    prompt_character_raw: d.prompt_character_raw,
    params,
  };
  const metaPre = $("metaPre");
  if(metaPre) metaPre.textContent = JSON.stringify(meta, null, 2);
}

async function openDetail(id){
  const iid = Number(id);
  setSelectedTile(iid);

  // Show UI immediately (do NOT wait for image).
  updateViewportHeightVar();
  const overlay = $("overlay");
  if(overlay){
    overlay.classList.remove("hidden");
    overlay.classList.add("open");
    overlay.classList.toggle("mobileDetail", isMobile());
    overlay.setAttribute("aria-hidden", "false");
  }
  lockBodyScroll();
  renderDetailLoading(iid);

  const token = ++_detailRenderToken;

  // If we already prefetched detail, render instantly.
  const cached = _detailCache.get(detailCacheKey(iid));
  if(cached){
    currentDetail = cached;
    renderDetailFull(cached);
    return;
  }

  // Fetch in parallel; once it arrives, update UI.
  fetchDetailCached(iid, { source: "overlay_open", page: currentPreviewPage(), mode: currentPreviewMode() }).then((d) => {
    if(token !== _detailRenderToken) return;
    currentDetail = d;
    renderDetailFull(d);
  }).catch((_e) => {
    // keep the overlay open, but show minimal info
    const metaPre = $("metaPre");
    if(metaPre) metaPre.textContent = "(failed to load detail)";
  });
}

function resetDetailOverlayUiState(){
  const slide = $("slide");
  if(slide) slide.scrollTop = 0;
  document.querySelectorAll("#overlay details").forEach((el) => { try{ el.open = false; }catch(_e){} });
  $("dTitle")?.classList.remove("expanded");
  $("imgOverlayInfo")?.classList.remove("expanded");
}

function closeDetail(){
  _detailRenderToken += 1; // cancel pending render
  const overlay = $("overlay");
  if(overlay){
    overlay.classList.remove("open");
    overlay.setAttribute("aria-hidden", "true");
  }
  const overlayImg = $("overlayImg");
  if(overlayImg) overlayImg.src = "";
  resetOverlayImage();
  $("overlayFixedBtns")?.classList.add("hidden");
  $("overlay")?.classList.remove("mobileDetail");
  $("imgOverlayInfo")?.classList.add("hidden");
  resetDetailOverlayUiState();
  currentDetail = null;
  // clear selection
  document.querySelectorAll(".tile.selected").forEach(el => el.classList.remove("selected"));
  state.selectedTileId = null;
  unlockBodyScroll();
  updateViewportHeightVar();
}

let _copyAllBound = false;
function bindCopyAll(){
  if(_copyAllBound) return;
  _copyAllBound = true;

  document.addEventListener("click", (e) => {
    const btn = e.target?.closest?.("[data-copyall]");
    if(!btn) return;
    if(btn.disabled) return;
    if(!currentDetail) return;

    e.preventDefault();
    const k = String(btn.dataset.copyall || "");
    const idx = k.lastIndexOf("_");
    if(idx < 0) return;
    const section = k.slice(0, idx);
    const mode = k.slice(idx + 1);

    // Tag groups
    if(["artist","quality","character","other"].includes(section)){
      const arr = currentDetail.tags?.[section] || [];
      const text = (mode === "keep") ? joinKeep(arr) : joinPlain(arr);
      copyText(text);
      return;
    }

    if(section === "negative"){
      ensureDetailPromptUI(currentDetail);
      const arr = (currentDetail._ui_uc_tags && Array.isArray(currentDetail._ui_uc_tags)) ? currentDetail._ui_uc_tags : [];
      const text = (mode === "keep") ? joinKeep(arr) : joinPlain(arr);
      copyText(text);
      return;
    }

    // Prompt sections
    ensureDetailPromptUI(currentDetail);

    if(section === "promptMain"){
      const src = currentDetail.prompt_positive_raw || "";
      const text = (mode === "keep") ? promptTextForCopyKeep(src) : promptTextForCopyPlain(src);
      copyText(text);
      return;
    }
    if(section === "negMain"){
      const arr = (currentDetail._ui_uc_tags && Array.isArray(currentDetail._ui_uc_tags)) ? currentDetail._ui_uc_tags : [];
      const text = (mode === "keep") ? joinKeep(arr) : joinPlain(arr);
      copyText(text);
      return;
    }

    // Character blocks: char{idx} / char{idx}Neg
    const cm = section.match(/^char(\d+)(Neg)?$/);
    if(cm){
      const ci = parseInt(cm[1], 10);
      const isNeg = !!cm[2];
      const entry = (currentDetail._ui_char_entries || [])[ci];
      const src = entry ? (isNeg ? (entry.neg || "") : (entry.pos || "")) : "";
      const text = (mode === "keep") ? promptTextForCopyKeep(src) : promptTextForCopyPlain(src);
      copyText(text);
    }
  });
}

/* =====================
   Tag chips / suggest
   ===================== */

let _tagSuggestPortalBound = false;

function ensureTagSuggestPortal(){
  const box = $("tagSuggest");
  if(!box) return null;
  if(box.dataset.portalReady === "1") return box;
  document.body.appendChild(box);
  box.dataset.portalReady = "1";
  bindTagSuggestPortalHandlers();
  return box;
}

function bindTagSuggestPortalHandlers(){
  if(_tagSuggestPortalBound) return;
  _tagSuggestPortalBound = true;
  window.addEventListener("scroll", () => positionTagSuggest(), true);
  window.addEventListener("resize", () => positionTagSuggest());
}

function positionTagSuggest(){
  const box = $("tagSuggest");
  if(!box || box.classList.contains("hidden")) return;
  const input = $("tagInput");
  if(!input) return;
  const bar = input.closest?.(".searchInput");
  if(!bar) return;
  const r = bar.getBoundingClientRect();
  const pad = 8;
  let w = Math.min(520, Math.max(220, r.width));
  let left = Math.min(Math.max(pad, r.left), window.innerWidth - w - pad);
  let top = r.bottom + 6;

  // If the box would go off-screen bottom, flip upwards when possible.
  const h = box.offsetHeight || 240;
  if(top + h > (window.innerHeight - pad)){
    const up = r.top - 6 - h;
    if(up > pad) top = up;
  }

  box.style.left = `${left}px`;
  box.style.top = `${top}px`;
  box.style.width = `${w}px`;
}

function hideTagSuggest(){
  const box = $("tagSuggest");
  if(!box) return;
  box.classList.add("hidden");
  box.innerHTML = "";
}


function updateChips(){
  const wrap = $("tagChips");
  if(!wrap) return;
  wrap.innerHTML = "";

  const addChip = (tag, kind) => {
    const chip = document.createElement("div");
    chip.className = `chip ${kind}`;
    const kindLabel = (kind === "exclude") ? "除" : "絞";
    chip.innerHTML = `<span class="kind">${kindLabel}</span><span>${escapeHtml(tag)}</span>`;
    const x = document.createElement("button");
    x.textContent = "×";
    x.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      if(kind === "exclude"){
        state.preview.tags_not = (state.preview.tags_not || []).filter(t => t !== tag);
      }else{
        state.preview.tags = (state.preview.tags || []).filter(t => t !== tag);
      }
      updateChips();
    });
    chip.appendChild(x);
    wrap.appendChild(chip);
  };

  (state.preview.tags || []).forEach(tag => addChip(tag, "include"));
  (state.preview.tags_not || []).forEach(tag => addChip(tag, "exclude"));
}

let suggestTimer = null;
async function onTagInput(){
  const q = $("tagInput").value.trim();
  const box = ensureTagSuggestPortal();
  if(suggestTimer) clearTimeout(suggestTimer);
  suggestTimer = setTimeout(async () => {
    if(!box) return;

    const close = () => {
      hideTagSuggest();
    };

    const addTag = (tag, mode) => {
      const t = String(tag || "").trim();
      if(!t) return;
      if(mode === "exclude"){
        // ensure mutually exclusive
        state.preview.tags = (state.preview.tags || []).filter(x => x !== t);
        if(!(state.preview.tags_not || []).includes(t)){
          state.preview.tags_not = (state.preview.tags_not || []).concat([t]);
        }
      }else{
        state.preview.tags_not = (state.preview.tags_not || []).filter(x => x !== t);
        if(!(state.preview.tags || []).includes(t)){
          state.preview.tags = (state.preview.tags || []).concat([t]);
        }
      }
      updateChips();
      $("tagInput").value = "";
      close();
    };

    if(!q){
      close();
      return;
    }

    let data = [];
    try{
      const res = await apiFetch(API.suggest(q));
      data = (await apiJson(res)) || [];
    }catch(_e){
      close();
      return;
    }

    box.innerHTML = "";
    if(!data.length){
      const empty = document.createElement("div");
      empty.className = "sItem";
      empty.style.cursor = "default";
      empty.style.opacity = "0.7";
      empty.textContent = "候補なし";
      box.appendChild(empty);
    }

    data.forEach((item) => {
      const tag = item && item.tag ? String(item.tag) : "";
      if(!tag) return;
      const c = Number(item.count || 0);

      const div = document.createElement("div");
      div.className = "sItem";
      div.innerHTML = `
        <div class="sMain">
          <div class="sTag">${escapeHtml(tag)}</div>
          <div class="sMeta"><span>${c.toLocaleString()}件</span></div>
        </div>
        <div class="sActions">
          <button type="button" class="sBtn include">絞り込み</button>
          <button type="button" class="sBtn exclude">除外</button>
        </div>
      `;

      // Row click == include
      div.addEventListener("click", (e) => {
        e.preventDefault();
        addTag(tag, "include");
      });

      const btnIn = div.querySelector(".sBtn.include");
      const btnEx = div.querySelector(".sBtn.exclude");
      btnIn?.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        addTag(tag, "include");
      });
      btnEx?.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        addTag(tag, "exclude");
      });

      box.appendChild(div);
    });

    box.classList.remove("hidden");
    positionTagSuggest();
  }, 200);
}

/* =====================
   Calendar
   ===================== */

function ymd(d){
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,"0");
  const dd = String(d.getDate()).padStart(2,"0");
  return `${y}-${m}-${dd}`;
}
function ym(d){
  const y = d.getFullYear();
  const m = String(d.getMonth()+1).padStart(2,"0");
  return `${y}-${m}`;
}
function startOfMonth(d){ return new Date(d.getFullYear(), d.getMonth(), 1); }
function endOfMonth(d){ return new Date(d.getFullYear(), d.getMonth()+1, 0); }

function fmtCount(n){
  const v = (typeof n === 'number' ? n : parseInt(n||0,10) || 0);
  // Calendar count label: number only (no suffix)
  return v.toLocaleString();
}

async function loadYearCounts(){
  const res = await apiFetch(API.yearCounts);
  const data = await apiJson(res);
  state.calendar.yearCounts = new Map((data||[]).map(x => [String(x.year), Number(x.count||0)]));
}

async function loadYearMonthCounts(year){
  const y = String(year||'');
  if(!y) return;
  const res = await apiFetch(API.monthCounts(y));
  const data = await apiJson(res);
  const items = (data && data.items) ? data.items : (data||[]);
  state.calendar.monthCounts = new Map((items||[]).map(x => [String(x.ym), Number(x.count||0)]));
  state.calendar.loadedYear = y;
}

async function ensureYearLoaded(year){
  const y = String(year);
  if(state.calendar.yearCounts.size === 0){
    await loadYearCounts();
  }
  if(state.calendar.loadedYear !== y){
    await loadYearMonthCounts(y);
  }
}

async function loadMonthCounts(monthDate){
  const m = ym(monthDate);
  await ensureYearLoaded(monthDate.getFullYear());
  const res = await apiFetch(API.dayCounts(m));
  const data = await apiJson(res);
  state.calendar.counts = new Map((data||[]).map(x => [x.ymd, x.count]));
}

function hasCalendarSelection(){
  return !!(state.calendar.from || state.calendar.to);
}

function hasSingleDayCalendarSelection(){
  const f = state.calendar.from || "";
  const t = state.calendar.to || "";
  return !!f && f === t;
}

function hasMultiDayCalendarSelection(){
  const f = state.calendar.from || "";
  const t = state.calendar.to || "";
  return !!f && !!t && f !== t;
}

function shouldFilterCalendarYearOrMonth(){
  return !hasCalendarSelection() || hasMultiDayCalendarSelection();
}

function hasPendingCalendarAnchor(){
  const a = state.calendar.click_anchor || "";
  return !!a && state.calendar.from === a && state.calendar.to === a;
}

function clearCalendarAnchor(){
  state.calendar.click_anchor = "";
  state.calendar.click_ts = 0;
  state.calendar.keep_anchor = false;
}

function preserveCalendarAnchorForNavigation(){
  if(hasPendingCalendarAnchor()){
    state.calendar.keep_anchor = true;
    return;
  }
  clearCalendarAnchor();
}

function isSelected(key){
  const f = state.calendar.from;
  const t = state.calendar.to;
  if(!f && !t) return false;
  if(f && !t) return key === f;
  if(f && t) return (key >= f && key <= t);
  return false;
}

function applyCalendarToInputs(){
  $("dateFrom").value = state.calendar.from || "";
  $("dateTo").value = state.calendar.to || "";
  state.preview.date_from = state.calendar.from || "";
  state.preview.date_to = state.calendar.to || "";
}

function updateCalendarSelectionUI(){
  document.querySelectorAll(".calCell[data-key]").forEach(cell => {
    const k = cell.dataset.key;
    cell.classList.toggle("selected", isSelected(k));
    cell.classList.toggle("edgeFrom", !!state.calendar.from && k === state.calendar.from);
    cell.classList.toggle("edgeTo", !!state.calendar.to && k === state.calendar.to);
  });
}

function handleCalendarClickSelect(key){
  // Two-click range selection (drag is still supported).
  const now = Date.now();
  const a = state.calendar.click_anchor || '';
  const ts = state.calendar.click_ts || 0;
  const keepAnchor = !!state.calendar.keep_anchor;

  // While navigating across month/year panels, keep the first clicked day armed.
  if(a && (keepAnchor || (now - ts) <= 5000)){
    clearCalendarAnchor();
    if(key < a){
      state.calendar.from = key;
      state.calendar.to = a;
    }else{
      state.calendar.from = a;
      state.calendar.to = key;
    }
    applyCalendarToInputs();
    updateCalendarSelectionUI();
    search().catch(_e=>{});
    return;
  }

  // First click (or expired): select a single day immediately and arm the anchor.
  state.calendar.keep_anchor = false;
  state.calendar.click_anchor = key;
  state.calendar.click_ts = now;
  state.calendar.from = key;
  state.calendar.to = key;
  applyCalendarToInputs();
  updateCalendarSelectionUI();
  search().catch(_e=>{});
}

function onCalendarPressMove(e){
  const pk = state.calendar.press_key;
  if(!pk) return;
  const dx = Math.abs((e.clientX||0) - (state.calendar.press_x||0));
  const dy = Math.abs((e.clientY||0) - (state.calendar.press_y||0));
  if(dx + dy < 8) return;
  // Start drag selection
  window.removeEventListener('pointermove', onCalendarPressMove);
  window.removeEventListener('pointerup', onCalendarPressUp);
  window.removeEventListener('pointercancel', onCalendarPressUp);
  state.calendar.press_key = '';
  beginDragSelect(pk);
  // Update to the current hovered cell immediately
  onCalendarDragMove(e);
}

function onCalendarPressUp(_e){
  const pk = state.calendar.press_key;
  window.removeEventListener('pointermove', onCalendarPressMove);
  window.removeEventListener('pointerup', onCalendarPressUp);
  window.removeEventListener('pointercancel', onCalendarPressUp);
  state.calendar.press_key = '';
  if(!pk) return;
  handleCalendarClickSelect(pk);
}



function calendarLockScroll(){
  if(state.calendar._scrollLock) return;
  const grid = document.querySelector("#calendar .calGrid");
  const sc = grid?.closest?.(".filterOverlayBody");
  if(!sc) return;
  state.calendar._scrollLock = {
    el: sc,
    overflow: sc.style.overflow,
    overscroll: sc.style.overscrollBehavior,
  };
  sc.style.overflow = "hidden";
  sc.style.overscrollBehavior = "contain";
}

function calendarUnlockScroll(){
  const lock = state.calendar._scrollLock;
  if(!lock || !lock.el) return;
  lock.el.style.overflow = lock.overflow || "";
  lock.el.style.overscrollBehavior = lock.overscroll || "";
  state.calendar._scrollLock = null;
}

function onCalendarTouchStart(e, key){
  // Mobile (touch) uses Touch Events to reliably stop scroll while selecting.
  if(!e.touches || e.touches.length !== 1) return;
  const t = e.touches[0];
  calendarLockScroll();
  state.calendar.touch_key = key;
  state.calendar.touch_x = t.clientX;
  state.calendar.touch_y = t.clientY;

  window.addEventListener("touchmove", onCalendarTouchMove, { passive: false });
  window.addEventListener("touchend", onCalendarTouchEnd, { passive: true });
  window.addEventListener("touchcancel", onCalendarTouchEnd, { passive: true });
}

function onCalendarTouchMove(e){
  const pk = state.calendar.touch_key;
  if(!pk) return;
  const t = (e.touches && e.touches[0]) || null;
  if(!t) return;

  // stop overlay scroll immediately (iOS Safari will steal the gesture otherwise)
  e.preventDefault();

  const dx = Math.abs(t.clientX - (state.calendar.touch_x || 0));
  const dy = Math.abs(t.clientY - (state.calendar.touch_y || 0));

  if(!state.calendar.dragging){
    if(dx + dy < 8) return;
    state.calendar.touch_key = "";
    beginDragSelect(pk);
  }

  // While dragging: prevent overlay scroll and keep updating range.
  e.preventDefault();
  onCalendarDragMove({ clientX: t.clientX, clientY: t.clientY });
}

function onCalendarTouchEnd(_e){
  window.removeEventListener("touchmove", onCalendarTouchMove);
  window.removeEventListener("touchend", onCalendarTouchEnd);
  window.removeEventListener("touchcancel", onCalendarTouchEnd);

  calendarUnlockScroll();

  const pk = state.calendar.touch_key;
  state.calendar.touch_key = "";

  if(state.calendar.dragging){
    endDragSelect();
    return;
  }
  if(pk) handleCalendarClickSelect(pk);
}
function beginDragSelect(key){
  clearCalendarAnchor();
  state.calendar.dragging = true;
  state.calendar.drag_anchor = key;
  state.calendar.from = key;
  state.calendar.to = key;
  calendarLockScroll();
  applyCalendarToInputs();
  updateCalendarSelectionUI();

  // Track pointer globally so drag works even when leaving a cell.
  window.addEventListener("pointermove", onCalendarDragMove, { passive: false });
  window.addEventListener("pointerup", endDragSelect, { passive: false });
  window.addEventListener("pointercancel", endDragSelect, { passive: false });
}

function updateDragSelect(key){
  if(!state.calendar.dragging) return;
  const a = state.calendar.drag_anchor || key;
  if(key < a){
    state.calendar.from = key;
    state.calendar.to = a;
  }else{
    state.calendar.from = a;
    state.calendar.to = key;
  }
  applyCalendarToInputs();
  updateCalendarSelectionUI();
}

function endDragSelect(){
  if(!state.calendar.dragging) return;
  state.calendar.dragging = false;
  state.calendar.drag_anchor = "";

  window.removeEventListener("pointermove", onCalendarDragMove);
  window.removeEventListener("pointerup", endDragSelect);
  window.removeEventListener("pointercancel", endDragSelect);

  calendarUnlockScroll();

  // Fire and forget (do not block UI on slow disks).
  search().catch(_e => {});
}

function onCalendarDragMove(e){
  if(!state.calendar.dragging) return;
  const el = document.elementFromPoint(e.clientX, e.clientY);
  const cell = el?.closest?.(".calCell[data-key]");
  const key = cell?.dataset?.key;
  if(key) updateDragSelect(key);
}

function clearCalendar(){
  state.calendar.from = "";
  state.calendar.to = "";
  clearCalendarAnchor();
  $("dateFrom").value = "";
  $("dateTo").value = "";
  renderCalendar();
  state.preview.date_from = "";
  state.preview.date_to = "";
  search().catch(_e => {});
}

function formatCalendarRangeLabel(from, to){
  if(!from && !to) return { val: "—", sum: 0, status: "未選択" };
  if(from && (!to || from === to)){
    const d = parseInt(String(from).slice(-2), 10);
    return {
      val: String(d).padStart(2, "0"),
      sum: Number(state.calendar.counts.get(from) || 0),
      status: "選択",
    };
  }

  const sameMonth = String(from).slice(0, 7) === String(to).slice(0, 7);
  const sameYear = String(from).slice(0, 4) === String(to).slice(0, 4);

  let val = `${String(from).slice(8, 10)}–${String(to).slice(8, 10)}`;
  if(!sameMonth){
    if(sameYear){
      val = `${String(from).slice(5, 7)}/${String(from).slice(8, 10)}–${String(to).slice(5, 7)}/${String(to).slice(8, 10)}`;
    }else{
      val = `${String(from).slice(2, 4)}/${String(from).slice(5, 7)}–${String(to).slice(2, 4)}/${String(to).slice(5, 7)}`;
    }
  }

  if(!sameMonth){
    return { val, sum: 0, status: "範囲" };
  }

  let sum = 0;
  for(const [k, v] of state.calendar.counts.entries()){
    if(k >= from && k <= to) sum += Number(v || 0);
  }
  return { val, sum, status: "範囲" };
}

function setCalendarSelectionRange(from, to){
  state.calendar.from = from;
  state.calendar.to = to;
  applyCalendarToInputs();
}

async function openCalendarMonthView(nextMonth, zone){
  preserveCalendarAnchorForNavigation();
  state.calendar.month = new Date(nextMonth.getFullYear(), nextMonth.getMonth(), 1);
  await loadMonthCounts(state.calendar.month);
  if(zone) state.calendar.zone = zone;
  renderCalendar();
}

async function selectCalendarYear(year, monthIndex){
  clearCalendarAnchor();
  state.calendar.month = new Date(year, monthIndex, 1);
  setCalendarSelectionRange(`${year}-01-01`, `${year}-12-31`);
  await loadMonthCounts(state.calendar.month);
  state.calendar.zone = "mo";
  renderCalendar();
  search().catch(_e => {});
}

async function openCalendarYear(year, monthIndex){
  await openCalendarMonthView(new Date(year, monthIndex, 1), "mo");
}

async function selectCalendarMonth(year, monthIndex){
  clearCalendarAnchor();
  const monthDate = new Date(year, monthIndex, 1);
  const monthKey = ym(monthDate);
  const lastDay = endOfMonth(monthDate).getDate();
  state.calendar.month = monthDate;
  setCalendarSelectionRange(`${monthKey}-01`, `${monthKey}-${String(lastDay).padStart(2, "0")}`);
  await loadMonthCounts(state.calendar.month);
  state.calendar.zone = "dy";
  renderCalendar();
  search().catch(_e => {});
}

async function openCalendarMonth(year, monthIndex){
  await openCalendarMonthView(new Date(year, monthIndex, 1), "dy");
}

function renderCalendar(){
  const cal = $("calendar");
  const md = state.calendar.month;
  cal.innerHTML = "";

  // State for new 3-zone calendar UI
  if(!state.calendar.zone) state.calendar.zone = 'dy';
  if(typeof state.calendar.yrBase !== 'number') state.calendar.yrBase = md.getFullYear() - 6;

  const y = md.getFullYear();
  const mm = String(md.getMonth()+1).padStart(2,'0');

  const wrap = document.createElement('div');
  wrap.className = 'cal3';

  // --- top zones ---
  const zones = document.createElement('div');
  zones.className = 'calZones';
  const yearCount = Number(state.calendar.yearCounts.get(String(y)) || 0);
  const monthCount = Number(state.calendar.monthCounts.get(`${y}-${mm}`) || 0);

  const rangeLabel = formatCalendarRangeLabel(state.calendar.from, state.calendar.to);

  const mkZone = (id, lbl, val, sub, active) => {
    const z = document.createElement('button');
    z.type = 'button';
    z.className = 'calZone' + (active ? ' active' : '');
    z.dataset.zone = id;
    z.innerHTML = `
      <div class="czLbl">${lbl}</div>
      <div class="czVal">${escapeHtml(String(val))}</div>
      <div class="czSub">${escapeHtml(String(sub||''))}</div>
    `;
    z.addEventListener('click', (e) => {
      e.preventDefault();
      state.calendar.zone = id;
      renderCalendar();
    });
    return z;
  };

  zones.appendChild(mkZone('yr','YEAR', y, fmtCount(yearCount), state.calendar.zone==='yr'));
  zones.appendChild(document.createElement('div')).className = 'czDiv';
  zones.appendChild(mkZone('mo','MONTH', mm, fmtCount(monthCount), state.calendar.zone==='mo'));
  zones.appendChild(document.createElement('div')).className = 'czDiv';
  zones.appendChild(mkZone('dy','DAY', rangeLabel.val, (rangeLabel.sum ? fmtCount(rangeLabel.sum) : ''), state.calendar.zone==='dy'));
  wrap.appendChild(zones);

  // --- panels ---
  const panels = document.createElement('div');
  panels.className = 'calPanels';
  wrap.appendChild(panels);

  const pYr = document.createElement('div');
  pYr.className = 'calPanel' + (state.calendar.zone==='yr' ? ' active' : '');
  const pMo = document.createElement('div');
  pMo.className = 'calPanel' + (state.calendar.zone==='mo' ? ' active' : '');
  const pDy = document.createElement('div');
  pDy.className = 'calPanel' + (state.calendar.zone==='dy' ? ' active' : '');
  panels.appendChild(pYr);
  panels.appendChild(pMo);
  panels.appendChild(pDy);

  // Year panel
  const yrGrid = document.createElement('div');
  yrGrid.className = 'calYrGrid';
  const yrs = Array.from({length:12}, (_,i)=> state.calendar.yrBase + i);
  yrs.forEach((yy) => {
    const c = Number(state.calendar.yearCounts.get(String(yy)) || 0);
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'calYrCell' + (yy===y ? ' sel' : '');
    b.innerHTML = `<span class="t">${yy}</span><span class="n">${fmtCount(c)}</span>`;
    b.addEventListener('click', async (e) => {
      e.preventDefault();
      if(!shouldFilterCalendarYearOrMonth()){
        await openCalendarYear(yy, md.getMonth());
        return;
      }
      await selectCalendarYear(yy, md.getMonth());
    });
    yrGrid.appendChild(b);
  });
  const yrNav = document.createElement('div');
  yrNav.className = 'calYrNav';
  yrNav.innerHTML = `
    <button type="button" class="calNav" data-d="-8" aria-label="prev years">◀◀</button>
    <div class="calYrRange">${yrs[0]}–${yrs[yrs.length-1]}</div>
    <button type="button" class="calNav" data-d="8" aria-label="next years">▶▶</button>
  `;
  yrNav.querySelectorAll('.calNav').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.preventDefault();
      const d = parseInt(btn.dataset.d||'0',10) || 0;
      state.calendar.yrBase += d;
      renderCalendar();
    });
  });
  pYr.appendChild(yrGrid);
  pYr.appendChild(yrNav);

  // Month panel
  const moGrid = document.createElement('div');
  moGrid.className = 'calMoGrid';
  for(let mi=1; mi<=12; mi++){
    const mm2 = String(mi).padStart(2,'0');
    const key = `${y}-${mm2}`;
    const c = Number(state.calendar.monthCounts.get(key) || 0);
    const b = document.createElement('button');
    b.type='button';
    b.className = 'calMoCell' + (mm2===mm ? ' sel' : '');
    b.innerHTML = `<span class="t">${mi}</span><span class="n">${fmtCount(c)}</span>`;
    b.addEventListener('click', async (e) => {
      e.preventDefault();
      if(!shouldFilterCalendarYearOrMonth()){
        await openCalendarMonth(y, mi - 1);
        return;
      }
      await selectCalendarMonth(y, mi - 1);
    });
    moGrid.appendChild(b);
  }
  pMo.appendChild(moGrid);

  // Day panel
  const dyHead = document.createElement('div');
  dyHead.className = 'calDyHead';
  dyHead.innerHTML = `
    <div class="calDyTitle">${y}年 ${md.getMonth()+1}月</div>
    <div class="calDyNavs">
      <button type="button" class="calNav" id="calPrev" aria-label="prev">◀</button>
      <button type="button" class="calNav" id="calNext" aria-label="next">▶</button>
    </div>
  `;
  pDy.appendChild(dyHead);

  const dowRow = document.createElement('div');
  dowRow.className = 'calDow';
  ['月','火','水','木','金','土','日'].forEach((x, i) => {
    const s = document.createElement('div');
    s.className = 'calDowCell' + (i===5 ? ' sat' : i===6 ? ' sun' : '');
    s.textContent = x;
    dowRow.appendChild(s);
  });
  pDy.appendChild(dowRow);

  const grid = document.createElement("div");
  grid.className = "calGrid";

  const first = startOfMonth(md);
  const last = endOfMonth(md);
  // Monday-start layout (0..6)
  const startDow = (first.getDay() + 6) % 7;

  for(let i=0;i<startDow;i++){
    const cell = document.createElement("div");
    cell.className = "calCell other";
    cell.innerHTML = "";
    grid.appendChild(cell);
  }

  for(let day=1; day<=last.getDate(); day++){
    const dt = new Date(md.getFullYear(), md.getMonth(), day);
    const key = ymd(dt);
    const count = state.calendar.counts.get(key) || 0;

    const cell = document.createElement("div");
    cell.className = "calCell" + (isSelected(key) ? " selected" : "");
    if(count) cell.classList.add('has');
    const dow = dt.getDay();
    if(dow === 6) cell.classList.add('sat');
    if(dow === 0) cell.classList.add('sun');
    if(state.calendar.from && key === state.calendar.from) cell.classList.add("edgeFrom");
    if(state.calendar.to && key === state.calendar.to) cell.classList.add("edgeTo");
    cell.dataset.key = key;
    cell.innerHTML = `<div class="d">${day}</div><div class="c">${count ? String(count) : ""}</div>`;

    // click -> drag selection
    cell.addEventListener("pointerdown", (e) => {
      // Use pointer events for mouse/pen. Touch uses Touch Events (see touchstart below).
      if(e.pointerType === "touch") return;
      e.preventDefault();
      try{ e.currentTarget.setPointerCapture(e.pointerId); }catch(_e){}
      // Press -> drag (threshold) OR click (two-click range)
      state.calendar.press_key = key;
      state.calendar.press_x = e.clientX || 0;
      state.calendar.press_y = e.clientY || 0;
      window.addEventListener('pointermove', onCalendarPressMove, { passive: false });
      window.addEventListener('pointerup', onCalendarPressUp, { passive: false });
      window.addEventListener('pointercancel', onCalendarPressUp, { passive: false });
    });

    cell.addEventListener("touchstart", (e) => {
      onCalendarTouchStart(e, key);
    }, { passive: true });
    grid.appendChild(cell);
  }

  // Fill trailing placeholders so the grid keeps a stable 6x7 layout.
  const total = startDow + last.getDate();
  for(let i=total; i<42; i++){
    const cell = document.createElement("div");
    cell.className = "calCell other";
    grid.appendChild(cell);
  }

  pDy.appendChild(grid);

  // footer
  const foot = document.createElement('div');
  foot.className = 'calFoot';
  const f = state.calendar.from;
  const t = state.calendar.to;
  const txt = (!f && !t) ? '未選択' : (f && (!t || f===t)) ? f : `${f} 〜 ${t}`;
  foot.innerHTML = `
    <div class="calSel">${escapeHtml(txt)}</div>
    <div class="calFootBtns">
      <button type="button" class="calFootBtn" id="calToday">今日</button>
      <button type="button" class="calFootBtn" id="calClear">クリア</button>
    </div>
  `;
  pDy.appendChild(foot);

  cal.appendChild(wrap);

  // wire nav
  $("calPrev").onclick = async () => {
    await openCalendarMonthView(new Date(md.getFullYear(), md.getMonth() - 1, 1), 'dy');
  };
  $("calNext").onclick = async () => {
    await openCalendarMonthView(new Date(md.getFullYear(), md.getMonth() + 1, 1), 'dy');
  };
  $("calClear").onclick = () => clearCalendar();
  $("calToday").onclick = async () => {
    const now = new Date();
    await openCalendarMonthView(new Date(now.getFullYear(), now.getMonth(), 1), 'dy');
  };
}



/* =====================
   Mobile filter overlay
   ===================== */

let _filterHome = null;
let _filterHomeNext = null;
let _sidebarHome = null;
let _sidebarHomeNext = null;

function openFilterOverlay(){
  if(!isMobile()) return;
  const ov = $("filterOverlay");
  const body = $("filterOverlayBody");
  if(!ov || !body) return;

  const toolCard = document.querySelector("#viewPreview .toolCard");
  const sidebar = document.querySelector("#viewPreview .sidebar");
  if(!toolCard || !sidebar) return;

  if(!_filterHome){
    _filterHome = toolCard.parentElement;
    _filterHomeNext = toolCard.nextSibling;
  }
  if(!_sidebarHome){
    _sidebarHome = sidebar.parentElement;
    _sidebarHomeNext = sidebar.nextSibling;
  }

  body.appendChild(toolCard);
  body.appendChild(sidebar);

  ov.classList.remove("hidden");
  ov.setAttribute("aria-hidden", "false");
  document.body.classList.add("noScroll");
}

function closeFilterOverlay(){
  const ov = $("filterOverlay");
  if(!ov) return;

  const toolCard = document.querySelector("#filterOverlayBody .toolCard");
  const sidebar = document.querySelector("#filterOverlayBody .sidebar");

  if(toolCard && _filterHome){
    _filterHome.insertBefore(toolCard, _filterHomeNext);
  }
  if(sidebar && _sidebarHome){
    _sidebarHome.insertBefore(sidebar, _sidebarHomeNext);
  }

  ov.classList.add("hidden");
  ov.setAttribute("aria-hidden", "true");
  document.body.classList.remove("noScroll");
}

/* =====================
   Init
   ===================== */

async function initPreview(){
  if(_previewInited) return;
  await refreshFacets();
  setCreatorFilter($("filterCreator").value);
  setSoftwareFilter($("filterSoftware").value);
  state.preview.date_from = $("dateFrom").value;
  state.preview.date_to = $("dateTo").value;

  await loadYearCounts();
  await loadYearMonthCounts(state.calendar.month.getFullYear());
  await loadMonthCounts(state.calendar.month);
  renderCalendar();

  // restore controls
  const sortSel = $("sortBy");
  if(sortSel) sortSel.value = state.preview.sort || "newest";
  setGalleryView(state.preview.view || "grid", true);
  if(!isDesktop()) setGalleryView("grid", true);
  syncGalleryControls();
  await search();

  _previewInited = true;
}

function bindUI(){
  $("navUpload").addEventListener("click", () => setView("upload"));
  $("navPreview").addEventListener("click", async () => {
    setView("preview");
    if(!_previewInited){
      await initPreview();
      _previewInited = true;
    }
  });

  bindMobileSwipe();


  bindDropZone();
  $("uploadBookmarkToggle")?.addEventListener("click", () => {
    state.uploadBookmark.enabled = !state.uploadBookmark.enabled;
    if(state.uploadBookmark.enabled && !Number(state.uploadBookmark.list_id || 0)){
      state.uploadBookmark.list_id = _defaultUploadBookmarkListId();
    }
    syncUploadBookmarkControls();
  });
  $("uploadBookmarkList")?.addEventListener("change", (e) => {
    state.uploadBookmark.list_id = Number(e?.target?.value || 0);
    syncUploadBookmarkControls();
  });
  syncUploadBookmarkControls();
  // stopUploadBtn was removed from UI (uploads are auto-processed).

  $("searchBtn").addEventListener("click", async () => {
    setCreatorFilter($("filterCreator").value);
    setSoftwareFilter($("filterSoftware").value);
    state.preview.date_from = $("dateFrom").value;
    state.preview.date_to = $("dateTo").value;
    await search();
  });

  // bookmark filter clear (sidebar)
  $("bookmarkClearBtn")?.addEventListener("click", async (e) => {
    e.preventDefault();
    await clearBookmarkFilter();
  });

  $("creatorClearBtn")?.addEventListener("click", async (e) => {
    e.preventDefault();
    await clearCreatorFilter();
  });

  $("softwareClearBtn")?.addEventListener("click", async (e) => {
    e.preventDefault();
    await clearSoftwareFilter();
  });

  $("tagSearchBtn").addEventListener("click", async () => {
    await search();
  });

  $("tagInput").addEventListener("input", onTagInput);
  document.addEventListener("click", (e) => {
    const box = ensureTagSuggestPortal();
	    const bar = $("tagInput")?.closest?.(".searchInput");
	    if((bar && bar.contains(e.target)) || (box && box.contains(e.target))) return;
	    hideTagSuggest();
  });

  $("dedupToggle").addEventListener("click", async () => {
    state.preview.dedup_only = state.preview.dedup_only ? 0 : 1;
    $("dedupToggle").textContent = state.preview.dedup_only ? "ON" : "OFF";
    $("dedupToggle").classList.toggle("on", !!state.preview.dedup_only);
    $("dedupToggle").classList.toggle("off", !state.preview.dedup_only);
    await search();
  });

  // sort
  $("sortBy")?.addEventListener("change", async () => {
    state.preview.sort = $("sortBy").value || "newest";
    localStorage.setItem("gallery_sort", state.preview.sort);
    await search();
  });



  // view mode
  $("viewGrid")?.addEventListener("click", () => setGalleryView("grid"));
  $("viewList")?.addEventListener("click", () => setGalleryView("list"));

  // Bulk selection / delete (preview)
  $("pageSelectBtn")?.addEventListener("click", (e) => { e.preventDefault(); _togglePageSelect(); });
  $("allSelectBtn")?.addEventListener("click", (e) => { e.preventDefault(); _toggleAllSelect(); });
  $("bulkBookmarkBtn")?.addEventListener("click", async (e) => { e.preventDefault(); await openBulkBookmarkOverlay(); });
  $("bulkDeleteBtn")?.addEventListener("click", async (e) => { e.preventDefault(); await _bulkDelete(); });

  // Detail overlay close (be robust against partial merges / timing issues)
  document.querySelectorAll('#overlay button[aria-label="閉じる"]').forEach((btn) => {
    btn.addEventListener("click", closeDetail);
  });
  const bg = $("overlayBg") || document.querySelector("#overlay .overlayBg") || document.querySelector(".overlayBg");
  bg?.addEventListener("click", closeDetail);
  // Prevent touch-scrolling on the dark backdrop.
  bg?.addEventListener("touchmove", (e) => e.preventDefault(), { passive: false });

  // iOS/Safari: stop scroll-chaining (when the slide can't scroll anymore, don't scroll the background).
  const slideEl = $("slide");
  if(slideEl){
    let startY = 0;
    slideEl.addEventListener("touchstart", (e) => {
      if(e.touches && e.touches.length) startY = e.touches[0].clientY;
    }, { passive: true });
    slideEl.addEventListener("touchmove", (e) => {
      if(!(e.touches && e.touches.length)) return;
      const y = e.touches[0].clientY;
      const dy = y - startY;
      const atTop = slideEl.scrollTop <= 0;
      const atBottom = (slideEl.scrollTop + slideEl.clientHeight) >= (slideEl.scrollHeight - 1);
      if((atTop && dy > 0) || (atBottom && dy < 0)){
        e.preventDefault();
      }
    }, { passive: false });
  }


  // mobile: filename is one-line by default; tap to expand/collapse
  $("dTitle")?.addEventListener("click", () => {
    $("dTitle")?.classList.toggle("expanded");
  });

  $("imgOverlayInfo")?.addEventListener("click", () => {
    $("imgOverlayInfo")?.classList.toggle("expanded");
  });

  // mobile filter overlay
  const mfb = $("mobileFilterBtn");
  mfb?.addEventListener("click", (e) => { e.preventDefault(); openFilterOverlay(); });
  $("filterOverlayBg")?.addEventListener("click", closeFilterOverlay);
  $("filterOverlayClose")?.addEventListener("click", closeFilterOverlay);


// bookmark overlay
$("bmOverlayBg")?.addEventListener("click", closeBookmarkOverlay);
$("bmOverlayClose")?.addEventListener("click", closeBookmarkOverlay);
$("bulkBmOverlayBg")?.addEventListener("click", closeBulkBookmarkOverlay);
$("bulkBmOverlayClose")?.addEventListener("click", closeBulkBookmarkOverlay);
$("bulkBmCancelBtn")?.addEventListener("click", (e) => { e.preventDefault(); closeBulkBookmarkOverlay(); });
$("bulkBmCreateListBtn")?.addEventListener("click", async (e) => {
  e.preventDefault();
  try{
    const data = await _createBookmarkListInteractive();
    if(data?.lists) _syncBulkBookmarkListsMeta(data.lists);
  }catch(_e){
    alert("作成に失敗しました");
  }
});
$("bulkBmSaveBtn")?.addEventListener("click", async (e) => { e.preventDefault(); await saveBulkBookmarkOverlay(); });

// creator / bookmark creator picker
bindUserPick();
$("creatorAddBtn")?.addEventListener("click", (e) => { e.preventDefault(); openUserPick("creators"); });
$("bookmarkCreatorAddBtn")?.addEventListener("click", (e) => { e.preventDefault(); openUserPick("bookmarks"); });
$("bookmarkListAddBtn")?.addEventListener("click", async (e) => {
  e.preventDefault();
  try{
    await _createBookmarkListInteractive();
    await refreshBookmarkLists();
  }catch(_e){
    alert("作成に失敗しました");
  }
});
$("addCreatorFromDetailBtn")?.addEventListener("click", (e) => { e.preventDefault(); addCreatorFromDetail(); });

$("bmCreateListBtn")?.addEventListener("click", async (e) => {
  e.preventDefault();
  try{
    const data = await _createBookmarkListInteractive();
    if(data && _bmOverlayImageId) await openBookmarkOverlay(_bmOverlayImageId);
  }catch(_e){
    alert("作成に失敗しました");
  }
});

$("bmClearThisBtn")?.addEventListener("click", async (e) => {
  e.preventDefault();
  const iid = Number(_bmOverlayImageId || 0);
  if(!iid) return;
  try{
    const r = await apiFetch(withBookmarkContext(API.bookmarkClear(iid)), { method: "POST" });
    const j = await apiJson(r);
    _applyFavoriteState(iid, Number(j.favorite || 0));
    await refreshBookmarkLists();
    await openBookmarkOverlay(iid);
    if(state.preview.sort === "favorite" || state.preview.bm_any || state.preview.bm_list_id){
      await search();
    }
  }catch(_e){
    alert("解除に失敗しました");
  }
});


  $("favDetailBtn")?.addEventListener("click", async (e) => {
    e.preventDefault();
    if(!currentDetail) return;
    await onBookmarkButtonClick(currentDetail.id);
  });
  $("favDetailBtnFixed")?.addEventListener("click", async (e) => {
    e.preventDefault();
    if(!currentDetail) return;
    await onBookmarkButtonClick(currentDetail.id);
  });

  bindCopyAll();
  bindDetailToggleControls();
  syncDetailToggleUi();
}

async function boot(){
  bindUI();
  await ensureThumbPreferredFormat();
  // Ensure overlay uses animated open/close (do not rely on display:none).
  const _ov = $("overlay");
  if(_ov){
    _ov.classList.remove("hidden");
    _ov.classList.remove("open");
    _ov.setAttribute("aria-hidden", "true");
  }
  try{
    await loadMe();
  }catch(e){
    location.replace("/login.html");
    return;
  }

  const sp = new URLSearchParams(location.search || "");
  const viewParam = (sp.get("view") || "").toLowerCase();
  let view = "preview";
  // Default to preview even if a stale hash remains.
  if(viewParam === "upload" || viewParam === "preview") view = viewParam;

  setView(view, { pushState: false });
  if(view === "preview"){
    await initPreview();
  }else{
    await refreshFacets();
  }
}

// Back/forward: follow hash view without full reload.
window.addEventListener("popstate", () => {
  const hashView = (location.hash || "").replace("#", "").toLowerCase();
  const view = (hashView === "upload" || hashView === "preview") ? hashView : "preview";
  setView(view, { pushState: false });
  if(view === "preview" && !_previewInited){
    initPreview().catch(()=>{});
  }
});

// Switch between desktop(page) and mobile(scroll) when resizing.
let _resizeTimer = null;
window.addEventListener("resize", () => {
  if(!isMobile()) closeFilterOverlay();
  if(_resizeTimer) clearTimeout(_resizeTimer);
  _resizeTimer = setTimeout(async () => {
    const previewVisible = (_activeView === "preview");
    if(!previewVisible) return;
    const nextMode = isDesktop() ? "page" : "scroll";
    if(nextMode === state.preview.mode) return;
    // Re-run search in the new mode (keeps current filters).
    await search(1);
  }, 160);
});

updateViewportHeightVar();
window.addEventListener("resize", updateViewportHeightVar, { passive: true });
window.addEventListener("orientationchange", updateViewportHeightVar, { passive: true });
if(window.visualViewport){
  window.visualViewport.addEventListener("resize", updateViewportHeightVar, { passive: true });
  window.visualViewport.addEventListener("scroll", updateViewportHeightVar, { passive: true });
}

// Ensure DOM exists before binding (module scripts can still run before all nodes in some merges)
if(document.readyState === 'loading'){
  document.addEventListener('DOMContentLoaded', () => boot());
}else{
  boot();
}

