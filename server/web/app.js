const API = {
  login: "/api/auth/login",
  logout: "/api/auth/logout",
  pwLink: "/api/auth/password_link",
  me: "/api/me",
  upload: "/api/upload",
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
  favorite: (id) => `/api/images/${id}/favorite`,
  bulkDelete: "/api/images/bulk_delete",
  creators: "/api/stats/creators",
  software: "/api/stats/software",
  dayCounts: (month) => `/api/stats/day_counts?month=${encodeURIComponent(month)}`,
  monthCounts: (year) => `/api/stats/month_counts?year=${encodeURIComponent(year)}`,
  yearCounts: `/api/stats/year_counts`,
  suggest: (q) => `/api/tags/suggest?q=${encodeURIComponent(q)}&limit=24`,
};

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
  preview: {
    items: [],
    tags: [],
    dedup_only: 0,
    creator: "",
    software: "",
    date_from: "",
    date_to: "",
    fav_only: 0,
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
    picker: "",             // '' | 'year' | 'month'
  },
  selectedTileId: null,
};

let scrollObserver = null;
let _autoFillBurst = 0;

// Very small client-side caches to reduce perceived latency.
// (Do not persist to disk; keep it purely in-memory.)
const _pageCache = new Map(); // key -> { promise, data }
let _scrollPrefetch = null;   // { key, promise, data }
const _overlayWarmSet = new Set();

function invalidatePreviewCaches(){
  // Used when the underlying dataset changes (e.g. bulk delete) but the search key stays the same.
  _pageCache.clear();
  _scrollPrefetch = null;
  _overlayWarmSet.clear();
  // Detail cache can contain deleted ids; keep it simple.
  _detailCache.clear();
  _detailInFlight.clear();
}
let _previewSearchKey = "";

function isDesktop(){
  // Match CSS breakpoint.
  return (window.innerWidth || 0) > 980;
}

function isMobile(){
  return (window.innerWidth || 0) <= 720;
}

function $(id){ return document.getElementById(id); }

let _activeView = "preview";
let _previewInited = false;

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
    if(ov && !ov.classList.contains("hidden")) return true;
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

async function apiFetch(url, opts={}){
  const o = { credentials: "include", ...opts };
  o.headers = o.headers || {};
  const res = await fetch(url, o);
  if(res.status === 401){
    location.replace("/login.html");
    throw new Error("unauthorized");
  }
  return res;
}

function xhrPostForm(url, formData, onProgress){
  // Use XHR so we can show upload progress (fetch doesn't expose upload progress).
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", url, true);
    xhr.withCredentials = true;
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
      const text = xhr.responseText || "";
      if(xhr.status < 200 || xhr.status >= 300){
        reject(new Error(`${xhr.status} ${(text||"").slice(0,140)}`));
        return;
      }
      if(!text){ resolve(null); return; }
      try{ resolve(JSON.parse(text)); }
      catch(_e){ reject(new Error(`bad json: ${(text||"").slice(0,140)}`)); }
    };
    xhr.send(formData);
  });
}

async function apiJson(res){
  const text = await res.text();
  if(!res.ok){
    // API may return plain text (e.g. Internal Server Error). Avoid JSON.parse crash.
    const head = (text || "").slice(0, 140);
    throw new Error(`${res.status} ${head}`);
  }
  if(!text) return null;
  try{
    return JSON.parse(text);
  }catch(_e){
    const head = (text || "").slice(0, 140);
    throw new Error(`bad json: ${head}`);
  }
}

async function loadMe(){
  const res = await apiFetch(API.me);
  const me = await apiJson(res);
  state.user = me;
  $("meLabel").textContent = `${me.username} (${me.role})`;
  const isAdmin = (me.role === "admin" || me.role === "master");
  const menuAdmin = $("menuAdmin");
  if(menuAdmin) menuAdmin.classList.toggle("hidden", !isAdmin);
  const menuMaintenance = $("menuMaintenance");
  if(menuMaintenance) menuMaintenance.classList.toggle("hidden", !isAdmin);
}

async function doLogout(){
  try{
    await fetch(API.logout, { method: "POST", credentials: "include" });
  }catch(_e){}
  location.replace("/login.html");
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

function escapeHtml(s){
  return (s||"")
    .replaceAll("&","&amp;")
    .replaceAll("<","&lt;")
    .replaceAll(">","&gt;")
    .replaceAll('"',"&quot;");
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
  bump(state.uploadSummary.creators, detail.creator || "(unknown)", 1);
  bump(state.uploadSummary.software, detail.software || "(unknown)", 1);
  const groups = detail.tags || {};
  ["artist","quality","character","other"].forEach(g => {
    (groups[g] || []).forEach(t => bump(state.uploadSummary.tags, t.canonical, 1));
  });
}

function renderUploadSummary(){
  const box = $("uploadSummary");
  if(!box) return;

  const sortMap = (m) => Array.from(m.entries()).sort((a,b)=> (b[1]-a[1]) || String(a[0]).localeCompare(String(b[0])));
  const creators = sortMap(state.uploadSummary.creators);
  const softwares = sortMap(state.uploadSummary.software);
  const tags = sortMap(state.uploadSummary.tags).slice(0, 18);

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
  box.appendChild(renderList("制作者", creators));
  box.appendChild(renderList("ソフト", softwares));
  box.appendChild(renderList("Tag", tags));
}

function pickTopTags(detail, limit=6){
  const out = [];
  const groups = detail?.tags || {};
  ["artist","quality","character","other"].forEach(g => {
    (groups[g] || []).forEach(t => {
      if(out.length >= limit) return;
      out.push(t.canonical);
    });
  });
  return out;
}

function uploadProgressUpdate(){
  const total = (state.uploadQueue || []).length;
  let ok = 0, ng = 0, dup = 0, doing = 0;
  (state.uploadQueue || []).forEach(it => {
    if(!it) return;
    if(it.state === "完了") ok++;
    else if(it.state === "失敗") ng++;
    else if(it.state === "重複") dup++;
    else if(it.state === "アップロード中") doing++;
  });
  const done = ok + ng + dup;
  const text = total ? `完了 ${done}/${total}（成功 ${ok} / 重複 ${dup} / 失敗 ${ng}${doing ? ` / 送信中 ${doing}` : ""}）` : "";
  const elText = $("uploadProgressText");
  if(elText) elText.textContent = text;
  const fill = $("uploadProgressBar");
  if(fill){
    const pct = total ? Math.min(100, Math.max(0, (done / total) * 100)) : 0;
    fill.style.width = `${pct.toFixed(1)}%`;
  }
}

function _createUploadListItemElement(it){
  const div = document.createElement("div");
  div.className = "uploadItem";

  const left = document.createElement("div");
  left.className = "uploadLeft";

  let thumbEl = null;
  if(it.previewUrl){
    const img = document.createElement("img");
    img.className = "uploadThumb";
    img.src = it.previewUrl;
    img.alt = "";
    thumbEl = img;
  }else{
    const ph = document.createElement("div");
    ph.className = "uploadThumb ph";
    thumbEl = ph;
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

  left.appendChild(thumbEl);
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
    if(it.detail?.creator) chips.push(`<span class="uploadChip">${escapeHtml(it.detail.creator)}</span>`);
    if(it.detail){
      pickTopTags(it.detail, 6).forEach(t => chips.push(`<span class="uploadChip">${escapeHtml(t)}</span>`));
    }
    it._elChips.innerHTML = chips.join("");
  }
}

async function startUploadFiles(files){
  const imgs = (files || []).filter(f => f && f.type && f.type.startsWith("image/"));
  if(!imgs.length) return;

  // cleanup previews
  try{
    (state.uploadQueue || []).forEach(it => {
      if(it && it.previewUrl) URL.revokeObjectURL(it.previewUrl);
    });
  }catch(_e){}

  state.uploadStop = false;
  state.uploadZipJob = null;
  const jobBox = $("uploadZipJob");
  if(jobBox){ jobBox.classList.add("hidden"); jobBox.innerHTML = ""; }
  state.uploadQueue = imgs.map(f => ({
    file: f,
    state: "待機",
    previewUrl: (() => {
      try{ return URL.createObjectURL(f); }catch(_e){ return ""; }
    })(),
    detail: null,
  }));
  resetUploadSummary();
  renderUploadSummary();
  uploadListInit();
  uploadProgressUpdate();

  for(let i=0;i<state.uploadQueue.length;i++){
    if(state.uploadStop) break;
    const it = state.uploadQueue[i];
    it.state = "アップロード中";
    uploadListUpdateItem(it);
    uploadProgressUpdate();

    try{
      const fd = new FormData();
      fd.append("file", it.file, it.file.name);
      fd.append("last_modified_ms", String(it.file.lastModified || ""));
      const res = await apiFetch(API.upload, {method:"POST", body: fd});
      const data = await apiJson(res);
      it.state = data && data.dedup ? "重複" : "完了";

      // Use server-provided summary (avoid a second detail fetch per image).
      if(data && data.detail){
        it.detail = data.detail;
        applyUploadSummary(data.detail);
        renderUploadSummary();
      }else{
        it.detail = null;
      }
    }catch(e){
      it.state = "失敗";
    }
    uploadListUpdateItem(it);
    uploadProgressUpdate();
  }

  uploadProgressUpdate();
  await refreshStatsAndPreviewAfterChange();
}

async function startUpload(){
  const files = Array.from($("fileInput").files || []);
  if(!files.length) return;

  // If a zip is selected via the normal file picker, route to zip upload.
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
  // clear normal queue display
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

  const doZipUploadSingle = async () => {
    const fd = new FormData();
    fd.append("file", zipFile, zipFile.name);
    return await xhrPostForm(API.uploadZip, fd, (l, t) => setSendProgress(l, t || zipFile.size));
  };

  const doZipUploadChunked = async () => {
    const total = Number(zipFile.size || 0);
    // init
    const initFd = new FormData();
    initFd.append("filename", zipFile.name);
    initFd.append("total_bytes", String(total));
    const init = await xhrPostForm(API.uploadZipChunkInit, initFd);
    const token = init?.token;
    if(!token) throw new Error("chunk init failed");

    // Cloudflare Tunnel POST can hang on large bodies; always use small chunks.
    // 1.5MB = 1.5 * 1024 * 1024
    const chunkSize = 1572864;
    let offset = 0;
    while(offset < total){
      if(state.uploadStop) throw new Error("cancelled");
      const end = Math.min(total, offset + chunkSize);
      const blob = zipFile.slice(offset, end);
      const fd = new FormData();
      fd.append("token", token);
      fd.append("offset", String(offset));
      fd.append("chunk", blob, zipFile.name);
      await xhrPostForm(API.uploadZipChunkAppend, fd, (l, _t) => {
        setSendProgress(offset + Number(l||0), total);
      });
      offset = end;
      setSendProgress(offset, total);
    }

    const finFd = new FormData();
    finFd.append("token", token);
    return await xhrPostForm(API.uploadZipChunkFinish, finFd);
  };

  // Always use chunk upload for zip.
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
  state.uploadZipJob = { id: jobId, total: Number(data.total||0), done:0, failed:0, dup:0, status:"running", items:[] };

  const poll = async () => {
    if(state.uploadStop){
      _zipPollTimer = null;
      return;
    }
    try{
      const j = state.uploadZipJob;
      if(!j) return;

      const limit = 300;
      let after = Number(state.uploadZipLastSeq || 0);

      // Fetch all buffered items since the last seq (loop if the server has more than one page ready).
      while(true){
        const r = await apiFetch(`${API.uploadZipStatus(jobId)}?after_seq=${encodeURIComponent(String(after))}&limit=${encodeURIComponent(String(limit))}`);
        const st = await apiJson(r);

        j.total = Number(st.total||j.total||0);
        j.done = Number(st.done||0);
        j.failed = Number(st.failed||0);
        j.dup = Number(st.dup||0);
        j.status = String(st.status||"running");

        const items = (st.items || []);
        const newIts = items.map(x => ({
          seq: Number(x.seq||0),
          name: x.filename || "",
          previewUrl: x.thumb || "",
          state: x.state || "",
          detail: x.detail || null,
          image_id: x.image_id || null,
        })).filter(x => x.seq > 0);

        if(newIts.length){
          // Append to queue and DOM
          state.uploadQueue = (state.uploadQueue || []).concat(newIts);
          uploadListAppendItems(newIts);
          newIts.forEach(it => uploadListUpdateItem(it));

          // Update right-side summary only once per image_id.
          try{
            const seen = state._zipSeen || new Set();
            newIts.forEach(it => {
              const iid = Number(it.image_id||0);
              if(!iid || seen.has(iid)) return;
              seen.add(iid);
              if(it.detail){
                applyUploadSummary(it.detail);
              }
            });
            state._zipSeen = seen;
            renderUploadSummary();
          }catch(_e){}
        }

        const latest = Number(st.latest_seq || 0);
        if(latest > after) after = latest;

        if(items.length >= limit){
          // There may be more buffered items; keep draining.
          continue;
        }
        break;
      }

      state.uploadZipLastSeq = after;

      if(jobBox){
        const doneAll = j.done + j.failed + j.dup;
        const progText = (j.total && j.total > 0)
          ? `${doneAll}/${j.total}（成功 ${j.done} / 重複 ${j.dup} / 失敗 ${j.failed}）`
          : `スキャン中…（成功 ${j.done} / 重複 ${j.dup} / 失敗 ${j.failed}）`;
        jobBox.innerHTML = `
          <div class="row"><b>zip</b><span class="mut">${escapeHtml(zipFile.name)}</span></div>
          <div class="row"><span>進捗</span><span>${progText}</span></div>
        `;
      }

      // Progress bar shares UI
      const fill = $("uploadProgressBar");
      if(fill){
        const denom = j.total || 0;
        const doneAll = j.done + j.failed + j.dup;
        const pct = (denom && denom > 0) ? Math.min(100, Math.max(0, (doneAll / denom) * 100)) : 2;
        fill.style.width = `${pct.toFixed(1)}%`;
      }
      const elText = $("uploadProgressText");
      if(elText){
        const denom = j.total || 0;
        const doneAll = j.done + j.failed + j.dup;
        elText.textContent = (denom && denom > 0) ? `zip ${doneAll}/${denom}` : `zip スキャン中…`;
      }

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
    const [cRes, sRes] = await Promise.all([
      apiFetch(API.creators),
      apiFetch(API.software),
    ]);
    const creators = await apiJson(cRes);
    const softwares = await apiJson(sRes);
    fillSelect($("filterCreator"), creators.map(x => x.creator));
    fillSelect($("filterSoftware"), softwares.map(x => x.software));
    renderCreatorList(creators);
    renderSoftwareList(softwares);
  }catch(e){}
}


async function refreshStatsAndPreviewAfterChange(){
  // Used when the underlying dataset changes (e.g. upload / bulk delete / zip completion).
  // Important: paging caches must be invalidated even when filter key stays the same.
  invalidatePreviewCaches();
  await refreshFacets();
  await loadYearCounts();
  await loadYearMonthCounts(state.calendar.month.getFullYear());
  await loadMonthCounts(state.calendar.month);
  renderCalendar();
  await search(1);
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

function renderCreatorList(creators){
  const wrap = $("creatorList");
  if(!wrap) return;
  wrap.innerHTML = "";
  const total = creators.reduce((a,b)=>a+Number(b.count||0), 0);

  const all = document.createElement("div");
  all.className = "sideItem";
  all.innerHTML = `<div class="name">すべて</div><div class="cnt">${total.toLocaleString()}</div>`;
  all.addEventListener("click", async () => {
    $("filterCreator").value = "";
    state.preview.creator = "";
    await search();
  });
  wrap.appendChild(all);

  creators.slice(0, 40).forEach((x) => {
    const div = document.createElement("div");
    div.className = "sideItem";
    div.innerHTML = `<div class="name">${escapeHtml(x.creator)}</div><div class="cnt">${Number(x.count||0).toLocaleString()}</div>`;
    div.addEventListener("click", async () => {
      $("filterCreator").value = x.creator;
      state.preview.creator = x.creator;
      await search();
    });
    wrap.appendChild(div);
  });
}

function renderSoftwareList(softwares){
  const wrap = $("softwareList");
  if(!wrap) return;
  wrap.innerHTML = "";
  const total = softwares.reduce((a,b)=>a+Number(b.count||0), 0);

  const all = document.createElement("div");
  all.className = "sideItem";
  all.innerHTML = `<div class="name">すべて</div><div class="cnt">${total.toLocaleString()}</div>`;
  all.addEventListener("click", async () => {
    $("filterSoftware").value = "";
    state.preview.software = "";
    await search();
  });
  wrap.appendChild(all);

  softwares.slice(0, 40).forEach((x) => {
    const div = document.createElement("div");
    div.className = "sideItem";
    div.innerHTML = `<div class="name">${escapeHtml(x.software)}</div><div class="cnt">${Number(x.count||0).toLocaleString()}</div>`;
    div.addEventListener("click", async () => {
      $("filterSoftware").value = x.software;
      state.preview.software = x.software;
      await search();
    });
    wrap.appendChild(div);
  });
}

/* =====================
   Preview search / paging
   ===================== */

function buildPreviewSearchKey(){
  return [
    state.preview.creator || "",
    state.preview.software || "",
    (state.preview.tags || []).join(","),
    state.preview.date_from || "",
    state.preview.date_to || "",
    String(state.preview.dedup_only || 0),
    String(state.preview.fav_only || 0),
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
  return true;
}

function warmThumbUrls(urls, maxN){
  const n = Math.min(maxN || 0, urls.length);
  for(let i=0;i<n;i++){
    const u = urls[i];
    if(!u) continue;
    const img = new Image();
    // Use Image() so the browser performs normal image negotiation (Accept includes image/avif).
    img.decoding = "async";
    img.loading = "eager";
    img.src = u;
  }
}

function warmOverlayDerivatives(items){
  if(!items || !items.length) return;
  const maxN = isDesktop() ? 24 : 12;
  const ids = [];
  for(const it of items){
    if(ids.length >= maxN) break;
    const id = Number(it?.id || 0);
    if(!id || _overlayWarmSet.has(id)) continue;
    _overlayWarmSet.add(id);
    ids.push(id);
  }
  if(!ids.length) return;
  apiFetch("/api/cache/prefetch_derivatives", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ids, kind: "overlay" }),
  }).catch(()=>{});
}

// Detail prefetch cache: we want the detail text to be instant when opening the overlay.
const _detailCache = new Map();      // id -> detail json
const _detailInFlight = new Map();   // id -> Promise
let _detailRenderToken = 0;

async function fetchDetailCached(id){
  const iid = Number(id||0);
  if(!iid) throw new Error("bad id");
  if(_detailCache.has(iid)) return _detailCache.get(iid);
  if(_detailInFlight.has(iid)) return await _detailInFlight.get(iid);
  const p = (async () => {
    const res = await apiFetch(API.detail(iid));
    return await apiJson(res);
  })();
  _detailInFlight.set(iid, p);
  try{
    const d = await p;
    _detailCache.set(iid, d);
    return d;
  }finally{
    _detailInFlight.delete(iid);
  }
}

function prefetchDetails(items){
  if(!items || !items.length) return;
  // Keep concurrency low (detail payload can be large).
  const maxN = isDesktop() ? 16 : 10;
  const conc = isDesktop() ? 2 : 1;
  const ids = [];
  for(const it of items){
    if(ids.length >= maxN) break;
    const id = Number(it?.id||0);
    if(!id) continue;
    if(_detailCache.has(id) || _detailInFlight.has(id)) continue;
    ids.push(id);
  }
  if(!ids.length) return;

  let idx = 0;
  const worker = async () => {
    while(idx < ids.length){
      const id = ids[idx++];
      try{ await fetchDetailCached(id); }catch(_e){}
    }
  };
  for(let i=0;i<conc;i++) worker();
}

function pageCacheKey(page){
  // Key WITHOUT include_total so prefetch and normal navigation share cache.
  return buildPageQueryCore(page);
}

async function fetchPageCached(page, includeTotal){
  const key = pageCacheKey(page);
  const hit = _pageCache.get(key);
  if(hit?.data) return hit.data;
  if(hit?.promise) return await hit.promise;

  const qs = key + `&include_total=${includeTotal ? 1 : 0}`;
  const p = (async () => {
    const res = await apiFetch(`${API.pageList}?${qs}`);
    return await apiJson(res);
  })();
  _pageCache.set(key, { promise: p, data: null });
  try{
    const data = await p;
    const e = _pageCache.get(key);
    if(e){ e.data = data; e.promise = null; }
    return data;
  }catch(e){
    _pageCache.delete(key);
    throw e;
  }
}

function prefetchPage(page){
  const p = Math.max(1, Number(page || 1));
  const total = Number(state.preview.total_pages || 0);
  if(total && p > total) return;
  const key = pageCacheKey(p);
  if(_pageCache.get(key)?.data || _pageCache.get(key)?.promise) return;

  // Prefetch without COUNT to keep DB light.
  fetchPageCached(p, 0).then((data) => {
    const items = data?.items || [];
    warmThumbUrls(items.map(x=>x.thumb), 16);
    warmOverlayDerivatives(items);
    prefetchDetails(items);
  }).catch(()=>{});
}

function buildScrollQuery(cursor, includeTotal=1){
  const p = new URLSearchParams();
  if(state.preview.creator) p.set("creator", state.preview.creator);
  if(state.preview.software) p.set("software", state.preview.software);
  if(state.preview.tags.length) p.set("tags", state.preview.tags.join(","));
  if(state.preview.date_from) p.set("date_from", state.preview.date_from);
  if(state.preview.date_to) p.set("date_to", state.preview.date_to);
  if(state.preview.dedup_only) p.set("dedup_only", "1");
  if(state.preview.fav_only) p.set("fav_only", "1");
  if(state.preview.sort) p.set("sort", state.preview.sort);
  p.set("limit", String(state.preview.limit));
  p.set("include_total", includeTotal ? "1" : "0");
  if(cursor) p.set("cursor", cursor);
  return p.toString();
}

function ensureScrollObserver(){
  if(state.preview.mode !== "scroll") return;
  if(scrollObserver) return;
  const sentinel = $("scrollSentinel");
  if(!sentinel) return;
  scrollObserver = new IntersectionObserver(async (entries) => {
    const e = entries[0];
    if(!e || !e.isIntersecting) return;
    await loadMore();
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

function buildPageQueryCore(page){
  const p = new URLSearchParams();
  if(state.preview.creator) p.set("creator", state.preview.creator);
  if(state.preview.software) p.set("software", state.preview.software);
  if(state.preview.tags.length) p.set("tags", state.preview.tags.join(","));
  if(state.preview.date_from) p.set("date_from", state.preview.date_from);
  if(state.preview.date_to) p.set("date_to", state.preview.date_to);
  if(state.preview.dedup_only) p.set("dedup_only", "1");
  if(state.preview.fav_only) p.set("fav_only", "1");
  if(state.preview.sort) p.set("sort", state.preview.sort);
  p.set("page", String(page || 1));
  // PC: page-based (density like sample)
  p.set("limit", "16");
  return p.toString();
}

function buildPageQuery(page, includeTotal=1){
  const qs = buildPageQueryCore(page);
  return qs + `&include_total=${includeTotal ? 1 : 0}`;
}

async function search(page=1){
  state.preview.mode = isDesktop() ? "page" : "scroll";
  // scroll mode: fetch in smaller chunks to reduce initial load on mobile.
  // (page mode uses a fixed limit=16 in buildPageQuery)
  if(state.preview.mode === "scroll") state.preview.limit = 30;
  const keyChanged = resetPreviewCachesIfNeeded();
  setPagingUI();
  resetGrid();
  _resetBulkSelection();

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

      state.preview.items = data.items || [];
      state.preview.page = Number(data.page || state.preview.page);
      if(typeof data.total_pages !== "undefined" && data.total_pages !== null) state.preview.total_pages = Number(data.total_pages || 0);
      if(typeof data.total_count !== "undefined" && data.total_count !== null) state.preview.total_count = Number(data.total_count || 0);
      if(data.sort) state.preview.sort = data.sort;
      if(typeof data.fav_only !== "undefined") state.preview.fav_only = Number(data.fav_only || 0);

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
  updateGalleryTitle();
  syncGalleryControls();
  ensureScrollObserver();
  await loadMore();
}

async function loadMore(){
  if(state.preview.mode !== "scroll") return;
  if(state.preview.loading || state.preview.done) return;
  state.preview.loading = true;
  let appended = 0;
  try{
    const includeTotal = !state.preview.got_total;
    const qs = buildScrollQuery(state.preview.cursor, includeTotal ? 1 : 0);

    // Consume prefetched page if available.
    let data = null;
    if(_scrollPrefetch && _scrollPrefetch.key === qs){
      if(_scrollPrefetch.data){
        data = _scrollPrefetch.data;
      }else if(_scrollPrefetch.promise){
        data = await _scrollPrefetch.promise;
      }
      _scrollPrefetch = null;
    }
    if(!data){
      const res = await apiFetch(`${API.scrollList}?${qs}`);
      data = await apiJson(res);
    }

    const items = data.items || [];
    if(data.sort) state.preview.sort = data.sort;
    if(typeof data.fav_only !== "undefined") state.preview.fav_only = Number(data.fav_only || 0);
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
    warmOverlayDerivatives(items);
    prefetchDetails(items);
    updateGalleryTitle();

    // Prefetch next cursor chunk (no COUNT).
    if(state.preview.cursor){
      const nextQs = buildScrollQuery(state.preview.cursor, 0);
      if(!_scrollPrefetch || _scrollPrefetch.key !== nextQs){
        const p = (async () => {
          const r = await apiFetch(`${API.scrollList}?${nextQs}`);
          return await apiJson(r);
        })();
        _scrollPrefetch = { key: nextQs, promise: p, data: null };
        p.then(d => { if(_scrollPrefetch && _scrollPrefetch.key === nextQs){ _scrollPrefetch.data = d; _scrollPrefetch.promise = null; } }).catch(()=>{ if(_scrollPrefetch && _scrollPrefetch.key === nextQs) _scrollPrefetch = null; });
      }
    }
  } finally {
    state.preview.loading = false;

    // IntersectionObserver may not fire again if the sentinel remains visible.
    // Proactively load until the sentinel is pushed below the viewport.
    if(!state.preview.done && appended > 0){
      const sentinel = $("scrollSentinel");
      if(sentinel){
        const r = sentinel.getBoundingClientRect();
        const near = r.top < (window.innerHeight + 800);
        if(near && _autoFillBurst < 8){
          _autoFillBurst += 1;
          queueMicrotask(() => loadMore());
        }else if(!near){
          _autoFillBurst = 0;
        }
      }
    }
  }
}

function syncGalleryControls(){
  const sortSel = $("sortBy");
  if(sortSel) sortSel.value = state.preview.sort || "newest";

  const favBtn = $("favOnlyBtn");
  if(favBtn){
    favBtn.classList.toggle("active", !!state.preview.fav_only);
    favBtn.title = state.preview.fav_only ? "お気に入りのみ (ON)" : "お気に入りのみ (OFF)";
  }

  setGalleryView(state.preview.view || "grid", true);

  document.querySelectorAll(".sideTab").forEach(btn => {
    const k = btn.dataset.side;
    btn.classList.toggle("active", (k === "fav") ? !!state.preview.fav_only : !state.preview.fav_only);
  });
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
    if(hint){
      const p = Number(state.preview.page || 1);
      const tp = Number(state.preview.total_pages || 0);
      hint.textContent = tp ? `${p} / ${tp}` : "";
    }
    return;
  }

  const total = Number(state.preview.total_count || 0);
  const loaded = state.preview.items.length || 0;
  if(t) t.textContent = `ギャラリー (${(total || loaded).toLocaleString()})`;
  if(hint){
    if(!total) hint.textContent = state.preview.done ? "" : "…";
    else hint.textContent = state.preview.done ? "" : `${loaded.toLocaleString()} / ${total.toLocaleString()}`;
  }
}

function resetGrid(){
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
  const btnPage = $("pageSelectBtn");
  const btnAll = $("allSelectBtn");

  if(!b){
    btnDel && (btnDel.disabled = true);
    return;
  }

  const hasSel = b.all || (b.selected.size > 0);
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
    date_from: state.preview.date_from || "",
    date_to: state.preview.date_to || "",
    dedup_only: Number(state.preview.dedup_only || 0) ? 1 : 0,
    fav_only: Number(state.preview.fav_only || 0) ? 1 : 0,
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
  if(it.dedup_flag === 2) badges.push('<span class="badge dup">dup</span>');
  if(it.is_nsfw) badges.push('<span class="badge nsfw">NSFW</span>');

  const canSel = _canBulkSelectItem(it);
  const checked = _isTileSelected(it);

  div.innerHTML = `
    <div class="tileImg">
      <img loading="lazy" decoding="async" src="${it.thumb}" alt="">
      <label class="tileCheck" title="${canSel ? "選択" : "自分の画像のみ"}">
        <input type="checkbox" class="tileChk" ${checked ? "checked" : ""} ${canSel ? "" : "disabled"}>
        <span class="tileChkBox"></span>
      </label>
      <div class="tileBadges">
        ${badges.join("")}
        <button class="favBtn ${it.favorite ? "on" : ""}" title="お気に入り" aria-label="お気に入り">${it.favorite ? "★" : "☆"}</button>
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
      await toggleFavorite(it.id);
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
let _scrollLockY = 0;
function lockBodyScroll(){
  if(document.body.classList.contains("modalOpen")) return;
  _scrollLockY = window.scrollY || document.documentElement.scrollTop || 0;
  document.body.classList.add("modalOpen");
  document.body.style.position = "fixed";
  document.body.style.top = `-${_scrollLockY}px`;
  document.body.style.left = "0";
  document.body.style.right = "0";
  document.body.style.width = "100%";
}

function unlockBodyScroll(){
  if(!document.body.classList.contains("modalOpen")) return;
  document.body.classList.remove("modalOpen");
  document.body.style.position = "";
  document.body.style.top = "";
  document.body.style.left = "";
  document.body.style.right = "";
  document.body.style.width = "";
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

function joinKeep(tags){
  return tags.map(t => t.raw_one || t.canonical).join(", ");
}
function joinPlain(tags){
  if(!tags.length) return "";
  return tags.map(t => t.canonical).join(":") + ":";
}

function makeTagButton(tag, onClick){
  const b = document.createElement("button");
  b.className = "tagBtn";
  b.textContent = tag;
  b.addEventListener("click", onClick);
  return b;
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

async function toggleFavorite(id){
  try{
    const res = await apiFetch(API.favorite(id), {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({toggle: true}),
    });
    const data = await apiJson(res);
    const fav = Number(data.favorite || 0);

    // update list cache
    state.preview.items.forEach(it => {
      if(Number(it.id) === Number(id)) it.favorite = fav;
    });

    // update current detail cache
    if(currentDetail && Number(currentDetail.id) === Number(id)){
      currentDetail.favorite = fav;
      syncDetailFavorite();
    }

    // if order/filter depends on favorite, refresh page
    if(state.preview.sort === "favorite" || state.preview.fav_only){
      await search();
      return;
    }

    // quick UI update
    const tile = document.querySelector(`.tile[data-id="${id}"]`);
    const btn = tile?.querySelector(".favBtn");
    if(btn){
      btn.textContent = fav ? "★" : "☆";
      btn.classList.toggle("on", !!fav);
    }
  }catch(e){}
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

  const dSoft = $("dSoft");
  if(dSoft) dSoft.textContent = it.software || "";
  const dCreator = $("dCreator");
  if(dCreator) dCreator.textContent = it.creator || "";

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

  const isMobile = window.matchMedia && window.matchMedia("(max-width: 720px)").matches;
  const imgOverlayInfo = $("imgOverlayInfo");
  if(imgOverlayInfo){
    if(isMobile){
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

  const fixedBtns = $("overlayFixedBtns");
  if(fixedBtns){
    if(isMobile) fixedBtns.classList.remove("hidden");
    else fixedBtns.classList.add("hidden");
  }

  // links disabled until detail arrives
  const dlFile = $("dlFile");
  if(dlFile) dlFile.href = "#";
  const vf = $("viewFull");
  if(vf){ vf.href = "#"; vf.classList.add("hidden"); }
  const dlMeta = $("dlMeta");
  if(dlMeta) dlMeta.href = "#";
  const dlPotion = $("dlPotion");
  if(dlPotion){ dlPotion.href = "#"; dlPotion.classList.add("hidden"); }

  const secArtist = $("secArtist");
  const secQuality = $("secQuality");
  const secCharacter = $("secCharacter");
  const secOther = $("secOther");
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

  const dSoft = $("dSoft");
  if(dSoft) dSoft.textContent = d.software || "";
  const dCreator = $("dCreator");
  if(dCreator) dCreator.textContent = d.creator || "";

  // Mobile: show compact info as an overlay on the image (no blur)
  const isMobile = window.matchMedia && window.matchMedia("(max-width: 720px)").matches;
  const imgOverlayInfo = $("imgOverlayInfo");
  if(imgOverlayInfo){
    if(isMobile){
      const t = $("ioTitle"); if(t) t.textContent = d.filename || `#${d.id}`;
      const s = $("ioSub"); if(s) s.textContent = `${d.w}x${d.h}  ${fmtMtime(d.mtime)}`;
      const m = $("ioMeta"); if(m){
        const src = d.software || "";
        const by = d.creator ? `by ${d.creator}` : "";
        m.textContent = (src && by) ? `${src}  ${by}` : (src || by);
      }
      imgOverlayInfo.classList.remove("hidden");
    }else{
      imgOverlayInfo.classList.add("hidden");
    }
  }

  // Mobile: keep favorite/close buttons fixed on top-right
  const fixedBtns = $("overlayFixedBtns");
  if(fixedBtns){
    if(isMobile) fixedBtns.classList.remove("hidden");
    else fixedBtns.classList.add("hidden");
  }

  const dlFile = $("dlFile");
  if(dlFile) dlFile.href = d.download_file;
  const vf = $("viewFull");
  if(vf){
    vf.href = d.view_full || d.download_file;
    vf.classList.toggle("hidden", !(d.view_full || d.download_file));
  }
  const dlMeta = $("dlMeta");
  if(dlMeta) dlMeta.href = d.download_meta;
  const dlPotion = $("dlPotion");
  if(dlPotion){
    dlPotion.href = d.has_potion ? d.download_potion : "#";
    dlPotion.classList.toggle("hidden", !d.has_potion);
  }
  const secArtist = $("secArtist");
  const secQuality = $("secQuality");
  const secCharacter = $("secCharacter");
  const secOther = $("secOther");
  if(secArtist) secArtist.innerHTML = "";
  if(secQuality) secQuality.innerHTML = "";
  if(secCharacter) secCharacter.innerHTML = "";
  if(secOther) secOther.innerHTML = "";

  const fill = (box, arr) => {
    if(!arr.length){
      const s = document.createElement("span");
      s.className = "small";
      s.textContent = "(none)";
      box.appendChild(s);
      return;
    }
    arr.forEach(t => box.appendChild(makeTagButton(t.canonical, () => copyText(t.canonical + ":"))));
  };

  if(secArtist) fill(secArtist, d.tags?.artist || []);
  if(secQuality) fill(secQuality, d.tags?.quality || []);
  if(secCharacter) fill(secCharacter, d.tags?.character || []);
  if(secOther) fill(secOther, d.tags?.other || []);

  let params = null;
  if(d.params_json){
    try{ params = JSON.parse(d.params_json); }
    catch(_e){ params = { _raw: String(d.params_json) }; }
  }
  const meta = {
    software: d.software,
    model: d.model,
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
  const overlay = $("overlay");
  if(overlay) overlay.classList.remove("hidden");
  lockBodyScroll();
  renderDetailLoading(iid);

  const token = ++_detailRenderToken;

  // If we already prefetched detail, render instantly.
  const cached = _detailCache.get(iid);
  if(cached){
    currentDetail = cached;
    renderDetailFull(cached);
    return;
  }

  // Fetch in parallel; once it arrives, update UI.
  fetchDetailCached(iid).then((d) => {
    if(token !== _detailRenderToken) return;
    currentDetail = d;
    renderDetailFull(d);
  }).catch((_e) => {
    // keep the overlay open, but show minimal info
    const metaPre = $("metaPre");
    if(metaPre) metaPre.textContent = "(failed to load detail)";
  });
}

function closeDetail(){
  _detailRenderToken += 1; // cancel pending render
  const overlay = $("overlay");
  if(overlay) overlay.classList.add("hidden");
  const overlayImg = $("overlayImg");
  if(overlayImg) overlayImg.src = "";
  resetOverlayImage();
  $("overlayFixedBtns")?.classList.add("hidden");
  $("imgOverlayInfo")?.classList.add("hidden");
  currentDetail = null;
  // clear selection
  document.querySelectorAll(".tile.selected").forEach(el => el.classList.remove("selected"));
  state.selectedTileId = null;
  unlockBodyScroll();
}

function bindCopyAll(){
  document.querySelectorAll("[data-copyall]").forEach(btn => {
    btn.addEventListener("click", () => {
      if(!currentDetail) return;
      const k = btn.dataset.copyall;
      const [section, mode] = k.split("_");
      let arr = [];
      if(["artist","quality","character","other"].includes(section)) arr = currentDetail.tags?.[section] || [];
      const text = (mode === "keep") ? joinKeep(arr) : joinPlain(arr);
      copyText(text);
    });
  });
}

/* =====================
   Tag chips / suggest
   ===================== */

function updateChips(){
  const wrap = $("tagChips");
  wrap.innerHTML = "";
  state.preview.tags.forEach(tag => {
    const chip = document.createElement("div");
    chip.className = "chip";
    chip.innerHTML = `<span>${escapeHtml(tag)}</span>`;
    const x = document.createElement("button");
    x.textContent = "×";
    x.addEventListener("click", () => {
      state.preview.tags = state.preview.tags.filter(t => t !== tag);
      updateChips();
    });
    chip.appendChild(x);
    wrap.appendChild(chip);
  });
}

let suggestTimer = null;
async function onTagInput(){
  const q = $("tagInput").value.trim();
  const box = $("tagSuggest");
  if(suggestTimer) clearTimeout(suggestTimer);
  suggestTimer = setTimeout(async () => {
    if(!q){
      box.classList.add("hidden");
      box.innerHTML = "";
      return;
    }
    let data = [];
    try{
      const res = await apiFetch(API.suggest(q));
      data = (await apiJson(res)) || [];
    }catch(e){
      box.classList.add("hidden");
      box.innerHTML = "";
      return;
    }
    box.innerHTML = "";

    data.forEach(item => {
      const div = document.createElement("div");
      div.className = "suggestItem";
      div.innerHTML = `<span>${escapeHtml(item.tag)}</span><small>${item.count}</small>`;
      div.addEventListener("click", () => {
        if(!state.preview.tags.includes(item.tag)){
          state.preview.tags.push(item.tag);
          updateChips();
        }
        $("tagInput").value = "";
        box.classList.add("hidden");
        box.innerHTML = "";
      });
      box.appendChild(div);
    });

    box.classList.remove("hidden");
    const r = $("tagInput").getBoundingClientRect();
    box.style.left = r.left + "px";
    box.style.top = (r.bottom + window.scrollY) + "px";
    box.style.width = Math.min(520, r.width) + "px";
  }, 160);
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
  return v.toLocaleString() + '件';
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

  // If the second click comes soon, treat it as a range end.
  if(a && (now - ts) <= 5000){
    state.calendar.click_anchor = '';
    state.calendar.click_ts = 0;
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

function beginDragSelect(key){
  state.calendar.click_anchor = "";
  state.calendar.click_ts = 0;
  state.calendar.dragging = true;
  state.calendar.drag_anchor = key;
  state.calendar.from = key;
  state.calendar.to = key;
  applyCalendarToInputs();
  updateCalendarSelectionUI();

  // Track pointer globally so drag works even when leaving a cell.
  window.addEventListener("pointermove", onCalendarDragMove, { passive: true });
  window.addEventListener("pointerup", endDragSelect, { passive: true });
  window.addEventListener("pointercancel", endDragSelect, { passive: true });
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
  state.calendar.click_anchor = "";
  state.calendar.click_ts = 0;
  $("dateFrom").value = "";
  $("dateTo").value = "";
  renderCalendar();
  state.preview.date_from = "";
  state.preview.date_to = "";
  search().catch(_e => {});
}

function renderCalendar(){
  const cal = $("calendar");
  const md = state.calendar.month;
  const title = ym(md);
  cal.innerHTML = "";

  const head = document.createElement("div");
  head.className = "calHead";
    const y = md.getFullYear();
  const mm = String(md.getMonth()+1).padStart(2,'0');
  head.innerHTML = `
    <div class="row" style="gap:8px">
      <button id="calPrev" class="iconBtn" aria-label="prev"><span class="icon">◀</span></button>
      <button id="calYearBtn" class="calTitleBtn" type="button">${y}</button>
      <span class="calTitleSep">/</span>
      <button id="calMonthBtn" class="calTitleBtn" type="button">${mm}</button>
      <button id="calNext" class="iconBtn" aria-label="next"><span class="icon">▶</span></button>
    </div>
    <div class="row" style="gap:8px; align-items:center; justify-content:flex-end">
      <button id="calClear" class="iconBtn smallBtn" aria-label="clear">クリア</button>
    </div>
  `;
  cal.appendChild(head);

  // pickers (year/month)
  const pop = document.createElement('div');
  pop.className = 'calPopover';
  pop.style.display = 'none';
  cal.appendChild(pop);

  const closePicker = () => { state.calendar.picker = ''; pop.style.display = 'none'; pop.innerHTML=''; };
  const openYearPicker = () => {
    state.calendar.picker = (state.calendar.picker === 'year' ? '' : 'year');
    if(state.calendar.picker !== 'year'){ closePicker(); return; }
    const years = Array.from(state.calendar.yearCounts.keys()).sort((a,b)=> b.localeCompare(a));
    const ycur = String(y);
    const list = years.length ? years : [String(y-2), String(y-1), String(y), String(y+1)];
    pop.style.display = 'block';
    pop.innerHTML = `<div class="calPickList"></div>`;
    const wrap = pop.querySelector('.calPickList');
    list.forEach(yy => {
      const btn = document.createElement('button');
      btn.type='button';
      btn.className = 'calPickBtn' + (yy===ycur ? ' active' : '');
      const c = state.calendar.yearCounts.get(yy) || 0;
      btn.innerHTML = `<div>${yy}年</div><small>${fmtCount(c)}</small>`;
      btn.onclick = async (e) => {
        e.preventDefault(); e.stopPropagation();
        const ny = parseInt(yy,10);
        state.calendar.month = new Date(ny, md.getMonth(), 1);
        await loadMonthCounts(state.calendar.month);
        closePicker();
        renderCalendar();
      };
      wrap.appendChild(btn);
    });
  };

  const openMonthPicker = () => {
    state.calendar.picker = (state.calendar.picker === 'month' ? '' : 'month');
    if(state.calendar.picker !== 'month'){ closePicker(); return; }
    pop.style.display = 'block';
    pop.innerHTML = `<div class="calPickGrid"></div>`;
    const wrap = pop.querySelector('.calPickGrid');
    for(let mi=1; mi<=12; mi++){
      const mm2 = String(mi).padStart(2,'0');
      const ymKey = `${y}-${mm2}`;
      const c = state.calendar.monthCounts.get(ymKey) || 0;
      const btn = document.createElement('button');
      btn.type='button';
      btn.className = 'calPickBtn' + (mm2===mm ? ' active' : '');
      btn.innerHTML = `<div>${mi}月</div><small>${fmtCount(c)}</small>`;
      btn.onclick = async (e) => {
        e.preventDefault(); e.stopPropagation();
        state.calendar.month = new Date(y, mi-1, 1);
        await loadMonthCounts(state.calendar.month);
        closePicker();
        renderCalendar();
      };
      wrap.appendChild(btn);
    }
  };

  // wire title buttons
  $('calYearBtn').onclick = (e) => { e.preventDefault(); e.stopPropagation(); openYearPicker(); };
  $('calMonthBtn').onclick = (e) => { e.preventDefault(); e.stopPropagation(); openMonthPicker(); };
  // close picker when clicking outside calendar
  if(!state.calendar._doc_close_bound){
    state.calendar._doc_close_bound = true;
    document.addEventListener('click', (e) => {
      if(!state.calendar.picker) return;
      const root = $('calendar');
      if(root && !root.contains(e.target)){
        state.calendar.picker='';
        renderCalendar();
      }
    });
  }

  const grid = document.createElement("div");
  grid.className = "calGrid";
  const dow = ["日","月","火","水","木","金","土"];
  dow.forEach(x=>{
    const c = document.createElement("div");
    c.className = "calCell muted";
    c.style.minHeight = "26px";
    c.style.padding = "6px";
    c.style.cursor = "default";
    c.innerHTML = `<div class="d">${x}</div>`;
    grid.appendChild(c);
  });

  const first = startOfMonth(md);
  const last = endOfMonth(md);
  const startDow = first.getDay();

  for(let i=0;i<startDow;i++){
    const cell = document.createElement("div");
    cell.className = "calCell muted";
    cell.innerHTML = "";
    grid.appendChild(cell);
  }

  for(let day=1; day<=last.getDate(); day++){
    const dt = new Date(md.getFullYear(), md.getMonth(), day);
    const key = ymd(dt);
    const count = state.calendar.counts.get(key) || 0;

    const cell = document.createElement("div");
    cell.className = "calCell" + (isSelected(key) ? " selected" : "");
    if(state.calendar.from && key === state.calendar.from) cell.classList.add("edgeFrom");
    if(state.calendar.to && key === state.calendar.to) cell.classList.add("edgeTo");
    cell.dataset.key = key;
    cell.innerHTML = `<div class="d">${day}</div><div class="c">${count ? count+"件" : ""}</div>`;

    // click -> drag selection
    cell.addEventListener("pointerdown", (e) => {
      e.preventDefault();
      // Press -> drag (threshold) OR click (two-click range)
      state.calendar.press_key = key;
      state.calendar.press_x = e.clientX || 0;
      state.calendar.press_y = e.clientY || 0;
      window.addEventListener('pointermove', onCalendarPressMove, { passive: true });
      window.addEventListener('pointerup', onCalendarPressUp, { passive: true });
      window.addEventListener('pointercancel', onCalendarPressUp, { passive: true });
    });
    grid.appendChild(cell);
  }

  cal.appendChild(grid);

  $("calPrev").onclick = async () => {
    state.calendar.picker = '';
    state.calendar.click_anchor = '';
    state.calendar.click_ts = 0;
    state.calendar.month = new Date(md.getFullYear(), md.getMonth()-1, 1);
    await loadMonthCounts(state.calendar.month);
    renderCalendar();
  };
  $("calNext").onclick = async () => {
    state.calendar.picker = '';
    state.calendar.click_anchor = '';
    state.calendar.click_ts = 0;
    state.calendar.month = new Date(md.getFullYear(), md.getMonth()+1, 1);
    await loadMonthCounts(state.calendar.month);
    renderCalendar();
  };

  $("calClear").onclick = () => clearCalendar();
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
  state.preview.creator = $("filterCreator").value;
  state.preview.software = $("filterSoftware").value;
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

  // user menu (top-right)
  const hamburger = $("hamburger");
  const userMenu = $("userMenu");
  const menuAdmin = $("menuAdmin");
  const menuMaintenance = $("menuMaintenance");
  const menuPwLink = $("menuPwLink");
  const menuLogout = $("menuLogout");

  const closeMenu = () => userMenu?.classList.add("hidden");
  const toggleMenu = () => userMenu?.classList.toggle("hidden");

  hamburger?.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    toggleMenu();
  });
  menuAdmin?.addEventListener("click", () => location.assign("/admin.html"));
  menuMaintenance?.addEventListener("click", () => location.assign("/maintenance.html"));
  menuPwLink?.addEventListener("click", async () => {
    try{
      const r = await apiFetch(API.pwLink, { method: "POST" });
      const j = await apiJson(r);
      if(j && j.reset_url){
        location.assign(j.reset_url);
      }
    }catch(_e){
      alert("URL発行に失敗しました");
    }
  });
  menuLogout?.addEventListener("click", doLogout);
  document.addEventListener("click", () => closeMenu());

  bindDropZone();
  // stopUploadBtn was removed from UI (uploads are auto-processed).

  $("searchBtn").addEventListener("click", async () => {
    state.preview.creator = $("filterCreator").value;
    state.preview.software = $("filterSoftware").value;
    state.preview.date_from = $("dateFrom").value;
    state.preview.date_to = $("dateTo").value;
    await search();
  });

  $("tagSearchBtn").addEventListener("click", async () => {
    await search();
  });

  $("tagInput").addEventListener("input", onTagInput);
  document.addEventListener("click", (e) => {
    const box = $("tagSuggest");
    if(e.target === $("tagInput") || box.contains(e.target)) return;
    box.classList.add("hidden");
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

  // favorites-only
  $("favOnlyBtn")?.addEventListener("click", async () => {
    state.preview.fav_only = state.preview.fav_only ? 0 : 1;
    await search();
  });

  // side tabs (filter / favorites)
  document.querySelectorAll(".sideTab").forEach(btn => {
    btn.addEventListener("click", async () => {
      const k = btn.dataset.side;
      state.preview.fav_only = (k === "fav") ? 1 : 0;
      await search();
    });
  });

  // view mode
  $("viewGrid")?.addEventListener("click", () => setGalleryView("grid"));
  $("viewList")?.addEventListener("click", () => setGalleryView("list"));

  // Bulk selection / delete (preview)
  $("pageSelectBtn")?.addEventListener("click", (e) => { e.preventDefault(); _togglePageSelect(); });
  $("allSelectBtn")?.addEventListener("click", (e) => { e.preventDefault(); _toggleAllSelect(); });
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


  $("favDetailBtn")?.addEventListener("click", async (e) => {
    e.preventDefault();
    if(!currentDetail) return;
    await toggleFavorite(currentDetail.id);
  });
  $("favDetailBtnFixed")?.addEventListener("click", async (e) => {
    e.preventDefault();
    if(!currentDetail) return;
    await toggleFavorite(currentDetail.id);
  });

  bindCopyAll();
}

async function boot(){
  bindUI();
  try{
    await loadMe();
  }catch(e){
    location.replace("/login.html");
    return;
  }

  const sp = new URLSearchParams(location.search || "");
  const viewParam = (sp.get("view") || "").toLowerCase();
  const hashView = (location.hash || "").replace("#", "").toLowerCase();
  let view = "preview";
  if(hashView === "upload" || hashView === "preview") view = hashView;
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

// Ensure DOM exists before binding (module scripts can still run before all nodes in some merges)
if(document.readyState === 'loading'){
  document.addEventListener('DOMContentLoaded', () => boot());
}else{
  boot();
}

