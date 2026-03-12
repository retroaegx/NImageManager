import { $ } from "./lib/dom.js?v=20260307_01";
import { apiFetch, apiJson } from "./lib/http.js?v=20260307_01";
import { bindUserMenu } from "./lib/userMenu.js?v=20260312_02";
import { loadCurrentUser, logoutAndRedirect } from "./lib/session.js?v=20260307_01";

const API = {
  me: "/api/me",
  logout: "/api/auth/logout",
  selfPwLink: "/api/auth/password_link",

  reparseStart: "/api/admin/reparse_all_start",
  reparseState: "/api/admin/reparse_state",
  reparseOne: "/api/admin/reparse_one",
  reparseSkip: "/api/admin/reparse_skip",

  rebuildStart: "/api/admin/rebuild_stats_start",
  rebuildState: "/api/admin/rebuild_state",
};

async function loadMe(){
  return await loadCurrentUser({
    endpoint: API.me,
    requireAdmin: true,
    onLoaded: () => {
      bindUserMenu({
        logoutEndpoint: API.logout,
        passwordLinkEndpoint: API.selfPwLink,
        showAdmin: true,
        showMaintenance: true,
      });
    },
  });
}


async function doLogout(){
  await logoutAndRedirect(API.logout);
}

function fmt(n){
  return (Number(n||0)).toLocaleString();
}
function fmtTs(s){
  if(!s) return "";
  return String(s).replace("T"," ").replace("Z","");
}
function fmtAge(sec){
  if(sec === null || sec === undefined) return "-";
  const s = Math.max(0, Number(sec||0));
  if(s < 60) return `${Math.floor(s)}s`;
  const m = Math.floor(s/60);
  const r = Math.floor(s%60);
  return `${m}m ${r}s`;
}

function setButtonsLocked({ reparseActive=false, rebuildActive=false }={}){
  const reparseBtn = $("startReparseBtn");
  const rebuildBtn = $("startRebuildBtn");

  const reparseLock = $("reparseLockReason");
  const rebuildLock = $("rebuildLockReason");

  if(reparseBtn){
    const busy = !!reparseActive || !!rebuildActive;
    reparseBtn.disabled = busy;
    reparseBtn.textContent = reparseActive ? "再解析（実行中）" : "再解析（全件）";
  }
  if(rebuildBtn){
    const busy = !!rebuildActive || !!reparseActive;
    rebuildBtn.disabled = busy;
    rebuildBtn.textContent = rebuildActive ? "統計再集計（実行中）" : "統計再集計";
  }

  if(reparseLock){
    reparseLock.textContent = rebuildActive ? "統計再集計が実行中のため開始できません。" : "";
  }
  if(rebuildLock){
    rebuildLock.textContent = reparseActive ? "再解析が実行中のため開始できません。" : "";
  }
}

function setProgress(fillEl, pct){
  const p = Math.max(0, Math.min(100, Number(pct||0)));
  if(fillEl) fillEl.style.width = `${p}%`;
}

function stateToStatusLabel(kind, state){
  const run = state?.run || null;
  const active = !!state?.active;
  const hbAge = state?.hb_age_sec ?? null;

  if(!run){
    return { text: "未実行", tone: "idle" };
  }
  if(run.status === "running"){
    if(active) return { text: "実行中", tone: "run" };
    // running flag but heartbeat too old / stalled
    return { text: `実行中（更新停止? hb=${fmtAge(hbAge)}）`, tone: "warn" };
  }
  if(run.status === "done"){
    return { text: "完了", tone: "done" };
  }
  return { text: "停止", tone: "warn" };
}

function renderHistory(el, history){
  if(!el) return;
  const rows = (history || []).slice(0, 5);
  if(!rows.length){
    el.textContent = "履歴はありません";
    return;
  }
  el.innerHTML = "";
  rows.forEach(r => {
    const line = document.createElement("div");
    line.className = "maintHistoryRow";
    const s = r.status === "running" ? "実行中" : (r.status === "done" ? "完了" : "停止");
    const created = fmtTs(r.created_at);
    const updated = fmtTs(r.updated_at);
    const processed = fmt(r.processed);
    const errors = fmt(r.error_count);
    const last = r.last_image_id !== undefined ? ` last=${fmt(r.last_image_id)}` : "";
    line.textContent = `#${r.id} ${s}  開始:${created}  更新:${updated}  処理:${processed}  失敗:${errors}${last}`;
    el.appendChild(line);
  });
}

function showToast(text){
  const t = document.createElement("div");
  t.className = "maintToast";
  t.textContent = text;
  document.body.appendChild(t);
  requestAnimationFrame(() => t.classList.add("show"));
  setTimeout(() => {
    t.classList.remove("show");
    setTimeout(() => t.remove(), 250);
  }, 3500);
}

function notifyIfAllowed(title, body){
  try{
    if(!("Notification" in window)) return;
    if(Notification.permission === "granted"){
      new Notification(title, { body });
    }
  }catch(_e){}
}

let _prevReparseStatus = null;
let _prevRebuildStatus = null;

function renderReparseState(state){
  const run = state?.run || null;

  const status = stateToStatusLabel("reparse", state);
  $("reparseStatus").textContent = status.text;

  $("reparseSummary").textContent = run
    ? `run_id=${run.id} / status=${run.status}`
    : "";

  $("reparseStarted").textContent = run ? fmtTs(run.created_at) : "-";
  $("reparseUpdated").textContent = run ? fmtTs(run.updated_at) : "-";

  const total = Number(state?.total_images || 0);
  const processed = Number(run?.processed || 0);
  const updated = Number(run?.updated || 0);
  const errors = Number(run?.error_count || 0);

  $("reparseCounts").textContent = (run ? `${fmt(processed)} (成功 ${fmt(updated)} / 失敗 ${fmt(errors)})` : "-");

  const afterId = Number(state?.after_id || 0);
  const maxId = Number(state?.max_image_id || 0);
  $("reparseCursor").textContent = (maxId > 0 ? `${fmt(afterId)} / ${fmt(maxId)}` : fmt(afterId));

  const skipped = state?.skipped_total ?? 0;
  $("reparseSkip").textContent = fmt(skipped);

  $("reparseHeartbeat").textContent = fmtAge(state?.hb_age_sec);

  const pct = (total > 0) ? (processed / total * 100.0) : 0;
  setProgress($("reparseProgressFill"), pct);
  $("reparseProgressText").textContent = (total > 0 && run) ? `進捗: ${pct.toFixed(1)}%（${fmt(processed)} / ${fmt(total)}）` : "";

  renderHistory($("reparseHistory"), state?.history || []);

  // completion notification (only on transition while this page is open)
  const nowKey = run ? `${run.id}:${run.status}:${state?.active ? 1:0}` : "none";
  if(_prevReparseStatus && _prevReparseStatus.includes(":running:") && nowKey.includes(":done:")){
    showToast("再解析が完了しました");
    notifyIfAllowed("NIM", "再解析が完了しました");
    try{ document.title = "✅ 完了 - Maintenance - NIM"; }catch(_e){}
  }
  _prevReparseStatus = nowKey;
}

function renderRebuildState(state){
  const run = state?.run || null;

  const status = stateToStatusLabel("rebuild", state);
  $("rebuildStatus").textContent = status.text;

  $("rebuildSummary").textContent = run
    ? `run_id=${run.id} / status=${run.status}`
    : "";

  $("rebuildStarted").textContent = run ? fmtTs(run.created_at) : "-";
  $("rebuildUpdated").textContent = run ? fmtTs(run.updated_at) : "-";

  const processed = Number(run?.processed || 0);
  const updated = Number(run?.updated || 0);
  const errors = Number(run?.error_count || 0);
  $("rebuildCounts").textContent = run ? `${fmt(processed)} (成功 ${fmt(updated)} / 失敗 ${fmt(errors)})` : "-";

  $("rebuildHeartbeat").textContent = fmtAge(state?.hb_age_sec);

  // rebuild has no total denominator; just show activity bar
  const pct = (run && run.status === "running") ? 35 : (run && run.status === "done" ? 100 : 0);
  setProgress($("rebuildProgressFill"), pct);
  $("rebuildProgressText").textContent = run ? `status=${run.status}` : "";

  renderHistory($("rebuildHistory"), state?.history || []);

  const nowKey = run ? `${run.id}:${run.status}:${state?.active ? 1:0}` : "none";
  if(_prevRebuildStatus && _prevRebuildStatus.includes(":running:") && nowKey.includes(":done:")){
    showToast("統計再集計が完了しました");
    notifyIfAllowed("NIM", "統計再集計が完了しました");
    try{ document.title = "✅ 完了 - Maintenance - NIM"; }catch(_e){}
  }
  _prevRebuildStatus = nowKey;
}

function renderReparseErrors(state){
  const tb = $("reparseErrTbody");
  if(!tb) return;
  tb.innerHTML = "";

  const errors = state?.errors || [];
  if(!errors.length){
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 4;
    td.className = "small";
    td.style.opacity = "0.85";
    td.textContent = "失敗はありません";
    tr.appendChild(td);
    tb.appendChild(tr);
    return;
  }

  errors.forEach(e => {
    const tr = document.createElement("tr");

    const tdId = document.createElement("td");
    tdId.textContent = String(e.image_id || "");

    const tdErr = document.createElement("td");
    const st = e.stage ? `[${e.stage}] ` : "";
    tdErr.textContent = st + (e.error || "");

    const tdTime = document.createElement("td");
    tdTime.textContent = fmtTs(e.created_at || "");

    const tdOps = document.createElement("td");
    tdOps.style.whiteSpace = "nowrap";
    tdOps.style.display = "flex";
    tdOps.style.gap = "8px";

    const image_id = parseInt(e.image_id || "0", 10) || 0;
    const isSkip = !!e.skip;

    const retryBtn = document.createElement("button");
    retryBtn.className = "primaryBtn smallBtn";
    retryBtn.textContent = "再試行";
    retryBtn.addEventListener("click", async () => {
      if(!image_id) return;
      retryBtn.disabled = true;
      try{
        const res = await apiFetch(API.reparseOne, {
          method: "POST",
          body: JSON.stringify({ image_id, clear_skip: isSkip ? 1 : 0 }),
        });
        const j = await apiJson(res);
        if(!j.ok){
          alert("再試行に失敗しました");
        }
        await refreshAll(true);
      }catch(_err){
        alert("再試行に失敗しました");
      }finally{
        retryBtn.disabled = false;
      }
    });

    const skipBtn = document.createElement("button");
    skipBtn.className = "ghostBtn smallBtn";
    skipBtn.textContent = isSkip ? "skip解除" : "skip";
    skipBtn.addEventListener("click", async () => {
      if(!image_id) return;
      try{
        await apiFetch(API.reparseSkip, {
          method: "POST",
          body: JSON.stringify({ image_id, skip: isSkip ? 0 : 1 }),
        });
        await refreshAll(true);
      }catch(_err){
        alert("skip更新に失敗しました");
      }
    });

    tdOps.appendChild(retryBtn);
    tdOps.appendChild(skipBtn);

    tr.appendChild(tdId);
    tr.appendChild(tdErr);
    tr.appendChild(tdTime);
    tr.appendChild(tdOps);
    tb.appendChild(tr);
  });
}

let _pollTimer = null;

async function startReparse(){
  $("reparseErr").textContent = "";
  try{
    const res = await apiFetch(API.reparseStart, { method: "POST" });
    await apiJson(res);
    await refreshAll(true);
  }catch(e){
    $("reparseErr").textContent = String(e.message || e);
  }
}

async function startRebuild(){
  $("rebuildErr").textContent = "";
  try{
    const res = await apiFetch(API.rebuildStart, { method: "POST" });
    await apiJson(res);
    await refreshAll(true);
  }catch(e){
    $("rebuildErr").textContent = String(e.message || e);
  }
}

async function loadReparseState(){
  const res = await apiFetch(API.reparseState);
  const s = await apiJson(res);
  renderReparseState(s);
  renderReparseErrors(s);
  return s;
}

async function loadRebuildState(){
  const res = await apiFetch(API.rebuildState);
  const s = await apiJson(res);
  renderRebuildState(s);
  return s;
}

async function refreshAll(force=false){
  const [reparseState, rebuildState] = await Promise.all([
    loadReparseState(),
    loadRebuildState(),
  ]);

  const reparseActive = !!reparseState?.active;
  const rebuildActive = !!rebuildState?.active;
  setButtonsLocked({ reparseActive, rebuildActive });

  if(force) return;

  if(reparseActive || rebuildActive){
    if(!_pollTimer){
      _pollTimer = setTimeout(async () => {
        _pollTimer = null;
        await refreshAll(false);
      }, 1000);
    }
  }else{
    // idle: slow polling so reopening isn't required to refresh
    if(!_pollTimer){
      _pollTimer = setTimeout(async () => {
        _pollTimer = null;
        await refreshAll(false);
      }, 5000);
    }
  }
}

function bindUI(){
  $("startReparseBtn")?.addEventListener("click", startReparse);
  $("startRebuildBtn")?.addEventListener("click", startRebuild);
}

async function boot(){
  bindUI();
  const me = await loadMe();
  if(!me) return;
  await refreshAll(true);
  await refreshAll(false);
}

boot();
