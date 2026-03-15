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

  derivativeFillStart: "/api/admin/derivative_fill_start",
  derivativeFillState: "/api/admin/derivative_fill_state",
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
  return Number(n || 0).toLocaleString();
}

function fmtTs(s){
  if(!s) return "";
  return String(s).replace("T", " ").replace("Z", "");
}

function fmtAge(sec){
  if(sec === null || sec === undefined) return "-";
  const s = Math.max(0, Number(sec || 0));
  if(s < 60) return `${Math.floor(s)}s`;
  const m = Math.floor(s / 60);
  const r = Math.floor(s % 60);
  return `${m}m ${r}s`;
}

function setButtonsLocked({ reparseActive=false, rebuildActive=false, derivativeFillActive=false } = {}){
  const reparseBtn = $("startReparseBtn");
  const rebuildBtn = $("startRebuildBtn");
  const derivativeFillBtn = $("startDerivativeFillBtn");

  const reparseLock = $("reparseLockReason");
  const rebuildLock = $("rebuildLockReason");
  const derivativeFillLock = $("derivativeFillLockReason");

  const reparseBusy = !!reparseActive || !!rebuildActive || !!derivativeFillActive;
  const rebuildBusy = !!rebuildActive || !!reparseActive || !!derivativeFillActive;
  const derivativeBusy = !!derivativeFillActive || !!reparseActive || !!rebuildActive;

  if(reparseBtn){
    reparseBtn.disabled = reparseBusy;
    reparseBtn.textContent = reparseActive ? "再解析（実行中）" : "再解析（全件）";
  }
  if(rebuildBtn){
    rebuildBtn.disabled = rebuildBusy;
    rebuildBtn.textContent = rebuildActive ? "統計再集計（実行中）" : "統計再集計";
  }
  if(derivativeFillBtn){
    derivativeFillBtn.disabled = derivativeBusy;
    derivativeFillBtn.textContent = derivativeFillActive ? "派生画像補完（実行中）" : "派生画像補完";
  }

  if(reparseLock){
    reparseLock.textContent = rebuildActive
      ? "統計再集計が実行中のため開始できません。"
      : (derivativeFillActive ? "派生画像補完が実行中のため開始できません。" : "");
  }
  if(rebuildLock){
    rebuildLock.textContent = reparseActive
      ? "再解析が実行中のため開始できません。"
      : (derivativeFillActive ? "派生画像補完が実行中のため開始できません。" : "");
  }
  if(derivativeFillLock){
    derivativeFillLock.textContent = reparseActive
      ? "再解析が実行中のため開始できません。"
      : (rebuildActive ? "統計再集計が実行中のため開始できません。" : "");
  }
}

function setProgress(fillEl, pct){
  const p = Math.max(0, Math.min(100, Number(pct || 0)));
  if(fillEl) fillEl.style.width = `${p}%`;
}

function stateToStatusLabel(state){
  const run = state?.run || null;
  const active = !!state?.active;
  const hbAge = state?.hb_age_sec ?? null;

  if(!run) return { text: "未実行", tone: "idle" };
  if(run.status === "running"){
    if(active) return { text: "実行中", tone: "run" };
    return { text: `実行中（更新停止? hb=${fmtAge(hbAge)}）`, tone: "warn" };
  }
  if(run.status === "done") return { text: "完了", tone: "done" };
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
  rows.forEach((row) => {
    const line = document.createElement("div");
    line.className = "maintHistoryRow";
    const status = row.status === "running" ? "実行中" : (row.status === "done" ? "完了" : "停止");
    const last = row.last_image_id !== undefined ? `  last=${fmt(row.last_image_id)}` : "";
    line.textContent = `#${row.id} ${status}  開始:${fmtTs(row.created_at)}  更新:${fmtTs(row.updated_at)}  処理:${fmt(row.processed)}  成功:${fmt(row.updated)}  失敗:${fmt(row.error_count)}${last}`;
    el.appendChild(line);
  });
}

function renderSimpleErrors(el, errors, emptyText){
  if(!el) return;
  const rows = (errors || []).slice(0, 100);
  if(!rows.length){
    el.textContent = emptyText;
    return;
  }

  el.innerHTML = "";
  rows.forEach((row) => {
    const line = document.createElement("div");
    line.className = "maintHistoryRow";
    const stage = row.stage ? `[${row.stage}] ` : "";
    const imageId = row.image_id ? `image_id=${row.image_id} ` : "";
    line.textContent = `${fmtTs(row.created_at)}  ${imageId}${stage}${row.error || "error"}`;
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

let prevReparseStatus = null;
let prevRebuildStatus = null;
let prevDerivativeFillStatus = null;

function trackCompletion(prevKey, nextKey, doneText){
  if(prevKey && prevKey.includes(":running:") && nextKey.includes(":done:")){
    showToast(doneText);
    notifyIfAllowed("NIM", doneText);
    try{ document.title = `✅ ${doneText} - Maintenance - NIM`; }catch(_e){}
  }
}

function renderReparseState(state){
  const run = state?.run || null;
  const status = stateToStatusLabel(state);

  $("reparseStatus").textContent = status.text;
  $("reparseSummary").textContent = run ? `run_id=${run.id} / status=${run.status}` : "";
  $("reparseStarted").textContent = run ? fmtTs(run.created_at) : "-";
  $("reparseUpdated").textContent = run ? fmtTs(run.updated_at) : "-";

  const total = Number(state?.total_images || 0);
  const processed = Number(run?.processed || 0);
  const updated = Number(run?.updated || 0);
  const errors = Number(run?.error_count || 0);

  $("reparseCounts").textContent = run ? `${fmt(processed)} (成功 ${fmt(updated)} / 失敗 ${fmt(errors)})` : "-";

  const afterId = Number(state?.after_id || 0);
  const maxId = Number(state?.max_image_id || 0);
  $("reparseCursor").textContent = maxId > 0 ? `${fmt(afterId)} / ${fmt(maxId)}` : fmt(afterId);
  $("reparseSkip").textContent = fmt(state?.skipped_total ?? 0);
  $("reparseHeartbeat").textContent = fmtAge(state?.hb_age_sec);

  const pct = total > 0 ? (processed / total * 100) : 0;
  setProgress($("reparseProgressFill"), pct);
  $("reparseProgressText").textContent = total > 0 && run ? `進捗: ${pct.toFixed(1)}%（${fmt(processed)} / ${fmt(total)}）` : "";

  renderHistory($("reparseHistory"), state?.history || []);

  const nextKey = run ? `${run.id}:${run.status}:${state?.active ? 1 : 0}` : "none";
  trackCompletion(prevReparseStatus, nextKey, "再解析が完了しました");
  prevReparseStatus = nextKey;
}

function renderRebuildState(state){
  const run = state?.run || null;
  const status = stateToStatusLabel(state);

  $("rebuildStatus").textContent = status.text;
  $("rebuildSummary").textContent = run ? `run_id=${run.id} / status=${run.status}` : "";
  $("rebuildStarted").textContent = run ? fmtTs(run.created_at) : "-";
  $("rebuildUpdated").textContent = run ? fmtTs(run.updated_at) : "-";

  const processed = Number(run?.processed || 0);
  const updated = Number(run?.updated || 0);
  const errors = Number(run?.error_count || 0);
  $("rebuildCounts").textContent = run ? `${fmt(processed)} (成功 ${fmt(updated)} / 失敗 ${fmt(errors)})` : "-";
  $("rebuildHeartbeat").textContent = fmtAge(state?.hb_age_sec);

  const pct = run && run.status === "running" ? 35 : (run && run.status === "done" ? 100 : 0);
  setProgress($("rebuildProgressFill"), pct);
  $("rebuildProgressText").textContent = run ? `status=${run.status}` : "";

  renderHistory($("rebuildHistory"), state?.history || []);

  const nextKey = run ? `${run.id}:${run.status}:${state?.active ? 1 : 0}` : "none";
  trackCompletion(prevRebuildStatus, nextKey, "統計再集計が完了しました");
  prevRebuildStatus = nextKey;
}

function renderDerivativeFillState(state){
  const run = state?.run || null;
  const status = stateToStatusLabel(state);

  $("derivativeFillStatus").textContent = status.text;
  $("derivativeFillSummary").textContent = run
    ? `run_id=${run.id} / status=${run.status} / サムネ未作成候補=${fmt(state?.grid_missing || 0)} / オーバーレイ未作成候補=${fmt(state?.overlay_missing || 0)}`
    : `サムネ未作成候補=${fmt(state?.grid_missing || 0)} / オーバーレイ未作成候補=${fmt(state?.overlay_missing || 0)}`;

  $("derivativeFillStarted").textContent = run ? fmtTs(run.created_at) : "-";
  $("derivativeFillUpdated").textContent = run ? fmtTs(run.updated_at) : "-";

  const total = Number(state?.total_images || 0);
  const processed = Number(run?.processed || 0);
  const updated = Number(run?.updated || 0);
  const errors = Number(run?.error_count || 0);
  $("derivativeFillCounts").textContent = run ? `${fmt(processed)} (生成 ${fmt(updated)} / 失敗 ${fmt(errors)})` : "-";

  const afterId = Number(state?.after_id || 0);
  const maxId = Number(state?.max_image_id || 0);
  $("derivativeFillCursor").textContent = maxId > 0 ? `${fmt(afterId)} / ${fmt(maxId)}` : fmt(afterId);
  $("derivativeFillMissingGrid").textContent = fmt(state?.grid_missing || 0);
  $("derivativeFillMissingOverlay").textContent = fmt(state?.overlay_missing || 0);
  $("derivativeFillHeartbeat").textContent = fmtAge(state?.hb_age_sec);

  const pct = total > 0 ? (processed / total * 100) : 0;
  setProgress($("derivativeFillProgressFill"), pct);
  $("derivativeFillProgressText").textContent = total > 0 && run ? `進捗: ${pct.toFixed(1)}%（${fmt(processed)} / ${fmt(total)}）` : "";

  renderHistory($("derivativeFillHistory"), state?.history || []);
  renderSimpleErrors($("derivativeFillErrors"), state?.errors || [], "失敗はありません");

  const nextKey = run ? `${run.id}:${run.status}:${state?.active ? 1 : 0}` : "none";
  trackCompletion(prevDerivativeFillStatus, nextKey, "派生画像補完が完了しました");
  prevDerivativeFillStatus = nextKey;
}

function renderReparseErrors(state){
  const tbody = $("reparseErrTbody");
  if(!tbody) return;
  tbody.innerHTML = "";

  const errors = state?.errors || [];
  if(!errors.length){
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 4;
    td.className = "small";
    td.style.opacity = "0.85";
    td.textContent = "失敗はありません";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  errors.forEach((row) => {
    const tr = document.createElement("tr");

    const tdId = document.createElement("td");
    tdId.textContent = String(row.image_id || "");

    const tdErr = document.createElement("td");
    const stage = row.stage ? `[${row.stage}] ` : "";
    tdErr.textContent = stage + (row.error || "");

    const tdTime = document.createElement("td");
    tdTime.textContent = fmtTs(row.created_at || "");

    const tdOps = document.createElement("td");
    tdOps.style.whiteSpace = "nowrap";
    tdOps.style.display = "flex";
    tdOps.style.gap = "8px";

    const imageId = parseInt(row.image_id || "0", 10) || 0;
    const isSkip = !!row.skip;

    const retryBtn = document.createElement("button");
    retryBtn.className = "primaryBtn smallBtn";
    retryBtn.textContent = "再試行";
    retryBtn.addEventListener("click", async () => {
      if(!imageId) return;
      retryBtn.disabled = true;
      try{
        const res = await apiFetch(API.reparseOne, {
          method: "POST",
          body: JSON.stringify({ image_id: imageId, clear_skip: isSkip ? 1 : 0 }),
        });
        const json = await apiJson(res);
        if(!json.ok) alert("再試行に失敗しました");
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
      if(!imageId) return;
      try{
        await apiFetch(API.reparseSkip, {
          method: "POST",
          body: JSON.stringify({ image_id: imageId, skip: isSkip ? 0 : 1 }),
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
    tbody.appendChild(tr);
  });
}

let pollTimer = null;

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

async function startDerivativeFill(){
  $("derivativeFillErr").textContent = "";
  try{
    const res = await apiFetch(API.derivativeFillStart, { method: "POST" });
    await apiJson(res);
    await refreshAll(true);
  }catch(e){
    $("derivativeFillErr").textContent = String(e.message || e);
  }
}

async function loadReparseState(){
  const res = await apiFetch(API.reparseState);
  const state = await apiJson(res);
  renderReparseState(state);
  renderReparseErrors(state);
  return state;
}

async function loadRebuildState(){
  const res = await apiFetch(API.rebuildState);
  const state = await apiJson(res);
  renderRebuildState(state);
  return state;
}

async function loadDerivativeFillState(){
  const res = await apiFetch(API.derivativeFillState);
  const state = await apiJson(res);
  renderDerivativeFillState(state);
  return state;
}

async function refreshAll(force = false){
  const [reparseState, rebuildState, derivativeFillState] = await Promise.all([
    loadReparseState(),
    loadRebuildState(),
    loadDerivativeFillState(),
  ]);

  const reparseActive = !!reparseState?.active;
  const rebuildActive = !!rebuildState?.active;
  const derivativeFillActive = !!derivativeFillState?.active;

  setButtonsLocked({ reparseActive, rebuildActive, derivativeFillActive });

  if(force) return;

  const delay = (reparseActive || rebuildActive || derivativeFillActive) ? 1000 : 5000;
  if(!pollTimer){
    pollTimer = setTimeout(async () => {
      pollTimer = null;
      await refreshAll(false);
    }, delay);
  }
}

function bindUI(){
  $("startReparseBtn")?.addEventListener("click", startReparse);
  $("startRebuildBtn")?.addEventListener("click", startRebuild);
  $("startDerivativeFillBtn")?.addEventListener("click", startDerivativeFill);
}

async function boot(){
  bindUI();
  const me = await loadMe();
  if(!me) return;
  await refreshAll(true);
  await refreshAll(false);
}

boot();
