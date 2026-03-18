const ext = globalThis.browser ?? globalThis.chrome;

const BUTTON_FLAG = 'data-nim-transfer-button';
const TOAST_CONTAINER_ID = 'nim-transfer-toast-container';
const MAIN_IMAGE_SELECTOR = '.display-grid-images img.image-grid-image';
const MAIN_CANVAS_SELECTOR = '.display-grid-images canvas';
const DEBUG_PREFIX = '[NIM Transfer]';
const BRIDGE_REQUEST_TYPE = 'NIM_TRANSFER_FETCH_BLOB_REQUEST';
const BRIDGE_RESPONSE_TYPE = 'NIM_TRANSFER_FETCH_BLOB_RESPONSE';
const BRIDGE_READY_TYPE = 'NIM_TRANSFER_BRIDGE_READY';
const BRIDGE_SCRIPT_ID = 'nim-transfer-page-bridge';

function log(...args) {
  try { console.log(DEBUG_PREFIX, ...args); } catch (_) {}
}
function warn(...args) {
  try { console.warn(DEBUG_PREFIX, ...args); } catch (_) {}
}

function messageRuntime(payload) {
  return new Promise((resolve) => {
    try {
      ext.runtime.sendMessage(payload, (response) => {
        const runtimeError = ext.runtime?.lastError;
        if (runtimeError) {
          resolve({ ok: false, code: 'RUNTIME_ERROR', message: runtimeError.message || 'Extension runtime error' });
          return;
        }
        resolve(response);
      });
    } catch (error) {
      resolve({ ok: false, code: 'RUNTIME_ERROR', message: String(error?.message || error || 'Extension runtime error') });
    }
  });
}

function getToastContainer() {
  let container = document.getElementById(TOAST_CONTAINER_ID);
  if (container) return container;
  container = document.createElement('div');
  container.id = TOAST_CONTAINER_ID;
  Object.assign(container.style, {
    position: 'fixed',
    top: '16px',
    right: '16px',
    zIndex: '2147483647',
    display: 'flex',
    flexDirection: 'column',
    gap: '8px',
  });
  document.documentElement.appendChild(container);
  return container;
}

function showToast(message, kind = 'info') {
  const container = getToastContainer();
  const toast = document.createElement('div');
  toast.textContent = String(message || '');
  Object.assign(toast.style, {
    maxWidth: '320px',
    padding: '10px 12px',
    borderRadius: '10px',
    fontSize: '13px',
    lineHeight: '1.4',
    boxShadow: '0 8px 20px rgba(0,0,0,0.25)',
    color: '#ffffff',
    background: kind === 'error'
      ? 'rgba(165, 28, 48, 0.92)'
      : kind === 'success'
        ? 'rgba(13, 102, 60, 0.92)'
        : 'rgba(30, 41, 59, 0.92)',
  });
  container.appendChild(toast);
  window.setTimeout(() => {
    toast.remove();
    if (!container.childElementCount) container.remove();
  }, kind === 'error' ? 5200 : 3600);
}

async function canvasToBlob(canvas) {
  return await new Promise((resolve, reject) => {
    canvas.toBlob((blob) => {
      if (blob) resolve(blob);
      else reject(new Error('canvas から画像化できませんでした'));
    }, 'image/png');
  });
}

function getMainMediaElement() {
  return document.querySelector(MAIN_IMAGE_SELECTOR) || document.querySelector(MAIN_CANVAS_SELECTOR) || null;
}

function findDisplayPanelRoot() {
  const media = getMainMediaElement();
  if (!(media instanceof HTMLElement)) return null;

  let node = media.parentElement;
  while (node && node !== document.body) {
    const hasImages = Array.from(node.children).some((child) => child instanceof HTMLElement && child.classList.contains('display-grid-images'));
    const hasBottom = Array.from(node.children).some((child) => child instanceof HTMLElement && child.classList.contains('display-grid-bottom'));
    if (hasImages && hasBottom) return node;
    node = node.parentElement;
  }
  return null;
}

function getBottomToolbar() {
  const root = findDisplayPanelRoot();
  if (!root) return null;
  return Array.from(root.children).find((child) => child instanceof HTMLElement && child.classList.contains('display-grid-bottom')) || null;
}

function getSeedButton(toolbar) {
  if (!(toolbar instanceof HTMLElement)) return null;
  const buttons = Array.from(toolbar.querySelectorAll('button'));
  return buttons.find((button) => {
    const text = (button.textContent || '').replace(/\s+/g, ' ').trim();
    return /\b\d{6,}\b/.test(text) && /シード値をコピー/.test(text);
  }) || buttons.find((button) => /\b\d{6,}\b/.test((button.textContent || '').replace(/\s+/g, ' ').trim())) || null;
}

function inferSeedText() {
  const seedButton = getSeedButton(getBottomToolbar());
  const text = String(seedButton?.textContent || '').replace(/\s+/g, ' ').trim();
  const match = text.match(/\b(\d{6,})\b/);
  return match ? match[1] : '';
}

function inferFilenameFromPage(mimeType) {
  const extByType = { 'image/png': 'png', 'image/jpeg': 'jpg', 'image/webp': 'webp', 'image/avif': 'avif' };
  const extName = extByType[String(mimeType || '').toLowerCase()] || 'png';
  const seed = inferSeedText();
  return seed ? `novelai_${seed}.${extName}` : `novelai_${Date.now()}.${extName}`;
}

function injectBridgeScript() {
  if (document.getElementById(BRIDGE_SCRIPT_ID)) return;
  const script = document.createElement('script');
  script.id = BRIDGE_SCRIPT_ID;
  script.src = ext.runtime.getURL('page-bridge.js');
  script.async = false;
  (document.head || document.documentElement).appendChild(script);
}

function waitForBridgeReady(timeoutMs = 5000) {
  injectBridgeScript();
  return new Promise((resolve, reject) => {
    let done = false;
    const timer = window.setTimeout(() => {
      if (done) return;
      done = true;
      window.removeEventListener('message', onMessage);
      reject(new Error('NovelAI ページ応答がタイムアウトしました'));
    }, timeoutMs);
    const onMessage = (event) => {
      if (event.source !== window) return;
      if (event.data?.type !== BRIDGE_READY_TYPE) return;
      if (done) return;
      done = true;
      window.clearTimeout(timer);
      window.removeEventListener('message', onMessage);
      resolve();
    };
    window.addEventListener('message', onMessage);
    window.postMessage({ type: BRIDGE_READY_TYPE, ping: true }, '*');
  });
}

function requestBlobFromPage(blobUrl, timeoutMs = 15000) {
  injectBridgeScript();
  return new Promise((resolve, reject) => {
    const requestId = `nim-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    let done = false;
    const timer = window.setTimeout(() => {
      if (done) return;
      done = true;
      window.removeEventListener('message', onMessage);
      reject(new Error('NovelAI ページ応答がタイムアウトしました'));
    }, timeoutMs);
    const onMessage = (event) => {
      if (event.source !== window) return;
      const data = event.data || {};
      if (data.type !== BRIDGE_RESPONSE_TYPE || data.requestId !== requestId) return;
      if (done) return;
      done = true;
      window.clearTimeout(timer);
      window.removeEventListener('message', onMessage);
      if (!data.ok) {
        reject(new Error(String(data.message || 'blob fetch failed')));
        return;
      }
      resolve({ dataUrl: String(data.dataUrl || ''), mimeType: String(data.mimeType || '') });
    };
    window.addEventListener('message', onMessage);
    window.postMessage({ type: BRIDGE_REQUEST_TYPE, requestId, blobUrl }, '*');
  });
}

function dataUrlToBytes(dataUrl) {
  const match = /^data:([^;,]+)?(;base64)?,(.*)$/i.exec(String(dataUrl || ''));
  if (!match) throw new Error('ページから画像データを取得できませんでした');
  const mimeType = match[1] || 'application/octet-stream';
  const base64Flag = !!match[2];
  const payload = match[3] || '';
  const raw = base64Flag ? atob(payload) : decodeURIComponent(payload);
  const bytes = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i += 1) bytes[i] = raw.charCodeAt(i);
  return { bytes, mimeType };
}

async function extractCurrentImagePayload() {
  const img = document.querySelector(MAIN_IMAGE_SELECTOR);
  if (img instanceof HTMLImageElement) {
    const src = String(img.currentSrc || img.src || '').trim();
    log('selected image src', src || '(empty)');
    if (!src) throw new Error('画像 URL が見つかりません');
    if (src.startsWith('blob:')) {
      await waitForBridgeReady();
      const bridged = await requestBlobFromPage(src);
      const parsed = dataUrlToBytes(bridged.dataUrl);
      return {
        bytes: parsed.bytes,
        mimeType: bridged.mimeType || parsed.mimeType || 'image/png',
        filename: inferFilenameFromPage(bridged.mimeType || parsed.mimeType || 'image/png'),
        lastModifiedMs: Date.now(),
      };
    }
    const response = await fetch(src, { credentials: 'include' });
    if (!response.ok) throw new Error(`blob fetch failed (${response.status})`);
    const blob = await response.blob();
    return {
      bytes: new Uint8Array(await blob.arrayBuffer()),
      mimeType: blob.type || 'image/png',
      filename: inferFilenameFromPage(blob.type || 'image/png'),
      lastModifiedMs: Date.now(),
    };
  }
  const canvas = document.querySelector(MAIN_CANVAS_SELECTOR);
  if (canvas instanceof HTMLCanvasElement) {
    log('selected canvas size', `${canvas.width}x${canvas.height}`);
    const blob = await canvasToBlob(canvas);
    return {
      bytes: new Uint8Array(await blob.arrayBuffer()),
      mimeType: blob.type || 'image/png',
      filename: inferFilenameFromPage(blob.type || 'image/png'),
      lastModifiedMs: Date.now(),
    };
  }
  throw new Error('表示中の画像が見つかりません');
}

function createTransferIcon() {
  const svgNs = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(svgNs, 'svg');
  svg.setAttribute('viewBox', '0 0 24 24');
  svg.setAttribute('width', '16');
  svg.setAttribute('height', '16');
  svg.setAttribute('fill', 'none');
  svg.setAttribute('stroke', 'currentColor');
  svg.setAttribute('stroke-width', '2');
  svg.setAttribute('stroke-linecap', 'round');
  svg.setAttribute('stroke-linejoin', 'round');
  svg.setAttribute('aria-hidden', 'true');

  const path1 = document.createElementNS(svgNs, 'path');
  path1.setAttribute('d', 'M22 2 11 13');
  svg.appendChild(path1);

  const path2 = document.createElementNS(svgNs, 'path');
  path2.setAttribute('d', 'M22 2 15 22 11 13 2 9 22 2z');
  svg.appendChild(path2);

  return svg;
}

function createTransferButton(referenceButton) {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = String(referenceButton?.className || '').trim() || 'nim-transfer-button';
  button.setAttribute(BUTTON_FLAG, '1');
  button.setAttribute('aria-label', 'Transfer to NIM');
  button.setAttribute('title', 'Transfer to NIM');
  button.classList.add('nim-transfer-button');
  button.appendChild(createTransferIcon());
  button.style.display = 'inline-flex';
  button.style.alignItems = 'center';
  button.style.justifyContent = 'center';
  button.style.minWidth = '32px';
  button.style.minHeight = '32px';
  button.addEventListener('click', (event) => {
    event.preventDefault();
    event.stopPropagation();
    handleTransferClick(button).catch((error) => {
      warn('transfer click failed', error);
      showToast(String(error?.message || error || '登録に失敗しました'), 'error');
    });
  });
  return button;
}

function removeMisplacedButtons() {
  document.querySelectorAll(`button[${BUTTON_FLAG}="1"]`).forEach((button) => {
    const toolbar = button.closest('.display-grid-bottom');
    if (!toolbar) button.remove();
  });
}

function attachBottomTransferButton() {
  removeMisplacedButtons();
  const toolbar = getBottomToolbar();
  if (!(toolbar instanceof HTMLElement)) {
    return { ok: false, reason: 'no-toolbar' };
  }
  const existing = toolbar.querySelector(`button[${BUTTON_FLAG}="1"]`);
  if (existing) return { ok: true, reason: 'existing' };
  const seedButton = getSeedButton(toolbar);
  if (!(seedButton instanceof HTMLButtonElement)) {
    return { ok: false, reason: 'no-seed-button' };
  }
  const actionsGroup = seedButton.parentElement;
  if (!(actionsGroup instanceof HTMLElement)) {
    return { ok: false, reason: 'no-actions-group' };
  }
  const referenceButton = Array.from(actionsGroup.querySelectorAll('button')).find((button) => button !== seedButton) || seedButton;
  const transferButton = createTransferButton(referenceButton);
  seedButton.insertAdjacentElement('beforebegin', transferButton);
  return { ok: true, reason: 'attached' };
}

async function handleTransferClick(button) {
  button.disabled = true;
  button.style.opacity = '0.7';
  showToast('Transferring...', 'info');
  try {
    const payload = await extractCurrentImagePayload();
    const response = await messageRuntime({
      type: 'nim-upload-image',
      payload: {
        bytes: Array.from(payload.bytes),
        mimeType: payload.mimeType,
        filename: payload.filename,
        lastModifiedMs: payload.lastModifiedMs,
      },
    });
    if (!response?.ok) {
      if (response?.code === 'AUTH_REQUIRED') {
        showToast(response.message || 'Login required', 'error');
        await messageRuntime({ type: 'nim-open-options' });
        return;
      }
      throw new Error(response?.message || '登録に失敗しました');
    }
    showToast(response.message || 'Transferred', 'success');
  } finally {
    button.disabled = false;
    button.style.opacity = '1';
  }
}

function installObservers() {
  injectBridgeScript();

  let retryCount = 0;
  const maxRetries = 60;
  let retryTimer = 0;
  let observerTimer = 0;
  let lastStatus = '';

  const reportStatus = (result) => {
    const status = `${result.ok ? 'ok' : 'ng'}:${result.reason}`;
    if (status === lastStatus) return;
    lastStatus = status;
    if (result.reason === 'attached') {
      log('transfer button attached to display-grid-bottom');
      return;
    }
    if (result.reason === 'existing') return;
    if (result.reason === 'no-seed-button') {
      warn('seed button not found in display-grid-bottom');
      return;
    }
    if (result.reason === 'no-actions-group') {
      warn('seed button parent not found');
    }
  };

  const runAttach = () => {
    const result = attachBottomTransferButton();
    reportStatus(result);
    if (!result.ok && retryCount < maxRetries) {
      retryCount += 1;
      retryTimer = window.setTimeout(runAttach, 500);
      return;
    }
    retryTimer = 0;
  };

  const scheduleAttachFromObserver = () => {
    if (observerTimer) return;
    observerTimer = window.setTimeout(() => {
      observerTimer = 0;
      if (retryTimer) {
        window.clearTimeout(retryTimer);
        retryTimer = 0;
      }
      retryCount = 0;
      runAttach();
    }, 50);
  };

  const observer = new MutationObserver(() => {
    scheduleAttachFromObserver();
  });
  observer.observe(document.documentElement, { childList: true, subtree: true });
  runAttach();
}

log('content script loaded');
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', installObservers, { once: true });
} else {
  installObservers();
}
