const ext = globalThis.browser ?? globalThis.chrome;

function msg(key, substitutions) {
  const value = ext.i18n?.getMessage(key, substitutions);
  return value || key;
}

function errorText(code, fallbackKey = 'error_UNKNOWN') {
  const key = `error_${String(code || 'UNKNOWN').replace(/[^A-Za-z0-9_]/g, '_')}`;
  const value = msg(key);
  return value !== key ? value : msg(fallbackKey);
}

const BUTTON_FLAG = 'data-nim-transfer-button';
const TOAST_CONTAINER_ID = 'nim-transfer-toast-container';
const MAIN_IMAGE_SELECTOR = '.display-grid-images img.image-grid-image';
const MAIN_CANVAS_SELECTOR = '.display-grid-images canvas';
const DEBUG_PREFIX = '[NIM Transfer]';
const BRIDGE_REQUEST_TYPE = 'NIM_TRANSFER_FETCH_BLOB_REQUEST';
const BRIDGE_RESPONSE_TYPE = 'NIM_TRANSFER_FETCH_BLOB_RESPONSE';
const BRIDGE_READY_TYPE = 'NIM_TRANSFER_BRIDGE_READY';
const BRIDGE_SCRIPT_ID = 'nim-transfer-page-bridge';
const OVERLAY_HOST_ID = 'nim-overlay-extension-host';
const CONFIG_STORAGE_KEYS = {
  showNovelAiMenu: 'nim.show_novelai_menu',
  autoTransfer: 'nim.auto_transfer',
};
const CLOSE_MESSAGE_TYPES = new Set(['NIM_OVERLAY_CLOSE', 'nim-overlay-close']);
const AUTH_REQUIRED_MESSAGE_TYPES = new Set(['NIM_EMBED_AUTH_REQUIRED']);
const READY_MESSAGE_TYPES = new Set(['NIM_EMBED_READY']);

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
          resolve({ ok: false, code: 'RUNTIME_ERROR' });
          return;
        }
        resolve(response);
      });
    } catch (error) {
      resolve({ ok: false, code: 'RUNTIME_ERROR' });
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
      else reject(new Error(errorText('CANVAS_TO_BLOB_FAILED')));
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
    return /\b\d{6,}\b/.test(text) && /(シード値をコピー|Copy seed)/i.test(text);
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
      reject(new Error(errorText('PAGE_RESPONSE_TIMEOUT')));
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
      reject(new Error(errorText('PAGE_RESPONSE_TIMEOUT')));
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
  if (!match) throw new Error(errorText('PAGE_IMAGE_DATA_UNAVAILABLE'));
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
    if (!src) throw new Error(errorText('IMAGE_URL_NOT_FOUND'));
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
  throw new Error(errorText('VISIBLE_IMAGE_NOT_FOUND'));
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
  button.setAttribute('aria-label', msg('transferToNim'));
  button.setAttribute('title', msg('transferToNim'));
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
      showToast(String(error?.message || error || errorText('UPLOAD_FAILED')), 'error');
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
  await transferCurrentImage({ button, showProgressToast: true, showSuccessToast: true });
}

function createOverlayState() {
  return {
    host: null,
    shadowRoot: null,
    launcherButton: null,
    overlay: null,
    overlayToolbar: null,
    overlayMenuButton: null,
    overlayMenu: null,
    overlayTitle: null,
    iframe: null,
    loading: null,
    currentUrl: '',
    currentOrigin: '',
    loginUrl: '',
    autoLoginHintShown: false,
    isMenuOpen: false,
    isOpen: false,
    isReady: false,
  };
}

const overlayState = createOverlayState();

const extensionConfig = {
  showNovelAiMenu: true,
  autoTransfer: false,
};

const autoTransferState = {
  inFlight: false,
  scheduled: 0,
  attemptedSignatures: new Set(),
};

function rememberAttemptedSignature(signature) {
  if (!signature) return;
  autoTransferState.attemptedSignatures.add(signature);
  if (autoTransferState.attemptedSignatures.size <= 128) return;
  const oldest = autoTransferState.attemptedSignatures.values().next().value;
  if (oldest) autoTransferState.attemptedSignatures.delete(oldest);
}

function applyExtensionConfig(config) {
  extensionConfig.showNovelAiMenu = config?.showNovelAiMenu !== false;
  extensionConfig.autoTransfer = config?.autoTransfer === true;
  updateLauncherVisibility();
}

async function refreshExtensionConfig() {
  const response = await messageRuntime({ type: 'nim-get-config' });
  if (!response?.ok) return null;
  const config = response.config || {};
  applyExtensionConfig(config);
  return config;
}

function updateLauncherVisibility() {
  if (!overlayState.launcherButton) return;
  overlayState.launcherButton.hidden = overlayState.isOpen || !extensionConfig.showNovelAiMenu;
}

function getCurrentImageSignature() {
  const img = document.querySelector(MAIN_IMAGE_SELECTOR);
  if (img instanceof HTMLImageElement) {
    const src = String(img.currentSrc || img.src || '').trim();
    if (!src) return '';
    return `img:${src}`;
  }
  const canvas = document.querySelector(MAIN_CANVAS_SELECTOR);
  if (canvas instanceof HTMLCanvasElement) {
    if (!canvas.width || !canvas.height) return '';
    const seed = inferSeedText();
    return `canvas:${canvas.width}x${canvas.height}:${seed}`;
  }
  return '';
}

async function transferCurrentImage({ button = null, showProgressToast = true, showSuccessToast = true } = {}) {
  if (button) {
    button.disabled = true;
    button.style.opacity = '0.7';
  }
  if (showProgressToast) {
    showToast(msg('transferring'), 'info');
  }
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
        showToast(errorText(response?.code || 'AUTH_REQUIRED'), 'error');
        await messageRuntime({ type: 'nim-open-options' });
        return { ok: false, code: response?.code || 'AUTH_REQUIRED' };
      }
      throw new Error(errorText(response?.code || 'UPLOAD_FAILED'));
    }
    if (showSuccessToast) {
      showToast(msg('transferSuccess'), 'success');
    }
    return response;
  } finally {
    if (button) {
      button.disabled = false;
      button.style.opacity = '1';
    }
  }
}

async function maybeAutoTransfer() {
  if (!extensionConfig.autoTransfer) return;
  const signature = getCurrentImageSignature();
  if (!signature) return;
  if (autoTransferState.attemptedSignatures.has(signature)) return;
  if (autoTransferState.inFlight) return;

  autoTransferState.inFlight = true;
  rememberAttemptedSignature(signature);
  try {
    await transferCurrentImage({ showProgressToast: false, showSuccessToast: true });
  } catch (error) {
    warn('auto transfer failed', error);
    showToast(String(error?.message || error || errorText('UPLOAD_FAILED')), 'error');
  } finally {
    autoTransferState.inFlight = false;
  }
}

function scheduleAutoTransfer() {
  if (!extensionConfig.autoTransfer) return;
  if (autoTransferState.scheduled) return;
  autoTransferState.scheduled = window.setTimeout(() => {
    autoTransferState.scheduled = 0;
    maybeAutoTransfer().catch((error) => {
      warn('scheduled auto transfer failed', error);
    });
  }, 250);
}


function ensureOverlayHost() {
  const existingHost = document.getElementById(OVERLAY_HOST_ID);
  if (existingHost && overlayState.isReady) {
    return overlayState;
  }

  const host = existingHost || document.createElement('div');
  if (!existingHost) {
    host.id = OVERLAY_HOST_ID;
    document.documentElement.appendChild(host);
  }

  const shadowRoot = host.shadowRoot || host.attachShadow({ mode: 'open' });
  if (!shadowRoot.childNodes.length) {
    shadowRoot.innerHTML = `
      <style>
        :host {
          all: initial;
        }
        .nim-ui {
          position: fixed;
          inset: 0;
          z-index: 2147483646;
          pointer-events: none;
          font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
          color: #e5eefb;
        }
        .launcher {
          position: fixed;
          top: 12px;
          left: 12px;
          pointer-events: auto;
          display: inline-flex;
          align-items: center;
          gap: 8px;
          min-height: 36px;
          padding: 0 12px;
          border: 1px solid rgba(255,255,255,0.18);
          border-radius: 999px;
          background: rgba(15, 23, 42, 0.72);
          color: #f8fafc;
          backdrop-filter: blur(10px);
          -webkit-backdrop-filter: blur(10px);
          box-shadow: 0 12px 28px rgba(15, 23, 42, 0.35);
          cursor: pointer;
          font-size: 12px;
          font-weight: 700;
          transition: transform 120ms ease, background 120ms ease, opacity 120ms ease;
        }
        .launcher:hover {
          background: rgba(15, 23, 42, 0.82);
          transform: translateY(-1px);
        }
        .launcher[hidden] {
          display: none;
        }
        .overlay {
          position: fixed;
          inset: 0;
          display: none;
          pointer-events: auto;
          background: rgba(2, 6, 23, 0.58);
          backdrop-filter: blur(2px);
          -webkit-backdrop-filter: blur(2px);
        }
        .overlay[data-open="1"] {
          display: block;
        }
        .frame {
          position: absolute;
          inset: 0;
          width: 100%;
          height: 100%;
          border: 0;
          background: #050b17;
        }
        .toolbar {
          position: absolute;
          top: 12px;
          left: 12px;
          z-index: 2;
          display: flex;
          align-items: flex-start;
          gap: 8px;
          pointer-events: auto;
        }
        .toolbarLeft {
          position: relative;
          display: flex;
          align-items: flex-start;
          gap: 8px;
        }
        .toolbarButton,
        .toolbarLink {
          display: inline-flex;
          align-items: center;
          justify-content: center;
          min-height: 36px;
          padding: 0 12px;
          border-radius: 999px;
          border: 1px solid rgba(255,255,255,0.18);
          background: rgba(15, 23, 42, 0.76);
          color: #f8fafc;
          text-decoration: none;
          cursor: pointer;
          box-shadow: 0 10px 24px rgba(15, 23, 42, 0.25);
          font-size: 12px;
          font-weight: 700;
          backdrop-filter: blur(10px);
          -webkit-backdrop-filter: blur(10px);
          white-space: nowrap;
        }
        .toolbarButton:hover,
        .toolbarLink:hover {
          background: rgba(15, 23, 42, 0.88);
        }
        .toolbarButton[data-action="close"] {
          min-width: 72px;
        }
        .toolbarButton[data-action="reload"],
        .toolbarButton[data-action="toggle-menu"] {
          min-width: 36px;
          width: 36px;
          padding: 0;
          font-size: 16px;
          line-height: 1;
        }
        .toolbarMenu {
          position: absolute;
          top: 44px;
          right: 0;
          display: none;
          flex-direction: column;
          gap: 8px;
          width: max-content;
          min-width: 112px;
          padding: 8px;
          border: 1px solid rgba(255,255,255,0.14);
          border-radius: 16px;
          background: rgba(15, 23, 42, 0.86);
          box-shadow: 0 18px 36px rgba(15, 23, 42, 0.34);
          backdrop-filter: blur(12px);
          -webkit-backdrop-filter: blur(12px);
        }
        .toolbarMenu[data-open="1"] {
          display: flex;
        }
        .toolbarMenu .toolbarButton {
          width: 100%;
          justify-content: flex-start;
        }
        .toolbarTitle {
          display: none;
        }
        .loading {
          position: absolute;
          inset: 0;
          display: flex;
          align-items: center;
          justify-content: center;
          z-index: 1;
          background: linear-gradient(180deg, rgba(2,6,23,0.42), rgba(2,6,23,0.2));
          color: #e2e8f0;
          font-size: 14px;
          letter-spacing: 0.01em;
          pointer-events: none;
        }
        .loading[hidden] {
          display: none;
        }
      </style>
      <div class="nim-ui">
        <button type="button" class="launcher" aria-label="${msg('openNim')}">NIM</button>
        <div class="overlay" data-open="0" aria-hidden="true">
          <iframe class="frame" referrerpolicy="strict-origin-when-cross-origin"></iframe>
          <div class="loading">${msg('overlayLoading')}</div>
          <div class="toolbar">
            <div class="toolbarLeft">
              <button type="button" class="toolbarButton" data-action="close">${msg('overlayClose')}</button>
              <button type="button" class="toolbarButton" data-action="reload" aria-label="${msg('overlayReload')}" title="${msg('overlayReload')}">⟳</button>
              <button type="button" class="toolbarButton" data-action="toggle-menu" aria-label="${msg('overlayMenu')}" aria-expanded="false">⋯</button>
              <div class="toolbarMenu" data-open="0">
                <button type="button" class="toolbarButton" data-action="login">${msg('overlayLogin')}</button>
                <button type="button" class="toolbarButton" data-action="settings">${msg('overlaySettings')}</button>
                <button type="button" class="toolbarButton" data-action="newtab">${msg('overlayNewTab')}</button>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  overlayState.host = host;
  overlayState.shadowRoot = shadowRoot;
  overlayState.launcherButton = shadowRoot.querySelector('.launcher');
  overlayState.overlay = shadowRoot.querySelector('.overlay');
  overlayState.overlayToolbar = shadowRoot.querySelector('.toolbar');
  overlayState.overlayMenuButton = shadowRoot.querySelector('[data-action="toggle-menu"]');
  overlayState.overlayMenu = shadowRoot.querySelector('.toolbarMenu');
  overlayState.overlayTitle = shadowRoot.querySelector('.toolbarTitle');
  overlayState.iframe = shadowRoot.querySelector('.frame');
  overlayState.loading = shadowRoot.querySelector('.loading');

  function setOverlayMenuOpen(isOpen) {
    overlayState.isMenuOpen = Boolean(isOpen);
    if (overlayState.overlayMenu) {
      overlayState.overlayMenu.setAttribute('data-open', isOpen ? '1' : '0');
    }
    if (overlayState.overlayMenuButton) {
      overlayState.overlayMenuButton.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
    }
  }

  if (!overlayState.launcherButton.dataset.bound) {
    overlayState.launcherButton.dataset.bound = '1';
    overlayState.launcherButton.addEventListener('click', (event) => {
      event.preventDefault();
      openOverlay().catch((error) => {
        warn('overlay open failed', error);
        showToast(String(error?.message || error || errorText('NIM_OPEN_FAILED')), 'error');
      });
    });
  }

  shadowRoot.querySelectorAll('.toolbarButton').forEach((button) => {
    if (button.dataset.bound) return;
    button.dataset.bound = '1';
    button.addEventListener('click', (event) => {
      event.preventDefault();
      const action = button.getAttribute('data-action');
      if (action === 'close') {
        closeOverlay();
        return;
      }
      if (action === 'toggle-menu') {
        setOverlayMenuOpen(!overlayState.isMenuOpen);
        return;
      }
      if (action === 'login') {
        const targetUrl = overlayState.loginUrl || overlayState.currentUrl;
        if (!targetUrl) {
          showToast(errorText('LOGIN_URL_NOT_CONFIGURED'), 'error');
          return;
        }
        setOverlayMenuOpen(false);
        messageRuntime({ type: 'nim-open-url', url: targetUrl }).catch(() => {
          showToast(errorText('LOGIN_PAGE_OPEN_FAILED'), 'error');
        });
        return;
      }
      if (action === 'reload') {
        if (!overlayState.currentUrl || !overlayState.iframe) {
          showToast(errorText('RELOAD_FAILED'), 'error');
          return;
        }
        setOverlayMenuOpen(false);
        if (overlayState.loading) overlayState.loading.hidden = false;
        overlayState.iframe.src = overlayState.currentUrl;
        return;
      }
      if (action === 'settings') {
        setOverlayMenuOpen(false);
        messageRuntime({ type: 'nim-open-options' }).catch(() => {});
        return;
      }
      if (action === 'newtab') {
        if (!overlayState.currentUrl) {
          showToast(errorText('OVERLAY_URL_NOT_CONFIGURED'), 'error');
          return;
        }
        setOverlayMenuOpen(false);
        messageRuntime({ type: 'nim-open-url', url: overlayState.currentUrl }).catch(() => {
          showToast(errorText('OPEN_NEW_TAB_FAILED'), 'error');
        });
      }
    });
  });

  if (!overlayState.iframe.dataset.bound) {
    overlayState.iframe.dataset.bound = '1';
    overlayState.iframe.addEventListener('load', () => {
      if (overlayState.loading) overlayState.loading.hidden = true;
    });
  }

  if (!window.__nimOverlayClickBound) {
    window.__nimOverlayClickBound = true;
    window.addEventListener('pointerdown', (event) => {
      if (!overlayState.isOpen || !overlayState.isMenuOpen) return;
      const path = typeof event.composedPath === 'function' ? event.composedPath() : [];
      if (path.includes(overlayState.overlayMenu) || path.includes(overlayState.overlayMenuButton)) return;
      setOverlayMenuOpen(false);
    }, true);
  }

  if (!window.__nimOverlayCloseListenerInstalled) {
    window.__nimOverlayCloseListenerInstalled = true;
    window.addEventListener('message', (event) => {
      const iframeWindow = overlayState.iframe?.contentWindow;
      if (!iframeWindow || event.source !== iframeWindow) return;
      const type = String(event.data?.type || '');
      if (overlayState.currentOrigin && event.origin && event.origin !== overlayState.currentOrigin) return;
      if (CLOSE_MESSAGE_TYPES.has(type)) {
        closeOverlay();
        return;
      }
      if (AUTH_REQUIRED_MESSAGE_TYPES.has(type)) {
        overlayState.loginUrl = String(event.data?.loginUrl || overlayState.currentUrl || '').trim();
        if (!overlayState.autoLoginHintShown) {
          overlayState.autoLoginHintShown = true;
          showToast(msg('toast_login_in_other_tab'), 'info');
        }
        return;
      }
      if (READY_MESSAGE_TYPES.has(type)) {
        overlayState.loginUrl = '';
        return;
      }
    });

    window.addEventListener('keydown', (event) => {
      if (event.key === 'Escape' && overlayState.isOpen) {
        closeOverlay();
      }
    });
  }

  overlayState.isReady = true;
  return overlayState;
}

function setOverlayOpen(isOpen) {
  ensureOverlayHost();
  overlayState.isOpen = Boolean(isOpen);
  if (!overlayState.isOpen) {
    overlayState.isMenuOpen = false;
  }
  overlayState.overlay?.setAttribute('data-open', isOpen ? '1' : '0');
  if (!isOpen) {
    overlayState.overlayMenu?.setAttribute('data-open', '0');
    overlayState.overlayMenuButton?.setAttribute('aria-expanded', 'false');
  }
  overlayState.overlay?.setAttribute('aria-hidden', isOpen ? 'false' : 'true');
  updateLauncherVisibility();
}

function closeOverlay() {
  if (!overlayState.isReady) return;
  setOverlayOpen(false);
}

function resolveOverlayUrl(config) {
  return String(config?.baseUrl || '').trim();
}

function updateOverlayTitle(url) {
  if (!overlayState.overlayTitle) return;
  if (!url) {
    overlayState.overlayTitle.textContent = msg('overlayTitle');
    return;
  }
  try {
    const parsed = new URL(url);
    const path = `${parsed.pathname || '/'}${parsed.search || ''}`;
    overlayState.overlayTitle.textContent = `${parsed.host}${path}`;
  } catch (_) {
    overlayState.overlayTitle.textContent = url;
  }
}

async function openOverlay() {
  ensureOverlayHost();

  const response = await messageRuntime({ type: 'nim-get-config' });
  if (!response?.ok) {
    throw new Error(errorText(response?.code || 'GET_CONFIG_FAILED'));
  }

  const config = response.config || {};
  const overlayUrl = resolveOverlayUrl(config);
  if (!overlayUrl) {
    showToast(msg('toast_save_domain_first'), 'error');
    await messageRuntime({ type: 'nim-open-options' });
    return;
  }

  const session = await messageRuntime({ type: 'nim-check-session' }).catch(() => null);
  if (!session?.ok) {
    if (String(session?.code || '') === 'AUTH_REQUIRED') {
      showToast(msg('firefoxOverlayLoginRequired'), 'error');
      await messageRuntime({ type: 'nim-open-options' }).catch(() => {});
      return;
    }
    if (session?.message) {
      throw new Error(String(session.message));
    }
  }

  let nextOrigin = '';
  try {
    nextOrigin = new URL(overlayUrl).origin;
  } catch (_) {
    throw new Error(errorText('INVALID_OVERLAY_URL'));
  }

  const shouldReloadFrame = overlayState.currentUrl !== overlayUrl;
  overlayState.loginUrl = `${overlayUrl.replace(/\/$/, '')}/login.html`;
  overlayState.autoLoginHintShown = false;
  if (overlayState.loading) {
    overlayState.loading.hidden = !shouldReloadFrame;
  }

  if (shouldReloadFrame) {
    overlayState.currentUrl = overlayUrl;
    overlayState.currentOrigin = nextOrigin;
    overlayState.iframe.src = overlayUrl;
  }

  updateOverlayTitle(overlayUrl);
  setOverlayOpen(true);
}

function installOverlay() {
  ensureOverlayHost();
  updateLauncherVisibility();
}

function installObservers() {
  injectBridgeScript();
  installOverlay();
  refreshExtensionConfig().catch(() => {});

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
      installOverlay();
      runAttach();
      scheduleAutoTransfer();
    }, 50);
  };

  const observer = new MutationObserver(() => {
    scheduleAttachFromObserver();
  });
  observer.observe(document.documentElement, { childList: true, subtree: true });

  const imgLoadHandler = () => scheduleAutoTransfer();
  document.addEventListener('load', (event) => {
    const target = event.target;
    if (target instanceof HTMLImageElement || target instanceof HTMLCanvasElement) {
      imgLoadHandler();
    }
  }, true);

  if (ext.storage?.onChanged) {
    ext.storage.onChanged.addListener((changes, areaName) => {
      if (areaName !== 'local') return;
      if (!changes[CONFIG_STORAGE_KEYS.showNovelAiMenu] && !changes[CONFIG_STORAGE_KEYS.autoTransfer]) return;
      refreshExtensionConfig().catch(() => {});
      scheduleAutoTransfer();
    });
  }

  runAttach();
  scheduleAutoTransfer();
}

log('content script loaded');
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', installObservers, { once: true });
} else {
  installObservers();
}
