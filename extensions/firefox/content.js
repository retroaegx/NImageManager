const ext = globalThis.browser ?? globalThis.chrome;

function getExtApi() {
  try {
    return globalThis.browser ?? globalThis.chrome ?? ext ?? null;
  } catch (_) {
    return null;
  }
}

function hasRuntimeId(api) {
  try {
    return Boolean(api?.runtime?.id);
  } catch (_) {
    return false;
  }
}

function isExtensionContextAlive() {
  return hasRuntimeId(getExtApi());
}

function normalizeNodeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

const FALLBACK_MESSAGES = {
  transferToNim: 'Transfer to NIM',
  transferring: 'Transferring...',
  transferSuccess: 'Transferred',
  openNim: 'Open NIM',
  overlayLoading: 'Loading NIM...',
  overlayClose: 'Close',
  overlayReload: 'Reload',
  overlayMenu: 'Menu',
  overlayLogin: 'Login',
  overlaySettings: 'Settings',
  overlayNewTab: 'Open in new tab',
  autoTransferCompactLabel: 'NIM AF',
  autoTransferCompactOnTitle: 'Disable NIM Auto-forward',
  autoTransferCompactOffTitle: 'Enable NIM Auto-forward',
  autoTransferEnabledToast: 'Auto transfer enabled',
  autoTransferDisabledToast: 'Auto transfer disabled',
  error_UNKNOWN: 'Unexpected error',
};

function msg(key, substitutions) {
  if (!isExtensionContextAlive()) return FALLBACK_MESSAGES[key] || key;
  try {
    const api = getExtApi();
    const value = api?.i18n?.getMessage?.(key, substitutions);
    if (value) return value;
  } catch (_) {}
  return FALLBACK_MESSAGES[key] || key;
}

function errorText(code, fallbackKey = 'error_UNKNOWN') {
  const key = `error_${String(code || 'UNKNOWN').replace(/[^A-Za-z0-9_]/g, '_')}`;
  const value = msg(key);
  return value !== key ? value : msg(fallbackKey);
}

const BUTTON_FLAG = 'data-nim-transfer-button';
const BUTTON_WRAPPER_FLAG = 'data-nim-transfer-wrapper';
const TOAST_CONTAINER_ID = 'nim-transfer-toast-container';
const MAIN_IMAGE_SELECTOR = [
  '.display-grid-images img.image-grid-image',
  '.image-gen-output-region img.image-grid-image',
  '.image-gen-canvas img.image-grid-image',
].join(', ');
const MAIN_CANVAS_SELECTOR = [
  '.display-grid-images canvas',
  '.image-gen-output-region canvas',
  '.image-gen-canvas canvas',
].join(', ');
const DEBUG_PREFIX = '[NIM Transfer]';
const BRIDGE_REQUEST_TYPE = 'NIM_TRANSFER_FETCH_BLOB_REQUEST';
const BRIDGE_RESPONSE_TYPE = 'NIM_TRANSFER_FETCH_BLOB_RESPONSE';
const BRIDGE_READY_TYPE = 'NIM_TRANSFER_BRIDGE_READY';
const BRIDGE_SCRIPT_ID = 'nim-transfer-page-bridge';
const OVERLAY_HOST_ID = 'nim-overlay-extension-host';
const AUTO_TRANSFER_TOPBAR_HOST_ID = 'nim-auto-transfer-topbar-host';
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
    const api = getExtApi();
    if (!api || !hasRuntimeId(api)) {
      resolve({ ok: false, code: 'RUNTIME_ERROR' });
      return;
    }
    try {
      api.runtime.sendMessage(payload, (response) => {
        if (api.runtime?.lastError) {
          resolve({ ok: false, code: 'RUNTIME_ERROR' });
          return;
        }
        resolve(response);
      });
    } catch (_) {
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

function createStyleNode(cssText) {
  const style = document.createElement('style');
  style.textContent = String(cssText || '');
  return style;
}

function createElement(tagName, options = {}) {
  const element = document.createElement(tagName);
  const {
    className,
    text,
    attrs = null,
    dataset = null,
    style = null,
  } = options;

  if (className) element.className = className;
  if (text != null) element.textContent = String(text);
  if (attrs && typeof attrs === 'object') {
    for (const [name, value] of Object.entries(attrs)) {
      if (value == null) continue;
      element.setAttribute(name, String(value));
    }
  }
  if (dataset && typeof dataset === 'object') {
    for (const [name, value] of Object.entries(dataset)) {
      if (value == null) continue;
      element.dataset[name] = String(value);
    }
  }
  if (style && typeof style === 'object') {
    Object.assign(element.style, style);
  }
  return element;
}

async function canvasToBlob(canvas) {
  return await new Promise((resolve, reject) => {
    canvas.toBlob((blob) => {
      if (blob) resolve(blob);
      else reject(new Error(errorText('CANVAS_TO_BLOB_FAILED')));
    }, 'image/png');
  });
}

function scoreMediaCandidate(element) {
  if (!(element instanceof HTMLElement)) return Number.NEGATIVE_INFINITY;
  const rect = element.getBoundingClientRect();
  const style = window.getComputedStyle(element);
  if (rect.width <= 0 || rect.height <= 0 || style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
    return Number.NEGATIVE_INFINITY;
  }
  if (element instanceof HTMLImageElement) {
    const src = String(element.currentSrc || element.src || '').trim();
    if (!src || !element.complete || !element.naturalWidth || !element.naturalHeight) return Number.NEGATIVE_INFINITY;
  }
  if (element instanceof HTMLCanvasElement && (!element.width || !element.height)) return Number.NEGATIVE_INFINITY;

  const intersectionWidth = Math.max(0, Math.min(rect.right, window.innerWidth) - Math.max(rect.left, 0));
  const intersectionHeight = Math.max(0, Math.min(rect.bottom, window.innerHeight) - Math.max(rect.top, 0));
  const intersectionArea = intersectionWidth * intersectionHeight;
  const centerX = rect.left + rect.width / 2;
  const centerY = rect.top + rect.height / 2;
  const distanceSquared = ((centerX - window.innerWidth / 2) ** 2) + ((centerY - window.innerHeight / 2) ** 2);
  return intersectionArea > 0 ? (1e12 + intersectionArea * 1000 - distanceSquared) : -distanceSquared;
}

function findBestMediaCandidate(selector) {
  let best = null;
  let bestScore = Number.NEGATIVE_INFINITY;
  for (const candidate of Array.from(document.querySelectorAll(selector))) {
    const score = scoreMediaCandidate(candidate);
    if (score > bestScore) {
      best = candidate;
      bestScore = score;
    }
  }
  return best;
}

function getMainImageElement() {
  const image = findBestMediaCandidate(MAIN_IMAGE_SELECTOR);
  return image instanceof HTMLImageElement ? image : null;
}

function getMainCanvasElement() {
  const canvas = findBestMediaCandidate(MAIN_CANVAS_SELECTOR);
  return canvas instanceof HTMLCanvasElement ? canvas : null;
}

function getMainMediaElement() {
  return getMainImageElement() || getMainCanvasElement() || null;
}

function findDisplayPanelRoot() {
  const media = getMainMediaElement();
  if (!(media instanceof HTMLElement)) return null;

  const explicitRoot = media.closest('.display-grid-chrome');
  if (explicitRoot instanceof HTMLElement) return explicitRoot;

  const resultsStage = media.closest('.image-gen-results-stage');
  const stageChrome = resultsStage?.querySelector?.('.display-grid-chrome');
  if (stageChrome instanceof HTMLElement) return stageChrome;

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
  const scoped = root?.querySelector?.('.display-grid-bottom');
  if (scoped instanceof HTMLElement) return scoped;

  const visible = Array.from(document.querySelectorAll('.display-grid-bottom')).filter((toolbar) => {
    if (!(toolbar instanceof HTMLElement)) return false;
    const rect = toolbar.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  });
  visible.sort((a, b) => b.getBoundingClientRect().width - a.getBoundingClientRect().width);
  return visible[0] || null;
}

function getSeedButton(toolbar) {
  if (!(toolbar instanceof HTMLElement)) return null;
  const buttons = Array.from(toolbar.querySelectorAll('button'));
  return buttons.find((button) => {
    const text = normalizeNodeText(button.textContent);
    return /\b\d{6,}\b/.test(text) && /(シード値をコピー|Copy seed)/i.test(text);
  }) || buttons.find((button) => /\b\d{6,}\b/.test(normalizeNodeText(button.textContent))) || null;
}

function getVisibleToolbarButtons(root) {
  if (!(root instanceof HTMLElement)) return [];
  return Array.from(root.querySelectorAll('button')).filter((button) => {
    if (!(button instanceof HTMLButtonElement) || button.hasAttribute(BUTTON_FLAG)) return false;
    const rect = button.getBoundingClientRect();
    const style = window.getComputedStyle(button);
    return rect.width > 0 && rect.height > 0 && style.display !== 'none' && style.visibility !== 'hidden';
  });
}

function getSaveButton(toolbar) {
  const buttons = getVisibleToolbarButtons(toolbar);
  const semantic = buttons.find((button) => {
    const source = normalizeNodeText(`${button.getAttribute('aria-label') || ''} ${button.title || ''} ${button.textContent || ''}`);
    return /save|download|保存|ダウンロード/i.test(source);
  });
  if (semantic) return semantic;

  // NovelAI's current save icon has no accessible name, so retain its stable icon slot as a fallback.
  const currentSaveIcon = buttons.find((button) => button.querySelector('.sc-8ac360b8-53'));
  if (currentSaveIcon) return currentSaveIcon;

  // The compact image action group is ordered pin, copy, save (and sometimes palette).
  for (const button of buttons) {
    for (let group = button.parentElement; group && group !== toolbar; group = group.parentElement) {
      const groupedButtons = getVisibleToolbarButtons(group);
      if (groupedButtons.length >= 3 && groupedButtons.length <= 4) return groupedButtons[2] || null;
      if (groupedButtons.length > 4) break;
    }
  }
  return null;
}

function getCompactActionGroup(button, toolbar) {
  for (let group = button?.parentElement; group && group !== toolbar; group = group.parentElement) {
    const buttons = getVisibleToolbarButtons(group);
    if (buttons.length >= 3 && buttons.length <= 4) return group;
    if (buttons.length > 4) break;
  }
  return button?.parentElement instanceof HTMLElement ? button.parentElement : null;
}

function inferSeedText() {
  const seedButton = getSeedButton(getBottomToolbar());
  const text = normalizeNodeText(seedButton?.textContent);
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
  const api = getExtApi();
  let scriptUrl = '';
  try {
    scriptUrl = String(api?.runtime?.getURL?.('page-bridge.js') || '');
  } catch (_) {
    scriptUrl = '';
  }
  if (!scriptUrl) return;
  const script = document.createElement('script');
  script.id = BRIDGE_SCRIPT_ID;
  script.src = scriptUrl;
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

async function extractCurrentImagePayload(preferredMedia = null) {
  const img = preferredMedia instanceof HTMLImageElement ? preferredMedia : getMainImageElement();
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
  const canvas = preferredMedia instanceof HTMLCanvasElement ? preferredMedia : getMainCanvasElement();
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
    if (button.closest('.display-grid-bottom')) return;
    const wrapper = button.closest(`[${BUTTON_WRAPPER_FLAG}="1"]`);
    (wrapper || button).remove();
  });
}

function attachBottomTransferButton() {
  removeMisplacedButtons();
  const toolbar = getBottomToolbar();
  if (!(toolbar instanceof HTMLElement)) {
    return { ok: false, reason: 'no-toolbar' };
  }
  const saveButton = getSaveButton(toolbar);
  if (!(saveButton instanceof HTMLButtonElement)) {
    return { ok: false, reason: 'no-save-button' };
  }
  const actionsGroup = getCompactActionGroup(saveButton, toolbar);
  if (!(actionsGroup instanceof HTMLElement)) {
    return { ok: false, reason: 'no-actions-group' };
  }
  const saveNode = unwrapDirectChildInContainer(saveButton, actionsGroup) || saveButton;
  const existing = toolbar.querySelector(`button[${BUTTON_FLAG}="1"]`);
  if (existing) {
    const existingNode = existing.closest(`[${BUTTON_WRAPPER_FLAG}="1"]`) || existing;
    if (existingNode.parentElement === actionsGroup && existingNode.nextSibling === saveNode) {
      return { ok: true, reason: 'existing' };
    }
    existingNode.remove();
  }
  const transferButton = createTransferButton(saveButton);
  if (saveNode !== saveButton) {
    const wrapper = saveNode.cloneNode(false);
    wrapper.setAttribute(BUTTON_WRAPPER_FLAG, '1');
    wrapper.removeAttribute('id');
    wrapper.appendChild(transferButton);
    actionsGroup.insertBefore(wrapper, saveNode);
  } else {
    actionsGroup.insertBefore(transferButton, saveNode);
  }
  return { ok: true, reason: 'attached' };
}

async function handleTransferClick(button) {
  await transferCurrentImage({ button, showProgressToast: true, showSuccessToast: true });
}

const overlayState = {
  host: null,
  shadowRoot: null,
  launcherButton: null,
  autoTransferTopbarHost: null,
  autoTransferToggleLabel: null,
  autoTransferToggleButton: null,
  overlay: null,
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

const extensionConfig = {
  showNovelAiMenu: true,
  autoTransfer: false,
};

const autoTransferState = {
  inFlight: false,
  scheduled: 0,
  pendingMedia: null,
  attemptedSignatures: new Set(),
  failedSignatures: new Map(),
};

function rememberAttemptedSignature(signature) {
  if (!signature) return;
  autoTransferState.attemptedSignatures.add(signature);
  if (autoTransferState.attemptedSignatures.size <= 128) return;
  const oldest = autoTransferState.attemptedSignatures.values().next().value;
  if (oldest) autoTransferState.attemptedSignatures.delete(oldest);
}

function rememberFailedSignature(signature) {
  if (!signature) return 10000;
  const previous = autoTransferState.failedSignatures.get(signature);
  const attempts = Math.min(6, Number(previous?.attempts || 0) + 1);
  const delayMs = Math.min(60000, 5000 * (2 ** (attempts - 1)));
  autoTransferState.failedSignatures.set(signature, {
    attempts,
    retryAt: Date.now() + delayMs,
  });
  if (autoTransferState.failedSignatures.size > 128) {
    const oldest = autoTransferState.failedSignatures.keys().next().value;
    if (oldest) autoTransferState.failedSignatures.delete(oldest);
  }
  return delayMs;
}

function applyExtensionConfig(config) {
  extensionConfig.showNovelAiMenu = config?.showNovelAiMenu !== false;
  extensionConfig.autoTransfer = config?.autoTransfer === true;
  updateFloatingControlsVisibility();
  updateAutoTransferToggleButton();
}

async function refreshExtensionConfig() {
  const response = await messageRuntime({ type: 'nim-get-config' });
  if (!response?.ok) return null;
  const config = response.config || {};
  applyExtensionConfig(config);
  return config;
}

function isLikelyImageGenerationPage() {
  if (/(^|\/)(image|image-generation)(?:\/|$)/i.test(String(location.pathname || ''))) return true;
  if (/Image Generation|画像生成/i.test(String(document.title || ''))) return true;
  if (getMainMediaElement()) return true;
  if (getBottomToolbar()) return true;
  return false;
}

function isElementVisibleForAnchoring(element) {
  if (!(element instanceof HTMLElement)) return false;
  const rect = element.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0) return false;
  if (rect.bottom <= 0 || rect.top >= window.innerHeight) return false;
  const style = window.getComputedStyle(element);
  if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
  return true;
}

function scoreTopbarMenuButton(button) {
  if (!(button instanceof HTMLButtonElement)) return -1;
  if (button.id === AUTO_TRANSFER_TOPBAR_HOST_ID || button.closest(`#${AUTO_TRANSFER_TOPBAR_HOST_ID}`)) return -1;
  if (button.hasAttribute(BUTTON_FLAG)) return -1;
  if (!isElementVisibleForAnchoring(button)) return -1;

  const rect = button.getBoundingClientRect();
  if (rect.top > 180) return -1;

  const text = normalizeNodeText(button.textContent);
  const aria = normalizeNodeText(button.getAttribute('aria-label') || button.title);

  let score = 0;
  if (/menu|メニュー/i.test(aria)) score += 1000;
  if (/^(☰|≡|⋯|…)$/.test(text)) score += 900;
  if (button.querySelector('svg')) score += 120;
  if (rect.width >= 24 && rect.width <= 96 && rect.height >= 24 && rect.height <= 96) score += 160;
  score += Math.max(0, 600 - rect.top * 3);
  score += Math.max(0, rect.right);
  return score;
}

function scoreTopbarPlusButton(button) {
  if (!(button instanceof HTMLButtonElement)) return -1;
  if (button.id === AUTO_TRANSFER_TOPBAR_HOST_ID || button.closest(`#${AUTO_TRANSFER_TOPBAR_HOST_ID}`)) return -1;
  if (button.hasAttribute(BUTTON_FLAG)) return -1;
  if (!isElementVisibleForAnchoring(button)) return -1;

  const rect = button.getBoundingClientRect();
  if (rect.top > 180) return -1;

  const text = normalizeNodeText(button.textContent);
  const aria = normalizeNodeText(button.getAttribute('aria-label') || button.title);
  const source = `${text} ${aria}`.trim();

  let score = 0;
  if (/^\+$/.test(text)) score += 2500;
  if (/add|new|create|another|generate|追加|作成|新規|生成/i.test(source)) score += 600;
  if (button.querySelector('svg')) score += 100;
  if (rect.width >= 24 && rect.width <= 96 && rect.height >= 24 && rect.height <= 96) score += 140;
  score += Math.max(0, 500 - rect.top * 3);
  score += Math.max(0, 500 - rect.left);
  return score;
}

function findBestButton(container, scorer) {
  if (!(container instanceof HTMLElement)) return null;

  let best = null;
  let bestScore = -1;
  for (const button of Array.from(container.querySelectorAll('button'))) {
    const score = scorer(button);
    if (score > bestScore) {
      best = button;
      bestScore = score;
    }
  }
  return best;
}

function isInlineRowContainer(element) {
  if (!(element instanceof HTMLElement)) return false;
  const style = window.getComputedStyle(element);
  if (!style.display.includes('flex')) return false;
  if (style.flexDirection.startsWith('column')) return false;
  const rect = element.getBoundingClientRect();
  if (rect.width <= 0 || rect.height <= 0 || rect.top > 200) return false;
  return true;
}

const topbarAnchorState = {
  navbar: null,
  row: null,
  container: null,
  anchor: null,
  menuButton: null,
  plusButton: null,
  beforeNode: null,
  placement: '',
};

function unwrapDirectChildInContainer(node, container) {
  let current = node instanceof HTMLElement ? node : null;
  while (current && current.parentElement && current.parentElement !== container) {
    current = current.parentElement;
  }
  return current instanceof HTMLElement ? current : null;
}

function getStableTopbarTargetSnapshot() {
  const { navbar, row, container, anchor, beforeNode, menuButton, plusButton, placement } = topbarAnchorState;
  if (
    navbar instanceof HTMLElement && navbar.isConnected &&
    row instanceof HTMLElement && row.isConnected &&
    container instanceof HTMLElement && container.isConnected &&
    anchor instanceof HTMLElement && anchor.isConnected
  ) {
    return { navbar, row, container, anchor, beforeNode, menuButton, plusButton, placement: placement || '' };
  }
  return null;
}

function getTopbarRowFromNavbar(navbar) {
  if (!(navbar instanceof HTMLElement)) return null;
  if (navbar.classList.contains('image-gen-nav-row')) return navbar;
  const children = Array.from(navbar.children).filter((child) => child instanceof HTMLElement);
  for (const child of children) {
    if (isInlineRowContainer(child)) return child;
  }
  return isInlineRowContainer(navbar) ? navbar : null;
}

function findNearestInlineRowAncestor(node) {
  for (let current = node instanceof HTMLElement ? node : null; current && current !== document.body; current = current.parentElement) {
    if (isInlineRowContainer(current)) return current;
  }
  return null;
}

function findTopbarNavbarRoot() {
  const menuInNavRow = Array.from(document.querySelectorAll('.image-gen-nav-row button')).find((button) => {
    const source = normalizeNodeText(`${button.getAttribute('aria-label') || ''} ${button.title || ''} ${button.textContent || ''}`);
    return /menu|メニュー/i.test(source) && isElementVisibleForAnchoring(button);
  });
  const navRow = menuInNavRow?.closest('.image-gen-nav-row');
  if (navRow instanceof HTMLElement && isElementVisibleForAnchoring(navRow)) {
    return navRow;
  }

  const explicit = Array.from(document.querySelectorAll('.image-gen-navbar')).filter((node) => node instanceof HTMLElement && isElementVisibleForAnchoring(node));
  if (explicit.length) {
    explicit.sort((a, b) => a.getBoundingClientRect().top - b.getBoundingClientRect().top);
    return explicit[0];
  }

  const menuButton = findBestButton(document.body, scoreTopbarMenuButton);
  const plusButton = findBestButton(document.body, scoreTopbarPlusButton);
  return findNearestInlineRowAncestor(menuButton) || findNearestInlineRowAncestor(plusButton);
}

function findTopbarInsertionTarget() {
  const navbar = findTopbarNavbarRoot();
  const row = getTopbarRowFromNavbar(navbar) || (navbar instanceof HTMLElement ? navbar : null);
  if (!(row instanceof HTMLElement)) return getStableTopbarTargetSnapshot();

  const menuButton = findBestButton(row, scoreTopbarMenuButton);
  let plusButton = null;
  if (menuButton instanceof HTMLButtonElement) {
    const menuRect = menuButton.getBoundingClientRect();
    plusButton = Array.from(row.querySelectorAll('button'))
      .filter((button) => {
        if (!(button instanceof HTMLButtonElement) || button === menuButton || button.hasAttribute(BUTTON_FLAG)) return false;
        if (!isElementVisibleForAnchoring(button)) return false;
        const rect = button.getBoundingClientRect();
        return rect.right <= menuRect.left + 2 && Math.abs(rect.top - menuRect.top) <= Math.max(rect.height, menuRect.height);
      })
      .sort((a, b) => b.getBoundingClientRect().right - a.getBoundingClientRect().right)[0] || null;
  }
  if (!(plusButton instanceof HTMLButtonElement)) plusButton = findBestButton(row, scoreTopbarPlusButton);
  const menuNode = unwrapDirectChildInContainer(menuButton, row);
  const plusNode = unwrapDirectChildInContainer(plusButton, row);

  let anchor = null;
  let beforeNode = null;
  let placement = '';

  if (menuNode instanceof HTMLElement) {
    anchor = plusNode instanceof HTMLElement ? plusNode : menuNode;
    beforeNode = menuNode;
    placement = 'beforeMenu';
  } else if (plusNode instanceof HTMLElement) {
    anchor = plusNode;
    beforeNode = plusNode.nextSibling;
    placement = 'afterPlus';
  }

  if (!(anchor instanceof HTMLElement)) return getStableTopbarTargetSnapshot();

  topbarAnchorState.navbar = navbar instanceof HTMLElement ? navbar : row;
  topbarAnchorState.row = row;
  topbarAnchorState.container = row;
  topbarAnchorState.anchor = anchor;
  topbarAnchorState.menuButton = menuButton instanceof HTMLButtonElement ? menuButton : null;
  topbarAnchorState.plusButton = plusButton instanceof HTMLButtonElement ? plusButton : null;
  topbarAnchorState.beforeNode = beforeNode instanceof Node ? beforeNode : null;
  topbarAnchorState.placement = placement;
  return {
    navbar: navbar instanceof HTMLElement ? navbar : row,
    row,
    container: row,
    anchor,
    beforeNode: beforeNode instanceof Node ? beforeNode : null,
    menuButton: menuButton instanceof HTMLButtonElement ? menuButton : null,
    plusButton: plusButton instanceof HTMLButtonElement ? plusButton : null,
    placement,
  };
}

function createAutoTransferTopbarHost() {
  const host = document.createElement('div');
  host.id = AUTO_TRANSFER_TOPBAR_HOST_ID;
  Object.assign(host.style, {
    display: 'inline-flex',
    alignItems: 'center',
    flex: '0 0 auto',
    pointerEvents: 'auto',
    margin: '0 0 0 8px',
    whiteSpace: 'nowrap',
  });

  const shadowRoot = host.attachShadow({ mode: 'open' });
  shadowRoot.appendChild(createStyleNode(`
    :host {
      display: inline-flex;
      align-items: center;
      flex: 0 0 auto;
      white-space: nowrap;
      pointer-events: auto;
    }
    .autoTransferWrap {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .autoTransferLabel {
      color: #e2e8f0;
      font-size: 10px;
      text-align: right;
      font-weight: 800;
      line-height: 1;
      white-space: nowrap;
      user-select: none;
      text-shadow: 0 1px 1px rgba(0,0,0,0.28);
    }
    .autoTransferToggle {
      pointer-events: auto;
      position: relative;
      display: inline-flex;
      align-items: center;
      justify-content: flex-start;
      width: 52px;
      height: 30px;
      padding: 0;
      border: 1px solid rgba(148, 163, 184, 0.34);
      border-radius: 999px;
      background: rgba(30, 41, 59, 0.82);
      backdrop-filter: blur(10px);
      -webkit-backdrop-filter: blur(10px);
      box-shadow: 0 10px 24px rgba(15, 23, 42, 0.24);
      cursor: pointer;
      transition: background 140ms ease, border-color 140ms ease, opacity 120ms ease, transform 120ms ease;
    }
    .autoTransferToggle:hover { transform: translateY(-1px); }
    .autoTransferToggle[data-state="1"] {
      border-color: rgba(125, 211, 252, 0.42);
      background: rgba(8, 47, 73, 0.88);
    }
    .autoTransferToggle[data-busy="1"] {
      opacity: 0.72;
      cursor: wait;
    }
    .autoTransferToggle:focus-visible {
      outline: 2px solid rgba(125, 211, 252, 0.65);
      outline-offset: 2px;
    }
    .autoTransferThumb {
      position: absolute;
      top: 3px;
      left: 3px;
      width: 22px;
      height: 22px;
      border-radius: 50%;
      background: #e5e7eb;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.28);
      transition: transform 140ms ease, background 140ms ease;
    }
    .autoTransferToggle[data-state="1"] .autoTransferThumb {
      transform: translateX(22px);
      background: #7dd3fc;
    }
    :host([data-mode="compact"]) .autoTransferLabel {
      font-size: 8px;
    }
    :host([data-mode="compact"]) .autoTransferWrap {
      gap: 3px;
    }
    :host([data-mode="compact"]) .autoTransferToggle {
      width: 40px;
      height: 24px;
    }
    :host([data-mode="compact"]) .autoTransferThumb {
      top: 2px;
      left: 2px;
      width: 18px;
      height: 18px;
    }
    :host([data-mode="compact"]) .autoTransferToggle[data-state="1"] .autoTransferThumb {
      transform: translateX(18px);
    }
    :host([data-mode="toggleOnly"]) .autoTransferLabel {
      display: none;
    }
    :host([data-mode="toggleOnly"]) .autoTransferWrap {
      gap: 0;
    }
    :host([data-mode="toggleOnly"]) .autoTransferToggle {
      width: 46px;
      height: 28px;
    }
    :host([data-mode="toggleOnly"]) .autoTransferThumb {
      top: 3px;
      left: 3px;
      width: 20px;
      height: 20px;
    }
    :host([data-mode="toggleOnly"]) .autoTransferToggle[data-state="1"] .autoTransferThumb {
      transform: translateX(18px);
    }
  `));

  const wrap = createElement('div', { className: 'autoTransferWrap' });
  const label = createElement('span', { className: 'autoTransferLabel' });
  const toggle = createElement('button', {
    className: 'autoTransferToggle',
    attrs: {
      type: 'button',
      'aria-pressed': 'false',
    },
    dataset: {
      state: '0',
      busy: '0',
    },
  });
  const thumb = createElement('span', { className: 'autoTransferThumb' });
  toggle.appendChild(thumb);
  wrap.append(label, toggle);
  shadowRoot.appendChild(wrap);

  return host;
}

function ensureAutoTransferTopbarHost() {
  let host = document.getElementById(AUTO_TRANSFER_TOPBAR_HOST_ID);
  if (!host) {
    host = createAutoTransferTopbarHost();
  }

  const target = findTopbarInsertionTarget();
  if (target?.container && target.anchor) {
    let beforeNode = target.beforeNode instanceof Node && target.beforeNode.parentNode === target.container
      ? target.beforeNode
      : null;
    if (!beforeNode && target.anchor.parentElement === target.container) {
      beforeNode = target.anchor.nextSibling;
    }
    if (host.parentElement !== target.container || host.nextSibling !== beforeNode) {
      try {
        target.container.insertBefore(host, beforeNode || null);
      } catch (_) {
        target.container.appendChild(host);
      }
    }
    Object.assign(host.style, {
      position: '',
      top: '',
      right: '',
      left: '',
      bottom: '',
      zIndex: '',
      transform: '',
    });
  } else if (!host.isConnected) {
    document.documentElement.appendChild(host);
  }

  overlayState.autoTransferTopbarHost = host;
  overlayState.autoTransferToggleLabel = host.shadowRoot?.querySelector('.autoTransferLabel') || null;
  overlayState.autoTransferToggleButton = host.shadowRoot?.querySelector('.autoTransferToggle') || null;
  if (overlayState.autoTransferToggleButton && !overlayState.autoTransferToggleButton.dataset.bound) {
    overlayState.autoTransferToggleButton.dataset.bound = '1';
    overlayState.autoTransferToggleButton.addEventListener('click', (event) => {
      event.preventDefault();
      event.stopPropagation();
      toggleAutoTransferFromPage().catch((error) => {
        warn('auto transfer toggle failed', error);
      });
    });
  }
  return host;
}

function positionAutoTransferTopbarHost() {
  const host = ensureAutoTransferTopbarHost();
  const target = findTopbarInsertionTarget();
  const isVisible = isLikelyImageGenerationPage() && !overlayState.isOpen;

  if (!isVisible) {
    host.hidden = true;
    return host;
  }

  if (!target?.container || !target?.anchor) {
    host.hidden = false;
    host.dataset.mode = window.innerWidth >= 900 ? 'full' : 'toggleOnly';
    Object.assign(host.style, {
      position: 'fixed',
      top: 'calc(env(safe-area-inset-top, 0px) + 12px)',
      right: 'calc(env(safe-area-inset-right, 0px) + 76px)',
      left: 'auto',
      bottom: 'auto',
      zIndex: '2147483645',
      transform: 'none',
      margin: '0',
    });
    return host;
  }

  const plusRect = target.plusButton?.getBoundingClientRect?.() || target.anchor.getBoundingClientRect();
  const rowRect = target.row?.getBoundingClientRect?.() || target.container.getBoundingClientRect();
  const menuRect = target.menuButton?.getBoundingClientRect?.();
  const rightLimit = menuRect ? menuRect.left : rowRect.right;
  const availableWidth = Math.max(0, rightLimit - plusRect.right - 12);

  host.hidden = false;
  if (availableWidth >= 120) host.dataset.mode = 'full';
  else if (availableWidth >= 72) host.dataset.mode = 'compact';
  else host.dataset.mode = 'toggleOnly';

  host.style.margin = '0 8px 0 8px';
  return host;
}

function updateFloatingControlsVisibility() {
  const isVisible = isLikelyImageGenerationPage() && !overlayState.isOpen;
  if (overlayState.launcherButton) {
    overlayState.launcherButton.hidden = !isVisible || !extensionConfig.showNovelAiMenu;
  }
  const host = positionAutoTransferTopbarHost();
  if (host && !isVisible) {
    host.hidden = true;
  }
}

function updateAutoTransferToggleButton() {
  positionAutoTransferTopbarHost();
  const button = overlayState.autoTransferToggleButton;
  if (!(button instanceof HTMLButtonElement)) return;
  const enabled = extensionConfig.autoTransfer === true;
  if (overlayState.autoTransferToggleLabel instanceof HTMLElement) {
    overlayState.autoTransferToggleLabel.textContent = msg('autoTransferCompactLabel');
  }
  button.dataset.state = enabled ? '1' : '0';
  const title = enabled ? msg('autoTransferCompactOnTitle') : msg('autoTransferCompactOffTitle');
  button.title = title;
  button.setAttribute('aria-label', title);
  button.setAttribute('aria-pressed', enabled ? 'true' : 'false');
}

async function toggleAutoTransferFromPage() {
  ensureAutoTransferTopbarHost();
  const button = overlayState.autoTransferToggleButton;
  if (!(button instanceof HTMLButtonElement)) return;
  if (button.dataset.busy === '1') return;
  const nextValue = !extensionConfig.autoTransfer;
  button.dataset.busy = '1';
  button.disabled = true;
  try {
    const response = await messageRuntime({ type: 'nim-save-config', autoTransfer: nextValue });
    if (!response?.ok) throw new Error(errorText(response?.code || 'SAVE_FAILED'));
    applyExtensionConfig(response.config || { autoTransfer: nextValue, showNovelAiMenu: extensionConfig.showNovelAiMenu });
    if (nextValue) {
      scheduleAutoTransfer();
      showToast(msg('autoTransferEnabledToast'), 'success');
    } else {
      showToast(msg('autoTransferDisabledToast'), 'info');
    }
  } catch (error) {
    warn('auto transfer toggle save failed', error);
    showToast(String(error?.message || error || errorText('SAVE_FAILED')), 'error');
    updateAutoTransferToggleButton();
  } finally {
    button.dataset.busy = '0';
    button.disabled = false;
  }
}

function getCurrentImageSignature(preferredMedia = null) {
  const img = preferredMedia instanceof HTMLImageElement ? preferredMedia : getMainImageElement();
  if (img instanceof HTMLImageElement) {
    const src = String(img.currentSrc || img.src || '').trim();
    if (!src) return '';
    return `img:${src}`;
  }
  const canvas = preferredMedia instanceof HTMLCanvasElement ? preferredMedia : getMainCanvasElement();
  if (canvas instanceof HTMLCanvasElement) {
    if (!canvas.width || !canvas.height) return '';
    const seed = inferSeedText();
    return `canvas:${canvas.width}x${canvas.height}:${seed}`;
  }
  return '';
}

async function transferCurrentImage({ button = null, media = null, showProgressToast = true, showSuccessToast = true } = {}) {
  if (button) {
    button.disabled = true;
    button.style.opacity = '0.7';
  }
  if (showProgressToast) {
    showToast(msg('transferring'), 'info');
  }
  try {
    const payload = await extractCurrentImagePayload(media);
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
      const uploadError = new Error(errorText(response?.code || 'UPLOAD_FAILED'));
      uploadError.code = String(response?.code || 'UPLOAD_FAILED');
      throw uploadError;
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
  const media = autoTransferState.pendingMedia?.isConnected ? autoTransferState.pendingMedia : getMainMediaElement();
  if (!(media instanceof HTMLImageElement) && !(media instanceof HTMLCanvasElement)) return;
  const signature = getCurrentImageSignature(media);
  if (!signature) return;
  if (autoTransferState.attemptedSignatures.has(signature)) return;
  const failed = autoTransferState.failedSignatures.get(signature);
  if (failed?.retryAt && failed.retryAt > Date.now()) return;
  if (autoTransferState.inFlight) return;

  autoTransferState.inFlight = true;
  try {
    const response = await transferCurrentImage({ media, showProgressToast: false, showSuccessToast: true });
    if (!response?.ok) {
      const delayMs = rememberFailedSignature(signature);
      window.setTimeout(() => scheduleAutoTransfer(media), delayMs + 50);
      return;
    }
    autoTransferState.failedSignatures.delete(signature);
    rememberAttemptedSignature(signature);
    if (autoTransferState.pendingMedia === media) autoTransferState.pendingMedia = null;
  } catch (error) {
    if (String(error?.code || '') === 'UPLOAD_QUOTA_REACHED') {
      autoTransferState.failedSignatures.delete(signature);
      rememberAttemptedSignature(signature);
      if (autoTransferState.pendingMedia === media) autoTransferState.pendingMedia = null;
      warn('auto transfer stopped because upload quota was reached', error);
      showToast(String(error?.message || errorText('UPLOAD_QUOTA_REACHED')), 'error');
      return;
    }
    const delayMs = rememberFailedSignature(signature);
    window.setTimeout(() => scheduleAutoTransfer(media), delayMs + 50);
    warn('auto transfer failed', error);
    showToast(String(error?.message || error || errorText('UPLOAD_FAILED')), 'error');
  } finally {
    autoTransferState.inFlight = false;
  }
}

function scheduleAutoTransfer(preferredMedia = null) {
  if (!extensionConfig.autoTransfer) return;
  if (preferredMedia instanceof HTMLImageElement || preferredMedia instanceof HTMLCanvasElement) {
    autoTransferState.pendingMedia = preferredMedia;
  }
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
    shadowRoot.appendChild(createStyleNode(`
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
        top: calc(env(safe-area-inset-top, 0px) + 12px);
        left: calc(env(safe-area-inset-left, 0px) + 12px);
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
    `));

    const ui = createElement('div', { className: 'nim-ui' });
    const launcherButton = createElement('button', {
      className: 'launcher',
      text: 'NIM',
      attrs: {
        type: 'button',
        'aria-label': msg('openNim'),
      },
    });
    const overlay = createElement('div', {
      className: 'overlay',
      attrs: {
        'data-open': '0',
        'aria-hidden': 'true',
      },
    });
    const frame = createElement('iframe', {
      className: 'frame',
      attrs: {
        referrerpolicy: 'strict-origin-when-cross-origin',
        allow: 'storage-access',
      },
    });
    const loading = createElement('div', {
      className: 'loading',
      text: msg('overlayLoading'),
    });
    const toolbar = createElement('div', { className: 'toolbar' });
    const toolbarLeft = createElement('div', { className: 'toolbarLeft' });
    const closeButton = createElement('button', {
      className: 'toolbarButton',
      text: msg('overlayClose'),
      attrs: { type: 'button', 'data-action': 'close' },
    });
    const reloadButton = createElement('button', {
      className: 'toolbarButton',
      text: '⟳',
      attrs: {
        type: 'button',
        'data-action': 'reload',
        'aria-label': msg('overlayReload'),
        title: msg('overlayReload'),
      },
    });
    const toggleMenuButton = createElement('button', {
      className: 'toolbarButton',
      text: '⋯',
      attrs: {
        type: 'button',
        'data-action': 'toggle-menu',
        'aria-label': msg('overlayMenu'),
        'aria-expanded': 'false',
      },
    });
    const toolbarMenu = createElement('div', {
      className: 'toolbarMenu',
      attrs: { 'data-open': '0' },
    });
    const loginButton = createElement('button', {
      className: 'toolbarButton',
      text: msg('overlayLogin'),
      attrs: { type: 'button', 'data-action': 'login' },
    });
    const settingsButton = createElement('button', {
      className: 'toolbarButton',
      text: msg('overlaySettings'),
      attrs: { type: 'button', 'data-action': 'settings' },
    });
    const newTabButton = createElement('button', {
      className: 'toolbarButton',
      text: msg('overlayNewTab'),
      attrs: { type: 'button', 'data-action': 'newtab' },
    });

    toolbarMenu.append(loginButton, settingsButton, newTabButton);
    toolbarLeft.append(closeButton, reloadButton, toggleMenuButton, toolbarMenu);
    toolbar.appendChild(toolbarLeft);
    overlay.append(frame, loading, toolbar);
    ui.append(launcherButton, overlay);
    shadowRoot.appendChild(ui);
  }

  overlayState.host = host;
  overlayState.shadowRoot = shadowRoot;
  overlayState.launcherButton = shadowRoot.querySelector('.launcher');
  overlayState.autoTransferToggleLabel = shadowRoot.querySelector('.autoTransferLabel');
  overlayState.autoTransferToggleButton = shadowRoot.querySelector('.autoTransferToggle');
  overlayState.overlay = shadowRoot.querySelector('.overlay');
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

  if (overlayState.autoTransferToggleButton && !overlayState.autoTransferToggleButton.dataset.bound) {
    overlayState.autoTransferToggleButton.dataset.bound = '1';
    overlayState.autoTransferToggleButton.addEventListener('click', (event) => {
      event.preventDefault();
      toggleAutoTransferFromPage().catch((error) => {
        warn('auto transfer toggle failed', error);
      });
    });
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
  updateAutoTransferToggleButton();
  updateFloatingControlsVisibility();
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
  updateFloatingControlsVisibility();
  updateFloatingControlsVisibility();
  updateAutoTransferToggleButton();
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
  updateFloatingControlsVisibility();
  updateAutoTransferToggleButton();
}

let floatingControlsSyncInstalled = false;
let floatingControlsSyncFrame = 0;

function scheduleFloatingControlsSync() {
  if (floatingControlsSyncFrame) return;
  floatingControlsSyncFrame = window.requestAnimationFrame(() => {
    floatingControlsSyncFrame = 0;
    try {
      updateFloatingControlsVisibility();
    } catch (_) {}
  });
}

function installFloatingControlsSync() {
  if (floatingControlsSyncInstalled) return;
  floatingControlsSyncInstalled = true;

  window.addEventListener('resize', scheduleFloatingControlsSync, { passive: true });
  window.addEventListener('orientationchange', scheduleFloatingControlsSync, { passive: true });
  window.addEventListener('scroll', scheduleFloatingControlsSync, { passive: true, capture: true });
}

function installObservers() {
  installFloatingControlsSync();
  injectBridgeScript();
  try {
    installOverlay();
  } catch (error) {
    warn('floating controls setup failed; transfer observer will continue', error);
  }
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

    switch (result.reason) {
      case 'attached':
        log('transfer button attached to display-grid-bottom');
        return;
      case 'existing':
        return;
      case 'no-save-button':
        warn('save button not found in display-grid-bottom');
        return;
      case 'no-actions-group':
        warn('save button action group not found');
        return;
      default:
        return;
    }
  };

  const runAttach = () => {
    let result;
    try {
      result = attachBottomTransferButton();
    } catch (error) {
      warn('transfer button attach failed', error);
      result = { ok: false, reason: 'exception' };
    }
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
      scheduleFloatingControlsSync();
      runAttach();
      scheduleAutoTransfer();
    }, 120);
  };

  const observer = new MutationObserver((records) => {
    let newestMedia = null;
    for (const record of records) {
      for (const node of Array.from(record.addedNodes || [])) {
        if (!(node instanceof Element)) continue;
        if (node.matches?.(MAIN_IMAGE_SELECTOR) || node.matches?.(MAIN_CANVAS_SELECTOR)) newestMedia = node;
        const nested = node.querySelectorAll?.(`${MAIN_IMAGE_SELECTOR}, ${MAIN_CANVAS_SELECTOR}`);
        if (nested?.length) newestMedia = nested[nested.length - 1];
      }
    }
    scheduleAttachFromObserver();
    if (newestMedia) scheduleAutoTransfer(newestMedia);
  });
  observer.observe(document.documentElement, { childList: true, subtree: true });

  document.addEventListener('load', (event) => {
    const target = event.target;
    if (target instanceof HTMLImageElement || target instanceof HTMLCanvasElement) {
      if (target.matches?.(MAIN_IMAGE_SELECTOR) || target.matches?.(MAIN_CANVAS_SELECTOR)) {
        scheduleAutoTransfer(target);
      }
    }
  }, true);

  const storageApi = getExtApi()?.storage;
  if (storageApi?.onChanged) {
    storageApi.onChanged.addListener((changes, areaName) => {
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
