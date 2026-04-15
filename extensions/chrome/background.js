const ext = globalThis.browser ?? globalThis.chrome;

const STORAGE_KEYS = {
  baseUrl: 'nim.base_url',
  token: 'nim.token',
  user: 'nim.user',
  showNovelAiMenu: 'nim.show_novelai_menu',
  autoTransfer: 'nim.auto_transfer',
};

function extError(code, extra = {}) {
  const error = new Error(String(code || 'UNEXPECTED_ERROR'));
  error.code = String(code || 'UNEXPECTED_ERROR');
  Object.assign(error, extra);
  return error;
}

function apiBaseFrom(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    throw extError('DOMAIN_REQUIRED');
  }
  const withScheme = /^[a-z]+:\/\//i.test(raw) ? raw : `https://${raw}`;
  const url = new URL(withScheme);
  url.pathname = '';
  url.search = '';
  url.hash = '';
  return url.toString().replace(/\/$/, '');
}


function getStorage(keys) {
  return new Promise((resolve) => ext.storage.local.get(keys, resolve));
}

function setStorage(values) {
  return new Promise((resolve) => ext.storage.local.set(values, resolve));
}

function normalizeConfig(stored) {
  const baseUrl = String(stored[STORAGE_KEYS.baseUrl] || '').trim();
  return {
    baseUrl,
    token: String(stored[STORAGE_KEYS.token] || '').trim(),
    user: stored[STORAGE_KEYS.user] || null,
    showNovelAiMenu: stored[STORAGE_KEYS.showNovelAiMenu] !== false,
    autoTransfer: stored[STORAGE_KEYS.autoTransfer] === true,
  };
}

async function getConfig() {
  const stored = await getStorage(Object.values(STORAGE_KEYS));
  return normalizeConfig(stored || {});
}

async function saveConfig(nextConfig) {
  const payload = {};
  if (Object.prototype.hasOwnProperty.call(nextConfig, 'baseUrl')) {
    payload[STORAGE_KEYS.baseUrl] = nextConfig.baseUrl || '';
  }
  if (Object.prototype.hasOwnProperty.call(nextConfig, 'token')) {
    payload[STORAGE_KEYS.token] = nextConfig.token || '';
  }
  if (Object.prototype.hasOwnProperty.call(nextConfig, 'user')) {
    payload[STORAGE_KEYS.user] = nextConfig.user || null;
  }
  if (Object.prototype.hasOwnProperty.call(nextConfig, 'showNovelAiMenu')) {
    payload[STORAGE_KEYS.showNovelAiMenu] = Boolean(nextConfig.showNovelAiMenu);
  }
  if (Object.prototype.hasOwnProperty.call(nextConfig, 'autoTransfer')) {
    payload[STORAGE_KEYS.autoTransfer] = Boolean(nextConfig.autoTransfer);
  }
  await setStorage(payload);
}

async function clearSession() {
  await saveConfig({ token: '', user: null });
}

async function loginToServer({ baseUrl, username, password }) {
  const apiBase = apiBaseFrom(baseUrl);
  const response = await fetch(`${apiBase}/api/ext/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok || !data.ok) {
    throw extError(data.code || 'LOGIN_FAILED');
  }
  await saveConfig({
    baseUrl: apiBase,
    token: String(data.token || ''),
    user: data.user || null,
  });
  return data;
}

async function fetchSession() {
  const config = await getConfig();
  if (!config.baseUrl) {
    throw extError('DOMAIN_REQUIRED');
  }
  if (!config.token) {
    return { ok: false, code: 'AUTH_REQUIRED' };
  }
  const response = await fetch(`${config.baseUrl}/api/ext/session`, {
    method: 'GET',
    headers: { Authorization: `Bearer ${config.token}` },
  });
  const data = await response.json().catch(() => ({}));
  if (response.status === 401 || data.code === 'AUTH_REQUIRED') {
    await clearSession();
    return {
      ok: false,
      code: 'AUTH_REQUIRED',
      
      loginUrl: data.login_url || `${config.baseUrl}/login.html`,
    };
  }
  if (!response.ok || !data.ok) {
    throw extError(data.code || 'SESSION_CHECK_FAILED');
  }
  await saveConfig({ user: data.user || null });
  return {
    ok: true,
    user: data.user || null,
    loginUrl: data.login_url || `${config.baseUrl}/login.html`,
  };
}

function detectFilename(contentType, originalName) {
  const cleanName = String(originalName || '').trim();
  if (cleanName) {
    return cleanName;
  }
  const extByType = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/webp': 'webp',
    'image/avif': 'avif',
  };
  const extName = extByType[String(contentType || '').toLowerCase()] || 'png';
  return `novelai_${Date.now()}.${extName}`;
}

async function uploadImage(payload) {
  const config = await getConfig();
  if (!config.baseUrl) {
    return { ok: false, code: 'CONFIG_REQUIRED' };
  }
  if (!config.token) {
    return { ok: false, code: 'AUTH_REQUIRED', loginUrl: `${config.baseUrl}/login.html` };
  }

  const filename = detectFilename(payload.mimeType, payload.filename);
  const bytes = payload?.bytes instanceof Uint8Array ? payload.bytes : new Uint8Array(Array.isArray(payload?.bytes) ? payload.bytes : []);
  const blob = new Blob([bytes], { type: payload.mimeType || 'application/octet-stream' });
  const form = new FormData();
  form.append('file', blob, filename);
  if (payload.lastModifiedMs) {
    form.append('last_modified_ms', String(payload.lastModifiedMs));
  }

  const response = await fetch(`${config.baseUrl}/api/upload`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${config.token}` },
    body: form,
  });
  const data = await response.json().catch(() => ({}));

  if (response.status === 401 || data.code === 'AUTH_REQUIRED') {
    await clearSession();
    return {
      ok: false,
      code: 'AUTH_REQUIRED',
      
      loginUrl: data.login_url || `${config.baseUrl}/login.html`,
    };
  }
  if (!response.ok || !data.ok) {
    return {
      ok: false,
      code: data.code || 'UPLOAD_FAILED',
      status: response.status,
    };
  }
  return {
    ok: true,
    imageId: Number(data.image_id || 0),
    dedup: Boolean(data.dedup),
    
    detailUrl: data.detail_url || '',
  };
}

async function openOptionsPage() {
  if (ext.runtime.openOptionsPage) {
    await ext.runtime.openOptionsPage();
    return;
  }
  const url = ext.runtime.getURL('options.html');
  if (ext.tabs && ext.tabs.create) {
    await ext.tabs.create({ url });
  }
}

async function openLoginPage(loginUrl) {
  const targetUrl = String(loginUrl || '').trim();
  if (!targetUrl) {
    await openOptionsPage();
    return;
  }
  if (ext.tabs && ext.tabs.create) {
    await ext.tabs.create({ url: targetUrl });
    return;
  }
  await openOptionsPage();
}

async function openTab(targetUrl) {
  const url = String(targetUrl || '').trim();
  if (!url) {
    throw extError('URL_REQUIRED');
  }
  if (!(ext.tabs && ext.tabs.create)) {
    throw extError('OPEN_TAB_FAILED');
  }
  await ext.tabs.create({ url });
}

if (ext.runtime.onInstalled) {
  ext.runtime.onInstalled.addListener((details) => {
    if (details.reason === 'install') {
      saveConfig({ showNovelAiMenu: true, autoTransfer: false }).catch(() => {});
      openOptionsPage().catch(() => {});
    }
  });
}

ext.runtime.onMessage.addListener((message, sender, sendResponse) => {
  const type = String(message?.type || '');
  (async () => {
    if (type === 'nim-get-config') {
      return { ok: true, config: await getConfig() };
    }
    if (type === 'nim-save-base-url') {
      const baseUrl = apiBaseFrom(message.baseUrl || '');
      await saveConfig({ baseUrl });
      return { ok: true, baseUrl, config: await getConfig() };
    }
    if (type === 'nim-save-config') {
      const currentConfig = await getConfig();
      const rawBaseUrl = Object.prototype.hasOwnProperty.call(message, 'baseUrl')
        ? String(message.baseUrl || '').trim()
        : currentConfig.baseUrl;
      const nextBaseUrl = rawBaseUrl ? apiBaseFrom(rawBaseUrl) : '';
      await saveConfig({
        baseUrl: nextBaseUrl,
        showNovelAiMenu: Object.prototype.hasOwnProperty.call(message, 'showNovelAiMenu') ? Boolean(message.showNovelAiMenu) : currentConfig.showNovelAiMenu,
        autoTransfer: Object.prototype.hasOwnProperty.call(message, 'autoTransfer') ? Boolean(message.autoTransfer) : currentConfig.autoTransfer,
      });
      return { ok: true, config: await getConfig() };
    }
    if (type === 'nim-login') {
      const data = await loginToServer(message);
      return { ok: true, data };
    }
    if (type === 'nim-logout') {
      await clearSession();
      return { ok: true };
    }
    if (type === 'nim-check-session') {
      return await fetchSession();
    }
    if (type === 'nim-upload-image') {
      return await uploadImage(message.payload || {});
    }
    if (type === 'nim-open-options') {
      await openOptionsPage();
      return { ok: true };
    }
    if (type === 'nim-open-login-page') {
      await openLoginPage(message.loginUrl || '');
      return { ok: true };
    }
    if (type === 'nim-open-url') {
      await openTab(message.url || '');
      return { ok: true };
    }
    return { ok: false, code: 'UNKNOWN_MESSAGE' };
  })().then(sendResponse).catch((error) => {
    sendResponse({ ok: false, code: String(error?.code || error?.message || 'UNEXPECTED_ERROR') });
  });
  return true;
});
