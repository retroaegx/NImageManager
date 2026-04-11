const ext = globalThis.browser ?? globalThis.chrome;

const STORAGE_KEYS = {
  baseUrl: 'nim.base_url',
  token: 'nim.token',
  user: 'nim.user',
};


const AUTH_HEADER_NAME = 'Authorization';
const AUTH_SCHEME = 'Bearer';
const AUTH_EXCLUDED_PATHS = new Set([
  '/api/ext/login',
  '/api/auth/login',
  '/api/auth/logout',
]);

function shouldInjectAuthHeader(requestUrl, config) {
  const token = String(config?.token || '').trim();
  const baseUrl = String(config?.baseUrl || '').trim();
  if (!token || !baseUrl) {
    return false;
  }

  let request;
  let base;
  try {
    request = new URL(requestUrl);
    base = new URL(baseUrl);
  } catch (_) {
    return false;
  }

  if (request.origin !== base.origin) {
    return false;
  }
  if (AUTH_EXCLUDED_PATHS.has(request.pathname)) {
    return false;
  }
  return true;
}

function upsertRequestHeader(headers, name, value) {
  const normalized = String(name || '').toLowerCase();
  if (!normalized) {
    return headers;
  }

  let replaced = false;
  const nextHeaders = Array.isArray(headers) ? headers.map((header) => {
    if (String(header?.name || '').toLowerCase() !== normalized) {
      return header;
    }
    replaced = true;
    return { ...header, value };
  }) : [];

  if (!replaced) {
    nextHeaders.push({ name, value });
  }
  return nextHeaders;
}

async function injectAuthHeader(details) {
  if (!details?.url) {
    return {};
  }

  const config = await getConfig();
  if (!shouldInjectAuthHeader(details.url, config)) {
    return {};
  }

  const token = String(config.token || '').trim();
  if (!token) {
    return {};
  }

  return {
    requestHeaders: upsertRequestHeader(
      details.requestHeaders,
      AUTH_HEADER_NAME,
      `${AUTH_SCHEME} ${token}`
    ),
  };
}

function apiBaseFrom(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    throw new Error('ドメインを入力してください');
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
    throw new Error(data.message || data.detail?.message || 'ログインに失敗しました');
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
    throw new Error('ドメインが未設定です');
  }
  if (!config.token) {
    return { ok: false, code: 'AUTH_REQUIRED', message: 'Login required' };
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
      message: data.message || 'Login required',
      loginUrl: data.login_url || `${config.baseUrl}/login.html`,
    };
  }
  if (!response.ok || !data.ok) {
    throw new Error(data.message || 'セッション確認に失敗しました');
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
    return { ok: false, code: 'CONFIG_REQUIRED', message: 'Open extension settings and save the domain first.' };
  }
  if (!config.token) {
    return { ok: false, code: 'AUTH_REQUIRED', message: 'Login required', loginUrl: `${config.baseUrl}/login.html` };
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
      message: data.message || 'Login required',
      loginUrl: data.login_url || `${config.baseUrl}/login.html`,
    };
  }
  if (!response.ok || !data.ok) {
    return {
      ok: false,
      code: data.code || 'UPLOAD_FAILED',
      message: data.detail || data.message || `Upload failed (${response.status})`,
    };
  }
  return {
    ok: true,
    imageId: Number(data.image_id || 0),
    dedup: Boolean(data.dedup),
    message: data.message || 'Uploaded',
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
    throw new Error('URL が未設定です');
  }
  if (!(ext.tabs && ext.tabs.create)) {
    throw new Error('タブを開けません');
  }
  await ext.tabs.create({ url });
}

if (ext.webRequest?.onBeforeSendHeaders) {
  ext.webRequest.onBeforeSendHeaders.addListener(
    injectAuthHeader,
    { urls: ['<all_urls>'] },
    ['blocking', 'requestHeaders']
  );
}

if (ext.runtime.onInstalled) {
  ext.runtime.onInstalled.addListener((details) => {
    if (details.reason === 'install') {
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
      await saveConfig({ baseUrl: nextBaseUrl });
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
    return { ok: false, code: 'UNKNOWN_MESSAGE', message: 'unknown message' };
  })().then(sendResponse).catch((error) => {
    sendResponse({ ok: false, code: 'UNEXPECTED_ERROR', message: String(error?.message || error || 'unexpected error') });
  });
  return true;
});
