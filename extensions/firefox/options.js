const ext = globalThis.browser ?? globalThis.chrome;

function runtimeMessage(payload) {
  return new Promise((resolve) => {
    try {
      ext.runtime.sendMessage(payload, (response) => {
        const runtimeError = ext.runtime?.lastError;
        if (runtimeError) {
          resolve({ ok: false, message: runtimeError.message || 'Runtime error' });
          return;
        }
        resolve(response);
      });
    } catch (error) {
      resolve({ ok: false, message: String(error?.message || error || 'Runtime error') });
    }
  });
}

const $ = (id) => document.getElementById(id);

function setPill(id, text, kind) {
  const node = $(id);
  if (!node) return;
  node.textContent = text;
  node.className = `statusPill ${kind}`;
}

function applyConfig(config) {
  $('baseUrl').value = config.baseUrl || '';
  setPill('saveState', config.baseUrl ? 'Saved' : 'Not saved', config.baseUrl ? 'success' : 'idle');
  setPill('overlayState', config.baseUrl ? 'Ready' : 'Not configured', config.baseUrl ? 'success' : 'idle');
  setPill('loginState', config.token ? 'Logged in' : 'Not logged in', config.token ? 'success' : 'idle');
}

async function loadConfig() {
  const response = await runtimeMessage({ type: 'nim-get-config' });
  if (!response?.ok) {
    throw new Error(response?.message || 'Load failed');
  }
  applyConfig(response.config || {});
}

async function saveConfig() {
  const response = await runtimeMessage({
    type: 'nim-save-config',
    baseUrl: $('baseUrl').value,
  });
  if (!response?.ok) {
    setPill('saveState', 'Save failed', 'error');
    throw new Error(response?.message || 'Failed to save settings');
  }
  applyConfig(response.config || {});
}

async function login() {
  const response = await runtimeMessage({
    type: 'nim-login',
    baseUrl: $('baseUrl').value,
    username: $('username').value,
    password: $('password').value,
  });
  if (!response?.ok) {
    setPill('loginState', 'Login failed', 'error');
    throw new Error(response?.message || 'Login failed');
  }
  $('password').value = '';
  await loadConfig();
  setPill('loginState', 'Login OK', 'success');
}

async function logout() {
  const response = await runtimeMessage({ type: 'nim-logout' });
  if (!response?.ok) {
    setPill('loginState', 'Logout failed', 'error');
    throw new Error(response?.message || 'Logout failed');
  }
  await loadConfig();
  setPill('loginState', 'Logged out', 'idle');
}

async function checkSession() {
  const response = await runtimeMessage({ type: 'nim-check-session' });
  if (response?.ok) {
    setPill('loginState', 'Login OK', 'success');
    return;
  }
  if (response?.code === 'AUTH_REQUIRED') {
    setPill('loginState', 'Login required', 'error');
    return;
  }
  setPill('loginState', 'Check failed', 'error');
}

async function openLoginPage() {
  const baseUrl = String($('baseUrl').value || '').trim();
  const normalized = /^[a-z]+:\/\//i.test(baseUrl) ? baseUrl.replace(/\/$/, '') : `https://${baseUrl.replace(/\/$/, '')}`;
  await runtimeMessage({ type: 'nim-open-login-page', loginUrl: `${normalized}/login.html` });
}

async function openOverlayPage() {
  const response = await runtimeMessage({ type: 'nim-get-config' });
  const config = response?.config || {};
  const overlayUrl = String(config.baseUrl || '').trim();
  if (!overlayUrl) {
    setPill('overlayState', 'Domain required', 'error');
    throw new Error('Domain required');
  }
  const openResponse = await runtimeMessage({ type: 'nim-open-url', url: overlayUrl });
  if (!openResponse?.ok) {
    setPill('overlayState', 'Open failed', 'error');
    throw new Error(openResponse?.message || 'Open failed');
  }
}

$('saveConfig').addEventListener('click', () => saveConfig().catch(() => {}));
$('login').addEventListener('click', () => login().catch(() => {}));
$('logout').addEventListener('click', () => logout().catch(() => {}));
$('checkSession').addEventListener('click', () => checkSession().catch(() => setPill('loginState', 'Check failed', 'error')));
$('openLoginPage').addEventListener('click', () => openLoginPage().catch(() => setPill('loginState', 'Open failed', 'error')));
$('openOverlayPage').addEventListener('click', () => openOverlayPage().catch(() => setPill('overlayState', 'Open failed', 'error')));

document.addEventListener('DOMContentLoaded', () => {
  loadConfig().catch(() => {
    setPill('saveState', 'Load failed', 'error');
    setPill('overlayState', 'Load failed', 'error');
    setPill('loginState', 'Load failed', 'error');
  });
});
