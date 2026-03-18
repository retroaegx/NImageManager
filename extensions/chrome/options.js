const ext = globalThis.browser ?? globalThis.chrome;

function runtimeMessage(payload) {
  return new Promise((resolve) => ext.runtime.sendMessage(payload, resolve));
}

const $ = (id) => document.getElementById(id);

function setPill(id, text, kind) {
  const node = $(id);
  if (!node) return;
  node.textContent = text;
  node.className = `statusPill ${kind}`;
}

async function loadConfig() {
  const response = await runtimeMessage({ type: 'nim-get-config' });
  const config = response?.config || {};
  $('baseUrl').value = config.baseUrl || '';
  setPill('saveState', config.baseUrl ? 'Saved' : 'Not saved', config.baseUrl ? 'success' : 'idle');
  setPill('loginState', config.token ? 'Logged in' : 'Not logged in', config.token ? 'success' : 'idle');
}

async function saveBaseUrl() {
  const response = await runtimeMessage({ type: 'nim-save-base-url', baseUrl: $('baseUrl').value });
  if (!response?.ok) {
    setPill('saveState', 'Save failed', 'error');
    throw new Error(response?.message || 'Failed to save domain');
  }
  $('baseUrl').value = response.baseUrl || $('baseUrl').value;
  setPill('saveState', 'Saved', 'success');
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
  setPill('loginState', 'Login OK', 'success');
  await loadConfig();
}

async function logout() {
  const response = await runtimeMessage({ type: 'nim-logout' });
  if (!response?.ok) {
    setPill('loginState', 'Logout failed', 'error');
    throw new Error(response?.message || 'Logout failed');
  }
  setPill('loginState', 'Logged out', 'idle');
  await loadConfig();
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

$('saveBaseUrl').addEventListener('click', () => saveBaseUrl().catch(() => {}));
$('login').addEventListener('click', () => login().catch(() => {}));
$('logout').addEventListener('click', () => logout().catch(() => {}));
$('checkSession').addEventListener('click', () => checkSession().catch(() => setPill('loginState', 'Check failed', 'error')));
$('openLoginPage').addEventListener('click', () => openLoginPage().catch(() => setPill('loginState', 'Open failed', 'error')));

document.addEventListener('DOMContentLoaded', () => {
  loadConfig().catch(() => {
    setPill('saveState', 'Load failed', 'error');
    setPill('loginState', 'Load failed', 'error');
  });
});
