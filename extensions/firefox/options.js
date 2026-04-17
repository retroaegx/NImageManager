const ext = globalThis.browser ?? globalThis.chrome;

const $ = (id) => document.getElementById(id);
const msg = (key, substitutions) => ext.i18n?.getMessage(key, substitutions) || key;

let behaviorFeedbackTimer = null;
let behaviorSaveToken = 0;
let behaviorSaving = false;

function localizeDocument(root = document) {
  root.querySelectorAll('[data-i18n]').forEach((node) => {
    node.textContent = msg(node.dataset.i18n);
  });
  root.querySelectorAll('[data-i18n-placeholder]').forEach((node) => {
    node.setAttribute('placeholder', msg(node.dataset.i18nPlaceholder));
  });
  document.title = msg('optionsPageTitle');
  document.documentElement.lang = (ext.i18n?.getUILanguage?.() || 'en').toLowerCase().startsWith('ja') ? 'ja' : 'en';
}

function runtimeMessage(payload) {
  return new Promise((resolve) => {
    try {
      ext.runtime.sendMessage(payload, (response) => {
        const runtimeError = ext.runtime?.lastError;
        if (runtimeError) {
          resolve({ ok: false, code: 'RUNTIME_ERROR', message: runtimeError.message || 'RUNTIME_ERROR' });
          return;
        }
        resolve(response);
      });
    } catch (error) {
      resolve({ ok: false, code: 'RUNTIME_ERROR', message: String(error?.message || error || 'RUNTIME_ERROR') });
    }
  });
}

function statusText(key) {
  return msg(key);
}

function setPill(id, textKey, kind) {
  const node = $(id);
  if (!node) return;
  node.textContent = statusText(textKey);
  node.className = `statusPill ${kind}`;
}

function setBehaviorControlsDisabled(disabled) {
  ['showNovelAiMenu'].forEach((id) => {
    const input = $(id);
    const label = input?.closest('.toggleField');
    if (input) input.disabled = disabled;
    if (label) label.classList.toggle('isBusy', disabled);
  });
}

function hideBehaviorFeedback() {
  const node = $('behaviorState');
  if (!node) return;
  node.textContent = '';
  node.className = 'statusPill idle isHidden';
}

function showBehaviorFeedback(textKey, kind, { autoHideMs = 0 } = {}) {
  const node = $('behaviorState');
  if (!node) return;
  if (behaviorFeedbackTimer) {
    clearTimeout(behaviorFeedbackTimer);
    behaviorFeedbackTimer = null;
  }
  node.textContent = statusText(textKey);
  node.className = `statusPill ${kind}`;
  if (autoHideMs > 0) {
    behaviorFeedbackTimer = setTimeout(() => {
      behaviorFeedbackTimer = null;
      hideBehaviorFeedback();
    }, autoHideMs);
  }
}

function applyConfig(config) {
  $('baseUrl').value = config.baseUrl || '';
  $('showNovelAiMenu').checked = config.showNovelAiMenu !== false;
  setPill('saveState', config.baseUrl ? 'status_saved' : 'status_not_saved', config.baseUrl ? 'success' : 'idle');
  setPill('overlayState', config.baseUrl ? 'status_ready' : 'status_not_configured', config.baseUrl ? 'success' : 'idle');
  setPill('loginState', config.token ? 'status_logged_in' : 'status_not_logged_in', config.token ? 'success' : 'idle');
  if (!behaviorSaving) {
    hideBehaviorFeedback();
    setBehaviorControlsDisabled(false);
  }
}

async function loadConfig() {
  const response = await runtimeMessage({ type: 'nim-get-config' });
  if (!response?.ok) {
    throw new Error(response?.code || 'RUNTIME_ERROR');
  }
  applyConfig(response.config || {});
}

async function saveConfig() {
  const response = await runtimeMessage({
    type: 'nim-save-config',
    baseUrl: $('baseUrl').value,
    showNovelAiMenu: $('showNovelAiMenu').checked,
  });
  if (!response?.ok) {
    setPill('saveState', 'status_save_failed', 'error');
    throw new Error(response?.code || 'RUNTIME_ERROR');
  }
  applyConfig(response.config || {});
}

async function saveBehaviorConfig(previous) {
  const token = ++behaviorSaveToken;
  behaviorSaving = true;
  setBehaviorControlsDisabled(true);
  showBehaviorFeedback('status_saving', 'idle');
  const response = await runtimeMessage({
    type: 'nim-save-config',
    showNovelAiMenu: $('showNovelAiMenu').checked,
  });
  if (token !== behaviorSaveToken) {
    return;
  }
  behaviorSaving = false;
  if (!response?.ok) {
    $('showNovelAiMenu').checked = previous.showNovelAiMenu;
    setBehaviorControlsDisabled(false);
    showBehaviorFeedback('status_save_failed', 'error', { autoHideMs: 3000 });
    throw new Error(response?.code || 'RUNTIME_ERROR');
  }
  applyConfig(response.config || {});
  showBehaviorFeedback('status_saved', 'success', { autoHideMs: 1400 });
}

function bindBehaviorToggle(id) {
  $(id).addEventListener('change', async () => {
    if (behaviorSaving) return;
    const previous = {
      showNovelAiMenu: !$('showNovelAiMenu').checked,
    };
    if (id === 'showNovelAiMenu') previous.showNovelAiMenu = !$('showNovelAiMenu').checked;
    else previous.showNovelAiMenu = $('showNovelAiMenu').checked;
    try {
      await saveBehaviorConfig(previous);
    } catch (_) {}
  });
}

async function login() {
  const response = await runtimeMessage({
    type: 'nim-login',
    baseUrl: $('baseUrl').value,
    username: $('username').value,
    password: $('password').value,
  });
  if (!response?.ok) {
    setPill('loginState', 'status_check_failed', 'error');
    throw new Error(response?.code || 'LOGIN_FAILED');
  }
  $('password').value = '';
  await loadConfig();
  setPill('loginState', 'status_login_ok', 'success');
}

async function logout() {
  const response = await runtimeMessage({ type: 'nim-logout' });
  if (!response?.ok) {
    setPill('loginState', 'status_logout_failed', 'error');
    throw new Error(response?.code || 'RUNTIME_ERROR');
  }
  await loadConfig();
  setPill('loginState', 'status_logged_out', 'idle');
}

async function checkSession() {
  const response = await runtimeMessage({ type: 'nim-check-session' });
  if (response?.ok) {
    setPill('loginState', 'status_login_ok', 'success');
    return;
  }
  if (response?.code === 'AUTH_REQUIRED') {
    setPill('loginState', 'status_login_required', 'error');
    return;
  }
  setPill('loginState', 'status_check_failed', 'error');
}

async function openLoginPage() {
  const baseUrl = String($('baseUrl').value || '').trim();
  const normalized = /^[a-z]+:\/\//i.test(baseUrl) ? baseUrl.replace(/\/$/, '') : `https://${baseUrl.replace(/\/$/, '')}`;
  const response = await runtimeMessage({ type: 'nim-open-login-page', loginUrl: `${normalized}/login.html` });
  if (!response?.ok) throw new Error(response?.code || 'OPEN_FAILED');
}

async function openOverlayPage() {
  const response = await runtimeMessage({ type: 'nim-get-config' });
  const config = response?.config || {};
  const overlayUrl = String(config.baseUrl || '').trim();
  if (!overlayUrl) {
    setPill('overlayState', 'status_not_configured', 'error');
    throw new Error('CONFIG_REQUIRED');
  }
  const openResponse = await runtimeMessage({ type: 'nim-open-url', url: overlayUrl });
  if (!openResponse?.ok) {
    setPill('overlayState', 'status_check_failed', 'error');
    throw new Error(openResponse?.code || 'OPEN_FAILED');
  }
}

function bindAction(id, action, onError) {
  $(id).addEventListener('click', () => {
    action().catch((error) => onError(error));
  });
}

document.addEventListener('DOMContentLoaded', () => {
  localizeDocument();

  bindAction('saveConfig', saveConfig, () => setPill('saveState', 'status_save_failed', 'error'));
  bindAction('login', login, () => setPill('loginState', 'status_check_failed', 'error'));
  bindAction('logout', logout, () => setPill('loginState', 'status_logout_failed', 'error'));
  bindAction('checkSession', checkSession, () => setPill('loginState', 'status_check_failed', 'error'));
  bindAction('openLoginPage', openLoginPage, () => setPill('loginState', 'status_check_failed', 'error'));
  bindAction('openOverlayPage', openOverlayPage, () => setPill('overlayState', 'status_check_failed', 'error'));
  bindBehaviorToggle('showNovelAiMenu');

  loadConfig().catch(() => {
    setPill('saveState', 'status_load_failed', 'error');
    setPill('overlayState', 'status_load_failed', 'error');
    setPill('loginState', 'status_load_failed', 'error');
    showBehaviorFeedback('status_load_failed', 'error');
  });
});
