async function loadSettings() {
  try {
    const data = await fetchJson('/api/settings');
    confirmSqlEnabled = data.confirm_sql;
    explainSqlEnabled = data.explain_sql;
    clarifySqlEnabled = data.clarify_sql;
    showCostEnabled = data.show_cost || false;
    updateSettingsButtons();
  } catch (e) { /* ignore */ }
  // Also load LLM settings
  await loadLlmSettings();
}

async function loadLlmSettings() {
  try {
    const data = await fetchJson('/api/settings/llm');
    llmConnected = data.connected;
    const providerEl = document.getElementById('llm-provider');
    const modelEl = document.getElementById('llm-model');
    const baseUrlEl = document.getElementById('llm-base-url');
    const statusEl = document.getElementById('llm-status');

    if (providerEl) providerEl.value = data.provider || 'anthropic';
    if (modelEl) modelEl.value = data.model || '';
    if (baseUrlEl) baseUrlEl.value = data.base_url || '';

    // Show that key is set from environment (never expose actual value)
    const apiKeyEl = document.getElementById('llm-api-key');
    if (apiKeyEl) {
      if (data.has_api_key && !apiKeyEl.value) {
        apiKeyEl.placeholder = 'Set from environment';
      } else if (!data.has_api_key) {
        apiKeyEl.placeholder = 'Enter API key...';
      }
    }

    // Update status indicator
    if (statusEl) {
      if (data.connected) {
        statusEl.textContent = 'Connected';
        statusEl.className = 'llm-status connected';
      } else {
        statusEl.textContent = 'Not configured';
        statusEl.className = 'llm-status disconnected';
      }
    }

    onLlmProviderChange();
    promptLlmConfigIfNeeded();
  } catch (e) { /* ignore */ }
}

async function loadProjectHealth() {
  const container = document.getElementById('project-health');
  if (!container) return;
  container.innerHTML = '<div class="project-health-empty">Checking project health...</div>';

  try {
    const data = await fetchJson('/api/project-health');
    const checks = data.checks || [];
    if (!checks.length) {
      container.innerHTML = '<div class="project-health-empty">No health data available.</div>';
      return;
    }

    const summary = data.summary || {};
    const remediation = checks
      .filter(check => !check.ok && check.remediation)
      .map(check =>
        '<div class="project-health-hint"><strong>' + escapeHtml(check.name || '') + ':</strong> ' +
        escapeHtml(check.remediation || '') + '</div>'
      ).join('');

    const rows = checks.map(check =>
      '<div class="project-health-row' + (check.ok ? '' : ' fail') + '">' +
        '<span class="project-health-name">' + escapeHtml(check.name || '') + '</span>' +
        '<span class="project-health-status ' + (check.ok ? 'ok' : 'fail') + '">' +
          (check.ok ? 'OK' : 'FAIL') +
        '</span>' +
        '<span class="project-health-category">' + escapeHtml(check.category || 'check') + '</span>' +
        '<span class="project-health-detail">' + escapeHtml(check.detail || '') + '</span>' +
      '</div>'
    ).join('');

    container.innerHTML =
      '<div class="project-health-summary' + ((summary.fail_count || 0) > 0 ? ' has-failures' : ' healthy') + '">' +
        '<strong>' + escapeHtml(data.project_loaded ? (data.project_dir || 'Project loaded') : 'No project loaded') + '</strong>' +
        '<span>' + escapeHtml(
          String(summary.ok_count || 0) + ' OK • ' +
          String(summary.fail_count || 0) + ' failing'
        ) + '</span>' +
        (
          (summary.config_failures || summary.connectivity_failures || summary.project_failures)
            ? '<span>' + escapeHtml(
              'Config: ' + String(summary.config_failures || 0) +
              ' • Connectivity: ' + String(summary.connectivity_failures || 0) +
              ' • Project: ' + String(summary.project_failures || 0)
            ) + '</span>'
            : ''
        ) +
      '</div>' +
      (remediation ? '<div class="project-health-hints">' + remediation + '</div>' : '') +
      rows;
  } catch (e) {
    container.innerHTML = '<div class="project-health-empty">Failed to load project health.</div>';
  }
}

function onLlmProviderChange() {
  const provider = document.getElementById('llm-provider')?.value || 'anthropic';
  // Show/hide fields based on provider
  for (const field of document.querySelectorAll('.llm-field')) {
    const providers = (field.getAttribute('data-providers') || '').split(' ');
    field.hidden = !providers.includes(provider);
  }
  // Update placeholders
  const modelEl = document.getElementById('llm-model');
  if (modelEl) {
    const defaults = { anthropic: 'claude-haiku-4-5-20251001', ollama: 'qwen3.5:35b-a3b', github: 'gpt-4o' };
    modelEl.placeholder = defaults[provider] || 'Model name';
  }
}

async function saveLlmSettings() {
  const provider = document.getElementById('llm-provider').value;
  const apiKey = document.getElementById('llm-api-key').value.trim();
  const model = document.getElementById('llm-model').value.trim();
  const baseUrl = document.getElementById('llm-base-url').value.trim();
  const errorEl = document.getElementById('llm-error');
  const saveBtn = document.getElementById('llm-save-btn');

  errorEl.classList.remove('visible');
  saveBtn.disabled = true;
  saveBtn.textContent = 'Saving...';

  try {
    const data = await fetchJson('/api/settings/llm', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ provider, api_key: apiKey, model, base_url: baseUrl }),
    });

    llmConnected = data.connected;
    if (!data.connected) {
      errorEl.textContent = 'Failed to connect. Check your API key and settings.';
      errorEl.classList.add('visible');
    } else {
      // Clear the API key input after successful save (security)
      document.getElementById('llm-api-key').value = '';
    }
    await loadLlmSettings();
  } catch (e) {
    errorEl.textContent = 'Failed to save settings';
    errorEl.classList.add('visible');
  } finally {
    saveBtn.disabled = false;
    saveBtn.textContent = 'Save';
  }
}

function promptLlmConfigIfNeeded() {
  if (llmConnected) return;
  // Open settings panel directly so user can configure LLM
  const panel = document.getElementById('settings-panel');
  const overlay = document.getElementById('settings-overlay');
  panel.classList.add('open');
  overlay.classList.add('open');
  updateSettingsButtons();
}

function updateSessionIndicator(mode, name) {
  const el = document.getElementById('session-indicator');
  if (!el) return;

  if (!mode) {
    el.className = 'session-indicator';
    el.innerHTML = '';
    isEphemeralSession = false;
    return;
  }

  if (mode === 'explore') {
    isEphemeralSession = true;
    const tableCount = name || '?';
    el.className = 'session-indicator visible explore';
    el.innerHTML =
      '<span class="indicator-label">Explore</span>' +
      '<span class="indicator-name">' + escapeHtml(String(tableCount)) + ' table(s)</span>' +
      '<button class="indicator-action" data-action="save-project">Save</button>';
  } else if (mode === 'project') {
    isEphemeralSession = false;
    el.className = 'session-indicator visible project';
    el.innerHTML =
      '<span class="indicator-label">Project</span>' +
      '<span class="indicator-name">' + escapeHtml(name || '') + '</span>';
  }
}

async function toggleSetting(key, getCurrentValue, setCurrentValue) {
  const oldVal = getCurrentValue();
  setCurrentValue(!oldVal);
  updateSettingsButtons();
  try {
    await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ [key]: getCurrentValue() }),
    });
  } catch (e) {
    setCurrentValue(oldVal);
    updateSettingsButtons();
  }
}

function toggleConfirmSql() {
  toggleSetting('confirm_sql', () => confirmSqlEnabled, v => { confirmSqlEnabled = v; });
}

function toggleExplainSql() {
  toggleSetting('explain_sql', () => explainSqlEnabled, v => { explainSqlEnabled = v; });
}

function toggleClarifySql() {
  toggleSetting('clarify_sql', () => clarifySqlEnabled, v => { clarifySqlEnabled = v; });
}

function toggleShowCost() {
  toggleSetting('show_cost', () => showCostEnabled, v => {
    showCostEnabled = v;
    updateCostDisplay();
  });
}

function updateSettingsButtons() {
  // Update settings panel checkboxes
  const confirmCheck = document.getElementById('setting-confirm-sql');
  const explainCheck = document.getElementById('setting-explain-sql');
  const clarifyCheck = document.getElementById('setting-clarify-sql');
  const showCostCheck = document.getElementById('setting-show-cost');

  if (confirmCheck) confirmCheck.checked = confirmSqlEnabled;
  if (explainCheck) explainCheck.checked = explainSqlEnabled;
  if (clarifyCheck) clarifyCheck.checked = clarifySqlEnabled;
  if (showCostCheck) showCostCheck.checked = showCostEnabled;

  // Update settings gear button to show if any non-default settings are active
  const settingsBtn = document.getElementById('settings-toggle');
  if (settingsBtn) {
    const hasActiveSettings = confirmSqlEnabled || explainSqlEnabled || !clarifySqlEnabled || !showCostEnabled;
    settingsBtn.classList.toggle('active', hasActiveSettings);
  }
}

function toggleSettingsPanel() {
  const panel = document.getElementById('settings-panel');
  const overlay = document.getElementById('settings-overlay');
  const isOpen = panel.classList.contains('open');
  if (isOpen) {
    panel.classList.remove('open');
    overlay.classList.remove('open');
  } else {
    panel.classList.add('open');
    overlay.classList.add('open');
    // Sync checkbox states when opening
    updateSettingsButtons();
    loadProjectHealth();
  }
}
