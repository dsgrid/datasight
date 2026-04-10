// ---------------------------------------------------------------------------
// Landing page
// ---------------------------------------------------------------------------

function showLanding() {
  document.getElementById('landing-page').style.display = '';
  document.getElementById('main-layout').style.display = 'none';
  updateSessionIndicator(null);
}

function hideLanding() {
  document.getElementById('landing-page').style.display = 'none';
  document.getElementById('main-layout').style.display = '';
}

async function initLanding() {
  // Check LLM status and show/hide config section
  try {
    const llmData = await fetchJson('/api/settings/llm');
    llmConnected = llmData.connected;
    const configEl = document.getElementById('landing-llm-config');
    if (!llmData.connected) {
      configEl.style.display = '';
      // Populate fields
      document.getElementById('landing-llm-provider').value = llmData.provider || 'anthropic';
      if (llmData.model) document.getElementById('landing-llm-model').value = llmData.model;
      onLandingProviderChange();
    } else {
      configEl.style.display = 'none';
    }
  } catch (e) { /* ignore */ }

  // Load recent projects into the landing card
  try {
    const data = await fetchJson('/api/projects/recent');
    const container = document.getElementById('landing-recent');
    if (data.projects && data.projects.length > 0) {
      container.innerHTML = data.projects.slice(0, 5).map(p =>
        '<div class="landing-recent-item" data-path="' + escapeHtml(p.path) + '">' +
        '<span class="recent-name">' + escapeHtml(p.name) + '</span>' +
        '<span class="recent-path">' + escapeHtml(p.path) + '</span>' +
        '</div>'
      ).join('');
    } else {
      container.innerHTML = '<div style="font-size:0.75rem;color:var(--text-secondary)">No recent projects</div>';
    }
  } catch (e) { /* ignore */ }
}

function onLandingProviderChange() {
  const provider = document.getElementById('landing-llm-provider')?.value || 'anthropic';
  for (const field of document.querySelectorAll('.landing-llm-field')) {
    const providers = (field.getAttribute('data-providers') || '').split(' ');
    field.hidden = !providers.includes(provider);
  }
  const modelEl = document.getElementById('landing-llm-model');
  if (modelEl && !modelEl.value) {
    const defaults = { anthropic: 'claude-haiku-4-5-20251001', ollama: 'qwen3.5:35b-a3b', github: 'gpt-4o' };
    modelEl.placeholder = defaults[provider] || 'Model name';
  }
}

async function connectLlmFromLanding() {
  const provider = document.getElementById('landing-llm-provider').value;
  const apiKey = document.getElementById('landing-llm-api-key').value.trim();
  const model = document.getElementById('landing-llm-model').value.trim();
  const baseUrl = document.getElementById('landing-llm-base-url').value.trim();
  const errorEl = document.getElementById('landing-llm-error');
  const btn = document.getElementById('landing-llm-connect-btn');

  errorEl.textContent = '';
  btn.disabled = true;
  btn.textContent = 'Connecting...';

  try {
    const data = await fetchJson('/api/settings/llm', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ provider, api_key: apiKey, model, base_url: baseUrl }),
    });
    llmConnected = data.connected;
    if (data.connected) {
      document.getElementById('landing-llm-config').style.display = 'none';
      // Also update the settings panel
      await loadLlmSettings();
    } else {
      errorEl.textContent = 'Failed to connect. Check your API key and settings.';
    }
  } catch (e) {
    errorEl.textContent = 'Connection failed';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Connect';
  }
}

async function landingExplore() {
  const input = document.getElementById('landing-explore-input');
  const errorEl = document.getElementById('landing-explore-error');
  const btn = document.getElementById('landing-explore-btn');
  const path = input.value.trim();

  errorEl.textContent = '';
  if (!path) {
    errorEl.textContent = 'Please enter a file or directory path';
    return;
  }

  btn.disabled = true;
  btn.textContent = 'Loading...';

  try {
    const response = await fetch('/api/explore', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ paths: [path] })
    });
    const data = await response.json();

    if (!data.success) {
      errorEl.textContent = data.error || 'Failed to explore files';
      return;
    }

    // Success — transition to chat UI
    projectLoaded = true;
    currentProjectPath = null;
    llmConnected = !!data.llm_connected;

    hideLanding();
    updateSessionIndicator('explore', data.tables.length);
    clearSchemaSearchState();
    await loadSchema();
    await loadRecipes();
    setMeasureEditorState('', 'Load a saved project to edit measures.yaml overrides.', 'info');
    loadSettings();
    const starterRan = await maybeRunPendingStarter();
    if (!starterRan) showWelcome();

    if (!llmConnected) {
      window.setTimeout(promptLlmConfigIfNeeded, 300);
    }

  } catch (e) {
    errorEl.textContent = 'Failed to connect to server';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Explore';
  }
}

async function landingOpenProject(path) {
  const input = document.getElementById('landing-project-input');
  const errorEl = document.getElementById('landing-project-error');
  const btn = document.getElementById('landing-open-btn');
  path = path || input.value.trim();

  errorEl.textContent = '';
  if (!path) {
    errorEl.textContent = 'Please enter a project path';
    return;
  }

  if (btn) { btn.disabled = true; btn.textContent = 'Loading...'; }

  try {
    const response = await fetch('/api/projects/load', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path })
    });
    const data = await response.json();

    if (!data.success) {
      errorEl.textContent = data.error || 'Failed to load project';
      return;
    }

    projectLoaded = true;
    currentProjectPath = data.path;

    hideLanding();
    updateSessionIndicator('project', data.path.split('/').pop());
    clearSchemaSearchState();
    await loadSchema();
    await loadQueries();
    await loadRecipes();
    await loadMeasureOverridesEditor();
    loadSettings();
    loadConversations();
    loadBookmarks();
    loadReports();
    loadDashboard();
    restoreSession();
    const starterRan = await maybeRunPendingStarter();
    if (!starterRan) showWelcome();

  } catch (e) {
    errorEl.textContent = 'Failed to load project';
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Open'; }
  }
}

function landingOpenRecentProject(path) {
  landingOpenProject(path);
}

// Save popover
async function addFilesFromSidebar() {
  const input = document.getElementById('add-files-input');
  const path = input.value.trim();
  if (!path) return;

  input.disabled = true;
  try {
    const response = await fetch('/api/add-files', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ paths: [path] })
    });
    const data = await response.json();

    if (!data.success) {
      showToast(data.error || 'Failed to add files', 'error');
      return;
    }

    input.value = '';
    const added = data.added || [];
    showToast('Added ' + added.map(t => t.name).join(', '), 'success');

    // Update session indicator table count if in explore mode
    if (isEphemeralSession && data.schema_info) {
      updateSessionIndicator('explore', data.schema_info.length);
    }

    // Reload schema to show new tables
    clearSchemaSearchState();
    await loadSchema();
    await loadRecipes();
  } catch (e) {
    showToast('Failed to add files', 'error');
  } finally {
    input.disabled = false;
    input.focus();
  }
}

function showSavePopover() {
  document.getElementById('save-popover').classList.add('open');
  document.getElementById('save-popover-overlay').classList.add('open');
  document.getElementById('save-project-path').focus();
}

function hideSavePopover() {
  document.getElementById('save-popover').classList.remove('open');
  document.getElementById('save-popover-overlay').classList.remove('open');
}

async function saveFromPopover() {
  const pathInput = document.getElementById('save-project-path');
  const nameInput = document.getElementById('save-project-name');
  const descInput = document.getElementById('save-project-description');
  const errorEl = document.getElementById('save-popover-error');
  const btn = document.getElementById('save-popover-btn');
  const projectPath = pathInput.value.trim();
  const projectName = nameInput.value.trim();
  const description = descInput ? descInput.value.trim() : '';

  errorEl.textContent = '';
  if (!projectPath) {
    errorEl.textContent = 'Please enter a project directory path';
    return;
  }

  // Check if project files already exist
  try {
    const check = await fetchJson('/api/explore/check-project-path', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: projectPath }),
    });
    if (check.exists) {
      const ok = window.confirm(
        'The following files already exist and will be overwritten:\n\n' +
        check.files.join('\n') +
        '\n\nOverwrite?'
      );
      if (!ok) return;
    }
  } catch (e) { /* proceed anyway */ }

  btn.disabled = true;

  if (llmConnected) {
    // Use generate-project which saves + generates docs in one flow
    btn.textContent = 'Saving & generating...';
    hideSavePopover();
    showToast('Saving project and generating documentation...', 'info');

    try {
      const response = await fetch('/api/explore/generate-project', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          path: projectPath,
          name: projectName || null,
          description: description || null,
        })
      });

      const reader = response.body.getReader();
      const decoder = new window.TextDecoder();
      let buffer = '';
      let eventType = null;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop();
        for (const line of lines) {
          if (line.startsWith('event: ')) { eventType = line.slice(7); }
          else if (line.startsWith('data: ') && eventType) {
            const data = JSON.parse(line.slice(6));
            if (eventType === 'done') {
              currentProjectPath = data.path;
              updateSessionIndicator('project', data.name || data.path.split('/').pop());
              showToast('Project saved with documentation: ' + (data.files || []).join(', '), 'success');
              clearSchemaSearchState();
    await loadSchema();
    await loadQueries();
    await loadRecipes();
    await loadMeasureOverridesEditor();
    await loadProjectHealth();
            } else if (eventType === 'error') {
              showToast('Error: ' + (data.error || 'Unknown error'), 'error');
            }
            eventType = null;
          }
        }
      }
    } catch (e) {
      console.error('Generate project failed:', e);
      showToast('Failed to save project', 'error');
    }
  } else {
    // No LLM — just save without generating
    btn.textContent = 'Saving...';
    try {
      const response = await fetch('/api/explore/save-project', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: projectPath, name: projectName || null })
      });
      const data = await response.json();

      if (!data.success) {
        errorEl.textContent = data.error || 'Failed to save project';
        return;
      }

      hideSavePopover();
      currentProjectPath = data.path;
      updateSessionIndicator('project', projectName || data.path.split('/').pop());
      showToast('Project saved to ' + data.path, 'success');
      clearSchemaSearchState();
      await loadSchema();
      await loadQueries();
      await loadRecipes();
      await loadMeasureOverridesEditor();
      await loadProjectHealth();
    } catch (e) {
      errorEl.textContent = 'Failed to save project';
    }
  }

  // Clear inputs
  pathInput.value = '';
  nameInput.value = '';
  if (descInput) descInput.value = '';
  btn.disabled = false;
  btn.textContent = 'Save';
}

function showToast(message, type) {
  const toast = document.createElement('div');
  toast.className = 'toast ' + (type || 'info');
  toast.textContent = message;
  document.body.appendChild(toast);
  window.setTimeout(() => { toast.remove(); }, 5000);
}
