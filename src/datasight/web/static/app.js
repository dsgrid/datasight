// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let sessionId = localStorage.getItem('datasight-session') || crypto.randomUUID();
localStorage.setItem('datasight-session', sessionId);
let isStreaming = false;
let currentAssistantBubble = null;
let currentAssistantText = '';
let selectedTable = null;
let allQueries = [];
let schemaData = [];
let lastSql = '';
let queryLogEnabled = false;
let confirmSqlEnabled = false;
let explainSqlEnabled = false;
let clarifySqlEnabled = false;
let pendingConfirmResolve = null;
let sessionQueries = [];
let pinnedItems = [];
let pinnedIdCounter = 0;
let currentView = 'chat';
let fullscreenCardId = null;
let selectedCardIdx = -1;
let currentAbortController = null;

const messagesEl = document.getElementById('messages');
const inputEl = document.getElementById('user-input');
const sendBtn = document.getElementById('send-btn');
const stopBtn = document.getElementById('stop-btn');
const welcomeEl = document.getElementById('welcome');
const sidebar = document.getElementById('sidebar');

// Configure marked
marked.setOptions({
  highlight: function(code, lang) {
    if (lang && hljs.getLanguage(lang)) {
      return hljs.highlight(code, { language: lang }).value;
    }
    return hljs.highlightAuto(code).value;
  },
  breaks: true,
  gfm: true,
});

// Sanitization helpers
function sanitizeHtml(html) {
  return DOMPurify.sanitize(html, { ADD_TAGS: ['iframe'], ADD_ATTR: ['srcdoc', 'sandbox', 'allowfullscreen'] });
}

function sanitizeMarkdown(text) {
  return DOMPurify.sanitize(marked.parse(text));
}

function renderMarkdownInto(el, text) {
  el.innerHTML = sanitizeMarkdown(text);
  el.querySelectorAll('pre code').forEach(block => {
    hljs.highlightElement(block);
  });
  addCopyButtons(el);
}

async function fetchJson(url, opts) {
  const resp = await fetch(url, opts);
  if (!resp.ok) throw new Error('API error: ' + resp.status);
  return resp.json();
}

// ---------------------------------------------------------------------------
// Sidebar
// ---------------------------------------------------------------------------
function toggleSidebar() {
  sidebar.classList.toggle('collapsed');
  const btn = document.getElementById('sidebar-toggle');
  btn.classList.toggle('active', !sidebar.classList.contains('collapsed'));
}

function toggleRightPanel() {
  const panel = document.getElementById('right-panel');
  panel.classList.toggle('collapsed');
  document.getElementById('sql-panel-toggle').classList.toggle('active', !panel.classList.contains('collapsed'));
}

// ---------------------------------------------------------------------------
// Projects Panel
// ---------------------------------------------------------------------------
let currentProjectPath = null;
let projectLoaded = false;

function toggleProjectsPanel() {
  const panel = document.getElementById('projects-panel');
  const overlay = document.getElementById('projects-overlay');
  const isOpen = panel.classList.contains('open');
  if (isOpen) {
    panel.classList.remove('open');
    overlay.classList.remove('open');
  } else {
    panel.classList.add('open');
    overlay.classList.add('open');
    loadRecentProjects();
  }
}

function openProjectsPanel() {
  const panel = document.getElementById('projects-panel');
  const overlay = document.getElementById('projects-overlay');
  panel.classList.add('open');
  overlay.classList.add('open');
  loadRecentProjects();
}

async function loadRecentProjects() {
  const container = document.getElementById('projects-list');
  try {
    const data = await fetchJson('/api/projects/recent');
    if (!data.projects || data.projects.length === 0) {
      container.innerHTML = '<div class="no-queries">No recent projects.</div>';
      return;
    }
    container.innerHTML = data.projects.map(p => `
      <div class="project-item${p.is_current ? ' current' : ''}" onclick="loadProjectFromList('${escapeHtml(p.path)}')" data-path="${escapeHtml(p.path)}">
        <div class="project-item-info">
          <div class="project-item-name">${escapeHtml(p.name)}</div>
          <div class="project-item-path">${escapeHtml(p.path)}</div>
        </div>
        <button class="project-item-remove" onclick="event.stopPropagation(); removeRecentProject('${escapeHtml(p.path)}')" title="Remove from list">&times;</button>
      </div>
    `).join('');
  } catch (e) {
    console.error('Failed to load recent projects:', e);
    container.innerHTML = '<div class="no-queries">Failed to load projects.</div>';
  }
}

async function loadProjectFromList(path) {
  if (path === currentProjectPath && projectLoaded) return;
  await doLoadProject(path);
}

async function doLoadProject(path) {
  const errorEl = document.getElementById('project-error');
  errorEl.classList.remove('visible');

  // Show loading overlay
  const panel = document.getElementById('projects-panel');
  let loadingOverlay = document.getElementById('project-loading-overlay');
  if (!loadingOverlay) {
    loadingOverlay = document.createElement('div');
    loadingOverlay.id = 'project-loading-overlay';
    loadingOverlay.style.cssText = 'position:absolute;inset:0;background:var(--surface);display:flex;flex-direction:column;align-items:center;justify-content:center;padding:40px;text-align:center;z-index:10;';
    panel.appendChild(loadingOverlay);
  }
  loadingOverlay.innerHTML = `
    <div style="font-size:1.1rem;font-weight:500;margin-bottom:12px;">Loading project...</div>
    <div style="font-size:0.75rem;color:var(--text-secondary);font-family:'JetBrains Mono',monospace;word-break:break-all;">${escapeHtml(path)}</div>
  `;
  loadingOverlay.style.display = 'flex';

  try {
    const response = await fetch('/api/projects/load', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path })
    });
    const data = await response.json();

    // Hide loading overlay
    loadingOverlay.style.display = 'none';

    if (!data.success) {
      errorEl.textContent = data.error || 'Failed to load project';
      errorEl.classList.add('visible');
      return;
    }

    // Project loaded successfully - update state and refresh UI
    projectLoaded = true;
    currentProjectPath = data.path;
    updateSessionIndicator('project', data.path.split('/').pop());

    // Close the panel and ensure main layout is visible
    hideLanding();
    toggleProjectsPanel();

    // Clear chat and reload everything for the new project
    await clearChatForProjectSwitch();
    await loadSchema();
    await loadQueries();
    await loadConversations();
    await loadBookmarks();
    await loadDashboard();

    // Show welcome message for the new project
    showWelcome();

  } catch (e) {
    console.error('Failed to load project:', e);
    loadingOverlay.style.display = 'none';
    errorEl.textContent = 'Failed to load project: ' + e.message;
    errorEl.classList.add('visible');
  }
}

async function clearChatForProjectSwitch() {
  // Clear local state
  sessionId = window.crypto.randomUUID();
  window.localStorage.setItem('datasight-session', sessionId);
  currentAssistantBubble = null;
  currentAssistantText = '';
  sessionQueries = [];

  // Clear messages display
  const messagesEl = document.getElementById('messages');
  messagesEl.innerHTML = '';

  // Clear query history panel
  const historyEl = document.getElementById('query-history');
  if (historyEl) {
    historyEl.innerHTML = '<span class="no-sql">No queries yet.</span>';
  }

  // Clear dashboard (will be reloaded from new project)
  pinnedItems = [];
  pinnedIdCounter = 0;
  dashboardColumns = 0;
  updateDashboardBadge();
  renderDashboard();
}

function showWelcome() {
  const messagesEl = document.getElementById('messages');
  messagesEl.innerHTML = `
    <div class="welcome" id="welcome">
      <h2>Welcome to datasight</h2>
      <p>Ask questions about your data in plain English. I can query the database, analyze results, and create visualizations.</p>
      <p style="font-size:0.85rem;">Browse tables in the sidebar, or try one of these:</p>
      <div class="examples">
        <button class="example-btn" onclick="sendExample(this.textContent)">What tables are available and how many rows do they have?</button>
        <button class="example-btn" onclick="summarizeDataset()">Show me a summary of the data</button>
      </div>
    </div>
  `;
}

async function openProjectFromInput() {
  const input = document.getElementById('project-path-input');
  const errorEl = document.getElementById('project-error');
  const path = input.value.trim();

  if (!path) {
    errorEl.textContent = 'Please enter a project path';
    errorEl.classList.add('visible');
    return;
  }

  input.value = '';
  await doLoadProject(path);
}

async function removeRecentProject(path) {
  try {
    await fetch('/api/projects/recent/' + encodeURIComponent(path), { method: 'DELETE' });
    loadRecentProjects();
  } catch (e) {
    console.error('Failed to remove project:', e);
  }
}

// ---------------------------------------------------------------------------
// Quick Explore
// ---------------------------------------------------------------------------

let explorePaths = [];

function renderExploreFilesList() {
  const container = document.getElementById('explore-files-list');
  if (!explorePaths.length) {
    container.innerHTML = '';
    return;
  }
  container.innerHTML = explorePaths.map((p, i) => `
    <div class="explore-file-item">
      <span class="file-name" title="${escapeHtml(p)}">${escapeHtml(p.split('/').pop())}</span>
      <button class="remove-file" onclick="removeExplorePath(${i})" title="Remove">&times;</button>
    </div>
  `).join('');
}

function removeExplorePath(index) {
  explorePaths.splice(index, 1);
  renderExploreFilesList();
}

function addExplorePath() {
  const input = document.getElementById('explore-path-input');
  const errorEl = document.getElementById('explore-error');
  const path = input.value.trim();

  errorEl.classList.remove('visible');

  if (!path) return;

  if (!explorePaths.includes(path)) {
    explorePaths.push(path);
    renderExploreFilesList();
  }
  input.value = '';
}

async function exploreFromInput() {
  const input = document.getElementById('explore-path-input');
  const path = input.value.trim();

  // Add to list if non-empty
  if (path && !explorePaths.includes(path)) {
    explorePaths.push(path);
    renderExploreFilesList();
    input.value = '';
  }

  // Start exploring with whatever is in the list
  await startExplore();
}

async function startExplore() {
  if (!explorePaths.length) {
    const errorEl = document.getElementById('explore-error');
    errorEl.textContent = 'Add at least one file or directory path';
    errorEl.classList.add('visible');
    return;
  }

  const errorEl = document.getElementById('explore-error');
  const exploreBtn = document.getElementById('explore-btn');
  errorEl.classList.remove('visible');
  exploreBtn.disabled = true;
  exploreBtn.textContent = 'Loading...';

  try {
    const response = await fetch('/api/explore', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ paths: explorePaths })
    });
    const data = await response.json();

    if (!data.success) {
      errorEl.textContent = data.error || 'Failed to explore files';
      errorEl.classList.add('visible');
      return;
    }

    // Success - update UI state
    projectLoaded = true;
    currentProjectPath = null;
    explorePaths = [];
    renderExploreFilesList();

    llmConnected = !!data.llm_connected;
    updateSessionIndicator('explore', data.tables.length);

    // Transition to chat UI
    hideLanding();
    toggleProjectsPanel();
    await clearChatForProjectSwitch();
    await loadSchema();
    showWelcome();

    if (!llmConnected) {
      window.setTimeout(promptLlmConfigIfNeeded, 350);
    }

  } catch (e) {
    console.error('Failed to start explore:', e);
    errorEl.textContent = 'Failed to connect to server';
    errorEl.classList.add('visible');
  } finally {
    exploreBtn.disabled = false;
    exploreBtn.textContent = 'Explore';
  }
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------
function switchView(view) {
  currentView = view;
  const chatArea = document.querySelector('.chat-area');
  const dashboard = document.getElementById('dashboard');
  document.getElementById('tab-chat').classList.toggle('active', view === 'chat');
  document.getElementById('tab-dashboard').classList.toggle('active', view === 'dashboard');
  chatArea.style.display = view === 'chat' ? '' : 'none';
  dashboard.style.display = view === 'dashboard' ? '' : 'none';
  if (view === 'dashboard') {
    broadcastThemeToDashboard();
  } else {
    // Reset selection when leaving dashboard
    selectedCardIdx = -1;
    renderDashboard();
  }
}

let dashboardColumns = 0; // 0 = auto

function pinResult(btn) {
  const resultEl = btn.closest('.tool-result');
  if (!resultEl) return;
  const iframe = resultEl.querySelector('iframe');
  const type = iframe ? 'chart' : 'table';
  const html = iframe ? iframe.srcdoc : resultEl.querySelector('.result-table-wrap').outerHTML;
  // Get title from result data attribute, or try to find from chart title
  let title = resultEl.dataset.title || '';
  pinnedIdCounter++;
  pinnedItems.push({ id: pinnedIdCounter, type, html, title });
  updateDashboardBadge();
  renderDashboard();
  saveDashboard();
  const pinIcon = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M9.5 2L14 6.5 8.5 12 4 14 2 12 3.5 7.5z"/><path d="M2 14l4-4"/></svg>';
  btn.innerHTML = pinIcon + ' Pinned!';
  setTimeout(() => { btn.innerHTML = pinIcon + ' Pin'; }, 1200);
}

function unpinItem(id) {
  pinnedItems = pinnedItems.filter(item => item.id !== id);
  updateDashboardBadge();
  renderDashboard();
  saveDashboard();
}

function toggleCardFullscreen(id) {
  if (fullscreenCardId === id) {
    // Exit fullscreen
    fullscreenCardId = null;
    document.body.classList.remove('dashboard-fullscreen-active');
  } else {
    // Enter fullscreen
    fullscreenCardId = id;
    document.body.classList.add('dashboard-fullscreen-active');
  }
  renderDashboard();
}

function exitCardFullscreen() {
  if (fullscreenCardId !== null) {
    fullscreenCardId = null;
    document.body.classList.remove('dashboard-fullscreen-active');
    renderDashboard();
  }
}

function updateDashboardBadge() {
  const tab = document.getElementById('tab-dashboard');
  const existing = tab.querySelector('.badge');
  if (existing) existing.remove();
  if (pinnedItems.length > 0) {
    const badge = document.createElement('span');
    badge.className = 'badge';
    badge.textContent = pinnedItems.length;
    tab.appendChild(badge);
  }
}

function setDashboardColumns(cols) {
  dashboardColumns = cols;
  const grid = document.getElementById('dashboard-grid');
  if (cols === 0) {
    grid.style.gridTemplateColumns = '';
    grid.classList.remove('cols-1', 'cols-2', 'cols-3');
  } else {
    grid.classList.remove('cols-1', 'cols-2', 'cols-3');
    grid.classList.add('cols-' + cols);
    grid.style.gridTemplateColumns = 'repeat(' + cols + ', 1fr)';
  }
  // Update active button
  document.querySelectorAll('.layout-btn').forEach(btn => {
    btn.classList.toggle('active', parseInt(btn.dataset.cols) === cols);
  });
  // Resize charts
  grid.querySelectorAll('iframe').forEach(iframe => {
    try { iframe.contentWindow.Plotly.Plots.resize(iframe.contentDocument.getElementById('chart')); } catch(e) {}
  });
  // Persist column setting
  saveDashboard();
}

function renderDashboard() {
  const grid = document.getElementById('dashboard-grid');
  const empty = document.getElementById('dashboard-empty');
  const toolbar = document.getElementById('dashboard-toolbar');
  grid.innerHTML = '';

  if (pinnedItems.length === 0) {
    empty.style.display = '';
    toolbar.style.display = 'none';
    return;
  }
  empty.style.display = 'none';
  toolbar.style.display = '';

  pinnedItems.forEach((item, idx) => {
    const card = document.createElement('div');
    let cardClass = 'dashboard-card';
    if (fullscreenCardId === item.id) cardClass += ' fullscreen';
    if (selectedCardIdx === idx) cardClass += ' selected';
    card.className = cardClass;
    card.draggable = true;
    card.dataset.idx = idx;

    // Card header with drag handle and title
    const header = document.createElement('div');
    header.className = 'dashboard-card-header';

    const handle = document.createElement('div');
    handle.className = 'dashboard-drag-handle';
    handle.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="currentColor" opacity="0.4"><circle cx="5" cy="3" r="1.5"/><circle cx="11" cy="3" r="1.5"/><circle cx="5" cy="8" r="1.5"/><circle cx="11" cy="8" r="1.5"/><circle cx="5" cy="13" r="1.5"/><circle cx="11" cy="13" r="1.5"/></svg>';
    header.appendChild(handle);

    const titleInput = document.createElement('input');
    titleInput.type = 'text';
    titleInput.className = 'dashboard-card-title';
    titleInput.placeholder = 'Add title...';
    titleInput.value = item.title || '';
    titleInput.addEventListener('change', () => {
      item.title = titleInput.value;
      saveDashboard();
    });
    titleInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') titleInput.blur();
    });
    header.appendChild(titleInput);

    const fullscreenBtn = document.createElement('button');
    fullscreenBtn.className = 'dashboard-fullscreen-btn';
    fullscreenBtn.title = 'Toggle fullscreen';
    fullscreenBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M2 6V2h4M14 6V2h-4M2 10v4h4M14 10v4h-4"/></svg>';
    fullscreenBtn.onclick = (e) => {
      e.stopPropagation();
      toggleCardFullscreen(item.id);
    };
    header.appendChild(fullscreenBtn);

    card.appendChild(header);

    if (item.type === 'chart') {
      const iframe = document.createElement('iframe');
      iframe.sandbox = 'allow-scripts allow-same-origin allow-downloads';
      iframe.srcdoc = item.html;
      iframe.addEventListener('load', () => {
        const theme = document.documentElement.getAttribute('data-theme') || 'light';
        try { iframe.contentWindow.postMessage({ type: 'theme-change', theme }, '*'); } catch(e) {}
      });
      card.appendChild(iframe);
      // Resize chart when card resizes
      const ro = new window.ResizeObserver(() => {
        try { iframe.contentWindow.Plotly.Plots.resize(iframe.contentDocument.getElementById('chart')); } catch(e) {}
      });
      ro.observe(card);
    } else {
      const tableDiv = document.createElement('div');
      tableDiv.innerHTML = sanitizeHtml(item.html);
      const tableWrap = tableDiv.querySelector('.result-table-wrap');
      if (tableWrap) paginateTable(tableWrap);
      card.appendChild(tableDiv);
    }

    const actions = document.createElement('div');
    actions.className = 'dashboard-card-actions';
    const unpinBtn = document.createElement('button');
    unpinBtn.className = 'unpin-btn';
    unpinBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><line x1="4" y1="4" x2="12" y2="12"/><line x1="12" y1="4" x2="4" y2="12"/></svg> Unpin';
    unpinBtn.onclick = () => unpinItem(item.id);
    actions.appendChild(unpinBtn);
    card.appendChild(actions);

    // Drag and drop
    card.addEventListener('dragstart', (e) => {
      e.dataTransfer.setData('text/plain', idx);
      card.classList.add('dragging');
    });
    card.addEventListener('dragend', () => {
      card.classList.remove('dragging');
      grid.querySelectorAll('.dashboard-card').forEach(c => c.classList.remove('drag-over'));
    });
    card.addEventListener('dragover', (e) => {
      e.preventDefault();
      card.classList.add('drag-over');
    });
    card.addEventListener('dragleave', () => {
      card.classList.remove('drag-over');
    });
    card.addEventListener('drop', (e) => {
      e.preventDefault();
      card.classList.remove('drag-over');
      const fromIdx = parseInt(e.dataTransfer.getData('text/plain'));
      const toIdx = parseInt(card.dataset.idx);
      if (fromIdx !== toIdx && !isNaN(fromIdx) && !isNaN(toIdx)) {
        const moved = pinnedItems.splice(fromIdx, 1)[0];
        pinnedItems.splice(toIdx, 0, moved);
        renderDashboard();
        saveDashboard();
      }
    });

    grid.appendChild(card);
  });

  // Reapply column setting
  if (dashboardColumns > 0) {
    setDashboardColumns(dashboardColumns);
  }
}

function broadcastThemeToDashboard() {
  const theme = document.documentElement.getAttribute('data-theme') || 'light';
  document.querySelectorAll('#dashboard-grid iframe').forEach(iframe => {
    try { iframe.contentWindow.postMessage({ type: 'theme-change', theme }, '*'); } catch(e) {}
  });
}

async function loadDashboard() {
  try {
    const data = await fetchJson('/api/dashboard');
    pinnedItems = data.items || [];
    dashboardColumns = data.columns || 0;
    // Update next ID counter
    if (pinnedItems.length > 0) {
      pinnedIdCounter = Math.max(...pinnedItems.map(item => item.id || 0)) + 1;
    }
    updateDashboardBadge();
    renderDashboard();
  } catch (e) {
    console.error('Failed to load dashboard:', e);
  }
}

async function saveDashboard() {
  try {
    await fetch('/api/dashboard', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: pinnedItems, columns: dashboardColumns })
    });
  } catch (e) {
    console.error('Failed to save dashboard:', e);
  }
}

function syncChartScales() {
  const grid = document.getElementById('dashboard-grid');
  const iframes = grid.querySelectorAll('iframe');

  if (iframes.length < 2) {
    window.alert('Need at least 2 charts to sync scales.');
    return;
  }

  // Collect y-axis ranges from all charts
  let globalMin = Infinity;
  let globalMax = -Infinity;
  const chartFrames = [];

  iframes.forEach(iframe => {
    try {
      const plotlyDiv = iframe.contentDocument.getElementById('chart');
      if (plotlyDiv && plotlyDiv.data) {
        chartFrames.push(iframe);
        // Find min/max across all traces
        plotlyDiv.data.forEach(trace => {
          const values = trace.y || trace.values || [];
          values.forEach(v => {
            if (typeof v === 'number' && isFinite(v)) {
              if (v < globalMin) globalMin = v;
              if (v > globalMax) globalMax = v;
            }
          });
        });
      }
    } catch (e) {
      // Cross-origin or no Plotly - skip
    }
  });

  if (chartFrames.length < 2 || !isFinite(globalMin) || !isFinite(globalMax)) {
    window.alert('Could not read chart data. Make sure charts are fully loaded.');
    return;
  }

  // Add 5% padding to the range
  const padding = (globalMax - globalMin) * 0.05;
  const yMin = globalMin - padding;
  const yMax = globalMax + padding;

  // Apply the common range to all charts
  chartFrames.forEach(iframe => {
    try {
      const Plotly = iframe.contentWindow.Plotly;
      const plotlyDiv = iframe.contentDocument.getElementById('chart');
      if (Plotly && plotlyDiv) {
        Plotly.relayout(plotlyDiv, {
          'yaxis.range': [yMin, yMax],
          'yaxis.autorange': false
        });
      }
    } catch (e) {
      // Skip on error
    }
  });
}

async function exportDashboard() {
  if (pinnedItems.length === 0) {
    window.alert('No items to export. Pin some charts or tables first.');
    return;
  }

  // Prepare items for export
  const items = pinnedItems.map(item => ({
    type: item.type,
    html: item.html,
    title: item.title || ''
  }));

  // Determine columns from current setting
  const columns = dashboardColumns > 0 ? dashboardColumns : 2;

  try {
    const response = await fetch('/api/dashboard/export', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items, columns, title: 'datasight dashboard' })
    });

    if (!response.ok) throw new Error('Export failed');

    // Download the HTML file
    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'datasight-dashboard.html';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
  } catch (e) {
    console.error('Failed to export dashboard:', e);
    window.alert('Failed to export dashboard. Please try again.');
  }
}

async function toggleQueryLog() {
  try {
    const data = await fetchJson('/api/query-log/toggle', { method: 'POST' });
    queryLogEnabled = data.enabled;
    updateSettingsButtons();
  } catch (e) {
    console.error('Failed to toggle query log:', e);
  }
}

async function loadQueryLogState() {
  try {
    const data = await fetchJson('/api/query-log?n=0');
    queryLogEnabled = data.enabled;
    updateSettingsButtons();
  } catch (e) {
    // Ignore — button defaults to off
  }
}

let llmConnected = false;

async function loadSettings() {
  try {
    const data = await fetchJson('/api/settings');
    confirmSqlEnabled = data.confirm_sql;
    explainSqlEnabled = data.explain_sql;
    clarifySqlEnabled = data.clarify_sql;
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

let isEphemeralSession = false;

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
      '<button class="indicator-action" onclick="showSavePopover()">Save</button>';
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

function updateSettingsButtons() {
  // Update settings panel checkboxes
  const confirmCheck = document.getElementById('setting-confirm-sql');
  const explainCheck = document.getElementById('setting-explain-sql');
  const clarifyCheck = document.getElementById('setting-clarify-sql');
  const queryLogCheck = document.getElementById('setting-query-log');

  if (confirmCheck) confirmCheck.checked = confirmSqlEnabled;
  if (explainCheck) explainCheck.checked = explainSqlEnabled;
  if (clarifyCheck) clarifyCheck.checked = clarifySqlEnabled;
  if (queryLogCheck) queryLogCheck.checked = queryLogEnabled;

  // Update settings gear button to show if any non-default settings are active
  const settingsBtn = document.getElementById('settings-toggle');
  if (settingsBtn) {
    const hasActiveSettings = confirmSqlEnabled || explainSqlEnabled || !clarifySqlEnabled || queryLogEnabled;
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
  }
}

function updateSqlDisplay(sql) {
  lastSql = sql;
}

function handleToolDone(data) {
  sessionQueries.unshift(data);
  renderQueryHistory();
}

function renderQueryHistory() {
  const container = document.getElementById('query-history');
  if (sessionQueries.length === 0) {
    container.innerHTML = '<span class="no-sql">No queries yet.</span>';
    return;
  }
  container.innerHTML = '';
  sessionQueries.forEach((q, i) => {
    const card = document.createElement('div');
    card.className = 'query-card' + (q.error ? ' error' : '');

    const header = document.createElement('div');
    header.className = 'query-card-header';

    const pill = document.createElement('span');
    pill.className = 'query-card-pill';
    pill.textContent = q.tool === 'visualize_data' ? 'Chart' : 'SQL';
    header.appendChild(pill);

    const meta = document.createElement('span');
    meta.className = 'query-card-meta';
    const parts = [];
    if (q.timestamp) {
      const ts = new Date(q.timestamp);
      parts.push(ts.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }));
    }
    if (q.execution_time_ms != null) parts.push(Math.round(q.execution_time_ms) + ' ms');
    if (q.row_count != null) parts.push(q.row_count + ' rows');
    if (q.error) parts.push('error');
    meta.textContent = '· ' + parts.join(' · ');
    header.appendChild(meta);

    card.appendChild(header);

    const sqlPre = document.createElement('pre');
    sqlPre.className = 'query-card-sql';
    const sqlCode = document.createElement('code');
    sqlCode.className = 'language-sql';
    sqlCode.textContent = q.sql || '';
    hljs.highlightElement(sqlCode);
    sqlPre.appendChild(sqlCode);
    sqlPre.onclick = () => sqlPre.classList.toggle('expanded');
    card.appendChild(sqlPre);

    const actions = document.createElement('div');
    actions.className = 'query-card-actions';

    const copyBtn = document.createElement('button');
    copyBtn.className = 'query-card-btn';
    copyBtn.textContent = 'Copy';
    copyBtn.onclick = (e) => {
      e.stopPropagation();
      navigator.clipboard.writeText(q.sql || '').then(() => {
        copyBtn.textContent = 'Copied!';
        setTimeout(() => { copyBtn.textContent = 'Copy'; }, 1500);
      });
    };
    actions.appendChild(copyBtn);

    const rerunBtn = document.createElement('button');
    rerunBtn.className = 'query-card-btn';
    rerunBtn.textContent = 'Rerun';
    rerunBtn.onclick = (e) => {
      e.stopPropagation();
      inputEl.value = 'Run this SQL query:\n' + q.sql;
      inputEl.focus();
    };
    actions.appendChild(rerunBtn);

    if (q.sql) {
      const bookmarkBtn = document.createElement('button');
      bookmarkBtn.className = 'query-card-btn';
      bookmarkBtn.textContent = '★';
      bookmarkBtn.title = 'Bookmark this query';
      bookmarkBtn.onclick = (e) => {
        e.stopPropagation();
        bookmarkQuery(q.sql, q.tool, '');
        bookmarkBtn.textContent = '★!';
        setTimeout(() => { bookmarkBtn.textContent = '★'; }, 1200);
      };
      actions.appendChild(bookmarkBtn);
    }

    card.appendChild(actions);
    container.appendChild(card);
  });
}

async function loadSchema() {
  try {
    const data = await fetchJson('/api/schema');
    schemaData = data.tables || [];
    renderTables(schemaData);
  } catch (e) {
    document.getElementById('tables-container').innerHTML =
      '<div class="no-queries">Failed to load schema</div>';
  }
}

async function loadQueries() {
  try {
    const data = await fetchJson('/api/queries');
    allQueries = data.queries || [];
    renderQueries(allQueries);
  } catch (e) {
    document.getElementById('queries-list').innerHTML =
      '<div class="no-queries">Failed to load queries</div>';
  }
}

function renderTables(tables) {
  const container = document.getElementById('tables-container');
  if (!tables.length) {
    container.innerHTML = '<div class="no-queries">No tables found</div>';
    return;
  }

  container.innerHTML = tables.map((t, idx) => {
    const rowCount = t.row_count != null ? Number(t.row_count).toLocaleString() : '?';
    const isView = t.name.startsWith('v_');
    const icon = isView
      ? '<svg class="table-icon" viewBox="0 0 16 16" fill="none"><path d="M8 2C4.13 2 1 3.79 1 6v4c0 2.21 3.13 4 7 4s7-1.79 7-4V6c0-2.21-3.13-4-7-4z" stroke="currentColor" stroke-width="1.3" fill="none"/><path d="M1 6c0 2.21 3.13 4 7 4s7-1.79 7-4" stroke="currentColor" stroke-width="1.3" fill="none"/></svg>'
      : '<svg class="table-icon" viewBox="0 0 16 16" fill="none"><rect x="1" y="2" width="14" height="12" rx="2" stroke="currentColor" stroke-width="1.3"/><line x1="1" y1="5.5" x2="15" y2="5.5" stroke="currentColor" stroke-width="1.3"/><line x1="1" y1="9" x2="15" y2="9" stroke="currentColor" stroke-width="1"/><line x1="6" y1="5.5" x2="6" y2="14" stroke="currentColor" stroke-width="1"/></svg>';

    const cols = (t.columns || []).map(c =>
      '<div class="column-item" onclick="toggleColumnStats(this, \'' + escapeHtml(t.name) + '\', \'' + escapeHtml(c.name) + '\')">' +
        '<span class="col-name">' + escapeHtml(c.name) + '</span>' +
        '<span class="col-type">' + escapeHtml(c.dtype) + '</span>' +
        '<div class="col-stats"></div>' +
      '</div>'
    ).join('');

    return '<div class="table-item">' +
      '<div class="table-header" data-table="' + escapeHtml(t.name) + '" data-idx="' + idx + '" onclick="toggleTable(this)">' +
        icon +
        '<span class="table-name">' + escapeHtml(t.name) + '</span>' +
        '<span class="table-rows">' + rowCount + '</span>' +
        '<svg class="table-chevron" viewBox="0 0 16 16" fill="none"><path d="M6 4l4 4-4 4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>' +
      '</div>' +
      '<div class="column-list" id="cols-' + idx + '">' +
        '<button class="preview-btn" onclick="event.stopPropagation(); previewTable(\'' + escapeHtml(t.name) + '\', this)">Preview rows</button>' +
        '<div class="table-preview" id="preview-' + idx + '"></div>' +
        cols +
      '</div>' +
    '</div>';
  }).join('');
}

function toggleTable(headerEl) {
  const idx = headerEl.dataset.idx;
  const tableName = headerEl.dataset.table;
  const colList = document.getElementById('cols-' + idx);
  const wasExpanded = headerEl.classList.contains('expanded');

  // Toggle expand/collapse
  headerEl.classList.toggle('expanded');
  colList.classList.toggle('visible');

  // Update selected state
  const allHeaders = document.querySelectorAll('.table-header');
  allHeaders.forEach(h => h.classList.remove('selected'));

  if (!wasExpanded) {
    headerEl.classList.add('selected');
    selectedTable = tableName;
  } else {
    selectedTable = null;
  }

  // Filter queries
  filterQueries();
}

async function toggleColumnStats(el, tableName, colName) {
  const statsEl = el.querySelector('.col-stats');
  if (statsEl.innerHTML) {
    statsEl.innerHTML = '';
    return;
  }
  statsEl.innerHTML = '<span class="col-stats-loading">Loading...</span>';
  try {
    const data = await fetchJson('/api/column-stats/' + encodeURIComponent(tableName) + '/' + encodeURIComponent(colName));
    if (data.stats) {
      const s = data.stats;
      const parts = [];
      if (s.distinct != null) parts.push('<span>Distinct: <b>' + s.distinct + '</b></span>');
      if (s.nulls != null) parts.push('<span>Nulls: <b>' + s.nulls + '</b></span>');
      if (s.min != null) parts.push('<span>Min: <b>' + escapeHtml(String(s.min)) + '</b></span>');
      if (s.max != null) parts.push('<span>Max: <b>' + escapeHtml(String(s.max)) + '</b></span>');
      if (s.avg != null) parts.push('<span>Avg: <b>' + s.avg + '</b></span>');
      statsEl.innerHTML = parts.join('') || '<span>No stats</span>';
    } else {
      statsEl.innerHTML = '';
    }
  } catch (e) {
    statsEl.innerHTML = '';
  }
}

async function previewTable(tableName, btn) {
  const previewEl = btn.nextElementSibling;
  if (previewEl.innerHTML) {
    previewEl.innerHTML = '';
    btn.textContent = 'Preview rows';
    return;
  }
  btn.textContent = 'Loading...';
  try {
    const data = await fetchJson('/api/preview/' + encodeURIComponent(tableName));
    if (data.html) {
      previewEl.innerHTML = sanitizeHtml(data.html);
      bindTableEvents(previewEl);
      btn.textContent = 'Hide preview';
    } else {
      btn.textContent = 'Preview rows';
    }
  } catch (e) {
    btn.textContent = 'Preview rows';
  }
}

function filterQueries() {
  if (!selectedTable) {
    renderQueries(allQueries);
    return;
  }

  const filtered = allQueries.filter(q => {
    const sqlLower = (q.sql || '').toLowerCase();
    const qLower = (q.question || '').toLowerCase();
    const tLower = selectedTable.toLowerCase();
    return sqlLower.includes(tLower) || qLower.includes(tLower);
  });

  renderQueries(filtered.length > 0 ? filtered : allQueries, filtered.length > 0 ? selectedTable : null);
}

function renderQueries(queries, filterLabel) {
  const container = document.getElementById('queries-list');
  const countEl = document.getElementById('query-count');

  if (filterLabel) {
    countEl.textContent = queries.length + ' for ' + filterLabel;
  } else {
    countEl.textContent = queries.length ? String(queries.length) : '';
  }

  if (!queries.length) {
    container.innerHTML = '<div class="no-queries">No example queries</div>';
    return;
  }

  container.innerHTML = queries.map(q => {
    const sqlPreview = (q.sql || '').split('\n')[0].trim();
    return '<div class="query-item" onclick="useQuery(this)" data-question="' + escapeHtml(q.question) + '">' +
      '<div class="query-question">' + escapeHtml(q.question) + '</div>' +
      '<div class="query-sql-preview">' + escapeHtml(sqlPreview) + '</div>' +
    '</div>';
  }).join('');
}

function useQuery(el) {
  const question = el.dataset.question;
  if (question && !isStreaming) {
    sendMessage(question);
  }
}

// ---------------------------------------------------------------------------
// Auto-grow textarea
// ---------------------------------------------------------------------------
inputEl.addEventListener('input', function() {
  this.style.height = 'auto';
  this.style.height = Math.min(this.scrollHeight, 150) + 'px';
});

inputEl.addEventListener('keydown', function(e) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    handleSubmit(e);
  }
});

// ---------------------------------------------------------------------------
// Send message
// ---------------------------------------------------------------------------
function handleSubmit(e) {
  e.preventDefault();
  const text = inputEl.value.trim();
  if (!text || isStreaming) return false;
  sendMessage(text);
  return false;
}

function sendExample(text) {
  if (isStreaming) return;
  sendMessage(text);
}

async function summarizeDataset() {
  if (isStreaming) return;
  if (welcomeEl) welcomeEl.style.display = 'none';

  isStreaming = true;
  const btn = document.getElementById('summarize-btn');
  if (btn) btn.disabled = true;
  sendBtn.style.display = 'none';
  stopBtn.style.display = '';

  // Add a "system" style intro message
  const introRow = document.createElement('div');
  introRow.className = 'message-row assistant';
  const introBubble = document.createElement('div');
  introBubble.className = 'message-bubble';
  introBubble.innerHTML = '<strong>Dataset Summary</strong>';
  introRow.appendChild(introBubble);
  messagesEl.appendChild(introRow);

  currentAbortController = new window.AbortController();
  const typingEl = addTypingIndicator();
  let summaryText = '';
  let summaryBubble = null;

  try {
    const resp = await fetch('/api/summarize', {
      signal: currentAbortController.signal,
    });
    if (!resp.ok) throw new Error('Summarize API error: ' + resp.status);

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let typingRemoved = false;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();

      let eventType = '';
      for (const line of lines) {
        if (line.startsWith('event: ')) {
          eventType = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          if (!typingRemoved) {
            typingEl.remove();
            typingRemoved = true;
          }
          try {
            const data = JSON.parse(line.slice(6));
            if (eventType === 'token' && data.text) {
              summaryText += data.text;
              if (!summaryBubble) {
                const row = document.createElement('div');
                row.className = 'message-row assistant';
                summaryBubble = document.createElement('div');
                summaryBubble.className = 'message-bubble';
                row.appendChild(summaryBubble);
                messagesEl.appendChild(row);
              }
              renderMarkdownInto(summaryBubble, summaryText);
              scrollToBottom();
            } else if (eventType === 'error' && data.error) {
              addMessage('assistant', 'Error: ' + data.error);
            }
          } catch (parseErr) {
            console.error('Failed to parse SSE event:', parseErr, line);
          }
          eventType = '';
        }
      }
    }

    if (!typingRemoved) typingEl.remove();
  } catch (err) {
    if (err.name === 'AbortError') {
      if (document.contains(typingEl)) typingEl.remove();
      addMessage('assistant', 'Summary generation stopped.');
    } else {
      console.error('Summarize error:', err);
      if (document.contains(typingEl)) typingEl.remove();
      addMessage('assistant', 'Failed to generate summary. Please try again.');
    }
  }

  isStreaming = false;
  currentAbortController = null;
  if (btn) btn.disabled = false;
  sendBtn.style.display = '';
  stopBtn.style.display = 'none';
  inputEl.focus();
}

async function sendMessage(text) {
  if (welcomeEl) welcomeEl.style.display = 'none';
  const oldSuggestions = messagesEl.querySelector('.suggestions');
  if (oldSuggestions) oldSuggestions.remove();

  addMessage('user', text);

  inputEl.value = '';
  inputEl.style.height = 'auto';

  isStreaming = true;
  sendBtn.style.display = 'none';
  stopBtn.style.display = '';
  currentAssistantText = '';
  currentAssistantBubble = null;

  currentAbortController = new window.AbortController();
  const typingEl = addTypingIndicator();

  try {
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, session_id: sessionId }),
      signal: currentAbortController.signal,
    });

    if (!resp.ok) throw new Error('Chat API error: ' + resp.status);

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let typingRemoved = false;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();

      let eventType = '';
      for (const line of lines) {
        if (line.startsWith('event: ')) {
          eventType = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          if (!typingRemoved) {
            typingEl.remove();
            typingRemoved = true;
          }
          try {
            const data = JSON.parse(line.slice(6));
            handleSSEEvent(eventType, data);
          } catch (parseErr) {
            console.error('Failed to parse SSE event:', parseErr, line);
          }
          eventType = '';
        }
      }
    }

    if (!typingRemoved) typingEl.remove();
  } catch (err) {
    if (err.name === 'AbortError') {
      if (document.contains(typingEl)) typingEl.remove();
      addMessage('assistant', 'Generation stopped.');
    } else {
      console.error('Stream error:', err);
      if (document.contains(typingEl)) typingEl.remove();
      addMessage('assistant', 'Connection error. Please try again.');
    }
  }

  isStreaming = false;
  currentAbortController = null;
  sendBtn.style.display = '';
  stopBtn.style.display = 'none';
  currentAssistantBubble = null;
  inputEl.focus();
  loadConversations();
}

function stopGeneration() {
  if (currentAbortController) {
    currentAbortController.abort();
  }
}

// ---------------------------------------------------------------------------
// SSE event handler
// ---------------------------------------------------------------------------
function handleSSEEvent(eventType, data) {
  switch (eventType) {
    case 'tool_start':       handleToolStart(data); break;
    case 'tool_result':      handleToolResult(data); break;
    case 'tool_done':        handleToolDone(data); break;
    case 'token':            handleToken(data); break;
    case 'done':             finalize(); break;
    case 'suggestions':      handleSuggestions(data); break;
    case 'sql_confirm':      handleSqlConfirm(data); break;
    case 'sql_rejected':     handleSqlRejected(); break;
    case 'explanation_done': handleExplanationDone(); break;
  }
}

function finalizeAssistantBubble() {
  if (currentAssistantBubble && currentAssistantText) {
    renderMarkdownInto(currentAssistantBubble, currentAssistantText);
    currentAssistantBubble = null;
    currentAssistantText = '';
  }
}

function handleSqlConfirm(data) {
  // Finalize any in-progress explanation text
  finalizeAssistantBubble();

  const el = document.createElement('div');
  el.className = 'sql-confirm-dialog';
  el.innerHTML =
    '<div class="sql-confirm-header">' +
      '<span class="dot" style="animation:none;background:var(--yellow);opacity:1"></span>' +
      '<span>Review SQL before execution</span>' +
    '</div>' +
    '<textarea class="sql-confirm-editor" spellcheck="false">' + escapeHtml(data.sql) + '</textarea>' +
    '<div class="sql-confirm-actions">' +
      '<button class="sql-confirm-btn approve" onclick="respondSqlConfirm(this,\'' + escapeHtml(data.request_id) + '\',\'approve\')">Approve</button>' +
      '<button class="sql-confirm-btn edit" onclick="respondSqlConfirm(this,\'' + escapeHtml(data.request_id) + '\',\'edit\')">Approve with edits</button>' +
      '<button class="sql-confirm-btn reject" onclick="respondSqlConfirm(this,\'' + escapeHtml(data.request_id) + '\',\'reject\')">Reject</button>' +
    '</div>';
  messagesEl.appendChild(el);
  scrollToBottom();

  // Focus the textarea and apply syntax highlighting
  const textarea = el.querySelector('.sql-confirm-editor');
  textarea.focus();
}

async function respondSqlConfirm(btn, requestId, action) {
  const dialog = btn.closest('.sql-confirm-dialog');
  const textarea = dialog.querySelector('.sql-confirm-editor');
  const sql = textarea.value;

  // Disable buttons
  dialog.querySelectorAll('button').forEach(b => b.disabled = true);

  const label = action === 'approve' ? 'Approved' : action === 'edit' ? 'Approved (edited)' : 'Rejected';
  const style = action === 'reject' ? 'color:var(--red)' : 'color:var(--teal)';
  dialog.querySelector('.sql-confirm-actions').innerHTML =
    '<span style="' + style + ';font-size:0.85rem">' + label + '</span>';

  try {
    await fetch('/api/sql-confirm/' + requestId, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action, sql }),
    });
  } catch (e) {
    console.error('Failed to confirm SQL:', e);
  }
}

function handleSqlRejected() {
  // Visual feedback already handled by respondSqlConfirm
}

function handleExplanationDone() {
  // Finalize the explanation text bubble so tool results appear separately
  finalizeAssistantBubble();
}

function handleToolStart(data) {
  const el = document.createElement('div');
  el.className = 'tool-indicator';
  const toolLabel = data.tool === 'run_sql' ? 'Running SQL' : 'Creating visualization';
  let html = '<span class="dot"></span><span>' + toolLabel + '...</span>';
  if (data.input && data.input.sql) {
    const sqlPreview = data.input.sql.length > 80
      ? data.input.sql.substring(0, 80) + '...'
      : data.input.sql;
    html += '<span class="sql-preview">' + escapeHtml(sqlPreview) + '</span>';
    updateSqlDisplay(data.input.sql);
  }
  el.innerHTML = html;
  const delTool = _makeDeleteBtn('Delete');
  delTool.onclick = (e) => { e.stopPropagation(); deleteElement(el); };
  el.appendChild(delTool);
  messagesEl.appendChild(el);
  scrollToBottom();
}

function handleToolResult(data) {
  const indicators = messagesEl.querySelectorAll('.tool-indicator');
  if (indicators.length > 0) {
    const last = indicators[indicators.length - 1];
    const dot = last.querySelector('.dot');
    if (dot) {
      dot.style.animation = 'none';
      dot.style.background = 'var(--teal)';
      dot.style.opacity = '1';
    }
  }

  const resultEl = document.createElement('div');
  resultEl.className = 'tool-result';
  if (data.title) resultEl.dataset.title = data.title;

  if (data.type === 'chart') {
    const iframe = document.createElement('iframe');
    iframe.sandbox = 'allow-scripts allow-same-origin allow-downloads';
    iframe.srcdoc = data.html;
    iframe.style.width = '100%';
    iframe.style.height = '480px';
    iframe.style.border = '1px solid var(--border)';
    iframe.style.borderRadius = 'var(--radius)';
    iframe.style.background = 'var(--surface)';
    iframe.addEventListener('load', () => {
      const theme = document.documentElement.getAttribute('data-theme') || 'light';
      try { iframe.contentWindow.postMessage({ type: 'theme-change', theme }, '*'); } catch(e) {}
    });
    resultEl.appendChild(iframe);
  } else {
    const tableContainer = document.createElement('div');
    tableContainer.innerHTML = sanitizeHtml(data.html);
    while (tableContainer.firstChild) resultEl.appendChild(tableContainer.firstChild);
    const tableWrap = resultEl.querySelector('.result-table-wrap');
    if (tableWrap) paginateTable(tableWrap);
  }

  const pinBtn = document.createElement('button');
  pinBtn.className = 'pin-btn';
  pinBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M9.5 2L14 6.5 8.5 12 4 14 2 12 3.5 7.5z"/><path d="M2 14l4-4"/></svg> Pin';
  pinBtn.onclick = () => pinResult(pinBtn);
  resultEl.appendChild(pinBtn);

  if (lastSql) {
    const sql = lastSql;
    const name = data.title || '';
    const bmBtn = document.createElement('button');
    bmBtn.className = 'bookmark-btn';
    bmBtn.innerHTML = '★ Bookmark';
    bmBtn.onclick = () => {
      bookmarkQuery(sql, data.type === 'chart' ? 'visualize_data' : 'run_sql', name);
      bmBtn.innerHTML = '★ Saved!';
      setTimeout(() => { bmBtn.innerHTML = '★ Bookmark'; }, 1200);
    };
    resultEl.appendChild(bmBtn);
  }

  const delResult = _makeDeleteBtn('Delete result');
  delResult.onclick = (e) => { e.stopPropagation(); deleteElement(resultEl); };
  resultEl.appendChild(delResult);

  messagesEl.appendChild(resultEl);
  scrollToBottom();
}

function handleToken(data) {
  if (!currentAssistantBubble) {
    const row = document.createElement('div');
    row.className = 'message-row assistant';
    const bubble = document.createElement('div');
    bubble.className = 'message-bubble';
    const del = _makeDeleteBtn('Delete response');
    del.className = 'msg-delete-btn msg-delete-single';
    del.onclick = (e) => { e.stopPropagation(); deleteElement(row); };
    row.appendChild(del);
    row.appendChild(bubble);
    messagesEl.appendChild(row);
    currentAssistantBubble = bubble;
    currentAssistantText = '';
  }
  currentAssistantText += data.text;
  renderMarkdownInto(currentAssistantBubble, currentAssistantText);
  scrollToBottom();
}

function addCopyButtons(container) {
  container.querySelectorAll('pre').forEach(pre => {
    if (pre.querySelector('.copy-btn')) return;
    const btn = document.createElement('button');
    btn.className = 'copy-btn';
    btn.textContent = 'Copy';
    btn.addEventListener('click', () => {
      const code = pre.querySelector('code');
      const text = code ? code.textContent : pre.textContent;
      navigator.clipboard.writeText(text).then(() => {
        btn.textContent = 'Copied!';
        setTimeout(() => { btn.textContent = 'Copy'; }, 1500);
      });
    });
    pre.appendChild(btn);
  });
}

function finalize() {
  if (currentAssistantBubble && currentAssistantText) {
    renderMarkdownInto(currentAssistantBubble, currentAssistantText);
    // If this looks like a clarifying question, add clickable option buttons
    if (clarifySqlEnabled) {
      const options = extractClarifyOptions(currentAssistantText);
      if (options.length >= 2) {
        const wrap = document.createElement('div');
        wrap.className = 'clarify-options';
        options.forEach(opt => {
          const btn = document.createElement('button');
          btn.className = 'clarify-option-btn';
          btn.textContent = opt;
          btn.onclick = () => {
            wrap.remove();
            sendMessage(opt);
          };
          wrap.appendChild(btn);
        });
        messagesEl.appendChild(wrap);
        scrollToBottom();
      }
    }
  }
}

function extractClarifyOptions(text) {
  // Only extract options from clarifying questions, not analysis text.
  // Requirements: text must contain a "?" line BEFORE the list items,
  // and list items must use "— description" format (not ":" which is analysis).
  if (!text.includes('?')) return [];
  const lines = text.split('\n');

  const optionRe = /^[-*]?\s*\*\*(.+?)\*\*\s*[—–-]/;
  const bulletRe = /^[-*]\s+(.+?)\s*[—–-]/;
  const plainRe  = /^[-*]\s+(\w[\w\s]*?)\s*[—–]\s+\S/;

  // Find each "?" line and collect options that follow it.
  // Return the first group that has 2+ options.
  for (let q = 0; q < lines.length; q++) {
    if (!lines[q].includes('?')) continue;
    const options = [];
    for (let i = q + 1; i < lines.length; i++) {
      const match = lines[i].match(optionRe) ||
                    lines[i].match(bulletRe) ||
                    lines[i].match(plainRe);
      if (match) {
        options.push(match[1].trim());
      }
    }
    if (options.length >= 2) return options;
  }
  return [];
}

function handleSuggestions(data) {
  const suggestions = data.suggestions;
  if (!suggestions || suggestions.length === 0) return;
  // Remove any previous suggestions
  const old = messagesEl.querySelector('.suggestions');
  if (old) old.remove();
  const wrap = document.createElement('div');
  wrap.className = 'suggestions';
  suggestions.forEach(text => {
    const btn = document.createElement('button');
    btn.className = 'suggestion-btn';
    btn.textContent = text;
    btn.onclick = () => {
      wrap.remove();
      sendExample(text);
    };
    wrap.appendChild(btn);
  });
  messagesEl.appendChild(wrap);
  scrollToBottom();
}

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

const TRASH_SVG = '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M2 4h12M5.33 4V2.67a1.33 1.33 0 0 1 1.34-1.34h2.66a1.33 1.33 0 0 1 1.34 1.34V4M12.67 4v9.33a1.33 1.33 0 0 1-1.34 1.34H4.67a1.33 1.33 0 0 1-1.34-1.34V4"/></svg>';
const COPY_SVG = '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="5" width="9" height="9" rx="1.5"/><path d="M5 11H3.5A1.5 1.5 0 0 1 2 9.5v-7A1.5 1.5 0 0 1 3.5 1h7A1.5 1.5 0 0 1 12 2.5V5"/></svg>';

function _makeDeleteBtn(title) {
  const btn = document.createElement('button');
  btn.className = 'msg-delete-btn';
  btn.title = title || 'Delete';
  btn.innerHTML = TRASH_SVG;
  return btn;
}

function _makeCopyBtn(text) {
  const btn = document.createElement('button');
  btn.className = 'msg-copy-btn';
  btn.title = 'Copy prompt';
  btn.innerHTML = COPY_SVG;
  btn.onclick = (e) => {
    e.stopPropagation();
    navigator.clipboard.writeText(text).then(() => {
      btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M3.5 8.5l3 3 6-7"/></svg>';
      setTimeout(() => { btn.innerHTML = COPY_SVG; }, 1200);
    });
  };
  return btn;
}

function deleteUserBlock(userRow) {
  // Remove this user message and all downstream siblings until the next user message-row
  let el = userRow.nextElementSibling;
  while (el) {
    if (el.classList.contains('message-row') && el.classList.contains('user')) break;
    const next = el.nextElementSibling;
    el.remove();
    el = next;
  }
  userRow.remove();
}

function deleteElement(el) {
  el.remove();
}

function addMessage(role, text) {
  const row = document.createElement('div');
  row.className = 'message-row ' + role;
  const bubble = document.createElement('div');
  bubble.className = 'message-bubble';
  if (role === 'user') {
    bubble.textContent = text;
    // Action buttons for user messages
    const actions = document.createElement('div');
    actions.className = 'msg-actions';
    actions.appendChild(_makeCopyBtn(text));
    const del = _makeDeleteBtn('Delete question and responses');
    del.onclick = (e) => { e.stopPropagation(); deleteUserBlock(row); };
    actions.appendChild(del);
    row.appendChild(actions);
  } else {
    renderMarkdownInto(bubble, text);
    const del = _makeDeleteBtn('Delete response');
    del.className = 'msg-delete-btn msg-delete-single';
    del.onclick = (e) => { e.stopPropagation(); deleteElement(row); };
    row.appendChild(del);
  }
  row.appendChild(bubble);
  messagesEl.appendChild(row);
  scrollToBottom();
  return bubble;
}

function addTypingIndicator() {
  const row = document.createElement('div');
  row.className = 'message-row assistant';
  const bubble = document.createElement('div');
  bubble.className = 'message-bubble';
  bubble.innerHTML = '<div class="typing-indicator"><span></span><span></span><span></span></div>';
  row.appendChild(bubble);
  messagesEl.appendChild(row);
  scrollToBottom();
  return row;
}

function scrollToBottom() {
  requestAnimationFrame(() => { messagesEl.scrollTop = messagesEl.scrollHeight; });
}

function escapeHtml(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

function slugify(str) {
  return str.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').substring(0, 60) || 'datasight';
}

// ---------------------------------------------------------------------------
// Clear chat
// ---------------------------------------------------------------------------
async function clearChat() {
  // Start a fresh session (old one remains accessible from history)
  sessionId = crypto.randomUUID();
  localStorage.setItem('datasight-session', sessionId);

  messagesEl.innerHTML = '';
  if (welcomeEl) {
    messagesEl.appendChild(welcomeEl);
    welcomeEl.style.display = '';
  }
  currentAssistantBubble = null;
  currentAssistantText = '';
  sessionQueries = [];
  lastSql = '';
  renderQueryHistory();
  loadConversations();

  // Clear dashboard
  pinnedItems = [];
  pinnedIdCounter = 0;
  updateDashboardBadge();
  renderDashboard();
}

// ---------------------------------------------------------------------------
// Interactive tables — sort & filter
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------
const PAGE_SIZE = 25;

function paginateTable(wrap) {
  const tbody = wrap.querySelector('tbody');
  if (!tbody) return;
  bindTableEvents(wrap.parentElement || wrap);
  wrap.dataset.page = '0';
  applyPage(wrap);
}

function applyPage(wrap) {
  const tbody = wrap.querySelector('tbody');
  if (!tbody) return;
  const page = parseInt(wrap.dataset.page || '0');
  const rows = Array.from(tbody.querySelectorAll('tr:not(.filtered-out)'));
  const totalRows = rows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  const start = page * PAGE_SIZE;
  const end = start + PAGE_SIZE;

  // Hide/show all tbody rows based on pagination
  const allRows = tbody.querySelectorAll('tr');
  let visibleIdx = 0;
  allRows.forEach(row => {
    if (row.classList.contains('filtered-out')) {
      row.classList.add('paginated-out');
      return;
    }
    if (visibleIdx >= start && visibleIdx < end) {
      row.classList.remove('paginated-out');
    } else {
      row.classList.add('paginated-out');
    }
    visibleIdx++;
  });

  // Render pagination controls
  const container = wrap.querySelector('.table-pagination');
  if (!container) return;

  const totalDataRows = parseInt(wrap.dataset.totalRows || totalRows);
  const displayedRows = Math.min(totalDataRows, allRows.length);

  if (totalRows <= PAGE_SIZE) {
    // No pagination needed — just show row count
    container.innerHTML = '<span class="page-info">' + totalRows + ' row' + (totalRows !== 1 ? 's' : '') +
      (totalDataRows > displayedRows ? ' (showing ' + displayedRows + ' of ' + totalDataRows + ')' : '') + '</span>';
    return;
  }

  container.innerHTML =
    '<button class="page-btn"' + (page === 0 ? ' disabled' : '') +
      ' onclick="goToPage(this,' + (page - 1) + ')">Prev</button>' +
    '<span class="page-info">Page ' + (page + 1) + ' of ' + totalPages +
      ' (' + totalRows + ' rows)</span>' +
    '<button class="page-btn"' + (page >= totalPages - 1 ? ' disabled' : '') +
      ' onclick="goToPage(this,' + (page + 1) + ')">Next</button>';
}

function goToPage(btn, page) {
  const wrap = btn.closest('.result-table-wrap');
  if (!wrap) return;
  wrap.dataset.page = String(page);
  applyPage(wrap);
}

function sortTable(th) {
  const table = th.closest('table');
  const colIdx = parseInt(th.dataset.col);
  const tbody = table.querySelector('tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));

  // Determine sort direction
  const wasAsc = th.classList.contains('sort-asc');
  table.querySelectorAll('th').forEach(h => {
    h.classList.remove('sort-asc', 'sort-desc');
    h.querySelector('.sort-arrow').textContent = '';
  });

  const asc = !wasAsc;
  th.classList.add(asc ? 'sort-asc' : 'sort-desc');
  th.querySelector('.sort-arrow').textContent = asc ? '\u25B2' : '\u25BC';

  rows.sort((a, b) => {
    const aText = a.children[colIdx]?.textContent ?? '';
    const bText = b.children[colIdx]?.textContent ?? '';
    const aNum = parseFloat(aText.replace(/,/g, ''));
    const bNum = parseFloat(bText.replace(/,/g, ''));
    if (!isNaN(aNum) && !isNaN(bNum)) {
      return asc ? aNum - bNum : bNum - aNum;
    }
    return asc ? aText.localeCompare(bText) : bText.localeCompare(aText);
  });

  rows.forEach(r => tbody.appendChild(r));
  const wrap = th.closest('.result-table-wrap');
  if (wrap) { wrap.dataset.page = '0'; applyPage(wrap); }
}

function bindTableEvents(root) {
  root.querySelectorAll('.result-table-wrap').forEach(function(wrap) {
    var filter = wrap.querySelector('.table-filter');
    if (filter && !filter.dataset.bound) {
      filter.addEventListener('input', function() { filterTable(filter); });
      filter.dataset.bound = '1';
    }
    var csvBtn = wrap.querySelector('.export-csv-btn');
    if (csvBtn && !csvBtn.dataset.bound) {
      csvBtn.addEventListener('click', function() { exportTableCsv(csvBtn); });
      csvBtn.dataset.bound = '1';
    }
    wrap.querySelectorAll('th[data-col]').forEach(function(th) {
      if (!th.dataset.bound) {
        th.addEventListener('click', function() { sortTable(th); });
        th.dataset.bound = '1';
      }
    });
  });
}

function filterTable(input) {
  const wrap = input.closest('.result-table-wrap');
  const tbody = wrap.querySelector('tbody');
  if (!tbody) return;
  const term = input.value.toLowerCase();
  const rows = tbody.querySelectorAll('tr');

  rows.forEach(row => {
    const text = row.textContent.toLowerCase();
    const match = !term || text.includes(term);
    row.classList.toggle('filtered-out', !match);
  });

  // Reset to first page and re-paginate
  wrap.dataset.page = '0';
  applyPage(wrap);
}

function exportTableCsv(btn) {
  const wrap = btn.closest('.result-table-wrap');
  if (!wrap) return;
  const table = wrap.querySelector('.result-table');
  if (!table) return;

  function csvCell(text) {
    if (/[",\n\r]/.test(text)) {
      return '"' + text.replace(/"/g, '""') + '"';
    }
    return text;
  }

  const headers = Array.from(table.querySelectorAll('thead th'))
    .map(th => csvCell(th.textContent.trim()));
  const lines = [headers.join(',')];

  table.querySelectorAll('tbody tr').forEach(row => {
    if (row.classList.contains('filtered-out')) return;
    const cells = Array.from(row.querySelectorAll('td'))
      .map(td => csvCell(td.textContent.trim()));
    lines.push(cells.join(','));
  });

  const csv = lines.join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  const resultEl = wrap.closest('.tool-result');
  const title = (resultEl && resultEl.dataset.title) ? slugify(resultEl.dataset.title) : 'datasight-export';
  a.download = title + '.csv';
  a.click();
  URL.revokeObjectURL(url);

  btn.textContent = 'Downloaded!';
  setTimeout(() => { btn.textContent = 'Download CSV'; }, 1500);
}

// ---------------------------------------------------------------------------
// Theme
// ---------------------------------------------------------------------------
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('datasight-theme', theme);

  // Swap highlight.js stylesheet
  const hljsLink = document.getElementById('hljs-theme');
  hljsLink.href = theme === 'dark'
    ? 'https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github-dark.min.css'
    : 'https://cdnjs.cloudflare.com/ajax/libs/highlight.js/11.9.0/styles/github.min.css';

  // Toggle icon visibility
  document.getElementById('theme-icon-sun').style.display = theme === 'dark' ? 'block' : 'none';
  document.getElementById('theme-icon-moon').style.display = theme === 'dark' ? 'none' : 'block';

  // Broadcast theme to chart iframes (chat + dashboard)
  document.querySelectorAll('.tool-result iframe, #dashboard-grid iframe').forEach(iframe => {
    try { iframe.contentWindow.postMessage({ type: 'theme-change', theme: theme }, '*'); } catch(e) {}
  });
}

function toggleTheme() {
  const current = document.documentElement.getAttribute('data-theme') || 'dark';
  applyTheme(current === 'dark' ? 'light' : 'dark');
}

// Auto-follow OS theme changes when user hasn't explicitly chosen
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
  if (!localStorage.getItem('datasight-theme')) {
    applyTheme(e.matches ? 'dark' : 'light');
  }
});

// ---------------------------------------------------------------------------
// Conversations
// ---------------------------------------------------------------------------
async function loadConversations() {
  try {
    const data = await fetchJson('/api/conversations');
    renderConversations(data.conversations || []);
  } catch (e) { /* ignore */ }
}

function renderConversations(conversations) {
  const container = document.getElementById('conversations-list');
  if (!container) return;
  if (conversations.length === 0) {
    container.innerHTML = '<div class="no-queries">No conversations yet.</div>';
    return;
  }
  container.innerHTML = conversations.map(c => {
    const active = c.session_id === sessionId ? ' active' : '';
    const msgs = c.message_count + ' message' + (c.message_count !== 1 ? 's' : '');
    return '<button class="conversation-item' + active + '" onclick="loadConversation(\'' +
      escapeHtml(c.session_id) + '\')" title="' + escapeHtml(c.title) + '">' +
      '<span class="conversation-title">' + escapeHtml(c.title) + '</span>' +
      '<span class="conversation-meta">' + msgs + '</span></button>';
  }).join('');
}

async function loadConversation(sid) {
  if (sid === sessionId && messagesEl.querySelector('.message-row')) return;
  try {
    const data = await fetchJson('/api/conversations/' + sid);
    if (!data.events || data.events.length === 0) return;

    // Switch to this session
    sessionId = sid;
    localStorage.setItem('datasight-session', sessionId);

    // Clear current UI
    messagesEl.innerHTML = '';
    currentAssistantBubble = null;
    currentAssistantText = '';
    sessionQueries = [];
    lastSql = '';

    // Replay events
    for (const evt of data.events) {
      switch (evt.event) {
        case 'user_message':
          addMessage('user', evt.data.text);
          break;
        case 'tool_start':
          handleToolStart(evt.data);
          break;
        case 'tool_result':
          handleToolResult(evt.data);
          break;
        case 'tool_done':
          handleToolDone(evt.data);
          break;
        case 'assistant_message':
          addMessage('assistant', evt.data.text);
          currentAssistantBubble = null;
          currentAssistantText = '';
          break;
        case 'suggestions':
          handleSuggestions(evt.data);
          break;
      }
    }

    renderQueryHistory();
    loadConversations();
    if (currentView !== 'chat') switchView('chat');
  } catch (e) {
    console.error('Failed to load conversation:', e);
  }
}

async function restoreSession() {
  try {
    const data = await fetchJson('/api/conversations/' + sessionId);
    if (data.events && data.events.length > 0) {
      await loadConversation(sessionId);
    }
  } catch (e) { /* no prior session */ }
}

// ---------------------------------------------------------------------------
// Bookmarks
// ---------------------------------------------------------------------------
async function bookmarkQuery(sql, tool, name) {
  try {
    await fetch('/api/bookmarks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql, tool: tool || 'run_sql', name: name || '' }),
    });
    loadBookmarks();
  } catch (e) { /* ignore */ }
}

async function deleteBookmark(id) {
  try {
    await fetch('/api/bookmarks/' + id, { method: 'DELETE' });
    loadBookmarks();
  } catch (e) { /* ignore */ }
}

async function clearAllBookmarks() {
  try {
    await fetch('/api/bookmarks', { method: 'DELETE' });
    loadBookmarks();
  } catch (e) { /* ignore */ }
}

async function clearAllConversations() {
  try {
    await fetch('/api/conversations', { method: 'DELETE' });
    loadConversations();
  } catch (e) { /* ignore */ }
}

async function loadBookmarks() {
  try {
    const data = await fetchJson('/api/bookmarks');
    renderBookmarks(data.bookmarks || []);
  } catch (e) { /* ignore */ }
}

function renderBookmarks(bookmarks) {
  const container = document.getElementById('bookmarks-list');
  if (!container) return;
  if (bookmarks.length === 0) {
    container.innerHTML = '<div class="no-queries">No bookmarks yet.</div>';
    return;
  }
  container.innerHTML = '';
  bookmarks.forEach(b => {
    const item = document.createElement('div');
    item.className = 'bookmark-item';
    item.title = b.sql;
    item.onclick = () => {
      inputEl.value = 'Run this SQL query:\n' + b.sql;
      inputEl.focus();
    };

    const nameEl = document.createElement('span');
    nameEl.className = 'bookmark-name';
    nameEl.textContent = b.name || b.sql.substring(0, 60);
    item.appendChild(nameEl);

    const del = document.createElement('button');
    del.className = 'bookmark-delete';
    del.textContent = '×';
    del.title = 'Remove bookmark';
    del.onclick = (e) => { e.stopPropagation(); deleteBookmark(b.id); };
    item.appendChild(del);

    container.appendChild(item);
  });
}

// ---------------------------------------------------------------------------
// Sidebar resize
// ---------------------------------------------------------------------------
(function() {
  const handle = document.getElementById('sidebar-resize-handle');
  const sidebar = document.getElementById('sidebar');
  if (!handle || !sidebar) return;
  let dragging = false;

  handle.addEventListener('mousedown', function(e) {
    e.preventDefault();
    dragging = true;
    handle.classList.add('active');
    sidebar.style.transition = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  });

  function onMouseMove(e) {
    if (!dragging) return;
    const newWidth = Math.max(200, Math.min(e.clientX, window.innerWidth * 0.6));
    document.documentElement.style.setProperty('--sidebar-width', newWidth + 'px');
  }

  function onMouseUp() {
    dragging = false;
    handle.classList.remove('active');
    sidebar.style.transition = '';
    document.removeEventListener('mousemove', onMouseMove);
    document.removeEventListener('mouseup', onMouseUp);
  }
})();

// ---------------------------------------------------------------------------
// Export mode
// ---------------------------------------------------------------------------
let exportMode = false;
let exportExcludeIndices = new Set();

function toggleExportMode() {
  exportMode = !exportMode;
  const btn = document.getElementById('export-toggle');
  btn.classList.toggle('active', exportMode);

  if (exportMode) {
    exportExcludeIndices.clear();
    addExportCheckboxes();
    showExportBar();
  } else {
    removeExportCheckboxes();
    hideExportBar();
  }
}

function addExportCheckboxes() {
  let turnIdx = 0;
  // Group turns: user message + tools + assistant response(s) until next user
  // This matches the backend's turn-based exclusion semantics
  const children = Array.from(messagesEl.children);
  let i = 0;
  while (i < children.length) {
    const el = children[i];
    // Only process user message rows (turns are user-initiated)
    if (el.classList.contains('message-row') && el.classList.contains('user')) {
      const idx = turnIdx;
      // Collect user message and ALL following siblings until the next user message
      // This includes tools, assistant responses, suggestions, etc.
      const block = [el];
      let j = i + 1;
      while (j < children.length) {
        const child = children[j];
        // Stop at the next user message (start of next turn)
        if (child.classList.contains('message-row') && child.classList.contains('user')) {
          break;
        }
        block.push(child);
        j++;
      }

      const btn = document.createElement('button');
      btn.className = 'export-trash-btn';
      btn.dataset.msgIdx = idx;
      btn.title = 'Exclude from export';
      btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M2 4h12M5.33 4V2.67a1.33 1.33 0 0 1 1.34-1.34h2.66a1.33 1.33 0 0 1 1.34 1.34V4M12.67 4v9.33a1.33 1.33 0 0 1-1.34 1.34H4.67a1.33 1.33 0 0 1-1.34-1.34V4"/></svg>';
      btn.onclick = function(e) {
        e.stopPropagation();
        const isExcluded = exportExcludeIndices.has(idx);
        if (isExcluded) {
          exportExcludeIndices.delete(idx);
          block.forEach(b => b.classList.remove('export-excluded'));
          btn.classList.remove('active');
          btn.title = 'Exclude from export';
        } else {
          exportExcludeIndices.add(idx);
          block.forEach(b => b.classList.add('export-excluded'));
          btn.classList.add('active');
          btn.title = 'Restore to export';
        }
      };
      el.appendChild(btn);
      turnIdx++;
      i = j;
    } else {
      i++;
    }
  }
}

function removeExportCheckboxes() {
  messagesEl.querySelectorAll('.export-trash-btn').forEach(el => el.remove());
  messagesEl.querySelectorAll('.export-excluded').forEach(el => el.classList.remove('export-excluded'));
}

function showExportBar() {
  if (document.getElementById('export-bar')) return;
  const bar = document.createElement('div');
  bar.id = 'export-bar';
  bar.innerHTML =
    '<span>Select messages to include in export</span>' +
    '<div>' +
      '<button class="export-bar-btn cancel" onclick="toggleExportMode()">Cancel</button>' +
      '<button class="export-bar-btn confirm" onclick="doExport()">Export HTML</button>' +
    '</div>';
  document.querySelector('.chat-area').appendChild(bar);
}

function hideExportBar() {
  const bar = document.getElementById('export-bar');
  if (bar) bar.remove();
}

async function doExport() {
  try {
    const resp = await fetch('/api/export/' + sessionId, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ exclude_indices: Array.from(exportExcludeIndices) }),
    });
    if (!resp.ok) throw new Error('Export failed');
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'datasight-export.html';
    a.click();
    URL.revokeObjectURL(url);
    toggleExportMode();
  } catch (e) {
    console.error('Export failed:', e);
    window.alert('Export failed. Please try again.');
  }
}

// ---------------------------------------------------------------------------
// Keyboard shortcuts
// ---------------------------------------------------------------------------
let shortcutsModalOpen = false;

function showShortcutsModal() {
  if (shortcutsModalOpen) { hideShortcutsModal(); return; }
  shortcutsModalOpen = true;
  const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
  const mod = isMac ? '&#8984;' : 'Ctrl';
  const overlay = document.createElement('div');
  overlay.id = 'shortcuts-modal-overlay';
  overlay.onclick = (e) => { if (e.target === overlay) hideShortcutsModal(); };
  overlay.innerHTML =
    '<div class="shortcuts-modal">' +
      '<div class="shortcuts-modal-header">' +
        '<span>Keyboard Shortcuts</span>' +
        '<button class="shortcuts-close" onclick="hideShortcutsModal()">&times;</button>' +
      '</div>' +
      '<div class="shortcuts-list">' +
        '<div class="shortcut-row"><kbd>/</kbd> or <kbd>' + mod + '</kbd>+<kbd>K</kbd><span>Focus question input</span></div>' +
        '<div class="shortcut-row"><kbd>' + mod + '</kbd>+<kbd>B</kbd><span>Toggle sidebar</span></div>' +
        '<div class="shortcut-row"><kbd>N</kbd><span>New conversation</span></div>' +
        '<div class="shortcut-row"><kbd>D</kbd><span>Toggle dashboard view</span></div>' +
        '<div class="shortcut-row"><kbd>&#8592;&#8593;&#8594;&#8595;</kbd><span>Navigate dashboard cards</span></div>' +
        '<div class="shortcut-row"><kbd>Enter</kbd><span>Fullscreen selected card</span></div>' +
        '<div class="shortcut-row"><kbd>Escape</kbd><span>Exit fullscreen / close modal</span></div>' +
        '<div class="shortcut-row"><kbd>?</kbd><span>Show this help</span></div>' +
      '</div>' +
    '</div>';
  document.body.appendChild(overlay);
}

function hideShortcutsModal() {
  shortcutsModalOpen = false;
  const overlay = document.getElementById('shortcuts-modal-overlay');
  if (overlay) overlay.remove();
}

document.addEventListener('keydown', function(e) {
  const tag = document.activeElement.tagName;
  const isInput = tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || document.activeElement.isContentEditable;
  const mod = e.metaKey || e.ctrlKey;

  // Escape: exit fullscreen, close modals, or blur input
  if (e.key === 'Escape') {
    if (fullscreenCardId !== null) { exitCardFullscreen(); return; }
    if (shortcutsModalOpen) { hideShortcutsModal(); return; }
    if (isInput) { document.activeElement.blur(); return; }
    return;
  }

  // Mod+K: focus input (always, even from input)
  if (mod && e.key === 'k') {
    e.preventDefault();
    inputEl.focus();
    return;
  }

  // Mod+B: toggle sidebar
  if (mod && e.key === 'b' && !e.shiftKey) {
    e.preventDefault();
    toggleSidebar();
    return;
  }

  // Shortcuts below only apply when not typing in an input
  if (isInput) return;

  // N: new conversation
  if (e.key === 'n' && !mod && !e.shiftKey && !e.altKey) {
    clearChat();
    return;
  }

  // / : focus input
  if (e.key === '/') {
    e.preventDefault();
    inputEl.focus();
    return;
  }

  // ? : show shortcuts help
  if (e.key === '?') {
    e.preventDefault();
    showShortcutsModal();
    return;
  }

  // D: toggle dashboard view
  if (e.key === 'd' && !mod && !e.shiftKey && !e.altKey) {
    e.preventDefault();
    switchView(currentView === 'dashboard' ? 'chat' : 'dashboard');
    return;
  }

  // Arrow keys: navigate dashboard cards (only when in dashboard view)
  if (currentView === 'dashboard' && pinnedItems.length > 0) {
    const cols = dashboardColumns > 0 ? dashboardColumns : Math.floor(document.getElementById('dashboard-grid').offsetWidth / 466) || 1;
    if (e.key === 'ArrowRight') {
      e.preventDefault();
      selectedCardIdx = Math.min(selectedCardIdx + 1, pinnedItems.length - 1);
      if (selectedCardIdx < 0) selectedCardIdx = 0;
      renderDashboard();
      return;
    }
    if (e.key === 'ArrowLeft') {
      e.preventDefault();
      selectedCardIdx = Math.max(selectedCardIdx - 1, 0);
      renderDashboard();
      return;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      const newIdx = selectedCardIdx + cols;
      if (newIdx < pinnedItems.length) selectedCardIdx = newIdx;
      else if (selectedCardIdx < 0) selectedCardIdx = 0;
      renderDashboard();
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      const newIdx = selectedCardIdx - cols;
      if (newIdx >= 0) selectedCardIdx = newIdx;
      renderDashboard();
      return;
    }
    // Enter: toggle fullscreen on selected card
    if (e.key === 'Enter' && selectedCardIdx >= 0 && selectedCardIdx < pinnedItems.length) {
      e.preventDefault();
      toggleCardFullscreen(pinnedItems[selectedCardIdx].id);
      return;
    }
    // Delete/Backspace: unpin selected card
    if ((e.key === 'Delete' || e.key === 'Backspace') && selectedCardIdx >= 0 && selectedCardIdx < pinnedItems.length) {
      e.preventDefault();
      unpinItem(pinnedItems[selectedCardIdx].id);
      if (selectedCardIdx >= pinnedItems.length) selectedCardIdx = pinnedItems.length - 1;
      return;
    }
  }
});

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
        '<div class="landing-recent-item" onclick="landingOpenRecentProject(\'' + escapeHtml(p.path) + '\')">' +
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
    await loadSchema();
    loadSettings();
    showWelcome();

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
    await loadSchema();
    await loadQueries();
    loadSettings();
    loadConversations();
    loadBookmarks();
    loadDashboard();
    restoreSession();
    showWelcome();

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
    await loadSchema();
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
              await loadSchema();
              await loadQueries();
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
      await loadSchema();
      await loadQueries();
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

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
async function initApp() {
  applyTheme(localStorage.getItem('datasight-theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'));

  // Check if a project/session is already loaded (e.g., server restart with state)
  try {
    const projectData = await fetchJson('/api/project');
    projectLoaded = projectData.loaded;
    currentProjectPath = projectData.path;

    if (projectLoaded) {
      // Already loaded — go straight to chat
      hideLanding();
      if (projectData.is_ephemeral) {
        updateSessionIndicator('explore', projectData.tables?.length || '?');
      } else {
        updateSessionIndicator('project', projectData.name || projectData.path?.split('/').pop());
      }
      loadSchema();
      loadQueries();
      loadQueryLogState();
      loadSettings();
      loadConversations();
      loadBookmarks();
      loadDashboard();
      restoreSession();
      return;
    }
  } catch (e) {
    console.error('Failed to check project status:', e);
  }

  // No project loaded — show landing page
  showLanding();
  await initLanding();
  loadSettings();
}

initApp();
