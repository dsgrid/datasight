// ---------------------------------------------------------------------------
// Sidebar
// ---------------------------------------------------------------------------
function toggleSidebar() {
  sidebar.classList.toggle('collapsed');
  const btn = document.getElementById('sidebar-toggle');
  btn.classList.toggle('active', !sidebar.classList.contains('collapsed'));
}

function toggleSidebarSection(sectionId) {
  const section = document.getElementById(sectionId);
  if (!section) return;
  section.classList.toggle('collapsed');
  // Persist collapsed state
  const key = 'datasight-collapsed-sections';
  let collapsed = {};
  try { collapsed = JSON.parse(localStorage.getItem(key) || '{}'); } catch (_) { /* ignore */ }
  collapsed[sectionId] = section.classList.contains('collapsed');
  localStorage.setItem(key, JSON.stringify(collapsed));
}

function restoreCollapsedSections() {
  try {
    const collapsed = JSON.parse(localStorage.getItem('datasight-collapsed-sections') || '{}');
    for (const [id, isCollapsed] of Object.entries(collapsed)) {
      if (isCollapsed) {
        const section = document.getElementById(id);
        if (section) section.classList.add('collapsed');
      }
    }
  } catch (_) { /* ignore */ }
}

function toggleRightPanel() {
  const panel = document.getElementById('right-panel');
  panel.classList.toggle('collapsed');
  document.getElementById('sql-panel-toggle').classList.toggle('active', !panel.classList.contains('collapsed'));
}

// ---------------------------------------------------------------------------
// Projects Panel
// ---------------------------------------------------------------------------
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
    recentProjectsCache = data.projects || [];
    if (!data.projects || data.projects.length === 0) {
      container.innerHTML = '<div class="no-queries">No recent projects.</div>';
      return;
    }
    container.innerHTML = data.projects.map(p => `
      <div class="project-item${p.is_current ? ' current' : ''}" data-path="${escapeHtml(p.path)}">
        <div class="project-item-info">
          <div class="project-item-name">${escapeHtml(p.name)}</div>
          <div class="project-item-path">${escapeHtml(p.path)}</div>
        </div>
        <button class="project-item-remove" data-path="${escapeHtml(p.path)}" title="Remove from list">&times;</button>
      </div>
    `).join('');
  } catch (e) {
    console.error('Failed to load recent projects:', e);
    showToast('Failed to load recent projects.', 'error');
    container.innerHTML = '<div class="no-queries">Failed to load projects.</div>';
  }
}

async function ensureRecentProjectsLoaded() {
  if (recentProjectsCache.length > 0) return recentProjectsCache;
  try {
    const data = await fetchJson('/api/projects/recent');
    recentProjectsCache = data.projects || [];
  } catch (e) {
    recentProjectsCache = [];
  }
  return recentProjectsCache;
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
    await loadRecipes();
    await loadMeasureOverridesEditor();
    await loadConversations();
    await loadBookmarks();
    await loadReports();
    await loadDashboard();
    await loadProjectHealth();

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
  sessionTotalCost = 0;

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
  clearSchemaSearchState();
  clearSchemaInsightCaches();
  clearSavedItemCaches();
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
        <button class="example-btn" data-example="What tables are available and how many rows do they have?">What tables are available and how many rows do they have?</button>
        <button class="example-btn" data-action="summarize-dataset">Show me a summary of the data</button>
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

function renderExploreFilesList() {
  const container = document.getElementById('explore-files-list');
  if (!explorePaths.length) {
    container.innerHTML = '';
    return;
  }
  container.innerHTML = explorePaths.map((p, i) => `
    <div class="explore-file-item">
      <span class="file-name" title="${escapeHtml(p)}">${escapeHtml(p.split('/').pop())}</span>
      <button class="remove-file" data-index="${i}" title="Remove">&times;</button>
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
    await loadProjectHealth();
    await loadRecipes();
    showWelcome();

    if (!llmConnected) {
      window.setTimeout(promptLlmConfigIfNeeded, 350);
    }

  } catch (e) {
    console.error('Failed to start explore:', e);
    errorEl.textContent = 'Failed to connect to server';
    errorEl.classList.add('visible');
    showToast('Failed to start explore session.', 'error');
  } finally {
    exploreBtn.disabled = false;
    exploreBtn.textContent = 'Explore';
  }
}
