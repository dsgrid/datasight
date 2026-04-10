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

function pinResult(btn) {
  const resultEl = btn.closest('.tool-result');
  if (!resultEl) return;
  const iframe = resultEl.querySelector('iframe');
  const type = iframe ? 'chart' : 'table';
  const html = iframe ? iframe.srcdoc : resultEl.querySelector('.result-table-wrap').outerHTML;
  // Get title from result data attribute, or try to find from chart title
  let title = resultEl.dataset.title || '';
  const sourceMeta = lastToolMeta ? {
    question: resultEl.dataset.question || title || '',
    sql: lastToolMeta.sql || lastSql || '',
    tool: lastToolMeta.tool || lastToolName || '',
    execution_time_ms: lastToolMeta.execution_time_ms,
    row_count: lastToolMeta.row_count,
    column_count: lastToolMeta.column_count,
    error: lastToolMeta.error || '',
    chart_type: type === 'chart' && lastPlotlySpec && Array.isArray(lastPlotlySpec.data) && lastPlotlySpec.data[0]
      ? (lastPlotlySpec.data[0].type || '')
      : '',
  } : null;
  pinnedIdCounter++;
  pinnedItems.push({ id: pinnedIdCounter, type, html, title, source_meta: sourceMeta });
  if (pinnedItems.length === 1 && !localStorage.getItem('datasight-dashboard-story-hint-seen')) {
    showToast('Add a note or section in Dashboard to turn pinned results into a readable analysis.', 'info');
    localStorage.setItem('datasight-dashboard-story-hint-seen', '1');
  }
  updateDashboardBadge();
  renderDashboard();
  saveDashboard();
  const pinIcon = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M9.5 2L14 6.5 8.5 12 4 14 2 12 3.5 7.5z"/><path d="M2 14l4-4"/></svg>';
  btn.innerHTML = pinIcon + ' Pinned!';
  setTimeout(() => { btn.innerHTML = pinIcon + ' Pin'; }, 1200);
}

function addDashboardNote() {
  addDashboardItemAt('note', pinnedItems.length);
}

function addDashboardSection() {
  addDashboardItemAt('section', pinnedItems.length);
}

function addDashboardItemAt(type, index) {
  pinnedIdCounter++;
  const item = type === 'section'
    ? {
      id: pinnedIdCounter,
      type: 'section',
      title: 'Section',
      markdown: 'Short introduction for this section.',
    }
    : {
      id: pinnedIdCounter,
      type: 'note',
      title: 'Note',
      markdown: '## Observation\n\nAdd context for the pinned results here.',
    };
  const insertAt = Math.max(0, Math.min(index, pinnedItems.length));
  pinnedItems.splice(insertAt, 0, item);
  updateDashboardBadge();
  renderDashboard();
  saveDashboard();
  switchView('dashboard');
}

function createDashboardInsertControl(index) {
  const insert = document.createElement('div');
  insert.className = 'dashboard-insert-control';
  insert.innerHTML =
    '<span class="dashboard-insert-line"></span>' +
    '<div class="dashboard-insert-actions">' +
      '<button type="button" class="dashboard-insert-btn">+ Note</button>' +
      '<button type="button" class="dashboard-insert-btn secondary">+ Section</button>' +
    '</div>' +
    '<span class="dashboard-insert-line"></span>';

  const buttons = insert.querySelectorAll('.dashboard-insert-btn');
  buttons[0].onclick = () => addDashboardItemAt('note', index);
  buttons[1].onclick = () => addDashboardItemAt('section', index);
  return insert;
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

  let activeSectionId = null;

  pinnedItems.forEach((item, idx) => {
    grid.appendChild(createDashboardInsertControl(idx));

    const card = document.createElement('div');
    let cardClass = 'dashboard-card';
    if (item.type === 'section') cardClass += ' dashboard-section-card';
    if (activeSectionId !== null && item.type !== 'section') cardClass += ' dashboard-card-grouped';
    if (fullscreenCardId === item.id) cardClass += ' fullscreen';
    if (selectedCardIdx === idx) cardClass += ' selected';
    card.className = cardClass;
    card.draggable = true;
    card.dataset.idx = idx;

    if (item.type === 'section') {
      activeSectionId = item.id;
    }

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

    if (item.type !== 'section') {
      const fullscreenBtn = document.createElement('button');
      fullscreenBtn.className = 'dashboard-fullscreen-btn';
      fullscreenBtn.title = 'Toggle fullscreen';
      fullscreenBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M2 6V2h4M14 6V2h-4M2 10v4h4M14 10v4h-4"/></svg>';
      fullscreenBtn.onclick = (e) => {
        e.stopPropagation();
        toggleCardFullscreen(item.id);
      };
      header.appendChild(fullscreenBtn);
    }

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
    } else if (item.type === 'note' || item.type === 'section') {
      const noteWrap = document.createElement('div');
      noteWrap.className = item.type === 'section' ? 'dashboard-section-wrap' : 'dashboard-note-wrap';

      const noteEditor = document.createElement('textarea');
      noteEditor.className = item.type === 'section' ? 'dashboard-section-editor' : 'dashboard-note-editor';
      noteEditor.value = item.markdown || '';
      noteEditor.placeholder = item.type === 'section'
        ? 'Add a short section introduction...'
        : 'Write markdown notes for this dashboard...';
      noteEditor.addEventListener('input', () => {
        item.markdown = noteEditor.value;
        renderMarkdownInto(notePreview, item.markdown || '');
      });
      noteEditor.addEventListener('change', () => {
        item.markdown = noteEditor.value;
        saveDashboard();
      });

      const notePreview = document.createElement('div');
      notePreview.className = item.type === 'section' ? 'dashboard-section-preview' : 'dashboard-note-preview';
      renderMarkdownInto(notePreview, item.markdown || '');

      noteWrap.appendChild(noteEditor);
      noteWrap.appendChild(notePreview);
      card.appendChild(noteWrap);
    } else {
      const tableDiv = document.createElement('div');
      tableDiv.innerHTML = sanitizeHtml(item.html);
      const tableWrap = tableDiv.querySelector('.result-table-wrap');
      if (tableWrap) paginateTable(tableWrap);
      card.appendChild(tableDiv);
    }

    const actions = document.createElement('div');
    actions.className = 'dashboard-card-actions';

    if (item.source_meta && (item.type === 'chart' || item.type === 'table')) {
      const details = document.createElement('details');
      details.className = 'dashboard-source-details';
      const summary = document.createElement('summary');
      summary.textContent = 'Source';
      details.appendChild(summary);

      const meta = item.source_meta;
      const body = document.createElement('div');
      body.className = 'dashboard-source-meta';
      const rows = [
        ['Question', meta.question || item.title || ''],
        ['Tool', meta.tool || item.type],
        ['Rows', meta.row_count != null ? String(meta.row_count) : ''],
        ['Columns', meta.column_count != null ? String(meta.column_count) : ''],
        ['Execution', meta.execution_time_ms != null ? Math.round(meta.execution_time_ms) + ' ms' : ''],
        ['Chart', meta.chart_type || ''],
      ].filter(([, value]) => value);

      rows.forEach(([label, value]) => {
        const row = document.createElement('div');
        row.className = 'dashboard-source-row';
        row.innerHTML = '<strong>' + escapeHtml(label) + '</strong><span>' + escapeHtml(value) + '</span>';
        body.appendChild(row);
      });

      if (meta.sql) {
        const sql = document.createElement('pre');
        sql.className = 'dashboard-source-sql';
        sql.textContent = meta.sql;
        body.appendChild(sql);
      }

      if (meta.error) {
        const error = document.createElement('div');
        error.className = 'dashboard-source-error';
        error.textContent = meta.error;
        body.appendChild(error);
      }

      details.appendChild(body);
      actions.appendChild(details);
    }

    const unpinBtn = document.createElement('button');
    unpinBtn.className = 'unpin-btn';
    unpinBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><line x1="4" y1="4" x2="12" y2="12"/><line x1="12" y1="4" x2="4" y2="12"/></svg> Remove';
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

  grid.appendChild(createDashboardInsertControl(pinnedItems.length));

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
    showToast('Failed to load dashboard.', 'error');
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
    showToast('Failed to save dashboard.', 'error');
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
    title: item.title || '',
    markdown: item.markdown || '',
    source_meta: item.source_meta || null,
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
    showToast('Failed to export dashboard.', 'error');
    window.alert('Failed to export dashboard. Please try again.');
  }
}

