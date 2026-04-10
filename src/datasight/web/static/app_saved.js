// ---------------------------------------------------------------------------
// Conversations
// ---------------------------------------------------------------------------
async function loadConversations() {
  try {
    const data = await fetchJson('/api/conversations');
    conversationsCache = data.conversations || [];
    renderConversations(conversationsCache);
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
    return '<button class="conversation-item' + active + '" data-session-id="' +
      escapeHtml(c.session_id) + '" title="' + escapeHtml(c.title) + '">' +
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
    sessionTotalCost = 0;
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
    showToast('Failed to load conversation.', 'error');
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
    bookmarksCache = data.bookmarks || [];
    renderBookmarks(bookmarksCache);
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
      inputEl.value = 'Run this SQL query and display the results as a table:\n' + b.sql;
      inputEl.dispatchEvent(new window.Event('input'));
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
// Reports
// ---------------------------------------------------------------------------
async function saveReport(sql, tool, name, plotlySpec) {
  try {
    const body = { sql, tool: tool || 'run_sql', name: name || '' };
    if (plotlySpec) body.plotly_spec = plotlySpec;
    await fetch('/api/reports', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    loadReports();
  } catch (e) { /* ignore */ }
}

async function updateReport(id, fields) {
  try {
    await fetch('/api/reports/' + id, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(fields),
    });
    loadReports();
  } catch (e) { /* ignore */ }
}

function editReport(report) {
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  const modal = document.createElement('div');
  modal.className = 'modal-dialog';

  const header = document.createElement('div');
  header.className = 'modal-header';
  const title = document.createElement('h3');
  title.textContent = 'Edit Report';
  title.style.margin = '0';
  header.appendChild(title);
  const closeBtn = document.createElement('button');
  closeBtn.className = 'modal-close-btn';
  closeBtn.textContent = '×';
  closeBtn.onclick = () => { overlay.classList.remove('open'); modal.classList.remove('open'); setTimeout(() => overlay.remove(), 200); };
  header.appendChild(closeBtn);
  modal.appendChild(header);

  const content = document.createElement('div');
  content.className = 'modal-content';

  const nameLabel = document.createElement('label');
  nameLabel.className = 'modal-label';
  nameLabel.textContent = 'Name';
  const nameInput = document.createElement('input');
  nameInput.type = 'text';
  nameInput.value = report.name || '';
  nameLabel.appendChild(nameInput);
  content.appendChild(nameLabel);

  const sqlLabel = document.createElement('label');
  sqlLabel.className = 'modal-label';
  sqlLabel.textContent = 'SQL';
  const sqlInput = document.createElement('textarea');
  sqlInput.value = report.sql || '';
  sqlInput.rows = 8;
  sqlLabel.appendChild(sqlInput);
  content.appendChild(sqlLabel);

  let vizInput = null;
  const origVizJson = report.plotly_spec ? JSON.stringify(report.plotly_spec, null, 2) : '';
  if (report.tool === 'visualize_data') {
    const details = document.createElement('details');
    details.className = 'modal-details';
    const summary = document.createElement('summary');
    summary.textContent = 'Visualization (JSON)';
    details.appendChild(summary);
    vizInput = document.createElement('textarea');
    vizInput.value = origVizJson;
    vizInput.rows = 10;
    details.appendChild(vizInput);
    content.appendChild(details);
  }

  modal.appendChild(content);

  const errorEl = document.createElement('div');
  errorEl.className = 'modal-error';
  modal.appendChild(errorEl);

  const actions = document.createElement('div');
  actions.className = 'modal-actions';
  const cancelBtn = document.createElement('button');
  cancelBtn.className = 'modal-btn secondary';
  cancelBtn.textContent = 'Cancel';
  cancelBtn.onclick = () => { overlay.classList.remove('open'); modal.classList.remove('open'); setTimeout(() => overlay.remove(), 200); };
  const saveBtn = document.createElement('button');
  saveBtn.className = 'modal-btn primary';
  saveBtn.textContent = 'Save';
  saveBtn.onclick = () => {
    const fields = {};
    const newSql = sqlInput.value.trim();
    const newName = nameInput.value.trim();
    if (newSql && newSql !== report.sql) fields.sql = newSql;
    if (newName !== (report.name || '')) fields.name = newName;
    if (vizInput) {
      const newViz = vizInput.value.trim();
      if (newViz && newViz !== origVizJson) {
        try {
          fields.plotly_spec = JSON.parse(newViz);
        } catch (e) {
          errorEl.textContent = 'Invalid JSON in visualization spec';
          errorEl.classList.add('visible');
          return;
        }
      }
    }
    if (Object.keys(fields).length > 0) updateReport(report.id, fields);
    overlay.classList.remove('open');
    modal.classList.remove('open');
    setTimeout(() => overlay.remove(), 200);
  };
  actions.appendChild(cancelBtn);
  actions.appendChild(saveBtn);
  modal.appendChild(actions);

  overlay.onclick = (e) => { if (e.target === overlay) { overlay.classList.remove('open'); modal.classList.remove('open'); setTimeout(() => overlay.remove(), 200); } };
  document.body.appendChild(overlay);
  document.body.appendChild(modal);
  requestAnimationFrame(() => { overlay.classList.add('open'); modal.classList.add('open'); });
  sqlInput.focus();
}

async function deleteReport(id) {
  try {
    await fetch('/api/reports/' + id, { method: 'DELETE' });
    loadReports();
  } catch (e) { /* ignore */ }
}

async function clearAllReports() {
  try {
    await fetch('/api/reports', { method: 'DELETE' });
    loadReports();
  } catch (e) { /* ignore */ }
}

async function loadReports() {
  try {
    const data = await fetchJson('/api/reports');
    reportsCache = data.reports || [];
    renderReports(reportsCache);
  } catch (e) { /* ignore */ }
}

async function runReport(id) {
  try {
    const resp = await fetch('/api/reports/' + id + '/run', { method: 'POST' });
    const data = await resp.json();
    if (!data.ok || !data.html) return;
    lastToolMeta = data.meta || null;
    lastSql = (data.meta && data.meta.sql) ? data.meta.sql : lastSql;
    lastToolName = (data.meta && data.meta.tool) ? data.meta.tool : (data.type === 'chart' ? 'visualize_data' : 'run_sql');
    lastPlotlySpec = data.plotly_spec || null;

    const resultEl = document.createElement('div');
    resultEl.className = 'tool-result';
    resultEl.dataset.resultType = data.type || '';
    if (data.title) resultEl.dataset.title = data.title;
    if (data.title) resultEl.dataset.question = data.title;

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
      tableContainer.innerHTML = window.DOMPurify ? window.DOMPurify.sanitize(data.html) : data.html;
      while (tableContainer.firstChild) resultEl.appendChild(tableContainer.firstChild);
      const tableWrap = resultEl.querySelector('.result-table-wrap');
      if (tableWrap) paginateTable(tableWrap);
    }

    const sourceMeta = buildSourceMeta(resultEl.dataset.question || '', data.type || '', data.meta || {});
    const sourceDetails = renderSourceDetails(sourceMeta, data.title || '');
    resultEl.appendChild(sourceDetails);

    const pinBtn = document.createElement('button');
    pinBtn.className = 'pin-btn';
    pinBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M9.5 2L14 6.5 8.5 12 4 14 2 12 3.5 7.5z"/><path d="M2 14l4-4"/></svg> Pin';
    pinBtn.onclick = () => pinResult(pinBtn);
    resultEl.appendChild(pinBtn);

    if (lastSql) {
      const bmBtn = document.createElement('button');
      bmBtn.className = 'bookmark-btn';
      bmBtn.innerHTML = '★ Bookmark';
      bmBtn.onclick = () => {
        bookmarkQuery(lastSql, lastToolName, data.title || '');
        bmBtn.innerHTML = '★ Saved!';
        setTimeout(() => { bmBtn.innerHTML = '★ Bookmark'; }, 1200);
      };
      resultEl.appendChild(bmBtn);

      const rptBtn = document.createElement('button');
      rptBtn.className = 'bookmark-btn';
      rptBtn.style.right = '208px';
      rptBtn.innerHTML = '⟳ Save Report';
      rptBtn.onclick = () => {
        saveReport(lastSql, lastToolName, data.title || '', lastPlotlySpec);
        rptBtn.innerHTML = '⟳ Saved!';
        setTimeout(() => { rptBtn.innerHTML = '⟳ Save Report'; }, 1200);
      };
      resultEl.appendChild(rptBtn);
    }

    const delResult = _makeDeleteBtn('Delete result');
    delResult.onclick = (e) => { e.stopPropagation(); deleteElement(resultEl); };
    resultEl.appendChild(delResult);

    messagesEl.appendChild(resultEl);
    scrollToBottom();
  } catch (e) { /* ignore */ }
}

function renderReports(reports) {
  const container = document.getElementById('reports-list');
  if (!container) return;
  if (reports.length === 0) {
    container.innerHTML = '<div class="no-queries">No saved reports yet.</div>';
    return;
  }
  container.innerHTML = '';
  reports.forEach(r => {
    const item = document.createElement('div');
    item.className = 'bookmark-item';
    item.title = r.sql;
    item.onclick = () => runReport(r.id);

    const icon = r.tool === 'visualize_data' ? '📊 ' : '📋 ';
    const nameEl = document.createElement('span');
    nameEl.className = 'bookmark-name';
    nameEl.textContent = icon + (r.name || r.sql.substring(0, 50));
    item.appendChild(nameEl);

    const btnGroup = document.createElement('span');
    btnGroup.className = 'bookmark-actions';

    const edit = document.createElement('button');
    edit.className = 'bookmark-edit';
    edit.textContent = '✎';
    edit.title = 'Edit report';
    edit.onclick = (e) => { e.stopPropagation(); editReport(r); };
    btnGroup.appendChild(edit);

    const del = document.createElement('button');
    del.className = 'bookmark-delete bookmark-delete-grouped';
    del.textContent = '×';
    del.title = 'Remove report';
    del.onclick = (e) => { e.stopPropagation(); deleteReport(r.id); };
    btnGroup.appendChild(del);

    item.appendChild(btnGroup);
    container.appendChild(item);
  });
}

