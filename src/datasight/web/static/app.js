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
let sessionQueries = [];
let pinnedItems = [];
let pinnedIdCounter = 0;
let currentView = 'chat';

const messagesEl = document.getElementById('messages');
const inputEl = document.getElementById('user-input');
const sendBtn = document.getElementById('send-btn');
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
  if (view === 'dashboard') broadcastThemeToDashboard();
}

function pinResult(btn) {
  const resultEl = btn.closest('.tool-result');
  if (!resultEl) return;
  const iframe = resultEl.querySelector('iframe');
  const type = iframe ? 'chart' : 'table';
  const html = iframe ? iframe.srcdoc : resultEl.querySelector('.result-table-wrap').outerHTML;
  pinnedIdCounter++;
  pinnedItems.push({ id: pinnedIdCounter, type, html });
  updateDashboardBadge();
  renderDashboard();
  const pinIcon = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M9.5 2L14 6.5 8.5 12 4 14 2 12 3.5 7.5z"/><path d="M2 14l4-4"/></svg>';
  btn.innerHTML = pinIcon + ' Pinned!';
  setTimeout(() => { btn.innerHTML = pinIcon + ' Pin'; }, 1200);
}

function unpinItem(id) {
  pinnedItems = pinnedItems.filter(item => item.id !== id);
  updateDashboardBadge();
  renderDashboard();
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

function renderDashboard() {
  const grid = document.getElementById('dashboard-grid');
  const empty = document.getElementById('dashboard-empty');
  grid.innerHTML = '';

  if (pinnedItems.length === 0) {
    empty.style.display = '';
    return;
  }
  empty.style.display = 'none';

  pinnedItems.forEach(item => {
    const card = document.createElement('div');
    card.className = 'dashboard-card';

    if (item.type === 'chart') {
      const iframe = document.createElement('iframe');
      iframe.sandbox = 'allow-scripts allow-same-origin allow-downloads';
      iframe.srcdoc = item.html;
      iframe.addEventListener('load', () => {
        const theme = document.documentElement.getAttribute('data-theme') || 'light';
        try { iframe.contentWindow.postMessage({ type: 'theme-change', theme }, '*'); } catch(e) {}
      });
      card.appendChild(iframe);
    } else {
      const tableDiv = document.createElement('div');
      tableDiv.innerHTML = item.html;
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

    grid.appendChild(card);
  });
}

function broadcastThemeToDashboard() {
  const theme = document.documentElement.getAttribute('data-theme') || 'light';
  document.querySelectorAll('#dashboard-grid iframe').forEach(iframe => {
    try { iframe.contentWindow.postMessage({ type: 'theme-change', theme }, '*'); } catch(e) {}
  });
}

async function toggleQueryLog() {
  try {
    const resp = await fetch('/api/query-log/toggle', { method: 'POST' });
    const data = await resp.json();
    queryLogEnabled = data.enabled;
    updateQueryLogButton();
  } catch (e) {
    console.error('Failed to toggle query log:', e);
  }
}

function updateQueryLogButton() {
  const btn = document.getElementById('query-log-toggle');
  btn.classList.toggle('active', queryLogEnabled);
  btn.title = queryLogEnabled ? 'Query logging ON — click to disable' : 'Query logging OFF — click to enable';
}

async function loadQueryLogState() {
  try {
    const resp = await fetch('/api/query-log?n=0');
    const data = await resp.json();
    queryLogEnabled = data.enabled;
    updateQueryLogButton();
  } catch (e) {
    // Ignore — button defaults to off
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
    const resp = await fetch('/api/schema');
    const data = await resp.json();
    schemaData = data.tables || [];
    renderTables(schemaData);
  } catch (e) {
    document.getElementById('tables-container').innerHTML =
      '<div class="no-queries">Failed to load schema</div>';
  }
}

async function loadQueries() {
  try {
    const resp = await fetch('/api/queries');
    const data = await resp.json();
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
    const resp = await fetch('/api/column-stats/' + encodeURIComponent(tableName) + '/' + encodeURIComponent(colName));
    const data = await resp.json();
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
    const resp = await fetch('/api/preview/' + encodeURIComponent(tableName));
    const data = await resp.json();
    if (data.html) {
      previewEl.innerHTML = data.html;
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

async function sendMessage(text) {
  if (welcomeEl) welcomeEl.style.display = 'none';
  const oldSuggestions = messagesEl.querySelector('.suggestions');
  if (oldSuggestions) oldSuggestions.remove();

  addMessage('user', text);

  inputEl.value = '';
  inputEl.style.height = 'auto';

  isStreaming = true;
  sendBtn.disabled = true;
  currentAssistantText = '';
  currentAssistantBubble = null;

  const typingEl = addTypingIndicator();

  try {
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: text, session_id: sessionId }),
    });

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let typingRemoved = false;

    while (true) { // eslint-disable-line no-constant-condition
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
          const data = JSON.parse(line.slice(6));
          handleSSEEvent(eventType, data);
          eventType = '';
        }
      }
    }

    if (!typingRemoved) typingEl.remove();
  } catch (err) {
    console.error('Stream error:', err);
    if (document.contains(typingEl)) typingEl.remove();
    addMessage('assistant', 'Connection error. Please try again.');
  }

  isStreaming = false;
  sendBtn.disabled = false;
  currentAssistantBubble = null;
  inputEl.focus();
  loadConversations();
}

// ---------------------------------------------------------------------------
// SSE event handler
// ---------------------------------------------------------------------------
function handleSSEEvent(eventType, data) {
  switch (eventType) {
    case 'tool_start':  handleToolStart(data); break;
    case 'tool_result': handleToolResult(data); break;
    case 'tool_done':   handleToolDone(data); break;
    case 'token':       handleToken(data); break;
    case 'done':        finalize(); break;
    case 'suggestions': handleSuggestions(data); break;
  }
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
    tableContainer.innerHTML = data.html;
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

  messagesEl.appendChild(resultEl);
  scrollToBottom();
}

function handleToken(data) {
  if (!currentAssistantBubble) {
    const row = document.createElement('div');
    row.className = 'message-row assistant';
    const bubble = document.createElement('div');
    bubble.className = 'message-bubble';
    row.appendChild(bubble);
    messagesEl.appendChild(row);
    currentAssistantBubble = bubble;
    currentAssistantText = '';
  }
  currentAssistantText += data.text;
  currentAssistantBubble.innerHTML = marked.parse(currentAssistantText);
  currentAssistantBubble.querySelectorAll('pre code').forEach(block => {
    hljs.highlightElement(block);
  });
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
    currentAssistantBubble.innerHTML = marked.parse(currentAssistantText);
    currentAssistantBubble.querySelectorAll('pre code').forEach(block => {
      hljs.highlightElement(block);
    });
    addCopyButtons(currentAssistantBubble);
  }
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
function addMessage(role, text) {
  const row = document.createElement('div');
  row.className = 'message-row ' + role;
  const bubble = document.createElement('div');
  bubble.className = 'message-bubble';
  if (role === 'user') {
    bubble.textContent = text;
  } else {
    bubble.innerHTML = marked.parse(text);
    bubble.querySelectorAll('pre code').forEach(block => {
      hljs.highlightElement(block);
    });
    addCopyButtons(bubble);
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
    const resp = await fetch('/api/conversations');
    const data = await resp.json();
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
      c.session_id + '\')" title="' + escapeHtml(c.title) + '">' +
      '<span class="conversation-title">' + escapeHtml(c.title) + '</span>' +
      '<span class="conversation-meta">' + msgs + '</span></button>';
  }).join('');
}

async function loadConversation(sid) {
  if (sid === sessionId && messagesEl.querySelector('.message-row')) return;
  try {
    const resp = await fetch('/api/conversations/' + sid);
    const data = await resp.json();
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
    const resp = await fetch('/api/conversations/' + sessionId);
    const data = await resp.json();
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
    const resp = await fetch('/api/bookmarks');
    const data = await resp.json();
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
// Init
// ---------------------------------------------------------------------------
applyTheme(localStorage.getItem('datasight-theme') || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'));
loadSchema();
loadQueries();
loadQueryLogState();
loadConversations();
loadBookmarks();
restoreSession();
