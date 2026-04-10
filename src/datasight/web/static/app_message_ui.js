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
  sessionTotalCost = 0;
  lastSql = '';
  lastPlotlySpec = null;
  lastToolName = '';
  renderQueryHistory();
  updateCostDisplay();
  loadConversations();

  // Clear dashboard
  pinnedItems = [];
  pinnedIdCounter = 0;
  updateDashboardBadge();
  renderDashboard();
}

