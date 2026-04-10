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
      showToast('Connection lost — check your network or server.', 'error');
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
    case 'done':             finalize(data); break;
    case 'suggestions':      handleSuggestions(data); break;
    case 'sql_confirm':      handleSqlConfirm(data); break;
    case 'sql_rejected':     handleSqlRejected(); break;
    case 'explanation_done': handleExplanationDone(); break;
    case 'error':
      if (data.error) {
        addMessage('assistant', 'Error: ' + data.error);
        showToast(data.error, 'error');
      }
      break;
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
      '<button class="sql-confirm-btn approve" data-request-id="' + escapeHtml(data.request_id) + '" data-sql-action="approve">Approve</button>' +
      '<button class="sql-confirm-btn edit" data-request-id="' + escapeHtml(data.request_id) + '" data-sql-action="edit">Approve with edits</button>' +
      '<button class="sql-confirm-btn reject" data-request-id="' + escapeHtml(data.request_id) + '" data-sql-action="reject">Reject</button>' +
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
    showToast('Failed to send SQL confirmation.', 'error');
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
  lastToolName = data.tool || '';
  lastPlotlySpec = (data.input && data.input.plotly_spec) ? data.input.plotly_spec : null;
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
    tableContainer.innerHTML = sanitizeHtml(data.html);
    while (tableContainer.firstChild) resultEl.appendChild(tableContainer.firstChild);
    const tableWrap = resultEl.querySelector('.result-table-wrap');
    if (tableWrap) paginateTable(tableWrap);
  }

  const pinBtn = document.createElement('button');
  pinBtn.className = 'pin-btn';
  pinBtn.innerHTML = '<svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M9.5 2L14 6.5 8.5 12 4 14 2 12 3.5 7.5z"/><path d="M2 14l4-4"/></svg> Pin';
  pinBtn.onclick = () => pinResult(pinBtn);
  const initialSourceMeta = buildSourceMeta(resultEl.dataset.question || '', data.type || '', {});
  const sourceDetails = renderSourceDetails(initialSourceMeta, data.title || '');
  resultEl.appendChild(sourceDetails);
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

    const reportSql = sql;
    const reportTool = lastToolName || (data.type === 'chart' ? 'visualize_data' : 'run_sql');
    const reportSpec = lastPlotlySpec;
    const reportName = name;
    const rptBtn = document.createElement('button');
    rptBtn.className = 'bookmark-btn';
    rptBtn.innerHTML = '⟳ Save Report';
    rptBtn.onclick = () => {
      saveReport(reportSql, reportTool, reportName, reportSpec);
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

function finalize(costData) {
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
  if (costData && costData.estimated_cost != null) {
    sessionTotalCost += costData.estimated_cost;
    // Stamp turn cost on the most recent query card
    if (sessionQueries.length > 0) {
      sessionQueries[0].turn_cost = costData.estimated_cost;
      renderQueryHistory();
    }
    updateCostDisplay();
  }
}

function updateCostDisplay() {
  const el = document.getElementById('cost-display');
  if (!el) return;
  if (!showCostEnabled || sessionTotalCost === 0) {
    el.style.display = 'none';
    return;
  }
  el.textContent = 'session ' + formatCost(sessionTotalCost);
  el.style.display = '';
}

function formatCost(cost) {
  if (cost < 0.01) return '$' + cost.toFixed(4);
  return '$' + cost.toFixed(2);
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

