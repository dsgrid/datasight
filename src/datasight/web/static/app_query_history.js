function updateSqlDisplay(sql) {
  lastSql = sql;
}

function buildSourceMeta(question, fallbackType, meta) {
  const merged = meta || {};
  return {
    question: question || '',
    sql: merged.sql || lastSql || '',
    tool: merged.tool || lastToolName || fallbackType || '',
    execution_time_ms: merged.execution_time_ms,
    row_count: merged.row_count,
    column_count: merged.column_count,
    error: merged.error || '',
    chart_type: merged.chart_type || (
      fallbackType === 'chart' && lastPlotlySpec && Array.isArray(lastPlotlySpec.data) && lastPlotlySpec.data[0]
        ? (lastPlotlySpec.data[0].type || '')
        : ''
    ),
  };
}

function renderSourceDetails(meta, fallbackTitle) {
  const details = document.createElement('details');
  details.className = 'dashboard-source-details live-result-source-details';
  const summary = document.createElement('summary');
  summary.textContent = 'Source';
  details.appendChild(summary);

  const body = document.createElement('div');
  body.className = 'dashboard-source-meta';
  const rows = [
    ['Question', meta.question || fallbackTitle || ''],
    ['Tool', meta.tool || ''],
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
  return details;
}

function handleToolDone(data) {
  lastToolMeta = data;
  sessionQueries.unshift(data);
  renderQueryHistory();

  const results = messagesEl.querySelectorAll('.tool-result');
  if (results.length > 0) {
    const lastResult = results[results.length - 1];
    const meta = buildSourceMeta(
      lastResult.dataset.question || lastResult.dataset.title || '',
      lastResult.dataset.resultType || '',
      data,
    );
    const existing = lastResult.querySelector('.live-result-source-details');
    const details = renderSourceDetails(meta, lastResult.dataset.title || '');
    if (existing) {
      existing.replaceWith(details);
    } else {
      const pinBtn = lastResult.querySelector('.pin-btn');
      if (pinBtn) {
        pinBtn.insertAdjacentElement('beforebegin', details);
      } else {
        lastResult.appendChild(details);
      }
    }
  }
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
    if (showCostEnabled && q.turn_cost != null) parts.push(formatCost(q.turn_cost));
    meta.textContent = '· ' + parts.join(' · ');
    header.appendChild(meta);

    card.appendChild(header);

    const sqlPre = document.createElement('pre');
    sqlPre.className = 'query-card-sql';
    const sqlCode = document.createElement('code');
    sqlCode.className = 'language-sql';
    sqlCode.textContent = q.formatted_sql || q.sql || '';
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
      inputEl.value = 'Run this SQL query and display the results as a table:\n' + q.sql;
      inputEl.dispatchEvent(new window.Event('input'));
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
