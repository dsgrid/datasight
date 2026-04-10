function getCommandPaletteResults(query) {
  const results = [];
  const q = query.trim().toLowerCase();

  const actions = [
    { type: 'action', group: 'Actions', title: 'Switch to Chat', subtitle: 'View', score: 800, run: () => switchView('chat') },
    { type: 'action', group: 'Actions', title: 'Switch to Dashboard', subtitle: 'View', score: 800, run: () => switchView('dashboard') },
    { type: 'action', group: 'Actions', title: 'Toggle SQL Panel', subtitle: 'Panels', score: 760, run: () => toggleRightPanel() },
    { type: 'action', group: 'Actions', title: 'Open Project Switcher', subtitle: 'Projects', score: 760, run: () => openProjectsPanel() },
    { type: 'action', group: 'Actions', title: 'Open Settings', subtitle: 'Settings', score: 740, run: () => toggleSettingsPanel() },
    { type: 'action', group: 'Actions', title: 'Open Project Health', subtitle: 'Diagnostics', score: 745, run: () => openProjectHealthPanel() },
    { type: 'action', group: 'Actions', title: 'New Chat', subtitle: 'Conversation', score: 760, run: () => clearChat() },
    { type: 'action', group: 'Actions', title: 'Summarize Dataset', subtitle: 'Analysis', score: 780, run: () => summarizeDataset() },
    { type: 'action', group: 'Actions', title: 'Profile This Dataset', subtitle: 'Starter', score: 790, run: () => runStarterAction('profile') },
    { type: 'action', group: 'Actions', title: 'Inspect Key Measures', subtitle: 'Starter', score: 785, run: () => runStarterAction('measures') },
    { type: 'action', group: 'Actions', title: 'Find Key Dimensions', subtitle: 'Starter', score: 780, run: () => runStarterAction('dimensions') },
    { type: 'action', group: 'Actions', title: 'Build a Trend Chart', subtitle: 'Starter', score: 780, run: () => runStarterAction('trend') },
    { type: 'action', group: 'Actions', title: 'Audit Nulls and Outliers', subtitle: 'Starter', score: 780, run: () => runStarterAction('quality') },
    { type: 'action', group: 'Actions', title: 'Open Inspect Tools', subtitle: 'Sidebar', score: 720, run: () => openSidebarSection('inspect-section') },
    { type: 'action', group: 'Actions', title: 'Inspect Dataset Profile', subtitle: 'Deterministic', score: 760, run: () => runInspectAction('profile', 'dataset') },
    { type: 'action', group: 'Actions', title: 'Inspect Dataset Measures', subtitle: 'Deterministic', score: 755, run: () => runInspectAction('measures', 'dataset') },
    { type: 'action', group: 'Actions', title: 'Inspect Dataset Quality', subtitle: 'Deterministic', score: 750, run: () => runInspectAction('quality', 'dataset') },
    { type: 'action', group: 'Actions', title: 'Inspect Dataset Dimensions', subtitle: 'Deterministic', score: 750, run: () => runInspectAction('dimensions', 'dataset') },
    { type: 'action', group: 'Actions', title: 'Inspect Dataset Trends', subtitle: 'Deterministic', score: 750, run: () => runInspectAction('trend', 'dataset') },
    { type: 'action', group: 'Actions', title: 'Add Dashboard Note', subtitle: 'Dashboard', score: 730, run: () => { switchView('dashboard'); addDashboardNote(); } },
    { type: 'action', group: 'Actions', title: 'Add Dashboard Section', subtitle: 'Dashboard', score: 730, run: () => { switchView('dashboard'); addDashboardSection(); } },
    { type: 'action', group: 'Actions', title: 'Open Example Queries', subtitle: 'Sidebar', score: 710, run: () => openSidebarSection('queries-section') },
    { type: 'action', group: 'Actions', title: 'Open Recipes', subtitle: 'Sidebar', score: 710, run: () => openSidebarSection('recipes-section') },
    { type: 'action', group: 'Actions', title: 'Open Bookmarks', subtitle: 'Sidebar', score: 710, run: () => openSidebarSection('bookmarks-section') },
    { type: 'action', group: 'Actions', title: 'Open Saved Reports', subtitle: 'Sidebar', score: 710, run: () => openSidebarSection('reports-section') },
    { type: 'action', group: 'Actions', title: 'Open Conversation History', subtitle: 'Sidebar', score: 710, run: () => openSidebarSection('conversations-section') },
  ];

  actions.forEach(action => {
    const score = scorePaletteResult(q, [action.title, action.subtitle], action.score);
    if (score >= 0) {
      results.push({ ...action, score });
    }
  });

  schemaData.forEach(table => {
    const tableScore = scorePaletteResult(q, [table.name], 620);
    if (tableScore >= 0) {
      results.push({
        type: 'table',
        group: 'Tables',
        title: table.name,
        subtitle: 'Table',
        score: tableScore,
        run: () => {
          switchView('chat');
          closeCommandPalette();
          focusTableInSidebar(table.name);
        },
      });
      results.push({
        type: 'action',
        group: 'Table Actions',
        title: 'Preview ' + table.name,
        subtitle: 'Preview rows',
        score: tableScore - 20,
        run: () => {
          closeCommandPalette();
          previewTableInSidebar(table.name);
        },
      });
      results.push({
        type: 'action',
        group: 'Table Actions',
        title: 'Ask about ' + table.name,
        subtitle: 'Explain table',
        score: tableScore - 30,
        run: () => {
          closeCommandPalette();
          askAboutTable(table.name);
        },
      });
    }

    (table.columns || []).forEach(column => {
      const columnScore = scorePaletteResult(q, [column.name, table.name + ' ' + column.name], 560);
      if (columnScore >= 0) {
        results.push({
          type: 'column',
          group: 'Columns',
          title: column.name,
          subtitle: table.name,
          score: columnScore,
          run: () => {
            switchView('chat');
            closeCommandPalette();
            focusTableInSidebar(table.name, column.name);
          },
        });
        results.push({
          type: 'action',
          group: 'Column Actions',
          title: 'Column stats for ' + table.name + '.' + column.name,
          subtitle: 'Inspect stats',
          score: columnScore - 20,
          run: () => {
            closeCommandPalette();
            openColumnStatsInSidebar(table.name, column.name);
          },
        });
        results.push({
          type: 'action',
          group: 'Column Actions',
          title: 'Ask about ' + table.name + '.' + column.name,
          subtitle: 'Explain column',
          score: columnScore - 30,
          run: () => {
            closeCommandPalette();
            askAboutColumn(table.name, column.name);
          },
        });
      }
    });
  });

  recentProjectsCache.forEach(project => {
    const score = scorePaletteResult(q, [project.name, project.path], 500);
    if (score >= 0) {
      results.push({
        type: 'project',
        group: 'Projects',
        title: project.name,
        subtitle: project.path,
        score,
        run: () => {
          closeCommandPalette();
          if (project.path !== currentProjectPath || !projectLoaded) {
            doLoadProject(project.path);
          }
        },
      });
    }
  });

  bookmarksCache.forEach(bookmark => {
    const bookmarkTitle = bookmark.name || (bookmark.sql || '').substring(0, 60);
    const bookmarkScore = scorePaletteResult(q, [bookmarkTitle, bookmark.sql], 520);
    if (bookmarkScore >= 0) {
      results.push({
        type: 'bookmark',
        group: 'Bookmarks',
        title: bookmarkTitle,
        subtitle: 'Saved SQL',
        score: bookmarkScore,
        run: () => {
          closeCommandPalette();
          openSidebarSection('bookmarks-section');
          switchView('chat');
          inputEl.value = 'Run this SQL query and display the results as a table:\n' + bookmark.sql;
          inputEl.dispatchEvent(new window.Event('input'));
          inputEl.focus();
        },
      });
    }
  });

  reportsCache.forEach(report => {
    const reportTitle = report.name || (report.sql || '').substring(0, 60);
    const reportScore = scorePaletteResult(q, [reportTitle, report.sql], 530);
    if (reportScore >= 0) {
      results.push({
        type: 'report',
        group: 'Reports',
        title: reportTitle,
        subtitle: report.tool === 'visualize_data' ? 'Saved chart report' : 'Saved query report',
        score: reportScore,
        run: () => {
          closeCommandPalette();
          openSidebarSection('reports-section');
          runReport(report.id);
        },
      });
    }
  });

  conversationsCache.forEach(conversation => {
    const conversationScore = scorePaletteResult(
      q,
      [conversation.title, conversation.session_id],
      510
    );
    if (conversationScore >= 0) {
      results.push({
        type: 'conversation',
        group: 'Conversations',
        title: conversation.title,
        subtitle: String(conversation.message_count || 0) + ' messages',
        score: conversationScore,
        run: () => {
          closeCommandPalette();
          openSidebarSection('conversations-section');
          loadConversation(conversation.session_id);
        },
      });
    }
  });

  recipesCache.forEach(recipe => {
    const recipeScore = scorePaletteResult(q, [recipe.title, recipe.category, recipe.reason, recipe.prompt], 540);
    if (recipeScore >= 0) {
      results.push({
        type: 'recipe',
        group: 'Recipes',
        title: recipe.title,
        subtitle: recipe.reason || recipe.category || 'Recipe',
        score: recipeScore,
        run: () => {
          closeCommandPalette();
          openSidebarSection('recipes-section');
          useRecipePrompt(recipe.prompt);
        },
      });
    }
  });

  results.sort((a, b) => {
    if ((b.score || 0) !== (a.score || 0)) return (b.score || 0) - (a.score || 0);
    return a.title.localeCompare(b.title);
  });

  return results.slice(0, 60);
}

function renderCommandPalette() {
  const container = document.getElementById('command-palette-results');
  if (!container) return;
  const input = document.getElementById('command-palette-input');
  const query = input ? input.value.trim().toLowerCase() : '';
  if (commandPaletteResults.length === 0) {
    container.innerHTML = '<div class="command-palette-empty">No matching actions, schema items, projects, bookmarks, reports, or conversations.</div>';
    return;
  }

  let html = '';
  let currentGroup = '';
  commandPaletteResults.forEach((item, idx) => {
    if (item.group && item.group !== currentGroup) {
      currentGroup = item.group;
      html += '<div class="command-palette-group">' + escapeHtml(currentGroup) + '</div>';
    }
    html +=
      '<button class="command-palette-item' + (idx === commandPaletteSelectedIdx ? ' selected' : '') +
      '" data-result-idx="' + idx + '">' +
        '<span class="command-palette-item-main">' + highlightMatch(item.title, query) + '</span>' +
        '<span class="command-palette-item-sub">' + highlightMatch(item.subtitle || '', query) + '</span>' +
      '</button>';
  });
  container.innerHTML = html;
  const selected = container.querySelector('.command-palette-item.selected');
  if (selected) selected.scrollIntoView({ block: 'nearest' });
}

function updateCommandPaletteResults() {
  const input = document.getElementById('command-palette-input');
  commandPaletteResults = getCommandPaletteResults(input ? input.value : '');
  commandPaletteSelectedIdx = Math.min(commandPaletteSelectedIdx, Math.max(commandPaletteResults.length - 1, 0));
  renderCommandPalette();
}

async function openCommandPalette() {
  await ensureRecentProjectsLoaded();
  commandPaletteOpen = true;
  commandPaletteSelectedIdx = 0;
  document.getElementById('command-palette-overlay').classList.add('open');
  document.getElementById('command-palette').classList.add('open');
  const input = document.getElementById('command-palette-input');
  input.value = '';
  updateCommandPaletteResults();
  input.focus();
}

function closeCommandPalette() {
  commandPaletteOpen = false;
  document.getElementById('command-palette-overlay').classList.remove('open');
  document.getElementById('command-palette').classList.remove('open');
}

function executeCommandPaletteResult(idx) {
  const item = commandPaletteResults[idx];
  if (!item) return;
  closeCommandPalette();
  item.run();
}

function renderSchemaQuickActions(tableName, columnName) {
  const actions = [];
  if (columnName) {
    actions.push(
      '<button class="schema-action-btn" data-action="column-stats" data-table="' + escapeHtml(tableName) +
      '" data-column="' + escapeHtml(columnName) + '">Stats</button>'
    );
    actions.push(
      '<button class="schema-action-btn" data-action="ask-column" data-table="' + escapeHtml(tableName) +
      '" data-column="' + escapeHtml(columnName) + '">Ask</button>'
    );
  } else {
    actions.push(
      '<button class="schema-action-btn" data-action="preview-table-sidebar" data-table="' + escapeHtml(tableName) + '">Preview</button>'
    );
    actions.push(
      '<button class="schema-action-btn" data-action="ask-table" data-table="' + escapeHtml(tableName) + '">Ask</button>'
    );
  }
  return '<div class="schema-quick-actions">' + actions.join('') + '</div>';
}

function renderTables(tables) {
  const container = document.getElementById('tables-container');
  const query = schemaSearchQuery;
  const visibleTables = getVisibleSchemaEntries(tables, query);

  if (!visibleTables.length) {
    container.innerHTML = '<div class="no-queries">No matching tables or columns</div>';
    selectedTable = null;
    updateInspectScopeLabel();
    filterQueries();
    return;
  }

  if (selectedTable && !visibleTables.some(entry => entry.table.name === selectedTable)) {
    selectedTable = null;
    updateInspectScopeLabel();
    filterQueries();
  }

  container.innerHTML = visibleTables.map(({ table: t, tableMatches, matchingColumns }, idx) => {
    const rowCount = t.row_count != null ? Number(t.row_count).toLocaleString() : '?';
    const isView = t.name.startsWith('v_');
    const autoExpand = selectedTable === t.name || (!!query && matchingColumns.length > 0 && !tableMatches);
    const icon = isView
      ? '<svg class="table-icon" viewBox="0 0 16 16" fill="none"><path d="M8 2C4.13 2 1 3.79 1 6v4c0 2.21 3.13 4 7 4s7-1.79 7-4V6c0-2.21-3.13-4-7-4z" stroke="currentColor" stroke-width="1.3" fill="none"/><path d="M1 6c0 2.21 3.13 4 7 4s7-1.79 7-4" stroke="currentColor" stroke-width="1.3" fill="none"/></svg>'
      : '<svg class="table-icon" viewBox="0 0 16 16" fill="none"><rect x="1" y="2" width="14" height="12" rx="2" stroke="currentColor" stroke-width="1.3"/><line x1="1" y1="5.5" x2="15" y2="5.5" stroke="currentColor" stroke-width="1.3"/><line x1="1" y1="9" x2="15" y2="9" stroke="currentColor" stroke-width="1"/><line x1="6" y1="5.5" x2="6" y2="14" stroke="currentColor" stroke-width="1"/></svg>';

    const visibleColumns = query
      ? (tableMatches ? (t.columns || []) : matchingColumns)
      : (t.columns || []);

    const cols = visibleColumns.map(c =>
      '<div class="column-item' + (query && c.name.toLowerCase().includes(query) ? ' matched' : '') +
        '" data-table="' + escapeHtml(t.name) + '" data-column="' + escapeHtml(c.name) + '">' +
        '<div class="column-item-main">' +
          '<span class="col-name">' + highlightMatch(c.name, query) + '</span>' +
          '<span class="col-type">' + escapeHtml(c.dtype) + '</span>' +
        '</div>' +
        (query ? renderSchemaQuickActions(t.name, c.name) : '') +
        '<div class="col-stats"></div>' +
      '</div>'
    ).join('');

    return '<div class="table-item">' +
      '<div class="table-header' + (selectedTable === t.name ? ' selected' : '') + (autoExpand ? ' expanded' : '') +
        (query && (tableMatches || matchingColumns.length > 0) ? ' search-hit' : '') +
        '" data-table="' + escapeHtml(t.name) + '" data-idx="' + idx + '">' +
        icon +
        '<span class="table-name">' + highlightMatch(t.name, query) + '</span>' +
        '<span class="table-rows">' + rowCount + '</span>' +
        (query ? renderSchemaQuickActions(t.name, '') : '') +
        '<svg class="table-chevron" viewBox="0 0 16 16" fill="none"><path d="M6 4l4 4-4 4" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>' +
      '</div>' +
      '<div class="column-list' + (autoExpand ? ' visible' : '') + '" id="cols-' + idx + '">' +
        '<button class="preview-btn" data-table="' + escapeHtml(t.name) + '">Preview rows</button>' +
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
  updateInspectScopeLabel();

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
    const cacheKey = tableName + '.' + colName;
    let data = columnStatsCache.get(cacheKey);
    if (!data) {
      data = await fetchJson('/api/column-stats/' + encodeURIComponent(tableName) + '/' + encodeURIComponent(colName));
      columnStatsCache.set(cacheKey, data);
    }
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
    let data = tablePreviewCache.get(tableName);
    if (!data) {
      data = await fetchJson('/api/preview/' + encodeURIComponent(tableName));
      tablePreviewCache.set(tableName, data);
    }
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
    return '<div class="query-item" data-question="' + escapeHtml(q.question) + '">' +
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

