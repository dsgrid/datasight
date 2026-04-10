async function loadSchema() {
  try {
    const data = await fetchJson('/api/schema');
    schemaData = data.tables || [];
    clearSchemaInsightCaches();
    renderTables(schemaData);
    populateMeasureBuilderTables();
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

function updateInspectScopeLabel() {
  const el = document.getElementById('inspect-scope-label');
  if (!el) return;
  el.textContent = selectedTable ? ('Scope: selected table `' + selectedTable + '`') : 'Scope: dataset';
}

async function loadRecipes() {
  try {
    const data = await fetchJson('/api/recipes');
    recipesCache = data.recipes || [];
    renderRecipes(recipesCache);
  } catch (e) {
    const container = document.getElementById('recipes-list');
    if (container) {
      container.innerHTML = '<div class="no-queries">Failed to load recipes</div>';
    }
  }
}

function setMeasureEditorState(text, status, kind) {
  const editor = document.getElementById('measures-editor');
  const statusEl = document.getElementById('measures-editor-status');
  if (editor) editor.value = text || '';
  if (statusEl) {
    statusEl.textContent = status || '';
    statusEl.className = kind === 'error' ? 'no-queries error visible' : 'no-queries';
  }
}

function populateMeasureBuilderTables() {
  const tableEl = document.getElementById('measure-builder-table');
  if (!tableEl) return;

  const currentValue = tableEl.value;
  const tableNames = Array.from(new Set(
    [
      ...schemaData.map(table => table.name),
      ...measureEditorCatalog.map(item => item.table),
    ].filter(Boolean)
  )).sort();

  tableEl.innerHTML = '<option value="">Select table…</option>' +
    tableNames.map(name =>
      '<option value="' + escapeHtml(name) + '">' + escapeHtml(name) + '</option>'
    ).join('');

  if (selectedTable && tableNames.includes(selectedTable)) {
    tableEl.value = selectedTable;
  } else if (currentValue && tableNames.includes(currentValue)) {
    tableEl.value = currentValue;
  }
}

function populateWeightColumnOptions(tableName, selectedValue) {
  const weightEl = document.getElementById('measure-builder-weight-column');
  if (!weightEl) return;

  const table = schemaData.find(item => item.name === tableName);
  const numericColumns = table
    ? table.columns
      .filter(column => {
        const dtype = String(column.dtype || '').toLowerCase();
        return ['int', 'decimal', 'numeric', 'float', 'double', 'real', 'number'].some(token => dtype.includes(token));
      })
      .map(column => column.name)
    : [];

  weightEl.innerHTML = '<option value="">weight column (optional)</option>' +
    numericColumns.map(name =>
      '<option value="' + escapeHtml(name) + '">' + escapeHtml(name) + '</option>'
    ).join('');

  if (selectedValue && numericColumns.includes(selectedValue)) {
    weightEl.value = selectedValue;
  }
}

function setMultiSelectValues(selectEl, values) {
  if (!selectEl) return;
  const valueSet = new Set(values || []);
  Array.from(selectEl.options).forEach(option => {
    option.selected = valueSet.has(option.value);
  });
}

function getMultiSelectValues(selectEl) {
  if (!selectEl) return [];
  return Array.from(selectEl.selectedOptions).map(option => option.value);
}

function populateMeasureOverrideBuilder(measures) {
  measureEditorCatalog = measures || [];
  const select = document.getElementById('measure-builder-select');
  if (!select) return;

  const currentValue = select.value;
  select.innerHTML = '<option value="">Select inferred measure…</option>' +
    measureEditorCatalog.map(item => {
      const value = item.table + '.' + item.column;
      const label = item.table + '.' + item.column + ' [' + (item.default_aggregation || 'avg') + ']';
      return '<option value="' + escapeHtml(value) + '">' + escapeHtml(label) + '</option>';
    }).join('');

  if (currentValue && measureEditorCatalog.some(item => (item.table + '.' + item.column) === currentValue)) {
    select.value = currentValue;
  }
  populateMeasureBuilderTables();
}

function updateMeasureBuilderMode() {
  const modeEl = document.getElementById('measure-builder-mode');
  const tableEl = document.getElementById('measure-builder-table');
  const selectEl = document.getElementById('measure-builder-select');
  const nameEl = document.getElementById('measure-builder-name');
  const expressionEl = document.getElementById('measure-builder-expression');
  if (!modeEl || !tableEl || !selectEl || !nameEl || !expressionEl) return;

  const calculated = modeEl.value === 'calculated';
  selectEl.disabled = calculated;
  tableEl.disabled = false;
  nameEl.disabled = !calculated;
  expressionEl.disabled = !calculated;
  if (!calculated) {
    nameEl.value = '';
    expressionEl.value = '';
  }
  populateWeightColumnOptions(tableEl.value, document.getElementById('measure-builder-weight-column')?.value || '');
}

function applyMeasureBuilderSelection() {
  const modeEl = document.getElementById('measure-builder-mode');
  const tableEl = document.getElementById('measure-builder-table');
  const select = document.getElementById('measure-builder-select');
  const aggregationEl = document.getElementById('measure-builder-aggregation');
  const strategyEl = document.getElementById('measure-builder-average-strategy');
  const weightEl = document.getElementById('measure-builder-weight-column');
  const nameEl = document.getElementById('measure-builder-name');
  const expressionEl = document.getElementById('measure-builder-expression');
  const displayNameEl = document.getElementById('measure-builder-display-name');
  const formatEl = document.getElementById('measure-builder-format');
  const chartTypesEl = document.getElementById('measure-builder-chart-types');
  if (!select || !aggregationEl || !strategyEl || !weightEl || !modeEl || !tableEl || !nameEl || !expressionEl || !displayNameEl || !formatEl || !chartTypesEl) return;
  if (modeEl.value === 'calculated') return;

  const selected = measureEditorCatalog.find(item => (item.table + '.' + item.column) === select.value);
  if (!selected) {
    aggregationEl.value = 'avg';
    strategyEl.value = 'avg';
    weightEl.value = '';
    nameEl.value = '';
    expressionEl.value = '';
    displayNameEl.value = '';
    formatEl.value = '';
    chartTypesEl.value = '';
    return;
  }

  aggregationEl.value = selected.default_aggregation || 'avg';
  strategyEl.value = selected.average_strategy || 'avg';
  tableEl.value = selected.table || '';
  populateWeightColumnOptions(tableEl.value, selected.weight_column || '');
  nameEl.value = selected.name || '';
  expressionEl.value = selected.expression || '';
  displayNameEl.value = selected.display_name || '';
  formatEl.value = selected.format || '';
  setMultiSelectValues(chartTypesEl, selected.preferred_chart_types || []);
}

async function loadMeasureEditorCatalog() {
  if (!projectLoaded || !currentProjectPath) {
    populateMeasureOverrideBuilder([]);
    return;
  }

  try {
    const data = await fetchJson('/api/measures/editor/catalog');
    if (!data.ok) {
      populateMeasureOverrideBuilder([]);
      return;
    }
    populateMeasureOverrideBuilder(data.measures || []);
    applyMeasureBuilderSelection();
    updateMeasureBuilderMode();
  } catch (e) {
    populateMeasureOverrideBuilder([]);
  }
}

async function loadMeasureOverridesEditor() {
  if (!projectLoaded || !currentProjectPath) {
    setMeasureEditorState('', 'Load a saved project to edit measures.yaml overrides.', 'info');
    populateMeasureOverrideBuilder([]);
    return;
  }

  setMeasureEditorState(
    document.getElementById('measures-editor')?.value || '',
    'Loading measure overrides...',
    'info'
  );
  try {
    const [data] = await Promise.all([
      fetchJson('/api/measures/editor'),
      loadMeasureEditorCatalog(),
    ]);
    if (!data.ok) {
      setMeasureEditorState('', data.error || 'Failed to load measure overrides.', 'error');
      return;
    }
    const status = data.generated
      ? 'Loaded inferred scaffold. Save to create measures.yaml.'
      : ('Editing ' + (data.path || 'measures.yaml'));
    setMeasureEditorState(data.text || '', status, 'info');
  } catch (e) {
    setMeasureEditorState('', 'Failed to load measure overrides.', 'error');
  }
}

async function validateMeasureOverrides() {
  if (!projectLoaded || !currentProjectPath) {
    setMeasureEditorState('', 'Load a saved project to validate measures.yaml overrides.', 'error');
    return;
  }

  const editor = document.getElementById('measures-editor');
  const text = editor ? editor.value : '';
  try {
    const data = await fetchJson('/api/measures/editor/validate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
    if (!data.ok) {
      const message = (data.errors || [data.error || 'Validation failed.']).join(' ');
      setMeasureEditorState(text, message, 'error');
      return;
    }
    const warningText = (data.warnings || []).length
      ? (' Valid with warnings: ' + data.warnings.join(' '))
      : ' YAML looks valid.';
    setMeasureEditorState(text, 'Measure overrides validated.' + warningText, 'info');
  } catch (e) {
    setMeasureEditorState(text, 'Failed to validate measure overrides.', 'error');
  }
}

async function insertMeasureOverride() {
  if (!projectLoaded || !currentProjectPath) {
    setMeasureEditorState('', 'Load a saved project to edit measures.yaml overrides.', 'error');
    return;
  }

  const modeEl = document.getElementById('measure-builder-mode');
  const select = document.getElementById('measure-builder-select');
  const nameEl = document.getElementById('measure-builder-name');
  const expressionEl = document.getElementById('measure-builder-expression');
  const aggregationEl = document.getElementById('measure-builder-aggregation');
  const strategyEl = document.getElementById('measure-builder-average-strategy');
  const weightEl = document.getElementById('measure-builder-weight-column');
  const displayNameEl = document.getElementById('measure-builder-display-name');
  const formatEl = document.getElementById('measure-builder-format');
  const chartTypesEl = document.getElementById('measure-builder-chart-types');
  const editor = document.getElementById('measures-editor');
  if (!modeEl || !select || !nameEl || !expressionEl || !aggregationEl || !strategyEl || !weightEl || !displayNameEl || !formatEl || !chartTypesEl || !editor) return;

  const isCalculated = modeEl.value === 'calculated';
  const tableEl = document.getElementById('measure-builder-table');
  let table = '';
  let column = '';
  let name = '';
  let expression = '';
  if (isCalculated) {
    name = nameEl.value.trim();
    expression = expressionEl.value.trim();
    if (!name || !expression) {
      setMeasureEditorState(editor.value || '', 'Enter a calculated measure name and expression.', 'error');
      return;
    }
    table = tableEl ? tableEl.value.trim() : '';
    if (!table) {
      setMeasureEditorState(editor.value || '', 'Choose a target table for the calculated measure.', 'error');
      return;
    }
  } else {
    if (!select.value) {
      setMeasureEditorState(editor.value || '', 'Choose an inferred measure first.', 'error');
      return;
    }
    const parts = select.value.split('.');
    table = parts.shift() || '';
    column = parts.join('.');
  }
  try {
    const data = await fetchJson('/api/measures/editor/upsert', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        text: editor.value || '',
        table,
        column,
        name,
        expression,
        default_aggregation: aggregationEl.value,
        average_strategy: strategyEl.value,
        weight_column: weightEl.value.trim(),
        display_name: displayNameEl.value.trim(),
        format: formatEl.value.trim(),
        preferred_chart_types: getMultiSelectValues(chartTypesEl),
      }),
    });
    if (!data.ok) {
      setMeasureEditorState(editor.value || '', data.error || 'Failed to insert override.', 'error');
      return;
    }
    setMeasureEditorState(
      data.text || '',
      'Inserted override for ' + table + '.' + (column || name) + '.',
      'info'
    );
  } catch (e) {
    setMeasureEditorState(editor.value || '', 'Failed to insert override.', 'error');
  }
}

async function openMeasureOverrideEditorForCard(config) {
  if (!config || !config.table || !config.column) return;
  switchView('chat');
  openSidebarSection('measures-editor-section');
  await loadMeasureOverridesEditor();

  const select = document.getElementById('measure-builder-select');
  const modeEl = document.getElementById('measure-builder-mode');
  const tableEl = document.getElementById('measure-builder-table');
  const nameEl = document.getElementById('measure-builder-name');
  const expressionEl = document.getElementById('measure-builder-expression');
  const aggregationEl = document.getElementById('measure-builder-aggregation');
  const strategyEl = document.getElementById('measure-builder-average-strategy');
  const weightEl = document.getElementById('measure-builder-weight-column');
  const displayNameEl = document.getElementById('measure-builder-display-name');
  const formatEl = document.getElementById('measure-builder-format');
  const chartTypesEl = document.getElementById('measure-builder-chart-types');
  if (modeEl) modeEl.value = 'physical';
  if (tableEl) tableEl.value = config.table || '';
  if (select) select.value = config.table + '.' + config.column;
  if (nameEl) nameEl.value = '';
  if (expressionEl) expressionEl.value = '';
  if (aggregationEl) aggregationEl.value = config.defaultAggregation || 'avg';
  if (strategyEl) strategyEl.value = config.averageStrategy || 'avg';
  populateWeightColumnOptions(config.table || '', config.weightColumn || '');
  if (displayNameEl) displayNameEl.value = config.displayName || '';
  if (formatEl) formatEl.value = config.format || '';
  if (chartTypesEl) setMultiSelectValues(chartTypesEl, config.preferredChartTypes || []);
  updateMeasureBuilderMode();
  await insertMeasureOverride();
}

async function saveMeasureOverrides() {
  if (!projectLoaded || !currentProjectPath) {
    setMeasureEditorState('', 'Load a saved project to edit measures.yaml overrides.', 'error');
    return;
  }

  const editor = document.getElementById('measures-editor');
  const text = editor ? editor.value : '';
  setMeasureEditorState(text, 'Saving measure overrides...', 'info');

  try {
    const data = await fetchJson('/api/measures/editor', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
    if (!data.ok) {
      setMeasureEditorState(text, data.error || 'Failed to save measure overrides.', 'error');
      return;
    }

    await loadSchema();
    await loadQueries();
    await loadRecipes();
    await loadProjectHealth();
    await loadMeasureOverridesEditor();
    showToast('Saved measure overrides.', 'success');
  } catch (e) {
    setMeasureEditorState(text, 'Failed to save measure overrides.', 'error');
  }
}

function handleSchemaSearchInput(value) {
  schemaSearchQuery = (value || '').trim().toLowerCase();
  renderTables(schemaData);
}

function focusTableInSidebar(tableName, columnName) {
  const searchInput = document.getElementById('schema-search-input');
  if (searchInput) searchInput.value = '';
  schemaSearchQuery = '';
  selectedTable = tableName;
  updateInspectScopeLabel();
  renderTables(schemaData);
  populateMeasureBuilderTables();
  filterQueries();

  const header = Array.from(document.querySelectorAll('.table-header')).find(
    el => el.dataset.table === tableName
  );
  if (!header) return;
  if (sidebar.classList.contains('collapsed')) {
    toggleSidebar();
  }
  header.scrollIntoView({ block: 'center' });

  if (!header.classList.contains('expanded')) {
    toggleTable(header);
  }

  if (columnName) {
    const columnEl = Array.from(header.parentElement.querySelectorAll('.column-item')).find(el =>
      el.textContent.toLowerCase().includes(columnName.toLowerCase())
    );
    if (columnEl) {
      columnEl.scrollIntoView({ block: 'center' });
      columnEl.classList.add('matched');
      window.setTimeout(() => columnEl.classList.remove('matched'), 1200);
    }
  }
}

function useRecipePrompt(prompt) {
  switchView('chat');
  inputEl.value = prompt;
  inputEl.dispatchEvent(new window.Event('input'));
  inputEl.focus();
}

function useRecipeElement(el) {
  if (!el) return;
  useRecipePrompt(el.dataset.prompt || '');
}

function renderRecipes(recipes) {
  const container = document.getElementById('recipes-list');
  if (!container) return;
  if (!recipes.length) {
    container.innerHTML = '<div class="no-queries">No recipes yet.</div>';
    return;
  }
  container.innerHTML = recipes.map(recipe =>
    '<div class="query-item" data-prompt="' + escapeHtml(recipe.prompt) + '">' +
      '<div class="query-question">' + escapeHtml(recipe.title) + '</div>' +
      '<div class="query-sql-preview">' + escapeHtml(recipe.category || 'Recipe') + '</div>' +
      (recipe.reason ? '<div class="query-sql-preview">' + escapeHtml(recipe.reason) + '</div>' : '') +
    '</div>'
  ).join('');
}

function openSidebarSection(sectionId) {
  const section = document.getElementById(sectionId);
  if (!section) return;
  if (sidebar.classList.contains('collapsed')) {
    toggleSidebar();
  }
  if (section.classList.contains('collapsed')) {
    toggleSidebarSection(sectionId);
  }
  section.scrollIntoView({ block: 'nearest' });
}

function openProjectHealthPanel() {
  toggleSettingsPanel();
  loadProjectHealth();
  const panel = document.getElementById('project-health');
  if (panel) {
    panel.scrollIntoView({ block: 'nearest' });
  }
}

function previewTableInSidebar(tableName) {
  switchView('chat');
  focusTableInSidebar(tableName);
  const header = Array.from(document.querySelectorAll('.table-header')).find(
    el => el.dataset.table === tableName
  );
  if (!header) return;
  const previewBtn = header.parentElement.querySelector('.preview-btn');
  if (previewBtn && previewBtn.textContent !== 'Hide preview') {
    previewTable(tableName, previewBtn);
  }
}

function openColumnStatsInSidebar(tableName, columnName) {
  switchView('chat');
  focusTableInSidebar(tableName, columnName);
  const header = Array.from(document.querySelectorAll('.table-header')).find(
    el => el.dataset.table === tableName
  );
  if (!header) return;
  const columnEl = Array.from(header.parentElement.querySelectorAll('.column-item')).find(el =>
    el.textContent.toLowerCase().includes(columnName.toLowerCase())
  );
  if (!columnEl) return;
  const statsEl = columnEl.querySelector('.col-stats');
  if (!statsEl || !statsEl.innerHTML) {
    toggleColumnStats(columnEl, tableName, columnName);
  }
}

function askAboutTable(tableName) {
  switchView('chat');
  sendMessage('Describe the purpose of the `' + tableName + '` table, its key columns, likely joins, and the best first questions to ask about it.');
}

function askAboutColumn(tableName, columnName) {
  switchView('chat');
  sendMessage('Explain the `' + tableName + '.' + columnName + '` column, what it likely represents, how it should be used in analysis, and any data quality concerns.');
}

