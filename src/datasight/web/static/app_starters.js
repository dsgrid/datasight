function landingChooseStarter(starterId) {
  pendingStarterAction = starterId;
  document.querySelectorAll('.landing-starter').forEach(el => {
    el.classList.toggle('active', el.dataset.starterId === starterId);
  });

  const statusEl = document.getElementById('landing-starter-status');
  const config = STARTER_CONFIG[starterId];
  if (statusEl && config) {
    statusEl.innerHTML = 'Selected: <strong>' + escapeHtml(config.title) + '</strong>. ' +
      escapeHtml(config.status);
  }

  document.getElementById('landing-explore-input')?.focus();
}

function renderOverviewMetric(label, value) {
  return (
    '<div class="overview-metric">' +
      '<span class="overview-metric-label">' + escapeHtml(label) + '</span>' +
      '<strong class="overview-metric-value">' + escapeHtml(String(value)) + '</strong>' +
    '</div>'
  );
}

function renderOverviewList(title, items, emptyText, renderItem) {
  const body = items.length
    ? '<div class="overview-list">' + items.map(renderItem).join('') + '</div>'
    : '<div class="overview-empty">' + escapeHtml(emptyText) + '</div>';

  return (
    '<section class="overview-section">' +
      '<div class="overview-section-title">' + escapeHtml(title) + '</div>' +
      body +
    '</section>'
  );
}

function runStarterFollowup(encodedPrompt) {
  const prompt = decodeURIComponent(encodedPrompt || '');
  if (!prompt) return;
  sendMessage(prompt);
}

function renderStarterActions(actions) {
  if (!actions.length) return '';
  return (
    '<div class="starter-actions">' +
      actions.map(action =>
        '<button type="button" class="starter-action-btn" data-prompt="' +
        escapeHtml(encodeURIComponent(action.prompt)) + '">' + escapeHtml(action.label) + '</button>'
      ).join('') +
    '</div>'
  );
}

function renderDatasetOverview(overview) {
  const largestTables = overview.largest_tables || [];
  const dateColumns = overview.date_columns || [];
  const measureColumns = overview.measure_columns || [];
  const dimensionColumns = overview.dimension_columns || [];
  const qualityFlags = overview.quality_flags || [];
  const actions = [];

  if (largestTables.length) {
    actions.push({
      label: 'Profile ' + largestTables[0].name,
      prompt: 'Profile the `' + largestTables[0].name + '` table and highlight its most important columns.',
    });
  }
  if (dateColumns.length && measureColumns.length) {
    actions.push({
      label: 'Build a First Trend',
      prompt: 'Create a trend chart of `' + measureColumns[0].table + '.' + measureColumns[0].column +
        '` over `' + dateColumns[0].table + '.' + dateColumns[0].column + '` and explain why this is a good starting view.',
    });
  }
  if (dimensionColumns.length) {
    actions.push({
      label: 'Inspect Top Dimension',
      prompt: 'Explain whether `' + dimensionColumns[0].table + '.' + dimensionColumns[0].column +
        '` is a good grouping dimension and show the top categories.',
    });
  }

  const html =
    '<div class="starter-overview">' +
      '<div class="starter-overview-head">' +
        '<span class="starter-overview-kicker">Starter result</span>' +
        '<h3>Dataset profile</h3>' +
        '<p>A deterministic overview of the schema to help you get oriented before asking follow-up questions.</p>' +
      '</div>' +
      '<div class="overview-metrics">' +
        renderOverviewMetric('Tables', Number(overview.table_count || 0).toLocaleString()) +
        renderOverviewMetric('Columns', Number(overview.total_columns || 0).toLocaleString()) +
        renderOverviewMetric('Rows', Number(overview.total_rows || 0).toLocaleString()) +
      '</div>' +
      renderOverviewList(
        'Largest tables',
        largestTables,
        'No table metadata available.',
        item => '<div class="overview-item"><strong>' + escapeHtml(item.name) +
          '</strong><span>' + escapeHtml(Number(item.row_count || 0).toLocaleString() + ' rows') +
          '</span><span>' + escapeHtml(String(item.column_count || 0) + ' cols') + '</span></div>'
      ) +
      renderOverviewList(
        'Date coverage',
        dateColumns,
        'No obvious date columns detected.',
        item => '<div class="overview-item"><strong>' + escapeHtml(item.table + '.' + item.column) +
          '</strong><span>' + escapeHtml((item.min || '?') + ' → ' + (item.max || '?')) + '</span></div>'
      ) +
      renderOverviewList(
        'Measure candidates',
        measureColumns,
        'No obvious numeric measure columns detected.',
        item => '<div class="overview-item"><strong>' + escapeHtml(item.table + '.' + item.column) +
          '</strong><span>' + escapeHtml(item.dtype || 'unknown') + '</span></div>'
      ) +
      renderOverviewList(
        'Dimension candidates',
        dimensionColumns,
        'No obvious text dimensions detected.',
        item => {
          const sampleText = item.sample_values && item.sample_values.length
            ? 'Samples: ' + item.sample_values.join(', ')
            : 'No sample values';
          const stats = [];
          if (item.distinct_count != null) stats.push(item.distinct_count + ' distinct');
          if (item.null_rate != null) stats.push(item.null_rate + '% null');
          return '<div class="overview-item overview-item-rich"><strong>' +
            escapeHtml(item.table + '.' + item.column) + '</strong><span>' +
            escapeHtml(stats.join(' • ') || 'No quick stats') + '</span><span>' +
            escapeHtml(sampleText) + '</span></div>';
        }
      ) +
      (qualityFlags.length ? renderOverviewList(
        'Quick notes',
        qualityFlags,
        '',
        item => '<div class="overview-item overview-item-note"><span>' + escapeHtml(item) + '</span></div>'
      ) : '') +
      renderStarterActions(actions) +
    '</div>';

  addAssistantHtml(html, 'starter-overview-bubble');
}

function renderDimensionOverview(overview) {
  const dimensionColumns = overview.dimension_columns || [];
  const dateColumns = overview.date_columns || [];
  const measureColumns = overview.measure_columns || [];
  const suggestedBreakdowns = overview.suggested_breakdowns || [];
  const joinHints = overview.join_hints || [];
  const actions = [];

  if (suggestedBreakdowns.length) {
    actions.push({
      label: 'Run Top Breakdown',
      prompt: 'Show the most useful breakdown using `' + suggestedBreakdowns[0].table + '.' +
        suggestedBreakdowns[0].column + '` and explain what stands out.',
    });
  }
  if (measureColumns.length && dimensionColumns.length) {
    actions.push({
      label: 'Rank by Dimension',
      prompt: 'Aggregate `' + measureColumns[0].table + '.' + measureColumns[0].column + '` by `' +
        dimensionColumns[0].table + '.' + dimensionColumns[0].column +
        '` and show the top groups.',
    });
  }
  if (joinHints.length) {
    actions.push({
      label: 'Use Join Hint',
      prompt: 'Use this join hint in an example query: ' + joinHints[0],
    });
  }

  const html =
    '<div class="starter-overview">' +
      '<div class="starter-overview-head">' +
        '<span class="starter-overview-kicker">Starter result</span>' +
        '<h3>Key dimensions</h3>' +
        '<p>A deterministic pass over likely grouping fields, common breakdowns, and join hints.</p>' +
      '</div>' +
      '<div class="overview-metrics">' +
        renderOverviewMetric('Tables', Number(overview.table_count || 0).toLocaleString()) +
        renderOverviewMetric('Dimensions', Number(dimensionColumns.length || 0).toLocaleString()) +
        renderOverviewMetric('Measures', Number(measureColumns.length || 0).toLocaleString()) +
      '</div>' +
      renderOverviewList(
        'Best grouping columns',
        dimensionColumns,
        'No strong text dimensions detected.',
        item => {
          const stats = [];
          if (item.distinct_count != null) stats.push(item.distinct_count + ' distinct');
          if (item.null_rate != null) stats.push(item.null_rate + '% null');
          const sampleText = item.sample_values && item.sample_values.length
            ? 'Samples: ' + item.sample_values.join(', ')
            : 'No sample values';
          return '<div class="overview-item overview-item-rich"><strong>' +
            escapeHtml(item.table + '.' + item.column) + '</strong><span>' +
            escapeHtml(stats.join(' • ') || 'No quick stats') + '</span><span>' +
            escapeHtml(sampleText) + '</span></div>';
        }
      ) +
      renderOverviewList(
        'Suggested breakdowns',
        suggestedBreakdowns,
        'No obvious breakdown suggestions yet.',
        item => '<div class="overview-item overview-item-rich"><strong>' +
          escapeHtml(item.table + '.' + item.column) + '</strong><span>' +
          escapeHtml(item.reason || '') + '</span></div>'
      ) +
      renderOverviewList(
        'Date columns',
        dateColumns,
        'No obvious date columns detected.',
        item => '<div class="overview-item"><strong>' + escapeHtml(item.table + '.' + item.column) +
          '</strong><span>' + escapeHtml((item.min || '?') + ' → ' + (item.max || '?')) + '</span></div>'
      ) +
      renderOverviewList(
        'Measure columns',
        measureColumns,
        'No obvious measure columns detected.',
        item => '<div class="overview-item"><strong>' + escapeHtml(item.table + '.' + item.column) +
          '</strong><span>' + escapeHtml(item.dtype || 'unknown') + '</span></div>'
      ) +
      (joinHints.length ? renderOverviewList(
        'Join hints',
        joinHints,
        '',
        item => '<div class="overview-item overview-item-note"><span>' + escapeHtml(item) + '</span></div>'
      ) : '') +
      renderStarterActions(actions) +
    '</div>';

  addAssistantHtml(html, 'starter-overview-bubble');
}

function renderQualityOverview(overview) {
  const nullColumns = overview.null_columns || [];
  const numericFlags = overview.numeric_flags || [];
  const dateColumns = overview.date_columns || [];
  const notes = overview.notes || [];
  const actions = [];

  if (nullColumns.length) {
    actions.push({
      label: 'Investigate Nulls',
      prompt: 'Investigate why `' + nullColumns[0].table + '.' + nullColumns[0].column +
        '` has ' + nullColumns[0].null_rate + '% null values and show how those nulls are distributed.',
    });
  }
  if (numericFlags.length) {
    actions.push({
      label: 'Inspect Range Flag',
      prompt: 'Inspect the numeric quality issue on `' + numericFlags[0].table + '.' +
        numericFlags[0].column + '` and explain whether it is expected.',
    });
  }
  if (dateColumns.length) {
    actions.push({
      label: 'Check Freshness',
      prompt: 'Assess data freshness using `' + dateColumns[0].table + '.' + dateColumns[0].column +
        '` and summarize whether the latest data looks current.',
    });
  }

  const html =
    '<div class="starter-overview">' +
      '<div class="starter-overview-head">' +
        '<span class="starter-overview-kicker">Starter result</span>' +
        '<h3>Data quality audit</h3>' +
        '<p>A deterministic first pass over null-heavy columns, numeric range anomalies, and date coverage.</p>' +
      '</div>' +
      '<div class="overview-metrics">' +
        renderOverviewMetric('Tables', Number(overview.table_count || 0).toLocaleString()) +
        renderOverviewMetric('Null Issues', Number(nullColumns.length || 0).toLocaleString()) +
        renderOverviewMetric('Range Flags', Number(numericFlags.length || 0).toLocaleString()) +
      '</div>' +
      renderOverviewList(
        'Null-heavy columns',
        nullColumns,
        'No null-heavy columns detected.',
        item => '<div class="overview-item overview-item-rich"><strong>' +
          escapeHtml(item.table + '.' + item.column) + '</strong><span>' +
          escapeHtml((item.null_rate || 0) + '% null') + '</span><span>' +
          escapeHtml(String(item.null_count || 0) + ' nulls') + '</span></div>'
      ) +
      renderOverviewList(
        'Numeric range flags',
        numericFlags,
        'No obvious numeric range issues detected.',
        item => '<div class="overview-item overview-item-rich"><strong>' +
          escapeHtml(item.table + '.' + item.column) + '</strong><span>' +
          escapeHtml(item.issue || '') + '</span></div>'
      ) +
      renderOverviewList(
        'Date coverage',
        dateColumns,
        'No obvious date columns detected.',
        item => '<div class="overview-item"><strong>' + escapeHtml(item.table + '.' + item.column) +
          '</strong><span>' + escapeHtml((item.min || '?') + ' → ' + (item.max || '?')) + '</span></div>'
      ) +
      (notes.length ? renderOverviewList(
        'Quick notes',
        notes,
        '',
        item => '<div class="overview-item overview-item-note"><span>' + escapeHtml(item) + '</span></div>'
      ) : '') +
      renderStarterActions(actions) +
    '</div>';

  addAssistantHtml(html, 'starter-overview-bubble');
}

function renderTrendOverview(overview) {
  const trendCandidates = overview.trend_candidates || [];
  const breakoutDimensions = overview.breakout_dimensions || [];
  const chartRecommendations = overview.chart_recommendations || [];
  const notes = overview.notes || [];
  const actions = [];

  if (trendCandidates.length) {
    actions.push({
      label: 'Create Starter Chart',
      prompt: 'Create a line chart of `' + (trendCandidates[0].aggregation || 'sum').toUpperCase() +
        '(' + trendCandidates[0].table + '.' + trendCandidates[0].measure_column + ')` over `' +
        trendCandidates[0].table + '.' + trendCandidates[0].date_column + '`.',
    });
  }
  if (trendCandidates.length && breakoutDimensions.length) {
    actions.push({
      label: 'Add a Breakout',
      prompt: 'Create a trend chart of `' + (trendCandidates[0].aggregation || 'sum').toUpperCase() +
        '(' + trendCandidates[0].table + '.' + trendCandidates[0].measure_column + ')` over `' +
        trendCandidates[0].table + '.' + trendCandidates[0].date_column +
        '`, broken out by `' + breakoutDimensions[0].table + '.' + breakoutDimensions[0].column + '`.',
    });
  }
  if (chartRecommendations.length) {
    actions.push({
      label: 'Explain Chart Choice',
      prompt: 'Explain why `' + chartRecommendations[0].title + '` is a good starter chart for this dataset.',
    });
  }

  const html =
    '<div class="starter-overview">' +
      '<div class="starter-overview-head">' +
        '<span class="starter-overview-kicker">Starter result</span>' +
        '<h3>Trend chart ideas</h3>' +
        '<p>A deterministic pass over likely date columns, measures, and chart setups worth trying first.</p>' +
      '</div>' +
      '<div class="overview-metrics">' +
        renderOverviewMetric('Tables', Number(overview.table_count || 0).toLocaleString()) +
        renderOverviewMetric('Trend Pairs', Number(trendCandidates.length || 0).toLocaleString()) +
        renderOverviewMetric('Breakouts', Number(breakoutDimensions.length || 0).toLocaleString()) +
      '</div>' +
      renderOverviewList(
        'Best time-series pairs',
        trendCandidates,
        'No obvious date/measure pairs detected.',
        item => '<div class="overview-item overview-item-rich"><strong>' +
          escapeHtml((item.aggregation || 'sum').toUpperCase() + '(' + item.table + '.' + (item.measure_display_name || item.measure_column) + ')') +
          '</strong><span>' + escapeHtml('by ' + item.date_column) + '</span><span>' +
          escapeHtml((item.measure_role || 'measure') +
            (item.measure_format ? (' • format ' + item.measure_format) : '') +
            ' • ' + (item.date_range || '')) + '</span></div>'
      ) +
      renderOverviewList(
        'Chart recommendations',
        chartRecommendations,
        'No starter chart recommendations available.',
        item => '<div class="overview-item overview-item-rich"><strong>' +
          escapeHtml(item.title || '') + '</strong><span>' +
          escapeHtml((item.chart_type || '') + ' • ' + (item.aggregation || '').toUpperCase() +
            ((item.preferred_chart_types || []).length ? (' • pref ' + item.preferred_chart_types.join('/')) : '')) + '</span><span>' +
          escapeHtml(item.reason || '') + '</span></div>'
      ) +
      renderOverviewList(
        'Category breakouts',
        breakoutDimensions,
        'No obvious category breakouts detected.',
        item => {
          const stats = [];
          if (item.distinct_count != null) stats.push(item.distinct_count + ' distinct');
          if (item.null_rate != null) stats.push(item.null_rate + '% null');
          return '<div class="overview-item overview-item-rich"><strong>' +
            escapeHtml(item.table + '.' + item.column) + '</strong><span>' +
            escapeHtml(stats.join(' • ') || 'No quick stats') + '</span></div>';
        }
      ) +
      (notes.length ? renderOverviewList(
        'Quick notes',
        notes,
        '',
        item => '<div class="overview-item overview-item-note"><span>' + escapeHtml(item) + '</span></div>'
      ) : '') +
      renderStarterActions(actions) +
    '</div>';

  addAssistantHtml(html, 'starter-overview-bubble');
}

function renderMeasureOverview(overview) {
  const measures = overview.measures || [];
  const notes = overview.notes || [];
  const actions = [];

  if (measures.length) {
    const primary = measures[0];
    const primaryLabel = primary.display_name || (primary.table + '.' + primary.column);
    actions.push({
      label: 'Use Default Aggregation',
      prompt: 'Analyze `' + primaryLabel + '` using its default `' +
        primary.default_aggregation + '` aggregation. Use this rollup shape as a guide: `' +
        primary.recommended_rollup_sql + '`. Explain why that aggregation matches the metric.',
    });
    if (primary.role === 'power' || primary.role === 'capacity') {
      actions.push({
        label: 'Compare Peak vs Average',
        prompt: 'Compare average and peak behavior for `' + primaryLabel +
          '` over time and explain when AVG is better than MAX and vice versa.',
      });
    } else if (primary.role === 'energy') {
      actions.push({
        label: 'Build Energy Trend',
        prompt: 'Create a trend chart of total `' + primaryLabel +
          '` over time using an appropriate time grain and explain what stands out.',
      });
    }
  }

  const html =
    '<div class="starter-overview">' +
      '<div class="starter-overview-head">' +
        '<span class="starter-overview-kicker">Starter result</span>' +
        '<h3>Key measures</h3>' +
        '<p>An energy-aware pass over likely measures, their default aggregations, and the metrics that need extra care.</p>' +
      '</div>' +
      '<div class="overview-metrics">' +
        renderOverviewMetric('Tables', Number(overview.table_count || 0).toLocaleString()) +
        renderOverviewMetric('Measures', Number(measures.length || 0).toLocaleString()) +
        renderOverviewMetric('Guardrails', Number(measures.filter(item => (item.forbidden_aggregations || []).length).length || 0).toLocaleString()) +
      '</div>' +
      renderOverviewList(
        'Measure candidates',
        measures,
        'No obvious numeric measures detected.',
        item => {
          const additive = [];
          if (item.additive_across_category) additive.push('category');
          if (item.additive_across_time) additive.push('time');
          const unit = item.unit ? 'Unit: ' + item.unit : 'No inferred unit';
          const additiveText = additive.length ? 'Additive across ' + additive.join(' + ') : 'Not safely additive';
          const averagingText = item.weight_column
            ? ('Weighted avg by ' + item.weight_column)
            : ('Averaging: ' + (item.average_strategy || 'avg'));
          const displayNameText = item.display_name ? ('Display: ' + item.display_name + ' • ') : '';
          const formatText = item.format ? ('Format: ' + item.format + ' • ') : '';
          const chartText = (item.preferred_chart_types || []).length
            ? ('Charts: ' + item.preferred_chart_types.join('/') + ' • ')
            : '';
          return '<div class="overview-item overview-item-rich"><strong>' +
            escapeHtml(item.table + '.' + item.column) + '</strong><span>' +
            escapeHtml(item.role + ' • default ' + item.default_aggregation.toUpperCase()) + '</span><span>' +
            escapeHtml(displayNameText + formatText + chartText + unit + ' • ' + additiveText + ' • ' + averagingText) + '</span>' +
            '<button type="button" class="measure-override-btn" ' +
              'data-table="' + escapeHtml(item.table) + '" ' +
              'data-column="' + escapeHtml(item.column) + '" ' +
              'data-default-aggregation="' + escapeHtml(item.default_aggregation || 'avg') + '" ' +
              'data-average-strategy="' + escapeHtml(item.average_strategy || 'avg') + '" ' +
              'data-weight-column="' + escapeHtml(item.weight_column || '') + '" ' +
              'data-display-name="' + escapeHtml(item.display_name || '') + '" ' +
              'data-format="' + escapeHtml(item.format || '') + '" ' +
              'data-preferred-chart-types="' + escapeHtml((item.preferred_chart_types || []).join(',')) + '">' +
              'Edit override</button></div>';
        }
      ) +
      renderOverviewList(
        'Suggested SQL rollups',
        measures,
        'No rollup formulas available.',
        item => '<div class="overview-item overview-item-rich"><strong>' +
          escapeHtml(item.table + '.' + item.column) + '</strong><span>' +
          escapeHtml(item.recommended_rollup_sql || '') + '</span></div>'
      ) +
      renderOverviewList(
        'Aggregation guidance',
        measures,
        'No aggregation guidance available.',
        item => '<div class="overview-item overview-item-rich"><strong>' +
          escapeHtml(item.table + '.' + item.column) + '</strong><span>' +
          escapeHtml('Allowed: ' + (item.allowed_aggregations || []).join(', ')) + '</span><span>' +
          escapeHtml((item.forbidden_aggregations || []).length
            ? ('Avoid: ' + item.forbidden_aggregations.join(', ') +
              (item.weight_column ? (' • weighted by ' + item.weight_column) : '') +
              ' • ' + item.reason)
            : (item.reason || '')) + '</span></div>'
      ) +
      (notes.length ? renderOverviewList(
        'Quick notes',
        notes,
        '',
        item => '<div class="overview-item overview-item-note"><span>' + escapeHtml(item) + '</span></div>'
      ) : '') +
      renderStarterActions(actions) +
    '</div>';

  addAssistantHtml(html, 'starter-overview-bubble');
}

async function runInspectOverview(kind, scope) {
  if (isStreaming) return;
  if (welcomeEl) welcomeEl.style.display = 'none';

  const scopeTable = scope === 'table' ? selectedTable : null;
  if (scope === 'table' && !scopeTable) {
    showToast('Select a table in the sidebar first.', 'error');
    addMessage('assistant', 'Select a table in the sidebar first, then run the table-scoped inspect action again.');
    return;
  }

  let endpoint = '/api/dataset-overview';
  let failureMessage = 'Failed to build the dataset overview.';
  let toastMessage = 'Failed to profile the dataset.';

  if (kind === 'dimensions') {
    endpoint = '/api/dimension-overview';
    failureMessage = 'Failed to identify key dimensions.';
    toastMessage = 'Failed to analyze grouping dimensions.';
  } else if (kind === 'measures') {
    endpoint = '/api/measure-overview';
    failureMessage = 'Failed to identify key measures.';
    toastMessage = 'Failed to analyze likely measures.';
  } else if (kind === 'quality') {
    endpoint = '/api/quality-overview';
    failureMessage = 'Failed to run the data quality audit.';
    toastMessage = 'Failed to run the data quality inspection.';
  } else if (kind === 'trend') {
    endpoint = '/api/trend-overview';
    failureMessage = 'Failed to identify trend-chart ideas.';
    toastMessage = 'Failed to run the trend inspection.';
  }

  const typingEl = addTypingIndicator();
  try {
    const data = await fetchJson(scopeTable ? (endpoint + '?table=' + encodeURIComponent(scopeTable)) : endpoint);
    typingEl.remove();
    if (data.error || !data.overview) {
      addMessage('assistant', (data.error || failureMessage) + ' Load data and try again.');
      return;
    }
    if (kind === 'profile') renderDatasetOverview(data.overview);
    else if (kind === 'measures') renderMeasureOverview(data.overview);
    else if (kind === 'dimensions') renderDimensionOverview(data.overview);
    else if (kind === 'quality') renderQualityOverview(data.overview);
    else renderTrendOverview(data.overview);
  } catch (e) {
    if (document.contains(typingEl)) typingEl.remove();
    addMessage('assistant', failureMessage + ' Please try again.');
    showToast(toastMessage, 'error');
  }
}

async function runDatasetProfileStarter() {
  await runInspectOverview('profile', 'dataset');
}

async function runMeasureStarter() {
  await runInspectOverview('measures', 'dataset');
}

async function runDimensionStarter() {
  await runInspectOverview('dimensions', 'dataset');
}

async function runQualityStarter() {
  await runInspectOverview('quality', 'dataset');
}

async function runTrendStarter() {
  await runInspectOverview('trend', 'dataset');
}

async function runInspectAction(kind, scope) {
  switchView('chat');
  await runInspectOverview(kind, scope);
}

async function runStarterAction(starterId) {
  const config = STARTER_CONFIG[starterId] || STARTER_CONFIG.profile;
  pendingStarterAction = null;

  if (starterId === 'profile') {
    await runDatasetProfileStarter();
    return;
  }

  if (starterId === 'measures') {
    await runMeasureStarter();
    return;
  }

  if (starterId === 'dimensions') {
    await runDimensionStarter();
    return;
  }

  if (starterId === 'quality') {
    await runQualityStarter();
    return;
  }

  if (starterId === 'trend') {
    await runTrendStarter();
    return;
  }

  if (config.prompt) {
    sendMessage(config.prompt);
  }
}

async function maybeRunPendingStarter() {
  if (!pendingStarterAction) return false;
  const starterId = pendingStarterAction;
  await runStarterAction(starterId);
  return true;
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
      showToast('Summary failed — try again or check your API key.', 'error');
    }
  }

  isStreaming = false;
  currentAbortController = null;
  if (btn) btn.disabled = false;
  sendBtn.style.display = '';
  stopBtn.style.display = 'none';
  inputEl.focus();
}

