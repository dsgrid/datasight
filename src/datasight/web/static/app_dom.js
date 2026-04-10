(function (global) {
  function sanitizeHtml(html) {
    return global.DOMPurify.sanitize(html, {
      ADD_TAGS: ['iframe'],
      ADD_ATTR: ['srcdoc', 'sandbox', 'allowfullscreen'],
    });
  }

  function sanitizeMarkdown(text) {
    return global.DOMPurify.sanitize(global.marked.parse(text));
  }

  function renderMarkdownInto(el, text) {
    el.innerHTML = sanitizeMarkdown(text);
    el.querySelectorAll('pre code').forEach(block => {
      global.hljs.highlightElement(block);
    });
    addCopyButtons(el);
  }

  function bindInteractiveBubbleActions(container) {
    if (!container) return;
    container.querySelectorAll('.starter-action-btn[data-prompt]').forEach(btn => {
      btn.addEventListener('click', event => {
        event.preventDefault();
        event.stopPropagation();
        runStarterFollowup(btn.getAttribute('data-prompt') || '');
      });
    });
    container.querySelectorAll('.measure-override-btn[data-table][data-column]').forEach(btn => {
      btn.addEventListener('click', async event => {
        event.preventDefault();
        event.stopPropagation();
        await openMeasureOverrideEditorForCard({
          table: btn.getAttribute('data-table') || '',
          column: btn.getAttribute('data-column') || '',
          defaultAggregation: btn.getAttribute('data-default-aggregation') || 'avg',
          averageStrategy: btn.getAttribute('data-average-strategy') || 'avg',
          weightColumn: btn.getAttribute('data-weight-column') || '',
          displayName: btn.getAttribute('data-display-name') || '',
          format: btn.getAttribute('data-format') || '',
          preferredChartTypes: (btn.getAttribute('data-preferred-chart-types') || '').split(',').filter(Boolean),
        });
      });
    });
  }

  function bindSidebarListActions() {
    const recipesList = document.getElementById('recipes-list');
    if (recipesList && !recipesList.dataset.bound) {
      recipesList.dataset.bound = 'true';
      recipesList.addEventListener('click', event => {
        const item = event.target.closest('.query-item[data-prompt]');
        if (!item) return;
        event.preventDefault();
        useRecipeElement(item);
      });
    }

    const queriesList = document.getElementById('queries-list');
    if (queriesList && !queriesList.dataset.bound) {
      queriesList.dataset.bound = 'true';
      queriesList.addEventListener('click', event => {
        const item = event.target.closest('.query-item[data-question]');
        if (!item) return;
        event.preventDefault();
        useQuery(item);
      });
    }
  }

  function addAssistantHtml(html, extraClass) {
    const row = document.createElement('div');
    row.className = 'message-row assistant';
    const del = _makeDeleteBtn('Delete response');
    del.className = 'msg-delete-btn msg-delete-single';
    del.onclick = event => {
      event.stopPropagation();
      deleteElement(row);
    };
    row.appendChild(del);

    const bubble = document.createElement('div');
    bubble.className = 'message-bubble' + (extraClass ? ' ' + extraClass : '');
    bubble.innerHTML = sanitizeHtml(html);
    bindInteractiveBubbleActions(bubble);
    row.appendChild(bubble);
    messagesEl.appendChild(row);
    scrollToBottom();
    return bubble;
  }

  async function fetchJson(url, opts) {
    const resp = await fetch(url, opts);
    if (!resp.ok) throw new Error('API error: ' + resp.status);
    return resp.json();
  }

  function clearSchemaInsightCaches() {
    tablePreviewCache = new Map();
    columnStatsCache = new Map();
  }

  function clearSchemaSearchState() {
    schemaSearchQuery = '';
    selectedTable = null;
    const searchInput = document.getElementById('schema-search-input');
    if (searchInput) searchInput.value = '';
  }

  function clearSavedItemCaches() {
    bookmarksCache = [];
    reportsCache = [];
    conversationsCache = [];
    recipesCache = [];
  }

  function bindClick(selector, handler) {
    document.querySelectorAll(selector).forEach(el => {
      el.addEventListener('click', handler);
    });
  }

  function bindChange(selector, handler) {
    document.querySelectorAll(selector).forEach(el => {
      el.addEventListener('change', handler);
    });
  }

  function bindInput(selector, handler) {
    document.querySelectorAll(selector).forEach(el => {
      el.addEventListener('input', handler);
    });
  }

  function bindEnter(selector, handler) {
    document.querySelectorAll(selector).forEach(el => {
      el.addEventListener('keydown', event => {
        if (event.key !== 'Enter') return;
        event.preventDefault();
        handler(event);
      });
    });
  }

  global.sanitizeHtml = sanitizeHtml;
  global.sanitizeMarkdown = sanitizeMarkdown;
  global.renderMarkdownInto = renderMarkdownInto;
  global.bindInteractiveBubbleActions = bindInteractiveBubbleActions;
  global.bindSidebarListActions = bindSidebarListActions;
  global.addAssistantHtml = addAssistantHtml;
  global.fetchJson = fetchJson;
  global.clearSchemaInsightCaches = clearSchemaInsightCaches;
  global.clearSchemaSearchState = clearSchemaSearchState;
  global.clearSavedItemCaches = clearSavedItemCaches;
  global.DatasightDom = {
    bindClick,
    bindChange,
    bindInput,
    bindEnter,
  };
})(window);
