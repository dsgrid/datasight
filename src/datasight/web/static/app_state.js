(function (global) {
  function createStore(initialState) {
    const state = { ...initialState };
    return {
      state,
      get(key) {
        return state[key];
      },
      set(key, value) {
        state[key] = value;
        return value;
      },
      patch(values) {
        Object.assign(state, values);
        return state;
      },
    };
  }

  const initialState = {
    sessionId: global.localStorage.getItem('datasight-session') || global.crypto.randomUUID(),
    isStreaming: false,
    currentAssistantBubble: null,
    currentAssistantText: '',
    selectedTable: null,
    allQueries: [],
    schemaData: [],
    lastSql: '',
    lastPlotlySpec: null,
    lastToolName: '',
    lastToolMeta: null,
    confirmSqlEnabled: false,
    explainSqlEnabled: false,
    clarifySqlEnabled: false,
    showCostEnabled: true,
    sessionTotalCost: 0,
    pendingConfirmResolve: null,
    sessionQueries: [],
    pinnedItems: [],
    pinnedIdCounter: 0,
    currentView: 'chat',
    fullscreenCardId: null,
    selectedCardIdx: -1,
    currentAbortController: null,
    pendingStarterAction: 'profile',
    schemaSearchQuery: '',
    recentProjectsCache: [],
    commandPaletteOpen: false,
    commandPaletteResults: [],
    commandPaletteSelectedIdx: 0,
    tablePreviewCache: new Map(),
    columnStatsCache: new Map(),
    bookmarksCache: [],
    reportsCache: [],
    conversationsCache: [],
    recipesCache: [],
    measureEditorCatalog: [],
    currentProjectPath: null,
    projectLoaded: false,
    explorePaths: [],
    dashboardColumns: 0,
    llmConnected: false,
    isEphemeralSession: false,
    exportMode: false,
    exportExcludeIndices: new Set(),
    shortcutsModalOpen: false,
  };

  const store = createStore(initialState);
  global.DatasightStore = store;
  global.localStorage.setItem('datasight-session', store.get('sessionId'));

  const descriptors = {};
  Object.keys(initialState).forEach(key => {
    descriptors[key] = {
      configurable: true,
      enumerable: false,
      get() {
        return store.get(key);
      },
      set(value) {
        store.set(key, value);
      },
    };
  });
  Object.defineProperties(global, descriptors);

  const searchHelpers = global.DatasightSearchHelpers || {};
  global.scorePaletteResult = searchHelpers.scorePaletteResult;
  global.highlightMatch = searchHelpers.highlightMatch;
  global.getVisibleSchemaEntries = searchHelpers.getVisibleSchemaEntries;

  global.STARTER_CONFIG = {
    profile: {
      title: 'Profile this dataset',
      status: 'Open a file or project below to start with a structured overview.',
    },
    measures: {
      title: 'Inspect key measures',
      status: 'Open a file or project below to surface likely energy measures and safer default aggregations.',
    },
    dimensions: {
      title: 'Find key dimensions',
      status: 'Open a file or project below to surface the categories and breakdowns worth exploring.',
      prompt: 'Identify the most important dimensions, categories, and grouping columns in this dataset. Explain which tables and columns are the best starting points for analysis.',
    },
    trend: {
      title: 'Build a trend chart',
      status: 'Open a file or project below to start with the strongest time-series question in the data.',
      prompt: 'Find the most natural time-based analysis in this dataset and create a useful trend chart. Briefly explain which date column and measure you chose and why.',
    },
    quality: {
      title: 'Audit nulls and outliers',
      status: 'Open a file or project below to start with a data quality pass.',
      prompt: 'Audit this dataset for null-heavy columns, suspicious ranges, outliers, and other obvious data quality issues. Organize the results by severity and mention the tables and columns involved.',
    },
  };

  global.messagesEl = document.getElementById('messages');
  global.inputEl = document.getElementById('user-input');
  global.sendBtn = document.getElementById('send-btn');
  global.stopBtn = document.getElementById('stop-btn');
  global.welcomeEl = document.getElementById('welcome');
  global.sidebar = document.getElementById('sidebar');

  global.marked.setOptions({
    highlight(code, lang) {
      if (lang && global.hljs.getLanguage(lang)) {
        return global.hljs.highlight(code, { language: lang }).value;
      }
      return global.hljs.highlightAuto(code).value;
    },
    breaks: true,
    gfm: true,
  });
})(window);
