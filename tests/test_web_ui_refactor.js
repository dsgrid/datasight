const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function loadScript(relativePath, context) {
  const filePath = path.join(__dirname, '..', relativePath);
  const source = fs.readFileSync(filePath, 'utf8');
  vm.runInContext(source, context, { filename: filePath });
}

function createFakeElement(attributes = {}) {
  const listeners = {};
  return {
    attributes: { ...attributes },
    dataset: {},
    value: '',
    style: {},
    addEventListener(type, handler) {
      listeners[type] = listeners[type] || [];
      listeners[type].push(handler);
    },
    dispatch(type, extra = {}) {
      const event = {
        currentTarget: this,
        target: this,
        defaultPrevented: false,
        propagationStopped: false,
        preventDefault() {
          this.defaultPrevented = true;
        },
        stopPropagation() {
          this.propagationStopped = true;
        },
        ...extra,
      };
      (listeners[type] || []).forEach(handler => handler(event));
      return event;
    },
    getAttribute(name) {
      return this.attributes[name] ?? null;
    },
    setAttribute(name, value) {
      this.attributes[name] = value;
    },
  };
}

function createContext() {
  const elementsById = new Map();
  const selectorMap = new Map();
  const documentListeners = {};

  const document = {
    body: createFakeElement(),
    getElementById(id) {
      return elementsById.get(id) || null;
    },
    querySelectorAll(selector) {
      return selectorMap.get(selector) || [];
    },
    addEventListener(type, handler) {
      documentListeners[type] = documentListeners[type] || [];
      documentListeners[type].push(handler);
    },
    dispatch(type, extra = {}) {
      const event = {
        target: extra.target || null,
        currentTarget: document,
        preventDefault() {},
        stopPropagation() {},
        ...extra,
      };
      (documentListeners[type] || []).forEach(handler => handler(event));
      return event;
    },
  };

  const context = {
    console,
    document,
    window: null,
    fetch: async () => ({ ok: true, json: async () => ({}) }),
    DOMPurify: { sanitize(value) { return value; } },
    marked: {
      parse(value) { return value; },
      setOptions() {},
    },
    hljs: {
      getLanguage() { return true; },
      highlight(code) { return { value: code }; },
      highlightAuto(code) { return { value: code }; },
      highlightElement() {},
    },
    localStorage: {
      values: new Map(),
      getItem(key) {
        return this.values.has(key) ? this.values.get(key) : null;
      },
      setItem(key, value) {
        this.values.set(key, String(value));
      },
    },
    crypto: {
      randomUUID() {
        return 'test-session-id';
      },
    },
  };
  context.window = context;
  vm.createContext(context);
  context.__registerId = (id, element) => elementsById.set(id, element);
  context.__registerSelector = (selector, elements) => selectorMap.set(selector, elements);
  return context;
}

test('app_state exposes a single store-backed state surface', () => {
  const context = createContext();
  ['messages', 'user-input', 'send-btn', 'stop-btn', 'welcome', 'sidebar'].forEach(id => {
    context.__registerId(id, createFakeElement());
  });

  loadScript('src/datasight/web/static/app_state.js', context);

  assert.equal(context.DatasightStore.state.sessionId, 'test-session-id');
  assert.equal(context.currentView, 'chat');

  context.currentView = 'dashboard';
  context.sessionTotalCost = 12.5;

  assert.equal(context.DatasightStore.state.currentView, 'dashboard');
  assert.equal(context.DatasightStore.state.sessionTotalCost, 12.5);
  assert.equal(context.localStorage.getItem('datasight-session'), 'test-session-id');
});

test('bindAppEvents routes critical UI events through centralized listeners', () => {
  const context = createContext();
  ['messages', 'user-input', 'send-btn', 'stop-btn', 'welcome', 'sidebar', 'chat-form'].forEach(id => {
    context.__registerId(id, createFakeElement());
  });

  const sidebarToggle = createFakeElement();
  const landingStarter = createFakeElement({ 'data-starter-id': 'profile' });
  const chatForm = context.document.getElementById('chat-form');

  context.__registerSelector('#sidebar-toggle', [sidebarToggle]);
  context.__registerSelector('.landing-starter[data-starter-id]', [landingStarter]);

  let toggled = 0;
  let submitted = 0;
  let chosenStarter = null;

  context.toggleSidebar = () => { toggled += 1; };
  context.handleSubmit = event => {
    submitted += 1;
    event.preventDefault();
  };
  context.landingChooseStarter = starterId => {
    chosenStarter = starterId;
  };

  loadScript('src/datasight/web/static/app_state.js', context);
  loadScript('src/datasight/web/static/app_dom.js', context);
  loadScript('src/datasight/web/static/app_events.js', context);

  context.bindAppEvents();

  sidebarToggle.dispatch('click');
  landingStarter.dispatch('click');
  const submitEvent = chatForm.dispatch('submit');

  assert.equal(toggled, 1);
  assert.equal(submitted, 1);
  assert.equal(chosenStarter, 'profile');
  assert.equal(submitEvent.defaultPrevented, true);
  assert.equal(context.document.body.dataset.appEventsBound, 'true');
});

test('index template has no inline UI handlers and loads the refactor scripts', () => {
  const templatePath = path.join(__dirname, '..', 'src/datasight/web/templates/index.html');
  const template = fs.readFileSync(templatePath, 'utf8');

  assert.equal(/on(click|input|change|keydown|submit)=/.test(template), false);
  assert.match(template, /\/static\/app_state\.js\?v=1/);
  assert.match(template, /\/static\/app_dom\.js\?v=1/);
  assert.match(template, /\/static\/app_projects\.js\?v=1/);
  assert.match(template, /\/static\/app_dashboard\.js\?v=1/);
  assert.match(template, /\/static\/app_settings\.js\?v=1/);
  assert.match(template, /\/static\/app_query_history\.js\?v=1/);
  assert.match(template, /\/static\/app_schema_inspect\.js\?v=1/);
  assert.match(template, /\/static\/app_palette\.js\?v=1/);
  assert.match(template, /\/static\/app_chat_input\.js\?v=1/);
  assert.match(template, /\/static\/app_starters\.js\?v=1/);
  assert.match(template, /\/static\/app_stream\.js\?v=1/);
  assert.match(template, /\/static\/app_message_ui\.js\?v=1/);
  assert.match(template, /\/static\/app_tables\.js\?v=1/);
  assert.match(template, /\/static\/app_theme\.js\?v=1/);
  assert.match(template, /\/static\/app_saved\.js\?v=1/);
  assert.match(template, /\/static\/app_shell\.js\?v=1/);
  assert.match(template, /\/static\/app_landing\.js\?v=1/);
  assert.match(template, /\/static\/app_events\.js\?v=1/);
  assert.match(template, /\/static\/app_bootstrap\.js\?v=1/);
});

test('frontend scripts avoid inline html handlers in both templates and generated html helpers', () => {
  const projectsPath = path.join(__dirname, '..', 'src/datasight/web/static/app_projects.js');
  const dashboardPath = path.join(__dirname, '..', 'src/datasight/web/static/app_dashboard.js');
  const settingsPath = path.join(__dirname, '..', 'src/datasight/web/static/app_settings.js');
  const queryHistoryPath = path.join(__dirname, '..', 'src/datasight/web/static/app_query_history.js');
  const schemaInspectPath = path.join(__dirname, '..', 'src/datasight/web/static/app_schema_inspect.js');
  const palettePath = path.join(__dirname, '..', 'src/datasight/web/static/app_palette.js');
  const chatInputPath = path.join(__dirname, '..', 'src/datasight/web/static/app_chat_input.js');
  const startersPath = path.join(__dirname, '..', 'src/datasight/web/static/app_starters.js');
  const streamPath = path.join(__dirname, '..', 'src/datasight/web/static/app_stream.js');
  const messageUiPath = path.join(__dirname, '..', 'src/datasight/web/static/app_message_ui.js');
  const tablesPath = path.join(__dirname, '..', 'src/datasight/web/static/app_tables.js');
  const themePath = path.join(__dirname, '..', 'src/datasight/web/static/app_theme.js');
  const savedPath = path.join(__dirname, '..', 'src/datasight/web/static/app_saved.js');
  const shellPath = path.join(__dirname, '..', 'src/datasight/web/static/app_shell.js');
  const landingPath = path.join(__dirname, '..', 'src/datasight/web/static/app_landing.js');

  const projectsSource = fs.readFileSync(projectsPath, 'utf8');
  const dashboardSource = fs.readFileSync(dashboardPath, 'utf8');
  const settingsSource = fs.readFileSync(settingsPath, 'utf8');
  const queryHistorySource = fs.readFileSync(queryHistoryPath, 'utf8');
  const schemaInspectSource = fs.readFileSync(schemaInspectPath, 'utf8');
  const paletteSource = fs.readFileSync(palettePath, 'utf8');
  const chatInputSource = fs.readFileSync(chatInputPath, 'utf8');
  const startersSource = fs.readFileSync(startersPath, 'utf8');
  const streamSource = fs.readFileSync(streamPath, 'utf8');
  const messageUiSource = fs.readFileSync(messageUiPath, 'utf8');
  const tablesSource = fs.readFileSync(tablesPath, 'utf8');
  const themeSource = fs.readFileSync(themePath, 'utf8');
  const savedSource = fs.readFileSync(savedPath, 'utf8');
  const shellSource = fs.readFileSync(shellPath, 'utf8');
  const landingSource = fs.readFileSync(landingPath, 'utf8');

  assert.equal(/onclick=/.test(projectsSource), false);
  assert.equal(/onclick=/.test(dashboardSource), false);
  assert.equal(/onclick=/.test(settingsSource), false);
  assert.equal(/onclick=/.test(queryHistorySource), false);
  assert.equal(/onclick=/.test(schemaInspectSource), false);
  assert.equal(/onclick=/.test(paletteSource), false);
  assert.equal(/onclick=/.test(chatInputSource), false);
  assert.equal(/onclick=/.test(startersSource), false);
  assert.equal(/onclick=/.test(streamSource), false);
  assert.equal(/onclick=/.test(messageUiSource), false);
  assert.equal(/onclick=/.test(tablesSource), false);
  assert.equal(/onclick=/.test(themeSource), false);
  assert.equal(/onclick=/.test(savedSource), false);
  assert.equal(/onclick=/.test(shellSource), false);
  assert.equal(/onclick=/.test(landingSource), false);
  assert.match(projectsSource, /function toggleProjectsPanel/);
  assert.match(dashboardSource, /function renderDashboard/);
  assert.match(settingsSource, /^async function loadSettings\(/);
  assert.match(queryHistorySource, /function renderQueryHistory\(/);
  assert.match(schemaInspectSource, /async function loadSchema\(/);
  assert.match(paletteSource, /function getCommandPaletteResults\(/);
  assert.match(chatInputSource, /function handleSubmit\(/);
  assert.match(startersSource, /function landingChooseStarter\(/);
  assert.match(streamSource, /async function sendMessage\(/);
  assert.match(messageUiSource, /const TRASH_SVG/);
  assert.match(savedSource, /async function loadConversations\(/);
  assert.match(landingSource, /async function initLanding\(/);
});
