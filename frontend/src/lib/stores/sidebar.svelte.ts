/** Sidebar caches — bookmarks, reports, conversations, recent projects, measures. */

export interface Bookmark {
  id: string;
  sql: string;
  tool: string;
  name: string;
}

export interface Report {
  id: string;
  sql: string;
  tool: string;
  name: string;
  plotly_spec?: unknown;
}

export interface Conversation {
  session_id: string;
  title: string;
  message_count: number;
}

export interface RecentProject {
  path: string;
  name: string;
  last_used: string;
  is_current: boolean;
}

export interface MeasureCatalogEntry {
  table: string;
  name: string;
  dtype: string;
  aggregation: string;
  display_name?: string;
  format?: string;
  preferred_chart_types?: string[];
  is_override?: boolean;
  is_calculated?: boolean;
  expression?: string;
}

function createSidebarStore() {
  let bookmarksCache = $state<Bookmark[]>([]);
  let reportsCache = $state<Report[]>([]);
  let conversationsCache = $state<Conversation[]>([]);
  let recentProjectsCache = $state<RecentProject[]>([]);
  let measureEditorCatalog = $state<MeasureCatalogEntry[]>([]);
  let sidebarOpen = $state(true);
  let pendingStarterAction = $state("profile");

  return {
    get bookmarksCache() {
      return bookmarksCache;
    },
    set bookmarksCache(v: Bookmark[]) {
      bookmarksCache = v;
    },
    get reportsCache() {
      return reportsCache;
    },
    set reportsCache(v: Report[]) {
      reportsCache = v;
    },
    get conversationsCache() {
      return conversationsCache;
    },
    set conversationsCache(v: Conversation[]) {
      conversationsCache = v;
    },
    get recentProjectsCache() {
      return recentProjectsCache;
    },
    set recentProjectsCache(v: RecentProject[]) {
      recentProjectsCache = v;
    },
    get measureEditorCatalog() {
      return measureEditorCatalog;
    },
    set measureEditorCatalog(v: MeasureCatalogEntry[]) {
      measureEditorCatalog = v;
    },
    get sidebarOpen() {
      return sidebarOpen;
    },
    set sidebarOpen(v: boolean) {
      sidebarOpen = v;
    },
    get pendingStarterAction() {
      return pendingStarterAction;
    },
    set pendingStarterAction(v: string) {
      pendingStarterAction = v;
    },

    toggleSidebar() {
      sidebarOpen = !sidebarOpen;
    },

    clear() {
      bookmarksCache = [];
      reportsCache = [];
      conversationsCache = [];
      measureEditorCatalog = [];
    },
  };
}

export const sidebarStore = createSidebarStore();
