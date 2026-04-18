/** Dashboard state — pinned items, layout, fullscreen. */

export type DashboardItemType = "chart" | "table" | "note" | "section";
export type ViewMode = "chat" | "dashboard";

export type DashboardFilterScope =
  | { type: "all" }
  | { type: "cards"; cardIds: number[] };

export interface DashboardFilter {
  id: number;
  column: string;
  operator: "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "contains" | "in";
  value: unknown;
  scope: DashboardFilterScope;
  enabled?: boolean;
}

export type CardFilterStatus = "applied" | "not_applicable" | "excluded_by_scope";

export function getCardColumns(item: DashboardItem): string[] {
  const columns = item.source_meta?.meta?.columns;
  if (!Array.isArray(columns)) return [];
  return columns.filter(
    (column): column is string => typeof column === "string" && column.length > 0,
  );
}

export function isFilterableCard(item: DashboardItem): boolean {
  return Boolean(item.sql) && (item.type === "chart" || item.type === "table");
}

export function getCardFilterStatus(
  item: DashboardItem,
  filter: DashboardFilter,
): CardFilterStatus {
  if (filter.scope.type === "cards" && !filter.scope.cardIds.includes(item.id)) {
    return "excluded_by_scope";
  }
  const columns = getCardColumns(item);
  // If columns metadata is missing (fresh pin, never rerun), trust the filter
  // and let the server decide — matches prior behavior.
  if (columns.length === 0) return "applied";
  if (!columns.includes(filter.column)) return "not_applicable";
  return "applied";
}

export function filtersForCard(
  item: DashboardItem,
  filters: DashboardFilter[],
): DashboardFilter[] {
  return filters.filter(
    (f) => f.enabled !== false && getCardFilterStatus(item, f) === "applied",
  );
}

export interface DashboardItem {
  id: number;
  type: DashboardItemType;
  html?: string;
  title?: string;
  render_plotly_spec?: unknown;
  plotly_spec?: unknown;
  sql?: string;
  tool?: string;
  markdown?: string;
  source_meta?: {
    question: string;
    resultType: string;
    meta?: Record<string, unknown>;
  };
}

export function getAllCardColumns(items: DashboardItem[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const item of items) {
    if (!isFilterableCard(item)) continue;
    for (const col of getCardColumns(item)) {
      if (!seen.has(col)) {
        seen.add(col);
        result.push(col);
      }
    }
  }
  return result;
}

export function getValidDashboardFilters(
  filters: DashboardFilter[],
  columns: string[],
): DashboardFilter[] {
  const columnSet = new Set(columns);
  return filters.filter((filter) => columnSet.has(filter.column));
}

function createDashboardStore() {
  let currentView = $state<ViewMode>("chat");
  let pinnedItems = $state<DashboardItem[]>([]);
  let pinnedIdCounter = $state(0);
  let fullscreenCardId = $state<number | null>(null);
  let selectedCardIdx = $state(-1);
  let columns = $state(0); // 0 = auto
  let filters = $state<DashboardFilter[]>([]);
  let filterIdCounter = $state(0);

  return {
    get currentView() {
      return currentView;
    },
    set currentView(v: ViewMode) {
      currentView = v;
    },
    get pinnedItems() {
      return pinnedItems;
    },
    set pinnedItems(v: DashboardItem[]) {
      pinnedItems = v;
    },
    get pinnedIdCounter() {
      return pinnedIdCounter;
    },
    set pinnedIdCounter(v: number) {
      pinnedIdCounter = v;
    },
    get fullscreenCardId() {
      return fullscreenCardId;
    },
    set fullscreenCardId(v: number | null) {
      fullscreenCardId = v;
    },
    get selectedCardIdx() {
      return selectedCardIdx;
    },
    set selectedCardIdx(v: number) {
      selectedCardIdx = v;
    },
    get columns() {
      return columns;
    },
    set columns(v: number) {
      columns = v;
    },
    get filters() {
      return filters;
    },
    set filters(v: DashboardFilter[]) {
      filters = v;
      filterIdCounter = v.reduce((max, item) => Math.max(max, item.id), 0);
    },

    nextId(): number {
      return ++pinnedIdCounter;
    },

    addItem(item: Omit<DashboardItem, "id">) {
      const newItem = { ...item, id: ++pinnedIdCounter };
      pinnedItems = [...pinnedItems, newItem];
      return newItem;
    },

    removeItem(id: number) {
      pinnedItems = pinnedItems.filter((item) => item.id !== id);
    },

    updateItem(id: number, updates: Partial<DashboardItem>) {
      pinnedItems = pinnedItems.map((item) =>
        item.id === id ? { ...item, ...updates } : item,
      );
    },

    reorder(fromIndex: number, toIndex: number) {
      const items = [...pinnedItems];
      const [moved] = items.splice(fromIndex, 1);
      items.splice(toIndex, 0, moved);
      pinnedItems = items;
    },

    addFilter(filter: Omit<DashboardFilter, "id">) {
      const newFilter = { enabled: true, ...filter, id: ++filterIdCounter };
      filters = [
        ...filters.filter((item) => item.column !== newFilter.column),
        newFilter,
      ];
      return newFilter;
    },

    updateFilter(id: number, updates: Partial<Omit<DashboardFilter, "id">>) {
      filters = filters.map((item) =>
        item.id === id ? { ...item, ...updates } : item,
      );
    },

    removeFilter(id: number) {
      filters = filters.filter((item) => item.id !== id);
    },

    clearFilters() {
      filters = [];
      filterIdCounter = 0;
    },

    clear() {
      pinnedItems = [];
      pinnedIdCounter = 0;
      fullscreenCardId = null;
      selectedCardIdx = -1;
      columns = 0;
      filters = [];
      filterIdCounter = 0;
    },
  };
}

export const dashboardStore = createDashboardStore();
