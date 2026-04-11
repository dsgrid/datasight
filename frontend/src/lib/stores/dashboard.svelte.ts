/** Dashboard state — pinned items, layout, fullscreen. */

export type DashboardItemType = "chart" | "table" | "note" | "section";
export type ViewMode = "chat" | "dashboard";

export interface DashboardItem {
  id: number;
  type: DashboardItemType;
  html?: string;
  title?: string;
  plotly_spec?: unknown;
  markdown?: string;
  source_meta?: {
    question: string;
    resultType: string;
    meta?: Record<string, unknown>;
  };
}

function createDashboardStore() {
  let currentView = $state<ViewMode>("chat");
  let pinnedItems = $state<DashboardItem[]>([]);
  let pinnedIdCounter = $state(0);
  let fullscreenCardId = $state<number | null>(null);
  let selectedCardIdx = $state(-1);
  let columns = $state(0); // 0 = auto

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

    clear() {
      pinnedItems = [];
      pinnedIdCounter = 0;
      fullscreenCardId = null;
      selectedCardIdx = -1;
    },
  };
}

export const dashboardStore = createDashboardStore();
