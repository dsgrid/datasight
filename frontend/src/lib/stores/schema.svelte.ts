/** Schema data, search, and caches. */

export interface ColumnInfo {
  name: string;
  dtype: string;
}

export interface TableInfo {
  name: string;
  columns: ColumnInfo[];
  row_count?: number;
  is_view?: boolean;
}

export interface ExampleQuery {
  question: string;
  sql: string;
  tags?: string[];
}

export interface Recipe {
  title: string;
  prompt: string;
  category?: string;
}

export interface PreviewData {
  html: string | null;
  cached: boolean;
  error?: string;
}

export interface ColumnStats {
  distinct: number;
  nulls: number;
  min: unknown;
  max: unknown;
  avg?: number;
}

function createSchemaStore() {
  let schemaData = $state<TableInfo[]>([]);
  let selectedTable = $state<string | null>(null);
  let searchQuery = $state("");
  let allQueries = $state<ExampleQuery[]>([]);
  let recipesCache = $state<Recipe[]>([]);
  let tablePreviewCache = $state(new Map<string, PreviewData>());
  let columnStatsCache = $state(new Map<string, ColumnStats>());

  return {
    get schemaData() {
      return schemaData;
    },
    set schemaData(v: TableInfo[]) {
      schemaData = v;
    },
    get selectedTable() {
      return selectedTable;
    },
    set selectedTable(v: string | null) {
      selectedTable = v;
    },
    get searchQuery() {
      return searchQuery;
    },
    set searchQuery(v: string) {
      searchQuery = v;
    },
    get allQueries() {
      return allQueries;
    },
    set allQueries(v: ExampleQuery[]) {
      allQueries = v;
    },
    get recipesCache() {
      return recipesCache;
    },
    set recipesCache(v: Recipe[]) {
      recipesCache = v;
    },
    get tablePreviewCache() {
      return tablePreviewCache;
    },
    get columnStatsCache() {
      return columnStatsCache;
    },

    setPreview(table: string, data: PreviewData) {
      tablePreviewCache = new Map(tablePreviewCache).set(table, data);
    },

    setColumnStats(key: string, stats: ColumnStats) {
      columnStatsCache = new Map(columnStatsCache).set(key, stats);
    },

    clear() {
      schemaData = [];
      selectedTable = null;
      searchQuery = "";
      allQueries = [];
      recipesCache = [];
      tablePreviewCache = new Map();
      columnStatsCache = new Map();
    },
  };
}

export const schemaStore = createSchemaStore();
