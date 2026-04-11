/** Query history and cost tracking. */

export interface QueryEntry {
  tool: string;
  sql: string;
  timestamp: string;
  execution_time_ms?: number;
  row_count?: number;
  column_count?: number;
  error?: string;
  turn_cost?: number;
}

function createQueriesStore() {
  let sessionQueries = $state<QueryEntry[]>([]);
  let sessionTotalCost = $state(0);

  return {
    get sessionQueries() {
      return sessionQueries;
    },
    set sessionQueries(v: QueryEntry[]) {
      sessionQueries = v;
    },
    get sessionTotalCost() {
      return sessionTotalCost;
    },
    set sessionTotalCost(v: number) {
      sessionTotalCost = v;
    },

    addQuery(entry: QueryEntry) {
      // Most recent first
      sessionQueries = [entry, ...sessionQueries];
    },

    addCost(cost: number) {
      sessionTotalCost += cost;
      // Stamp cost on most recent query
      if (sessionQueries.length > 0) {
        sessionQueries = sessionQueries.map((q, i) =>
          i === 0 ? { ...q, turn_cost: cost } : q,
        );
      }
    },

    clear() {
      sessionQueries = [];
      sessionTotalCost = 0;
    },
  };
}

export const queriesStore = createQueriesStore();
