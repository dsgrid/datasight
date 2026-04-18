/** SQL editor page state — persists across view switches within a session. */

const STORAGE_KEY = "datasight-sql-editor-text";

function createSqlEditorStore() {
  let sql = $state(localStorage.getItem(STORAGE_KEY) ?? "");
  let running = $state(false);
  let resultHtml = $state<string | null>(null);
  let rowCount = $state(0);
  let elapsedMs = $state(0);
  let error = $state<string | null>(null);
  let validationErrors = $state<string[]>([]);
  let pendingInsert = $state<string | null>(null);

  return {
    get sql() {
      return sql;
    },
    set sql(v: string) {
      sql = v;
      localStorage.setItem(STORAGE_KEY, v);
    },
    get running() {
      return running;
    },
    set running(v: boolean) {
      running = v;
    },
    get resultHtml() {
      return resultHtml;
    },
    set resultHtml(v: string | null) {
      resultHtml = v;
    },
    get rowCount() {
      return rowCount;
    },
    set rowCount(v: number) {
      rowCount = v;
    },
    get elapsedMs() {
      return elapsedMs;
    },
    set elapsedMs(v: number) {
      elapsedMs = v;
    },
    get error() {
      return error;
    },
    set error(v: string | null) {
      error = v;
    },
    get validationErrors() {
      return validationErrors;
    },
    set validationErrors(v: string[]) {
      validationErrors = v;
    },
    get pendingInsert() {
      return pendingInsert;
    },
    set pendingInsert(v: string | null) {
      pendingInsert = v;
    },

    clearResult() {
      resultHtml = null;
      rowCount = 0;
      elapsedMs = 0;
      error = null;
      validationErrors = [];
    },

    clearAll() {
      sql = "";
      localStorage.setItem(STORAGE_KEY, "");
      resultHtml = null;
      rowCount = 0;
      elapsedMs = 0;
      error = null;
      validationErrors = [];
    },
  };
}

export const sqlEditorStore = createSqlEditorStore();
