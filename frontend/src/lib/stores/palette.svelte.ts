/** Command palette state. */

export interface PaletteResult {
  type: string;
  group: string;
  title: string;
  subtitle?: string;
  score: number;
  run: () => void;
}

function createPaletteStore() {
  let open = $state(false);
  let query = $state("");
  let results = $state<PaletteResult[]>([]);
  let selectedIdx = $state(0);

  return {
    get open() {
      return open;
    },
    set open(v: boolean) {
      open = v;
      if (!v) {
        query = "";
        results = [];
        selectedIdx = 0;
      }
    },
    get query() {
      return query;
    },
    set query(v: string) {
      query = v;
      selectedIdx = 0;
    },
    get results() {
      return results;
    },
    set results(v: PaletteResult[]) {
      results = v;
    },
    get selectedIdx() {
      return selectedIdx;
    },
    set selectedIdx(v: number) {
      selectedIdx = v;
    },

    toggle() {
      open = !open;
      if (!open) {
        query = "";
        results = [];
        selectedIdx = 0;
      }
    },

    moveSelection(delta: number) {
      if (results.length === 0) return;
      selectedIdx = (selectedIdx + delta + results.length) % results.length;
    },

    executeSelected() {
      if (results.length > 0 && selectedIdx < results.length) {
        results[selectedIdx].run();
        open = false;
        query = "";
        results = [];
        selectedIdx = 0;
      }
    },
  };
}

export const paletteStore = createPaletteStore();
