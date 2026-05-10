/** Per-table Tidy review drawer state.
 *
 * The drawer is a per-table, per-session ceremony: open → load
 * proposals → user edits / skips unwanted ones → apply all → close.
 * State here is intentionally ephemeral; nothing persists to localStorage.
 *
 * Each proposal carries a stable `id` (its index) so per-card preview
 * results, edits, approval state, and apply errors map back without
 * risking duplicates if the LLM returns proposals with identical
 * target_object_names.
 */

import {
  applyTidy,
  detectTidy,
  previewTidy,
  proposeTidy,
  type TidyApplyResult,
  type TidyDisposition,
  type TidyDispositionMode,
  type TidyMaterializeMode,
  type TidyProposal,
} from "$lib/api/tidy";

export type ProposalStatus =
  | "pending"
  | "applying"
  | "applied"
  | "apply_error";

export interface ProposalEdits {
  target_object_name: string;
  value_column: string;
  id_columns: string[];
  include_nulls: boolean;
}

export interface ProposalState {
  id: string;
  proposal: TidyProposal;
  edits: ProposalEdits;
  /** Opt-out flag. Default false = the proposal is applied when the user
   * clicks Apply. The card surfaces a Skip toggle so unwanted proposals
   * (typically among LLM-derived alternatives) can be excluded without
   * an upfront approve step on the common case. */
  skipped: boolean;
  status: ProposalStatus;
  applyError: string | null;
  applyResult: TidyApplyResult | null;
  preview: {
    loading: boolean;
    html: string | null;
    rowCount: number;
    error: string | null;
    open: boolean;
  };
}

export type DrawerStatus =
  | "idle"
  | "loading_deterministic"
  | "loaded_deterministic"
  | "loading_llm"
  | "loaded_with_llm"
  | "error";

function makeId(table: string, index: number, source: string): string {
  return `${table}::${source}::${index}`;
}

function buildEdits(p: TidyProposal): ProposalEdits {
  return {
    target_object_name: p.target_object_name,
    value_column: p.value_column,
    id_columns: [...p.id_columns],
    // Falls back to false when the server payload omits it, matching the
    // backend default — structural NAs in wide tables are usually not
    // information you want to carry into the long form.
    include_nulls: p.include_nulls ?? false,
  };
}

function makeState(p: TidyProposal, id: string): ProposalState {
  return {
    id,
    proposal: p,
    edits: buildEdits(p),
    skipped: false,
    status: "pending",
    applyError: null,
    applyResult: null,
    preview: {
      loading: false,
      html: null,
      rowCount: 0,
      error: null,
      open: false,
    },
  };
}

function applyEdits(p: TidyProposal, edits: ProposalEdits): TidyProposal {
  return {
    ...p,
    target_object_name: edits.target_object_name,
    value_column: edits.value_column,
    id_columns: [...edits.id_columns],
    include_nulls: edits.include_nulls,
  };
}

function createTidyStore() {
  let open = $state(false);
  let table = $state<string | null>(null);
  let status = $state<DrawerStatus>("idle");
  let errorMessage = $state<string | null>(null);
  let parseWarnings = $state<string[]>([]);
  let proposals = $state<ProposalState[]>([]);
  let mode = $state<TidyMaterializeMode>("view");
  let dispositionMode = $state<TidyDispositionMode>("keep");
  let dispositionRenameTo = $state("");
  let sampleRows = $state(0);
  let abortController = $state<AbortController | null>(null);

  // The store keeps its own onApplied hook so callers (App.svelte) can
  // reload the schema sidebar without introducing a circular import
  // between tidyStore and schemaStore.
  let appliedHook: ((schemaInfo: unknown) => void) | null = null;

  function reset(): void {
    abortController?.abort();
    abortController = null;
    table = null;
    status = "idle";
    errorMessage = null;
    parseWarnings = [];
    proposals = [];
    mode = "view";
    dispositionMode = "keep";
    dispositionRenameTo = "";
    sampleRows = 0;
  }

  return {
    get open() {
      return open;
    },
    get table() {
      return table;
    },
    get status() {
      return status;
    },
    get errorMessage() {
      return errorMessage;
    },
    get parseWarnings() {
      return parseWarnings;
    },
    get proposals() {
      return proposals;
    },
    get mode() {
      return mode;
    },
    set mode(v: TidyMaterializeMode) {
      mode = v;
      // The CLI safety rule: rename/drop with view leaves a dangling view.
      // When the user flips back to view, force keep so the disposition
      // picker can't end up in an invalid combination.
      if (v === "view" && dispositionMode !== "keep") {
        dispositionMode = "keep";
        dispositionRenameTo = "";
      }
    },
    get dispositionMode() {
      return dispositionMode;
    },
    set dispositionMode(v: TidyDispositionMode) {
      dispositionMode = v;
      if (v !== "rename") dispositionRenameTo = "";
    },
    get dispositionRenameTo() {
      return dispositionRenameTo;
    },
    set dispositionRenameTo(v: string) {
      dispositionRenameTo = v;
    },
    get sampleRows() {
      return sampleRows;
    },
    set sampleRows(v: number) {
      sampleRows = Math.max(0, Math.floor(v));
    },
    /** How many proposals will run on the next Apply click — anything
     * not skipped and not already applied. Drives the footer label and
     * disables the Apply button when zero. */
    get pendingCount() {
      return proposals.filter((p) => !p.skipped && p.status !== "applied")
        .length;
    },

    setAppliedHook(fn: ((schemaInfo: unknown) => void) | null): void {
      appliedHook = fn;
    },

    /** Open the drawer and load deterministic proposals only.
     *
     * The LLM advisor is intentionally NOT run here — that's a paid call
     * and the user should opt in via :func:`runAgent`. Deterministic hits
     * are cheap and instant; showing them up front gives the user
     * something to look at while they decide whether to engage the agent.
     */
    async start(tableName: string): Promise<void> {
      reset();
      open = true;
      table = tableName;
      status = "loading_deterministic";

      try {
        const resp = await detectTidy(tableName);
        if (resp.error) {
          errorMessage = resp.error;
          status = "error";
          return;
        }
        proposals = resp.proposals.map((p, i) =>
          makeState(p, makeId(tableName, i, "deterministic")),
        );
        status = "loaded_deterministic";
      } catch (err) {
        errorMessage = (err as Error).message ?? "Failed to load proposals";
        status = "error";
      }
    },

    /** Engage the LLM advisor. Appends LLM-derived proposals to the list
     * and respects the current ``sampleRows`` setting. Idempotent in the
     * happy case — re-running just appends more proposals (LLM output is
     * non-deterministic, so the user can hit Run again to retry).
     */
    async runAgent(): Promise<void> {
      if (table === null) return;
      const tableName = table;
      status = "loading_llm";

      const controller = new AbortController();
      abortController = controller;

      try {
        await proposeTidy({
          table: tableName,
          sampleRows,
          signal: controller.signal,
          onLlmStarted: () => {
            status = "loading_llm";
          },
          onLlmProposals: (list, warnings) => {
            const baseLen = proposals.length;
            const llmStates = list.map((p, i) =>
              makeState(p, makeId(tableName, baseLen + i, "llm")),
            );
            proposals = [...proposals, ...llmStates];
            parseWarnings = warnings;
          },
          onLlmError: (msg) => {
            // Non-fatal — deterministic results stay; surface as a warning.
            parseWarnings = [...parseWarnings, `LLM advisor failed: ${msg}`];
          },
          onError: (msg) => {
            errorMessage = msg;
            status = "error";
          },
          onDone: () => {
            if (status !== "error") status = "loaded_with_llm";
          },
        });
      } catch (err) {
        if ((err as Error).name === "AbortError") {
          // Cancelled mid-flight: drop back to the post-detect state so
          // the user can run again without reopening the drawer.
          status = "loaded_deterministic";
          return;
        }
        errorMessage = (err as Error).message ?? "Failed to run agent";
        status = "error";
      }
    },

    cancel(): void {
      abortController?.abort();
      abortController = null;
      if (status === "loading_llm") {
        status = "loaded_deterministic";
      }
    },

    close(): void {
      reset();
      open = false;
    },

    toggleSkip(id: string): void {
      proposals = proposals.map((p) =>
        p.id === id ? { ...p, skipped: !p.skipped } : p,
      );
    },

    editProposal(id: string, edits: Partial<ProposalEdits>): void {
      proposals = proposals.map((p) =>
        p.id === id ? { ...p, edits: { ...p.edits, ...edits } } : p,
      );
    },

    togglePreviewOpen(id: string): void {
      proposals = proposals.map((p) =>
        p.id === id
          ? { ...p, preview: { ...p.preview, open: !p.preview.open } }
          : p,
      );
    },

    async loadPreview(id: string): Promise<void> {
      const target = proposals.find((p) => p.id === id);
      if (!target) return;
      const merged = applyEdits(target.proposal, target.edits);

      proposals = proposals.map((p) =>
        p.id === id
          ? {
              ...p,
              preview: {
                ...p.preview,
                loading: true,
                error: null,
                open: true,
              },
            }
          : p,
      );

      try {
        const resp = await previewTidy(merged);
        proposals = proposals.map((p) =>
          p.id === id
            ? {
                ...p,
                preview: {
                  loading: false,
                  html: resp.html,
                  rowCount: resp.row_count,
                  error: resp.error,
                  open: true,
                },
              }
            : p,
        );
      } catch (err) {
        const message = (err as Error).message ?? "Preview failed";
        proposals = proposals.map((p) =>
          p.id === id
            ? {
                ...p,
                preview: {
                  ...p.preview,
                  loading: false,
                  error: message,
                  open: true,
                },
              }
            : p,
        );
      }
    },

    /** Apply every non-skipped, non-already-applied proposal in order;
     * stop on first failure.
     *
     * Sequential, not parallel: each apply mutates the database and the
     * subsequent ones depend on the latest schema_info (e.g., a proposal
     * that targets `sales_long` would collide with the prior one).
     */
    async applyAll(): Promise<{ applied: number; failed: number }> {
      const queue = proposals
        .filter((p) => !p.skipped && p.status !== "applied")
        .map((p) => p.id);
      let applied = 0;
      let failed = 0;
      let lastSchema: unknown = null;

      const disposition: TidyDisposition = {
        mode: dispositionMode,
        new_name:
          dispositionMode === "rename" ? dispositionRenameTo.trim() : undefined,
      };

      for (const id of queue) {
        const target = proposals.find((p) => p.id === id);
        if (!target) continue;
        const merged = applyEdits(target.proposal, target.edits);

        proposals = proposals.map((p) =>
          p.id === id
            ? { ...p, status: "applying", applyError: null, applyResult: null }
            : p,
        );

        try {
          const resp = await applyTidy({
            proposal: merged,
            mode,
            disposition,
          });
          if (resp.success) {
            applied += 1;
            lastSchema = resp.schema_info ?? lastSchema;
            proposals = proposals.map((p) =>
              p.id === id
                ? {
                    ...p,
                    status: "applied",
                    applyResult: resp.result ?? null,
                  }
                : p,
            );
          } else {
            failed += 1;
            proposals = proposals.map((p) =>
              p.id === id
                ? {
                    ...p,
                    status: "apply_error",
                    applyError: resp.error ?? "Apply failed",
                  }
                : p,
            );
            break;
          }
        } catch (err) {
          failed += 1;
          const message = (err as Error).message ?? "Apply failed";
          proposals = proposals.map((p) =>
            p.id === id
              ? { ...p, status: "apply_error", applyError: message }
              : p,
          );
          break;
        }
      }

      if (lastSchema !== null && appliedHook) {
        appliedHook(lastSchema);
      }

      return { applied, failed };
    },
  };
}

export const tidyStore = createTidyStore();
