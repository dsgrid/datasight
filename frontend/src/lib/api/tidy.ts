/** API client for the per-table Tidy review drawer.
 *
 * - detectTidy: GET /api/tidy/detect, returns deterministic proposals.
 *   Cheap, schema-only, fires automatically when the drawer opens.
 * - proposeTidy: SSE stream to /api/tidy/propose, runs the LLM advisor.
 *   Caller-driven (the drawer's "Run agent" button) so the model call
 *   only fires on explicit opt-in.
 * - previewTidy: POST /api/tidy/preview, returns a 50-row sample of the
 *   long-form result without materializing it.
 * - applyTidy: POST /api/tidy/apply, runs the reshape DDL and returns the
 *   updated schema_info so the caller can refresh the sidebar.
 */

import { fetchJson, postJson } from "$lib/api/client";
import type { TableInfo } from "$lib/stores/schema.svelte";

export interface TidyDimension {
  name: string;
  kind: string;
  /** SQL dtype for the long-form dimension column (VARCHAR, INTEGER, …).
   * Set by the deterministic detector for known period kinds; the LLM may
   * also propose one. The drawer treats this as read-only in v1. */
  dtype: string;
}

export interface TidyColumnMapping {
  column: string;
  dimension_values: Record<string, string>;
}

/** A single tidy-reshape proposal (deterministic or LLM-derived).
 *
 * Mirrors `TidySuggestion.to_dict()` plus three convenience fields the
 * server adds for the drawer (`preview_sql`, `reshape_sql_view`,
 * `reshape_sql_table`).
 */
export interface TidyProposal {
  pattern: string;
  table: string;
  dimensions: TidyDimension[];
  column_mappings: TidyColumnMapping[];
  id_columns: string[];
  value_column: string;
  target_object_name: string;
  rationale: string;
  reshape_sql: string;
  confidence: "high" | "medium" | "low";
  source: "deterministic" | "llm" | "user";
  /** When true, rows where the source value is NULL survive the reshape.
   * Default false drops them — most NULLs in wide tables are structural
   * placeholders for combinations that don't apply. Toggle on for data
   * where NULL is a meaningful "missing observation" you want to keep. */
  include_nulls: boolean;
  preview_sql?: string;
  reshape_sql_view?: string;
  reshape_sql_table?: string;
}

export type TidyMaterializeMode = "view" | "table";

export type TidyDispositionMode = "keep" | "rename" | "replace" | "drop";

export interface TidyDisposition {
  mode: TidyDispositionMode;
  new_name?: string;
}

export interface TidyApplyResult {
  table: string;
  target_object_name: string;
  final_target_name: string;
  object_type: TidyMaterializeMode;
  affected_columns: string[];
  row_count_source: number;
  row_count_target: number;
  source_disposition: TidyDispositionMode;
  source_renamed_to: string | null;
  dry_run: boolean;
}

/** Static drift summary returned by /api/tidy/apply. The slow LLM
 * repair runs separately via /api/tidy/grounding/repair so the apply
 * response can return immediately after the database mutation. */
export interface TidyGroundingDrift {
  needs_repair: boolean;
  drift_items: number;
}

export interface TidyApplyResponse {
  success: boolean;
  result?: TidyApplyResult;
  schema_info?: TableInfo[];
  /** Present when apply changed the schema and a drift check ran. */
  grounding_drift?: TidyGroundingDrift | null;
  error?: string;
}

/** Result of the LLM grounding repair. Mirrors the summary built on
 * the server in ``_run_grounding_repair``. */
export interface TidyGroundingRepairSummary {
  drift_items: number;
  files_written: string[];
  applied: boolean;
  /** Set when the repair was a no-op (e.g. "no_drift", "no_llm_changes"). */
  skipped?: string;
  /** Set when the LLM proposal failed SQL validation after retries. */
  validation_errors?: string[];
  /** Set when the LLM call itself crashed (timeout, etc). */
  error?: string;
}

export interface TidyGroundingRepairResponse {
  success: boolean;
  grounding_repair?: TidyGroundingRepairSummary;
  error?: string;
}

export interface TidyPreviewResponse {
  html: string | null;
  row_count: number;
  error: string | null;
}

export interface TidyDetectResponse {
  proposals: TidyProposal[];
  error: string | null;
}

export async function detectTidy(table: string): Promise<TidyDetectResponse> {
  const url = `/api/tidy/detect?table=${encodeURIComponent(table)}`;
  return fetchJson<TidyDetectResponse>(url);
}

export interface ProposeCallbacks {
  onLlmStarted?: () => void;
  onLlmProposals?: (
    proposals: TidyProposal[],
    parseWarnings: string[],
  ) => void;
  onLlmError?: (error: string) => void;
  onError?: (error: string) => void;
  onDone?: () => void;
}

export interface ProposeOptions extends ProposeCallbacks {
  table: string;
  sampleRows?: number;
  signal?: AbortSignal;
}

export async function proposeTidy(opts: ProposeOptions): Promise<void> {
  const body = JSON.stringify({
    table: opts.table,
    sample_rows: opts.sampleRows ?? 0,
  });

  const resp = await fetch("/api/tidy/propose", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
    signal: opts.signal,
  });

  if (!resp.ok) {
    throw new Error(`Tidy propose failed: HTTP ${resp.status}`);
  }

  const reader = resp.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let eventType = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop()!;

    for (const line of lines) {
      if (line.startsWith("event: ")) {
        eventType = line.slice(7).trim();
      } else if (line.startsWith("data: ")) {
        let data: unknown;
        try {
          data = JSON.parse(line.slice(6));
        } catch {
          eventType = "";
          continue;
        }
        dispatchProposeEvent(eventType, data, opts);
        eventType = "";
      }
    }
  }
}

function dispatchProposeEvent(
  eventType: string,
  data: unknown,
  cb: ProposeCallbacks,
): void {
  const payload = data as Record<string, unknown>;
  switch (eventType) {
    case "llm_started":
      cb.onLlmStarted?.();
      break;
    case "llm_proposals":
      cb.onLlmProposals?.(
        (payload.proposals as TidyProposal[]) ?? [],
        (payload.parse_warnings as string[]) ?? [],
      );
      break;
    case "llm_error":
      cb.onLlmError?.(String(payload.error ?? "LLM error"));
      break;
    case "error":
      cb.onError?.(String(payload.error ?? "Tidy propose error"));
      break;
    case "done":
      cb.onDone?.();
      break;
    default:
      // Unknown event types are ignored — keeps the contract forward-compatible.
      break;
  }
}

export async function previewTidy(
  proposal: TidyProposal,
): Promise<TidyPreviewResponse> {
  return postJson<TidyPreviewResponse>("/api/tidy/preview", { proposal });
}

export interface TidyRenderSqlResponse {
  sql: string;
  error: string | null;
}

/** Re-render the reshape DDL for a proposal in the requested mode.
 *
 * Used by the Show SQL panel so the displayed DDL reflects the user's
 * current edits and the global view/table toggle, not the frozen
 * snapshot baked at detect time.
 */
export async function renderTidySql(args: {
  proposal: TidyProposal;
  mode: TidyMaterializeMode;
}): Promise<TidyRenderSqlResponse> {
  return postJson<TidyRenderSqlResponse>("/api/tidy/render-sql", args);
}

export async function applyTidy(args: {
  proposal: TidyProposal;
  mode: TidyMaterializeMode;
  disposition: TidyDisposition;
}): Promise<TidyApplyResponse> {
  return postJson<TidyApplyResponse>("/api/tidy/apply", args);
}

/** Trigger the LLM grounding repair after a tidy apply. Slow (the LLM
 * call can take minutes on local models); the caller should show a
 * spinner. The server reads the pre-tidy schema snapshot it stashed
 * during /api/tidy/apply, so this only works while that snapshot is
 * still live (cleared on project change or after a successful repair). */
export async function repairGrounding(): Promise<TidyGroundingRepairResponse> {
  return postJson<TidyGroundingRepairResponse>(
    "/api/tidy/grounding/repair",
    {},
  );
}

/** Always-on grounding-drift status, polled by the header pill on
 * project load (and after applies/repairs). When ``available`` is
 * false the UI hides the pill — covers ephemeral sessions, no-project
 * states, and non-DuckDB backends. ``has_snapshot`` tells the pill
 * whether the repair button should be enabled or whether the user
 * needs the CLI's ``--from-csv`` fallback. */
export interface GroundingStatusResponse {
  available: boolean;
  reason?: string;
  needs_repair?: boolean;
  drift_items?: number;
  has_snapshot?: boolean;
}

export async function fetchGroundingStatus(): Promise<GroundingStatusResponse> {
  return fetchJson<GroundingStatusResponse>("/api/grounding/status");
}
