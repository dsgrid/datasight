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

export interface TidyApplyResponse {
  success: boolean;
  result?: TidyApplyResult;
  schema_info?: TableInfo[];
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
