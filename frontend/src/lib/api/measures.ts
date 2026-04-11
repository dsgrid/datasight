/** Measure editor API — catalog, validate, upsert, save. */

import { fetchJson, postJson } from "./client";
import { sidebarStore } from "$lib/stores/sidebar.svelte";
import type { MeasureCatalogEntry } from "$lib/stores/sidebar.svelte";

export interface MeasureEditorData {
  ok: boolean;
  text: string;
  path: string;
  generated?: boolean;
  error?: string;
}

export interface MeasureValidation {
  ok: boolean;
  errors: string[];
  warnings: string[];
}

export interface MeasureUpsertResult {
  ok: boolean;
  text: string;
  warnings: string[];
  error?: string;
  errors: string[];
}

export async function loadMeasureEditor(): Promise<MeasureEditorData> {
  return fetchJson<MeasureEditorData>("/api/measures/editor");
}

export async function loadMeasureCatalog(): Promise<MeasureCatalogEntry[]> {
  const data = await fetchJson<{
    ok: boolean;
    measures: MeasureCatalogEntry[];
    error?: string;
  }>("/api/measures/editor/catalog");
  sidebarStore.measureEditorCatalog = data.measures;
  return data.measures;
}

export async function saveMeasureYaml(text: string): Promise<MeasureEditorData> {
  return postJson<MeasureEditorData>("/api/measures/editor", { text });
}

export async function validateMeasureYaml(
  text: string,
): Promise<MeasureValidation> {
  return postJson<MeasureValidation>("/api/measures/editor/validate", { text });
}

export interface UpsertMeasureInput {
  text: string;
  table: string;
  column?: string;
  name?: string;
  expression?: string;
  default_aggregation?: string;
  average_strategy?: string;
  display_name?: string;
  format?: string;
  preferred_chart_types?: string[];
  weight_column?: string;
}

export async function upsertMeasure(
  input: UpsertMeasureInput,
): Promise<MeasureUpsertResult> {
  return postJson<MeasureUpsertResult>("/api/measures/editor/upsert", input);
}
