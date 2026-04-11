/** Schema, preview, and column stats API. */

import { fetchJson } from "./client";
import { schemaStore } from "$lib/stores/schema.svelte";
import type {
  TableInfo,
  ExampleQuery,
  Recipe,
  PreviewData,
  ColumnStats,
} from "$lib/stores/schema.svelte";

export async function loadSchema(): Promise<TableInfo[]> {
  const data = await fetchJson<{ tables: TableInfo[] }>("/api/schema");
  schemaStore.schemaData = data.tables;
  return data.tables;
}

export async function loadQueries(): Promise<ExampleQuery[]> {
  const data = await fetchJson<{ queries: ExampleQuery[] }>("/api/queries");
  schemaStore.allQueries = data.queries;
  return data.queries;
}

export async function loadRecipes(): Promise<Recipe[]> {
  const data = await fetchJson<{
    recipes: Recipe[];
    cached: boolean;
    error?: string;
  }>("/api/recipes");
  schemaStore.recipesCache = data.recipes;
  return data.recipes;
}

export async function loadPreview(tableName: string): Promise<PreviewData> {
  const cached = schemaStore.tablePreviewCache.get(tableName);
  if (cached) return cached;

  const data = await fetchJson<PreviewData>(
    `/api/preview/${encodeURIComponent(tableName)}`,
  );
  schemaStore.setPreview(tableName, data);
  return data;
}

export async function loadColumnStats(
  tableName: string,
  columnName: string,
): Promise<ColumnStats | null> {
  const key = `${tableName}.${columnName}`;
  const cached = schemaStore.columnStatsCache.get(key);
  if (cached) return cached;

  const data = await fetchJson<{
    stats: ColumnStats | null;
    cached: boolean;
    error?: string;
  }>(
    `/api/column-stats/${encodeURIComponent(tableName)}/${encodeURIComponent(columnName)}`,
  );

  if (data.stats) {
    schemaStore.setColumnStats(key, data.stats);
  }
  return data.stats;
}
