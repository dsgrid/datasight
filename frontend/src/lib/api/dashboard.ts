/** Dashboard persistence API. */

import { fetchJson, postJson, deleteRequest } from "./client";
import { dashboardStore } from "$lib/stores/dashboard.svelte";
import { sessionStore } from "$lib/stores/session.svelte";
import {
  filtersForCard,
  getAllCardColumns,
  getCardColumns,
} from "$lib/stores/dashboard.svelte";
import type { DashboardFilter, DashboardItem } from "$lib/stores/dashboard.svelte";

interface DashboardData {
  items: DashboardItem[];
  columns: number;
  filters?: DashboardFilter[];
  title?: string;
}

interface RunCardInput {
  item: DashboardItem;
  filters?: DashboardFilter[];
}

interface RunCardResponse {
  ok: boolean;
  html?: string;
  type?: "chart" | "table";
  title?: string;
  meta?: Record<string, unknown>;
  sql?: string;
  plotly_spec?: unknown;
  error?: string;
}

interface FilterValuesResponse {
  ok: boolean;
  values: unknown[];
  limit?: number;
  error?: string;
}

export async function loadDashboard(): Promise<DashboardData> {
  const data = await fetchJson<DashboardData>(
    `/api/dashboard?session_id=${encodeURIComponent(sessionStore.sessionId)}`,
  );
  applyDashboardData(data);
  return data;
}

export function applyDashboardData(data: DashboardData): void {
  dashboardStore.pinnedItems = data.items;
  dashboardStore.columns = data.columns;
  dashboardStore.filters = (data.filters || []).map((f) => ({
    ...f,
    scope: f.scope ?? { type: "all" },
  }));
  dashboardStore.title = data.title ?? "";
  dashboardStore.fullscreenCardId = null;
  dashboardStore.selectedCardIdx = -1;
  // Set ID counter to max existing ID
  const maxId = data.items.reduce((max, item) => Math.max(max, item.id), 0);
  dashboardStore.pinnedIdCounter = maxId;
}

export async function saveDashboard(): Promise<DashboardData> {
  return postJson<DashboardData>("/api/dashboard", {
    items: dashboardStore.pinnedItems,
    columns: dashboardStore.columns,
    filters: dashboardStore.filters,
    title: dashboardStore.title,
    session_id: sessionStore.sessionId,
  });
}

export async function clearDashboard(): Promise<void> {
  await deleteRequest("/api/dashboard");
  dashboardStore.clear();
}

export async function runDashboardCard({
  item,
  filters = dashboardStore.filters,
}: RunCardInput): Promise<RunCardResponse> {
  const cardFilters = filtersForCard(item, filters);
  return postJson<RunCardResponse>("/api/dashboard/run-card", {
    sql: item.sql,
    tool: item.tool || (item.type === "chart" ? "visualize_data" : "run_sql"),
    title: item.title || "Dashboard card",
    plotly_spec: item.plotly_spec,
    filters: cardFilters,
    allowed_columns: getCardColumns(item),
  });
}

export async function rerunDashboardCards(
  filters: DashboardFilter[] = dashboardStore.filters,
): Promise<void> {
  const items = dashboardStore.pinnedItems;
  for (const item of items) {
    if (!item.sql || (item.type !== "chart" && item.type !== "table")) continue;
    const result = await runDashboardCard({ item, filters });
    if (!result.html) continue;
    dashboardStore.updateItem(item.id, {
      html: result.html,
      render_plotly_spec: result.plotly_spec ?? item.render_plotly_spec,
      plotly_spec: item.plotly_spec ?? result.plotly_spec,
      source_meta: {
        question: item.source_meta?.question || "",
        resultType: result.type || item.source_meta?.resultType || item.type,
        meta: result.meta,
      },
    });
  }
  await saveDashboard();
}

export async function loadDashboardFilterValues(
  column: string,
): Promise<FilterValuesResponse> {
  const allowedColumns = getAllCardColumns(dashboardStore.pinnedItems);
  return postJson<FilterValuesResponse>("/api/dashboard/filter-values", {
    column,
    items: dashboardStore.pinnedItems,
    filters: dashboardStore.filters,
    allowed_columns: allowedColumns,
    limit: 100,
  });
}
