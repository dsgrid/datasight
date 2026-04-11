/** Dashboard persistence API. */

import { fetchJson, postJson, deleteRequest } from "./client";
import { dashboardStore } from "$lib/stores/dashboard.svelte";
import type { DashboardItem } from "$lib/stores/dashboard.svelte";

interface DashboardData {
  items: DashboardItem[];
  columns: number;
}

export async function loadDashboard(): Promise<DashboardData> {
  const data = await fetchJson<DashboardData>("/api/dashboard");
  dashboardStore.pinnedItems = data.items;
  dashboardStore.columns = data.columns;
  // Set ID counter to max existing ID
  const maxId = data.items.reduce((max, item) => Math.max(max, item.id), 0);
  dashboardStore.pinnedIdCounter = maxId;
  return data;
}

export async function saveDashboard(): Promise<DashboardData> {
  return postJson<DashboardData>("/api/dashboard", {
    items: dashboardStore.pinnedItems,
    columns: dashboardStore.columns,
  });
}

export async function clearDashboard(): Promise<void> {
  await deleteRequest("/api/dashboard");
  dashboardStore.clear();
}
