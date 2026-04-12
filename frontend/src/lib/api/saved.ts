/** Bookmarks, reports, and conversations API. */

import { fetchJson, postJson, deleteRequest, patchJson } from "./client";
import { sidebarStore } from "$lib/stores/sidebar.svelte";
import type { Bookmark, Report, Conversation } from "$lib/stores/sidebar.svelte";
import type { DashboardFilter, DashboardItem } from "$lib/stores/dashboard.svelte";

export interface ConversationData {
  events: unknown[];
  title: string;
  dashboard?: {
    items: DashboardItem[];
    columns: number;
    filters?: DashboardFilter[];
  };
}

// ── Bookmarks ──

export async function loadBookmarks(): Promise<Bookmark[]> {
  const data = await fetchJson<{ bookmarks: Bookmark[] }>("/api/bookmarks");
  sidebarStore.bookmarksCache = data.bookmarks;
  return data.bookmarks;
}

export async function addBookmark(
  sql: string,
  tool: string,
  name: string,
): Promise<Bookmark> {
  const data = await postJson<{ bookmark: Bookmark }>("/api/bookmarks", {
    sql,
    tool,
    name,
  });
  await loadBookmarks();
  return data.bookmark;
}

export async function deleteBookmark(id: string): Promise<void> {
  await deleteRequest(`/api/bookmarks/${encodeURIComponent(id)}`);
  await loadBookmarks();
}

export async function clearBookmarks(): Promise<void> {
  await deleteRequest("/api/bookmarks");
  sidebarStore.bookmarksCache = [];
}

// ── Reports ──

export async function loadReports(): Promise<Report[]> {
  const data = await fetchJson<{ reports: Report[] }>("/api/reports");
  sidebarStore.reportsCache = data.reports;
  return data.reports;
}

export async function addReport(
  sql: string,
  tool: string,
  name: string,
  plotlySpec?: unknown,
): Promise<Report> {
  const data = await postJson<{ report: Report }>("/api/reports", {
    sql,
    tool,
    name,
    plotly_spec: plotlySpec,
  });
  await loadReports();
  return data.report;
}

export async function updateReport(
  id: string,
  updates: { sql?: string; name?: string; plotly_spec?: unknown },
): Promise<Report> {
  const data = await patchJson<{ report: Report }>(
    `/api/reports/${encodeURIComponent(id)}`,
    updates,
  );
  await loadReports();
  return data.report;
}

export async function deleteReport(id: string): Promise<void> {
  await deleteRequest(`/api/reports/${encodeURIComponent(id)}`);
  await loadReports();
}

export async function clearReports(): Promise<void> {
  await deleteRequest("/api/reports");
  sidebarStore.reportsCache = [];
}

export async function runReport(id: string): Promise<{
  ok: boolean;
  html: string;
  type: string;
  title: string;
  meta: Record<string, unknown>;
  plotly_spec?: unknown;
  error?: string;
}> {
  return postJson(`/api/reports/${encodeURIComponent(id)}/run`, {});
}

// ── Conversations ──

export async function loadConversations(): Promise<Conversation[]> {
  const data = await fetchJson<{ conversations: Conversation[] }>(
    "/api/conversations",
  );
  sidebarStore.conversationsCache = data.conversations;
  return data.conversations;
}

export async function loadConversation(
  sessionId: string,
): Promise<ConversationData> {
  return fetchJson(`/api/conversations/${encodeURIComponent(sessionId)}`);
}

export async function clearConversations(): Promise<void> {
  await deleteRequest("/api/conversations");
  sidebarStore.conversationsCache = [];
}
