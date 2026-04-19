/** Project loading, recent projects, and explore API. */

import { fetchJson, postJson } from "./client";
import { sessionStore } from "$lib/stores/session.svelte";
import { sidebarStore } from "$lib/stores/sidebar.svelte";
import type { RecentProject } from "$lib/stores/sidebar.svelte";
import type { TableInfo } from "$lib/stores/schema.svelte";

export interface ProjectStatus {
  loaded: boolean;
  path: string | null;
  name: string | null;
  is_ephemeral: boolean;
  tables?: { name: string; source: string; row_count: number }[];
  has_time_series?: boolean;
  sql_dialect?: string;
}

export interface ExploreResult {
  success: boolean;
  tables: { name: string; source: string; row_count: number }[];
  schema_info: TableInfo[];
  llm_connected: boolean;
  error?: string;
}

export interface ValidateResult {
  valid: boolean;
  path: string;
  name?: string;
  error?: string;
}

export async function loadProject(path: string): Promise<void> {
  const result = await postJson<{ has_time_series?: boolean }>("/api/projects/load", { path });
  sessionStore.hasTimeSeries = Boolean(result?.has_time_series);
}

export async function getProjectStatus(): Promise<ProjectStatus> {
  return fetchJson<ProjectStatus>("/api/project");
}

export async function loadRecentProjects(): Promise<RecentProject[]> {
  const data = await fetchJson<{ projects: RecentProject[] }>(
    "/api/projects/recent",
  );
  sidebarStore.recentProjectsCache = data.projects;
  return data.projects;
}

export async function removeRecentProject(projectPath: string): Promise<void> {
  await fetchJson(`/api/projects/recent/${encodeURIComponent(projectPath)}`, {
    method: "DELETE",
  });
}

export async function validateProject(path: string): Promise<ValidateResult> {
  return postJson<ValidateResult>("/api/projects/validate", { path });
}

export async function explore(paths: string[]): Promise<ExploreResult> {
  const result = await postJson<ExploreResult>("/api/explore", { paths });
  if (result.success) {
    sessionStore.isEphemeralSession = true;
    sessionStore.ephemeralTablesInfo = result.tables;
  }
  return result;
}

export async function getExploreStatus(): Promise<{
  is_ephemeral: boolean;
  tables: { name: string; source: string; row_count: number }[];
  project_loaded: boolean;
  project_dir: string | null;
}> {
  return fetchJson("/api/explore/status");
}

export interface ScannedDataFile {
  path: string;
  name: string;
  type: "csv" | "parquet";
  size_bytes: number;
}

export interface ScanCwdResult {
  directory: string;
  files: ScannedDataFile[];
  truncated: boolean;
}

export async function scanCwdForDataFiles(): Promise<ScanCwdResult> {
  return fetchJson<ScanCwdResult>("/api/explore/scan-cwd");
}

export async function checkProjectPath(
  path: string,
): Promise<{ exists: boolean; files: string[] }> {
  return postJson("/api/explore/check-project-path", { path });
}

export async function saveExploreAsProject(
  path: string,
  name?: string,
): Promise<{ success: boolean; path?: string; error?: string }> {
  return postJson("/api/explore/save-project", { path, name: name || null });
}

export interface GenerateProjectEvent {
  type: "status" | "token" | "done" | "error";
  data: Record<string, unknown>;
}

export async function generateProjectStream(
  path: string,
  name: string | null,
  description: string | null,
  onEvent: (event: GenerateProjectEvent) => void,
): Promise<void> {
  const response = await fetch("/api/explore/generate-project", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path, name, description }),
  });
  if (!response.body) throw new Error("No response body");

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let eventType: string | null = null;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      if (line.startsWith("event: ")) {
        eventType = line.slice(7).trim();
      } else if (line.startsWith("data: ") && eventType) {
        try {
          const data = JSON.parse(line.slice(6));
          onEvent({
            type: eventType as GenerateProjectEvent["type"],
            data,
          });
        } catch {
          // Ignore malformed event data
        }
        eventType = null;
      }
    }
  }
}

export async function addFiles(
  paths: string[],
): Promise<{ success: boolean; error?: string }> {
  return postJson("/api/add-files", { paths });
}

export async function clearSession(
  sessionId: string,
): Promise<{ ok: boolean }> {
  return postJson("/api/clear", { session_id: sessionId });
}
