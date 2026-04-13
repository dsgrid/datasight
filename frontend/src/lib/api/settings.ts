/** Settings and LLM configuration API. */

import { fetchJson, postJson } from "./client";
import { settingsStore } from "$lib/stores/settings.svelte";
import type { LlmConfig } from "$lib/stores/settings.svelte";

export interface SettingsData {
  confirm_sql: boolean;
  explain_sql: boolean;
  clarify_sql: boolean;
  show_cost: boolean;
  show_provenance: boolean;
}

export async function loadSettings(): Promise<SettingsData> {
  const data = await fetchJson<SettingsData>("/api/settings");
  settingsStore.applyFromApi(data);
  return data;
}

export async function saveSettings(
  updates: Partial<SettingsData>,
): Promise<SettingsData> {
  const data = await postJson<SettingsData>("/api/settings", updates);
  settingsStore.applyFromApi(data);
  return data;
}

export async function loadLlmConfig(): Promise<LlmConfig> {
  const data = await fetchJson<LlmConfig>("/api/settings/llm");
  settingsStore.llmConfig = data;
  settingsStore.llmConnected = data.connected;
  return data;
}

export async function saveLlmConfig(config: {
  provider: string;
  api_key?: string;
  model: string;
  base_url?: string;
}): Promise<LlmConfig> {
  const data = await postJson<LlmConfig>("/api/settings/llm", config);
  settingsStore.llmConfig = data;
  settingsStore.llmConnected = data.connected;
  return data;
}

export interface HealthCheck {
  cached: boolean;
  [key: string]: unknown;
}

export async function loadProjectHealth(): Promise<HealthCheck> {
  return fetchJson<HealthCheck>("/api/project-health");
}
