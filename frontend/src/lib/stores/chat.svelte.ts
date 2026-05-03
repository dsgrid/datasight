/** Chat messages and streaming state. */

import type { PlotlySpecRef } from "$lib/types/plotly";

export type MessageRole = "user" | "assistant";

export interface ToolMeta {
  sql: string;
  formatted_sql?: string;
  tool: string;
  execution_time_ms?: number;
  row_count?: number;
  column_count?: number;
  columns?: string[];
  error?: string;
  validation?: {
    status: string;
    errors: string[];
  };
  turn_id?: string;
}

export interface SourceMeta {
  question: string;
  resultType: string;
  meta: ToolMeta;
}

export interface ProvenanceTool {
  tool?: string;
  sql?: string;
  formatted_sql?: string;
  validation?: {
    status: string;
    errors: string[];
  };
  execution?: {
    status: string;
    execution_time_ms?: number;
    row_count?: number;
    column_count?: number;
    columns?: string[];
    error?: string | null;
    timestamp?: string;
  };
}

export interface ProvenanceData {
  turn_id?: string;
  question?: string;
  model?: string;
  dialect?: string;
  project_dir?: string;
  tools: ProvenanceTool[];
  llm?: {
    api_calls?: number;
    input_tokens?: number;
    output_tokens?: number;
    cache_creation_input_tokens?: number;
    cache_read_input_tokens?: number;
    estimated_cost?: number;
  };
  warnings?: string[];
}

export type ChatEvent =
  | { type: "user_message"; content: string }
  | { type: "assistant_message"; content: string }
  | { type: "tool_start"; tool: string; sql?: string; plotlySpec?: unknown }
  | {
      type: "tool_result";
      resultType: "chart" | "table";
      html: string;
      title?: string;
      plotlySpec?: unknown;
      plotlySpecRef?: PlotlySpecRef;
    }
  | { type: "tool_done"; meta: ToolMeta }
  | { type: "provenance"; provenance: ProvenanceData }
  | { type: "sql_confirm"; sql: string; requestId: string }
  | { type: "suggestions"; suggestions: string[] }
  | { type: "clarify_options"; options: string[] }
  | { type: "error"; error: string }
  | { type: "typing" }
  | { type: "starter_overview"; kind: string; overview: Record<string, unknown> };

function createChatStore() {
  let messages = $state<ChatEvent[]>([]);
  let isStreaming = $state(false);
  let isReplaying = $state(false);
  let currentAssistantText = $state("");
  let abortController = $state<AbortController | null>(null);
  let lastSql = $state("");
  let lastToolName = $state("");
  let lastPlotlySpec = $state<unknown>(null);
  let lastToolMeta = $state<ToolMeta | null>(null);

  return {
    get messages() {
      return messages;
    },
    set messages(v: ChatEvent[]) {
      messages = v;
    },
    get isStreaming() {
      return isStreaming;
    },
    set isStreaming(v: boolean) {
      isStreaming = v;
    },
    get isReplaying() {
      return isReplaying;
    },
    set isReplaying(v: boolean) {
      isReplaying = v;
    },
    get isBusy() {
      return isStreaming || isReplaying;
    },
    get currentAssistantText() {
      return currentAssistantText;
    },
    set currentAssistantText(v: string) {
      currentAssistantText = v;
    },
    get abortController() {
      return abortController;
    },
    set abortController(v: AbortController | null) {
      abortController = v;
    },
    get lastSql() {
      return lastSql;
    },
    set lastSql(v: string) {
      lastSql = v;
    },
    get lastToolName() {
      return lastToolName;
    },
    set lastToolName(v: string) {
      lastToolName = v;
    },
    get lastPlotlySpec() {
      return lastPlotlySpec;
    },
    set lastPlotlySpec(v: unknown) {
      lastPlotlySpec = v;
    },
    get lastToolMeta() {
      return lastToolMeta;
    },
    set lastToolMeta(v: ToolMeta | null) {
      lastToolMeta = v;
    },

    pushMessage(event: ChatEvent) {
      messages = [...messages, event];
    },

    removeMessage(index: number) {
      messages = messages.filter((_, i) => i !== index);
    },

    clear() {
      messages = [];
      currentAssistantText = "";
      lastSql = "";
      lastToolName = "";
      lastPlotlySpec = null;
      lastToolMeta = null;
    },

    stopGeneration() {
      abortController?.abort();
    },
  };
}

export const chatStore = createChatStore();
