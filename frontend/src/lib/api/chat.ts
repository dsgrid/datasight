/** SSE-based chat streaming. */

import { chatStore } from "$lib/stores/chat.svelte";
import { queriesStore } from "$lib/stores/queries.svelte";
import { sessionStore } from "$lib/stores/session.svelte";
import { settingsStore } from "$lib/stores/settings.svelte";
import type { ToolMeta } from "$lib/stores/chat.svelte";

export type SSEEventType =
  | "tool_start"
  | "tool_result"
  | "tool_done"
  | "token"
  | "done"
  | "suggestions"
  | "sql_confirm"
  | "sql_rejected"
  | "explanation_done"
  | "error";

interface ToolStartData {
  tool: string;
  input?: { sql?: string; plotly_spec?: unknown };
}

interface ToolResultData {
  type: "chart" | "table";
  html: string;
  title?: string;
}

interface ToolDoneData {
  sql?: string;
  execution_time_ms?: number;
  row_count?: number;
  column_count?: number;
  error?: string;
}

interface TokenData {
  text: string;
}

interface DoneData {
  estimated_cost?: number;
}

interface SuggestionsData {
  suggestions: string[];
}

interface SqlConfirmData {
  sql: string;
  request_id: string;
}

interface ErrorData {
  error: string;
}

type SSEData =
  | ToolStartData
  | ToolResultData
  | ToolDoneData
  | TokenData
  | DoneData
  | SuggestionsData
  | SqlConfirmData
  | ErrorData;

function handleSSEEvent(eventType: SSEEventType, data: SSEData): void {
  switch (eventType) {
    case "tool_start": {
      const d = data as ToolStartData;
      chatStore.lastToolName = d.tool || "";
      chatStore.lastPlotlySpec = d.input?.plotly_spec ?? null;
      if (d.input?.sql) {
        chatStore.lastSql = d.input.sql;
      }
      chatStore.pushMessage({
        type: "tool_start",
        tool: d.tool,
        sql: d.input?.sql,
        plotlySpec: d.input?.plotly_spec,
      });
      break;
    }

    case "tool_result": {
      const d = data as ToolResultData;
      chatStore.pushMessage({
        type: "tool_result",
        resultType: d.type,
        html: d.html,
        title: d.title,
      });
      break;
    }

    case "tool_done": {
      const d = data as ToolDoneData;
      const meta: ToolMeta = {
        sql: d.sql || chatStore.lastSql,
        tool: chatStore.lastToolName,
        execution_time_ms: d.execution_time_ms,
        row_count: d.row_count,
        column_count: d.column_count,
        error: d.error,
      };
      chatStore.lastToolMeta = meta;
      chatStore.pushMessage({ type: "tool_done", meta });

      // Add to query history
      queriesStore.addQuery({
        tool: meta.tool,
        sql: meta.sql,
        timestamp: new Date().toISOString(),
        execution_time_ms: meta.execution_time_ms,
        row_count: meta.row_count,
        column_count: meta.column_count,
        error: meta.error,
      });
      break;
    }

    case "token": {
      const d = data as TokenData;
      chatStore.currentAssistantText += d.text;
      // Only push a new assistant_message event if we haven't started one
      const last = chatStore.messages[chatStore.messages.length - 1];
      if (last?.type === "assistant_message") {
        // Update in place — Svelte reactivity will pick up the currentAssistantText change
      } else {
        chatStore.pushMessage({
          type: "assistant_message",
          content: "",
        });
      }
      break;
    }

    case "explanation_done":
      // Finalize the current assistant text so the next tool result appears separately
      if (chatStore.currentAssistantText) {
        // Update the last assistant message with final content
        const msgs = [...chatStore.messages];
        for (let i = msgs.length - 1; i >= 0; i--) {
          if (msgs[i].type === "assistant_message") {
            msgs[i] = {
              type: "assistant_message",
              content: chatStore.currentAssistantText,
            };
            break;
          }
        }
        chatStore.messages = msgs;
        chatStore.currentAssistantText = "";
      }
      break;

    case "done": {
      const d = data as DoneData;
      // Finalize assistant text
      if (chatStore.currentAssistantText) {
        const msgs = [...chatStore.messages];
        for (let i = msgs.length - 1; i >= 0; i--) {
          if (msgs[i].type === "assistant_message") {
            msgs[i] = {
              type: "assistant_message",
              content: chatStore.currentAssistantText,
            };
            break;
          }
        }
        chatStore.messages = msgs;

        // Extract clarify options if enabled
        if (settingsStore.clarifySql) {
          const options = extractClarifyOptions(chatStore.currentAssistantText);
          if (options.length >= 2) {
            chatStore.pushMessage({ type: "clarify_options", options });
          }
        }
        chatStore.currentAssistantText = "";
      }

      if (d.estimated_cost != null) {
        queriesStore.addCost(d.estimated_cost);
      }
      break;
    }

    case "suggestions": {
      const d = data as SuggestionsData;
      if (d.suggestions?.length) {
        chatStore.pushMessage({ type: "suggestions", suggestions: d.suggestions });
      }
      break;
    }

    case "sql_confirm": {
      const d = data as SqlConfirmData;
      chatStore.pushMessage({
        type: "sql_confirm",
        sql: d.sql,
        requestId: d.request_id,
      });
      break;
    }

    case "sql_rejected":
      // Visual feedback handled by the SqlConfirmDialog component
      break;

    case "error": {
      const d = data as ErrorData;
      chatStore.pushMessage({ type: "error", error: d.error });
      break;
    }
  }
}

/**
 * Send a chat message and process the SSE stream.
 */
export async function sendMessage(text: string): Promise<void> {
  chatStore.pushMessage({ type: "user_message", content: text });
  chatStore.isStreaming = true;
  chatStore.currentAssistantText = "";

  const controller = new AbortController();
  chatStore.abortController = controller;

  // Show typing indicator
  chatStore.pushMessage({ type: "typing" });

  try {
    const resp = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: text,
        session_id: sessionStore.sessionId,
      }),
      signal: controller.signal,
    });

    if (!resp.ok) throw new Error(`Chat API error: ${resp.status}`);

    const reader = resp.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let typingRemoved = false;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop()!;

      let eventType: SSEEventType | "" = "";
      for (const line of lines) {
        if (line.startsWith("event: ")) {
          eventType = line.slice(7).trim() as SSEEventType;
        } else if (line.startsWith("data: ")) {
          if (!typingRemoved) {
            // Remove typing indicator
            chatStore.messages = chatStore.messages.filter(
              (m) => m.type !== "typing",
            );
            typingRemoved = true;
          }
          try {
            const data = JSON.parse(line.slice(6)) as SSEData;
            if (eventType) handleSSEEvent(eventType, data);
          } catch {
            console.error("Failed to parse SSE data:", line);
          }
          eventType = "";
        }
      }
    }

    if (!typingRemoved) {
      chatStore.messages = chatStore.messages.filter(
        (m) => m.type !== "typing",
      );
    }
  } catch (err) {
    // Remove typing indicator
    chatStore.messages = chatStore.messages.filter((m) => m.type !== "typing");

    if (err instanceof Error && err.name === "AbortError") {
      chatStore.pushMessage({
        type: "assistant_message",
        content: "Generation stopped.",
      });
    } else {
      console.error("Stream error:", err);
      chatStore.pushMessage({
        type: "error",
        error: "Connection error. Please try again.",
      });
    }
  }

  chatStore.isStreaming = false;
  chatStore.abortController = null;
}

/**
 * Respond to a SQL confirmation dialog.
 */
export async function respondSqlConfirm(
  requestId: string,
  action: "approve" | "edit" | "reject",
  sql: string,
): Promise<void> {
  try {
    await fetch(`/api/sql-confirm/${requestId}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action, sql }),
    });
  } catch (e) {
    console.error("Failed to confirm SQL:", e);
  }
}

/**
 * Extract clarify options from assistant text.
 * Only extracts from clarifying questions (text with "?" before list items).
 */
export function extractClarifyOptions(text: string): string[] {
  if (!text.includes("?")) return [];
  const lines = text.split("\n");

  const optionRe = /^[-*]?\s*\*\*(.+?)\*\*\s*[—–-]/;
  const bulletRe = /^[-*]\s+(.+?)\s*[—–-]/;
  const plainRe = /^[-*]\s+(\w[\w\s]*?)\s*[—–]\s+\S/;

  for (let q = 0; q < lines.length; q++) {
    if (!lines[q].includes("?")) continue;
    const options: string[] = [];
    for (let i = q + 1; i < lines.length; i++) {
      const match =
        lines[i].match(optionRe) ||
        lines[i].match(bulletRe) ||
        lines[i].match(plainRe);
      if (match) {
        options.push(match[1].trim());
      }
    }
    if (options.length >= 2) return options;
  }
  return [];
}
