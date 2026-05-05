import type { ChatEvent } from "$lib/stores/chat.svelte";
import { chatStore } from "$lib/stores/chat.svelte";
import { dashboardStore } from "$lib/stores/dashboard.svelte";
import { loadPlotlySpec } from "$lib/api/chat";
import { saveDashboard } from "$lib/api/dashboard";

type ToolResult = ChatEvent & { type: "tool_result" };

interface ToolContext {
  sql: string;
  tool: string;
  plotlySpec: unknown;
  meta?: Record<string, unknown>;
}

/**
 * Walks chatStore messages backward from `upTo` to find the tool that
 * produced the given tool_result, and the meta from its tool_done.
 */
export function findToolContextForResult(
  messages: readonly ChatEvent[],
  upTo: number,
): ToolContext | null {
  let toolMeta: Record<string, unknown> | undefined;
  for (let i = upTo + 1; i < messages.length; i++) {
    const msg = messages[i];
    if (msg.type === "tool_done") {
      toolMeta = msg.meta as unknown as Record<string, unknown>;
      break;
    }
    if (msg.type === "tool_start" || msg.type === "user_message") break;
  }
  for (let i = upTo; i >= 0; i--) {
    const msg = messages[i];
    if (msg.type === "tool_done") {
      return {
        sql: msg.meta.sql,
        tool: msg.meta.tool,
        plotlySpec: null,
        meta: msg.meta as unknown as Record<string, unknown>,
      };
    }
    if (msg.type === "tool_start") {
      return {
        sql: msg.sql || "",
        tool: msg.tool,
        plotlySpec: msg.plotlySpec,
        meta: toolMeta,
      };
    }
  }
  return null;
}

/** Pin a tool_result message to the dashboard, hydrating its Plotly spec. */
export async function pinResult(
  event: ToolResult,
  toolCtx: ToolContext | null,
): Promise<void> {
  let plotlySpec = event.plotlySpec;
  if (!plotlySpec && event.plotlySpecRef) {
    try {
      plotlySpec = await loadPlotlySpec(event.plotlySpecRef);
    } catch (err) {
      console.error("Failed to load Plotly spec before pinning:", err);
    }
  }
  dashboardStore.addItem({
    type: event.resultType === "chart" ? "chart" : "table",
    html: event.html,
    title: event.title || "",
    sql: toolCtx?.sql,
    tool:
      toolCtx?.tool ||
      (event.resultType === "chart" ? "visualize_data" : "run_sql"),
    render_plotly_spec: plotlySpec,
    plotly_spec: toolCtx?.plotlySpec ?? plotlySpec,
    source_meta: {
      question: "",
      resultType: event.resultType,
      meta: toolCtx?.meta,
    },
  });
  await saveDashboard();
}

/**
 * Pin the most recent tool_result in chat to the dashboard. Returns true if
 * a result was found and pinned.
 */
export async function pinLatestResult(): Promise<boolean> {
  const messages = chatStore.messages;
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg.type === "tool_result") {
      const ctx = findToolContextForResult(messages, i);
      await pinResult(msg, ctx);
      return true;
    }
  }
  return false;
}
