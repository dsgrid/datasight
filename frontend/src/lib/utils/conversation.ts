import type { ChatEvent, ToolMeta } from "$lib/stores/chat.svelte";
import type { QueryEntry } from "$lib/stores/queries.svelte";

type RawConversationEvent = Record<string, unknown>;

export interface ConversationReplay {
  messages: ChatEvent[];
  queries: QueryEntry[];
  totalCost: number;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function eventName(event: RawConversationEvent): string {
  return stringValue(event.event ?? event.type);
}

function eventPayload(event: RawConversationEvent): Record<string, unknown> {
  return Object.keys(asRecord(event.data)).length > 0 ? asRecord(event.data) : event;
}

function buildToolMeta(
  payload: Record<string, unknown>,
  lastSql: string,
  lastToolName: string,
): ToolMeta {
  const meta = Object.keys(asRecord(payload.meta)).length > 0
    ? asRecord(payload.meta)
    : payload;

  return {
    sql: stringValue(meta.sql) || lastSql,
    tool: stringValue(meta.tool) || lastToolName,
    execution_time_ms:
      typeof meta.execution_time_ms === "number" ? meta.execution_time_ms : undefined,
    row_count: typeof meta.row_count === "number" ? meta.row_count : undefined,
    column_count:
      typeof meta.column_count === "number" ? meta.column_count : undefined,
    columns: Array.isArray(meta.columns)
      ? meta.columns.filter((column): column is string => typeof column === "string")
      : undefined,
    error: typeof meta.error === "string" ? meta.error : undefined,
  };
}

export function replayConversationEvents(events: unknown[]): ConversationReplay {
  const messages: ChatEvent[] = [];
  const queries: QueryEntry[] = [];
  let totalCost = 0;
  let lastSql = "";
  let lastToolName = "";
  let lastPlotlySpec: unknown = null;

  for (const rawEvent of events) {
    const event = asRecord(rawEvent);
    const type = eventName(event);
    const payload = eventPayload(event);

    if (type === "user_message") {
      const content = stringValue(payload.text ?? payload.content);
      messages.push({ type: "user_message", content });
    } else if (type === "assistant_message") {
      const content = stringValue(payload.text ?? payload.content);
      messages.push({ type: "assistant_message", content });
    } else if (type === "tool_start") {
      const input = asRecord(payload.input);
      lastToolName = stringValue(payload.tool);
      lastSql = stringValue(input.sql ?? payload.sql);
      lastPlotlySpec = input.plotly_spec ?? input.plotlySpec ?? payload.plotlySpec ?? null;
      messages.push({
        type: "tool_start",
        tool: lastToolName,
        sql: lastSql,
        plotlySpec: lastPlotlySpec,
      });
    } else if (type === "tool_result") {
      const resultType = stringValue(
        payload.type ?? payload.result_type ?? payload.resultType,
      );
      messages.push({
        type: "tool_result",
        html: stringValue(payload.html),
        title: stringValue(payload.title),
        resultType: resultType === "chart" ? "chart" : "table",
      });
    } else if (type === "tool_done") {
      const meta = buildToolMeta(payload, lastSql, lastToolName);
      messages.push({ type: "tool_done", meta });
      if (meta.sql) {
        queries.unshift({
          tool: meta.tool,
          sql: meta.sql,
          timestamp: stringValue(payload.timestamp) || new Date(0).toISOString(),
          execution_time_ms: meta.execution_time_ms,
          row_count: meta.row_count,
          column_count: meta.column_count,
          error: meta.error,
        });
      }
    } else if (type === "suggestions") {
      const suggestions = Array.isArray(payload.suggestions)
        ? payload.suggestions.filter(
            (suggestion): suggestion is string => typeof suggestion === "string",
          )
        : [];
      messages.push({ type: "suggestions", suggestions });
    } else if (type === "error") {
      messages.push({ type: "error", error: stringValue(payload.error) });
    } else if (type === "done" && typeof payload.estimated_cost === "number") {
      totalCost += payload.estimated_cost;
      if (queries.length > 0) {
        queries[0] = { ...queries[0], turn_cost: payload.estimated_cost };
      }
    }
  }

  return { messages, queries, totalCost };
}
