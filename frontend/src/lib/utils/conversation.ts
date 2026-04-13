import type {
  ChatEvent,
  ProvenanceData,
  ProvenanceTool,
  ToolMeta,
} from "$lib/stores/chat.svelte";
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
    validation: asValidationMeta(meta.validation),
    turn_id: stringValue(meta.turn_id) || undefined,
  };
}

function asValidationMeta(value: unknown): ToolMeta["validation"] {
  const record = asRecord(value);
  const errors = Array.isArray(record.errors)
    ? record.errors.filter((error): error is string => typeof error === "string")
    : [];
  const status = stringValue(record.status);
  return status ? { status, errors } : undefined;
}

function asProvenanceTool(value: unknown): ProvenanceTool {
  const record = asRecord(value);
  const execution = asRecord(record.execution);
  const columns = Array.isArray(execution.columns)
    ? execution.columns.filter((column): column is string => typeof column === "string")
    : undefined;
  return {
    tool: stringValue(record.tool) || undefined,
    sql: stringValue(record.sql) || undefined,
    formatted_sql: stringValue(record.formatted_sql) || undefined,
    validation: asValidationMeta(record.validation),
    execution: {
      status: stringValue(execution.status) || "unknown",
      execution_time_ms:
        typeof execution.execution_time_ms === "number"
          ? execution.execution_time_ms
          : undefined,
      row_count: typeof execution.row_count === "number" ? execution.row_count : undefined,
      column_count:
        typeof execution.column_count === "number" ? execution.column_count : undefined,
      columns,
      error: typeof execution.error === "string" ? execution.error : null,
      timestamp: stringValue(execution.timestamp) || undefined,
    },
  };
}

function asProvenanceData(payload: Record<string, unknown>): ProvenanceData {
  const llm = asRecord(payload.llm);
  return {
    turn_id: stringValue(payload.turn_id) || undefined,
    question: stringValue(payload.question) || undefined,
    model: stringValue(payload.model) || undefined,
    dialect: stringValue(payload.dialect) || undefined,
    project_dir: stringValue(payload.project_dir) || undefined,
    tools: Array.isArray(payload.tools) ? payload.tools.map(asProvenanceTool) : [],
    llm: {
      api_calls: typeof llm.api_calls === "number" ? llm.api_calls : undefined,
      input_tokens: typeof llm.input_tokens === "number" ? llm.input_tokens : undefined,
      output_tokens: typeof llm.output_tokens === "number" ? llm.output_tokens : undefined,
      estimated_cost:
        typeof llm.estimated_cost === "number" ? llm.estimated_cost : undefined,
    },
    warnings: Array.isArray(payload.warnings)
      ? payload.warnings.filter((warning): warning is string => typeof warning === "string")
      : [],
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
    } else if (type === "provenance") {
      messages.push({ type: "provenance", provenance: asProvenanceData(payload) });
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
