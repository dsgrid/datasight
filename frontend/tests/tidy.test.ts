import { describe, it, expect, beforeEach, vi } from "vitest";
import { tidyStore } from "$lib/stores/tidy.svelte";
import type { TidyProposal } from "$lib/api/tidy";

function sseStream(events: string[]): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    start(controller) {
      for (const e of events) controller.enqueue(encoder.encode(e));
      controller.close();
    },
  });
  return new Response(stream, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

function makeProposal(overrides: Partial<TidyProposal> = {}): TidyProposal {
  const dim = { name: "year", kind: "year", dtype: "INTEGER" };
  return {
    pattern: "date_in_column_names",
    table: "sales",
    dimensions: [dim],
    column_mappings: [
      { column: "sales_2020", dimension_values: { year: "2020" } },
      { column: "sales_2021", dimension_values: { year: "2021" } },
    ],
    id_columns: ["region"],
    value_column: "sales",
    target_object_name: "sales_long",
    rationale: "Looks like wide-by-year",
    reshape_sql: "CREATE OR REPLACE TABLE sales_long AS ...",
    confidence: "high",
    source: "deterministic",
    include_nulls: true,
    preview_sql: "SELECT 1",
    reshape_sql_view: "CREATE OR REPLACE VIEW sales_long AS ...",
    reshape_sql_table: "CREATE OR REPLACE TABLE sales_long AS ...",
    ...overrides,
  };
}

beforeEach(() => {
  vi.restoreAllMocks();
  // Reset store between tests by closing it (clears all state).
  tidyStore.close();
});

function detectResponse(proposals: TidyProposal[], error: string | null = null): Response {
  return new Response(JSON.stringify({ proposals, error }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

describe("tidyStore.start", () => {
  it("loads deterministic proposals from /api/tidy/detect without firing the LLM", async () => {
    const det = makeProposal({ source: "deterministic" });
    const fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(detectResponse([det]));

    await tidyStore.start("sales");

    expect(tidyStore.status).toBe("loaded_deterministic");
    expect(tidyStore.proposals).toHaveLength(1);
    expect(tidyStore.proposals[0].proposal.source).toBe("deterministic");
    // Edits default to the proposal's own values.
    expect(tidyStore.proposals[0].edits.target_object_name).toBe("sales_long");
    // Only the detect endpoint was hit — propose stays untouched until Run.
    const calls = fetchSpy.mock.calls.map((c) => String(c[0]));
    expect(calls.some((u) => u.includes("/api/tidy/detect"))).toBe(true);
    expect(calls.some((u) => u.includes("/api/tidy/propose"))).toBe(false);
  });

  it("transitions to error status when /api/tidy/detect returns an error", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      detectResponse([], "No dataset loaded"),
    );

    await tidyStore.start("sales");

    expect(tidyStore.status).toBe("error");
    expect(tidyStore.errorMessage).toBe("No dataset loaded");
  });
});

describe("tidyStore.runAgent", () => {
  it("appends llm proposals to the existing deterministic list", async () => {
    const det = makeProposal({ source: "deterministic" });
    const llm = makeProposal({
      source: "llm",
      target_object_name: "sales_long_llm",
    });
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(detectResponse([det]))
      .mockResolvedValueOnce(
        sseStream([
          "event: llm_started\ndata: {}\n\n",
          `event: llm_proposals\ndata: ${JSON.stringify({
            proposals: [llm],
            parse_warnings: [],
          })}\n\n`,
          "event: done\ndata: {}\n\n",
        ]),
      );

    await tidyStore.start("sales");
    expect(tidyStore.proposals).toHaveLength(1);

    await tidyStore.runAgent();

    expect(tidyStore.status).toBe("loaded_with_llm");
    expect(tidyStore.proposals).toHaveLength(2);
    expect(tidyStore.proposals[1].proposal.source).toBe("llm");
  });

  it("surfaces llm errors as non-fatal warnings and stays in loaded_with_llm", async () => {
    const det = makeProposal();
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(detectResponse([det]))
      .mockResolvedValueOnce(
        sseStream([
          "event: llm_started\ndata: {}\n\n",
          'event: llm_error\ndata: {"error":"provider-down"}\n\n',
          "event: done\ndata: {}\n\n",
        ]),
      );

    await tidyStore.start("sales");
    await tidyStore.runAgent();

    expect(tidyStore.status).toBe("loaded_with_llm");
    expect(tidyStore.parseWarnings).toHaveLength(1);
    expect(tidyStore.parseWarnings[0]).toContain("provider-down");
    // Deterministic proposal stays.
    expect(tidyStore.proposals).toHaveLength(1);
  });
});

describe("tidyStore mutations", () => {
  it("toggles skip and counts only non-skipped, non-applied proposals", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      detectResponse([makeProposal()]),
    );
    await tidyStore.start("sales");
    const id = tidyStore.proposals[0].id;

    // Default: every proposal is pending and counts toward Apply.
    expect(tidyStore.pendingCount).toBe(1);
    tidyStore.toggleSkip(id);
    expect(tidyStore.pendingCount).toBe(0);
    expect(tidyStore.proposals[0].skipped).toBe(true);
    tidyStore.toggleSkip(id);
    expect(tidyStore.pendingCount).toBe(1);
    expect(tidyStore.proposals[0].skipped).toBe(false);
  });

  it("editProposal merges edits without losing other fields", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      detectResponse([makeProposal()]),
    );
    await tidyStore.start("sales");
    const id = tidyStore.proposals[0].id;

    tidyStore.editProposal(id, { target_object_name: "renamed_long" });
    expect(tidyStore.proposals[0].edits.target_object_name).toBe("renamed_long");
    expect(tidyStore.proposals[0].edits.value_column).toBe("sales");
    expect(tidyStore.proposals[0].edits.id_columns).toEqual(["region"]);
  });

  it("flipping mode back to view forces disposition to keep", () => {
    tidyStore.mode = "table";
    tidyStore.dispositionMode = "drop";
    expect(tidyStore.dispositionMode).toBe("drop");
    tidyStore.mode = "view";
    expect(tidyStore.dispositionMode).toBe("keep");
  });

  it("edits.include_nulls defaults from the proposal and survives toggling", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      detectResponse([makeProposal({ include_nulls: true })]),
    );
    await tidyStore.start("sales");
    const id = tidyStore.proposals[0].id;

    expect(tidyStore.proposals[0].edits.include_nulls).toBe(true);
    tidyStore.editProposal(id, { include_nulls: false });
    expect(tidyStore.proposals[0].edits.include_nulls).toBe(false);
    // Other edits remain.
    expect(tidyStore.proposals[0].edits.target_object_name).toBe("sales_long");
  });
});

describe("tidyStore.applyAll", () => {
  async function loadOne(): Promise<string> {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      detectResponse([makeProposal()]),
    );
    await tidyStore.start("sales");
    return tidyStore.proposals[0].id;
  }

  it("posts pending proposals and marks them applied on success", async () => {
    // No skip toggle needed — proposals default to pending.
    await loadOne();

    const applyMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          success: true,
          result: {
            table: "sales",
            target_object_name: "sales_long",
            final_target_name: "sales_long",
            object_type: "view",
            affected_columns: ["sales_2020", "sales_2021"],
            row_count_source: 2,
            row_count_target: 4,
            source_disposition: "keep",
            source_renamed_to: null,
            dry_run: false,
          },
          schema_info: [{ name: "sales_long" }],
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      ),
    );

    const result = await tidyStore.applyAll();

    expect(result).toEqual({ applied: 1, failed: 0 });
    expect(tidyStore.proposals[0].status).toBe("applied");
    expect(tidyStore.proposals[0].applyResult?.row_count_target).toBe(4);

    const applyCall = applyMock.mock.calls.find(
      (c) => c[0] === "/api/tidy/apply",
    );
    expect(applyCall).toBeDefined();
    const body = JSON.parse((applyCall![1] as RequestInit).body as string);
    expect(body.mode).toBe("view");
    expect(body.disposition.mode).toBe("keep");
    // The proposal carries include_nulls so the backend can route through
    // the right SQL builder.
    expect(body.proposal.include_nulls).toBe(true);
  });

  it("forwards the edited include_nulls flag in the apply payload", async () => {
    const id = await loadOne();
    tidyStore.editProposal(id, { include_nulls: false });

    const applyMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          success: true,
          result: {
            table: "sales",
            target_object_name: "sales_long",
            final_target_name: "sales_long",
            object_type: "view",
            affected_columns: ["sales_2020", "sales_2021"],
            row_count_source: 2,
            row_count_target: 3,
            source_disposition: "keep",
            source_renamed_to: null,
            dry_run: false,
          },
          schema_info: [],
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      ),
    );

    await tidyStore.applyAll();

    const applyCall = applyMock.mock.calls.find(
      (c) => c[0] === "/api/tidy/apply",
    );
    const body = JSON.parse((applyCall![1] as RequestInit).body as string);
    expect(body.proposal.include_nulls).toBe(false);
  });

  it("skipped proposals are excluded from the apply queue", async () => {
    // Two proposals: one default (pending), one user-skipped.
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      detectResponse([
        makeProposal({ target_object_name: "sales_long_v1" }),
        makeProposal({ target_object_name: "sales_long_v2" }),
      ]),
    );
    await tidyStore.start("sales");
    const skipId = tidyStore.proposals[1].id;
    tidyStore.toggleSkip(skipId);
    expect(tidyStore.pendingCount).toBe(1);

    const applyMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          success: true,
          result: {
            table: "sales",
            target_object_name: "sales_long_v1",
            final_target_name: "sales_long_v1",
            object_type: "view",
            affected_columns: ["sales_2020", "sales_2021"],
            row_count_source: 2,
            row_count_target: 4,
            source_disposition: "keep",
            source_renamed_to: null,
            dry_run: false,
          },
          schema_info: [],
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      ),
    );

    const result = await tidyStore.applyAll();
    expect(result).toEqual({ applied: 1, failed: 0 });
    // Only one apply call was made — the skipped proposal stayed pending.
    const applyCalls = applyMock.mock.calls.filter(
      (c) => c[0] === "/api/tidy/apply",
    );
    expect(applyCalls).toHaveLength(1);
    expect(tidyStore.proposals[0].status).toBe("applied");
    expect(tidyStore.proposals[1].status).toBe("pending");
    expect(tidyStore.proposals[1].skipped).toBe(true);
  });

  it("marks the proposal apply_error and stops the queue on failure", async () => {
    await loadOne();

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      new Response(
        JSON.stringify({ success: false, error: "schema mismatch" }),
        { status: 200, headers: { "content-type": "application/json" } },
      ),
    );

    const result = await tidyStore.applyAll();

    expect(result).toEqual({ applied: 0, failed: 1 });
    expect(tidyStore.proposals[0].status).toBe("apply_error");
    expect(tidyStore.proposals[0].applyError).toContain("schema mismatch");
  });
});
