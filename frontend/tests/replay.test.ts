import { describe, it, expect, beforeEach, vi } from "vitest";

/**
 * Tests for edit-and-replay: the first call must carry truncate_before_turn,
 * subsequent replayed prompts must not, and replay stops on error.
 */

interface CapturedRequest {
  body: Record<string, unknown>;
}

function makeFetchStub(
  scriptedResponses: Array<{ events: Array<{ event: string; data: unknown }> }>,
) {
  const requests: CapturedRequest[] = [];
  let callIndex = 0;
  const fetchStub = vi.fn(async (_url: string, init: RequestInit) => {
    const body = JSON.parse(init.body as string) as Record<string, unknown>;
    requests.push({ body });

    const script = scriptedResponses[callIndex++] ?? { events: [] };
    const lines = script.events
      .map((e) => `event: ${e.event}\ndata: ${JSON.stringify(e.data)}\n\n`)
      .join("");

    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(encoder.encode(lines));
        controller.close();
      },
    });
    return new Response(stream, {
      status: 200,
      headers: { "Content-Type": "text/event-stream" },
    });
  });
  return { fetchStub, requests };
}

describe("replayFromEdit", () => {
  beforeEach(async () => {
    vi.resetModules();
    const { chatStore } = await import("$lib/stores/chat.svelte");
    chatStore.clear();
    chatStore.messages = [];
  });

  it("sends truncate_before_turn only on the first replayed call", async () => {
    const okResponse = {
      events: [
        { event: "token", data: { text: "ok" } },
        { event: "done", data: {} },
      ],
    };
    const { fetchStub, requests } = makeFetchStub([
      okResponse,
      okResponse,
      okResponse,
    ]);
    vi.stubGlobal("fetch", fetchStub);

    const { replayFromEdit } = await import("$lib/api/chat");
    await replayFromEdit(1, "edited q1", ["original q2", "original q3"]);

    expect(requests).toHaveLength(3);
    expect(requests[0].body.message).toBe("edited q1");
    expect(requests[0].body.truncate_before_turn).toBe(1);
    expect(requests[1].body.message).toBe("original q2");
    expect(requests[1].body.truncate_before_turn).toBeUndefined();
    expect(requests[2].body.message).toBe("original q3");
    expect(requests[2].body.truncate_before_turn).toBeUndefined();
  });

  it("stops the replay loop after an error event", async () => {
    const okResponse = {
      events: [
        { event: "token", data: { text: "ok" } },
        { event: "done", data: {} },
      ],
    };
    const errorResponse = {
      events: [
        { event: "error", data: { error: "boom" } },
        { event: "done", data: {} },
      ],
    };
    const { fetchStub, requests } = makeFetchStub([
      okResponse,
      errorResponse,
      okResponse,
    ]);
    vi.stubGlobal("fetch", fetchStub);

    const { replayFromEdit } = await import("$lib/api/chat");
    await replayFromEdit(0, "edited q0", ["q1", "q2"]);

    // First two calls happen; third never fires because we stopped.
    expect(requests).toHaveLength(2);
    expect(requests[0].body.message).toBe("edited q0");
    expect(requests[1].body.message).toBe("q1");
  });

  it("omits truncate_before_turn from the body when no replay is in progress", async () => {
    const okResponse = {
      events: [
        { event: "token", data: { text: "ok" } },
        { event: "done", data: {} },
      ],
    };
    const { fetchStub, requests } = makeFetchStub([okResponse]);
    vi.stubGlobal("fetch", fetchStub);

    const { sendMessage } = await import("$lib/api/chat");
    await sendMessage("plain question");

    expect(requests[0].body.message).toBe("plain question");
    expect(requests[0].body).not.toHaveProperty("truncate_before_turn");
  });
});
