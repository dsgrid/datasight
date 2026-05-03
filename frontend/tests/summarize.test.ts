import { describe, it, expect, beforeEach, vi } from "vitest";
import { summarizeDataset } from "$lib/api/summarize";
import { chatStore } from "$lib/stores/chat.svelte";

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

beforeEach(() => {
  vi.restoreAllMocks();
  chatStore.messages = [];
  chatStore.isStreaming = false;
  chatStore.currentAssistantText = "";
  chatStore.abortController = null;
});

describe("summarizeDataset", () => {
  it("hits /api/summarize and streams tokens after a Dataset Summary header, without pushing a user message", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      sseStream([
        'event: token\ndata: {"text":"Hello"}\n\n',
        'event: token\ndata: {"text":" world"}\n\n',
        "event: done\ndata: {}\n\n",
      ]),
    );

    await summarizeDataset();

    expect(fetchSpy).toHaveBeenCalledWith(
      "/api/summarize",
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    const kinds = chatStore.messages.map((m) => m.type);
    expect(kinds).toEqual(["assistant_message"]);
    const assistant = chatStore.messages[0];
    expect(assistant.type === "assistant_message" && assistant.content).toBe(
      "**Dataset Summary**\n\nHello world",
    );
    expect(chatStore.isStreaming).toBe(false);
  });

  it("pushes an error message when the endpoint emits an error event", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      sseStream(['event: error\ndata: {"error":"boom"}\n\n']),
    );

    await summarizeDataset();

    const errors = chatStore.messages.filter((m) => m.type === "error");
    expect(errors).toHaveLength(1);
    expect(errors[0].type === "error" && errors[0].error).toBe("boom");
    expect(chatStore.isStreaming).toBe(false);
  });

  it("is a no-op while another stream is in flight", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    chatStore.isStreaming = true;
    await summarizeDataset();
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});
