/** Streams a dataset summary from /api/summarize into the chat view.
 *
 * This bypasses the normal agent loop and renders the response as a plain
 * assistant message. The endpoint returns SSE `token`, `done`, `error`
 * events — same shape as `/api/chat` for those three events.
 */

import { chatStore } from "$lib/stores/chat.svelte";

const HEADER = "**Dataset Summary**\n\n";

export async function summarizeDataset(): Promise<void> {
  if (chatStore.isStreaming) return;

  chatStore.isStreaming = true;
  chatStore.currentAssistantText = HEADER;
  chatStore.pushMessage({ type: "assistant_message", content: HEADER });

  const controller = new AbortController();
  chatStore.abortController = controller;
  chatStore.pushMessage({ type: "typing" });

  try {
    const resp = await fetch("/api/summarize", { signal: controller.signal });
    if (!resp.ok) throw new Error(`Summarize API error: ${resp.status}`);

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

      let eventType = "";
      for (const line of lines) {
        if (line.startsWith("event: ")) {
          eventType = line.slice(7).trim();
        } else if (line.startsWith("data: ")) {
          if (!typingRemoved) {
            chatStore.messages = chatStore.messages.filter(
              (m) => m.type !== "typing",
            );
            typingRemoved = true;
          }
          try {
            const data = JSON.parse(line.slice(6));
            if (eventType === "token" && typeof data.text === "string") {
              chatStore.currentAssistantText += data.text;
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
            } else if (eventType === "error" && data.error) {
              chatStore.pushMessage({ type: "error", error: String(data.error) });
            }
          } catch {
            console.error("Failed to parse SSE data:", line);
          }
          eventType = "";
        }
      }
    }

    if (!typingRemoved) {
      chatStore.messages = chatStore.messages.filter((m) => m.type !== "typing");
    }

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
      chatStore.currentAssistantText = "";
    }
  } catch (err) {
    chatStore.messages = chatStore.messages.filter((m) => m.type !== "typing");
    if (err instanceof Error && err.name === "AbortError") {
      chatStore.pushMessage({
        type: "assistant_message",
        content: "Summary generation stopped.",
      });
    } else {
      console.error("Summarize error:", err);
      chatStore.pushMessage({
        type: "error",
        error: "Failed to generate summary. Please try again.",
      });
    }
  }

  chatStore.isStreaming = false;
  chatStore.abortController = null;
}
