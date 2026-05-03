<script lang="ts">
  import { chatStore } from "$lib/stores/chat.svelte";
  import { dashboardStore } from "$lib/stores/dashboard.svelte";
  import { schemaStore } from "$lib/stores/schema.svelte";
  import { settingsStore } from "$lib/stores/settings.svelte";
  import type { ChatEvent } from "$lib/stores/chat.svelte";
  import { sendMessage, loadPlotlySpec, replayFromEdit } from "$lib/api/chat";
  import { summarizeDataset } from "$lib/api/summarize";
  import { addBookmark } from "$lib/api/saved";
  import { addReport } from "$lib/api/saved";
  import { saveDashboard } from "$lib/api/dashboard";
  import MessageBubble from "./MessageBubble.svelte";
  import ToolIndicator from "./ToolIndicator.svelte";
  import ChartResult from "./ChartResult.svelte";
  import TableResult from "./TableResult.svelte";
  import SqlConfirmDialog from "./SqlConfirmDialog.svelte";
  import SuggestionButtons from "./SuggestionButtons.svelte";
  import ClarifyOptions from "./ClarifyOptions.svelte";
  import StarterOverview from "./StarterOverview.svelte";
  import RunDetails from "./RunDetails.svelte";
  import { tick } from "svelte";

  interface Props {
    exportMode?: boolean;
    excludeIndices?: Set<number>;
    onToggleExclude?: (idx: number) => void;
  }

  let {
    exportMode = false,
    excludeIndices = new Set<number>(),
    onToggleExclude,
  }: Props = $props();

  // Map each event to its turn index. A turn is a user_message and the
  // tool/assistant events that follow it. Backend export semantics treat
  // exclude_indices as turn ordinals (incremented per user_message), so the
  // checkbox lives on the user_message row and dims the whole turn.
  let turnIndices = $derived.by(() => {
    const result: number[] = [];
    let count = -1;
    for (const m of chatStore.messages) {
      if (m.type === "user_message") count++;
      result.push(count);
    }
    return result;
  });

  let messagesEl = $state<HTMLElement | null>(null);

  /** Auto-scroll to bottom on new messages. */
  $effect(() => {
    // Access messages to track changes
    const _len = chatStore.messages.length;
    const _text = chatStore.currentAssistantText;
    tick().then(() => {
      if (messagesEl) {
        messagesEl.scrollTop = messagesEl.scrollHeight;
      }
    });
  });

  function deleteMessage(index: number) {
    chatStore.removeMessage(index);
  }

  function deleteUserBlock(index: number) {
    // Remove user message and all following messages until next user message
    const messages = [...chatStore.messages];
    let end = index + 1;
    while (end < messages.length && messages[end].type !== "user_message") {
      end++;
    }
    chatStore.messages = [
      ...messages.slice(0, index),
      ...messages.slice(end),
    ];
  }

  async function editAndReplay(index: number, newText: string) {
    if (chatStore.isStreaming) return;
    const turnIdx = turnIndices[index];
    if (turnIdx == null || turnIdx < 0) return;

    // Capture every later user prompt before truncating locally.
    const subsequent: string[] = [];
    for (let i = index + 1; i < chatStore.messages.length; i++) {
      const msg = chatStore.messages[i];
      if (msg.type === "user_message") subsequent.push(msg.content);
    }

    chatStore.messages = chatStore.messages.slice(0, index);
    await replayFromEdit(turnIdx, newText, subsequent);
  }

  async function pinResult(
    event: ChatEvent & { type: "tool_result" },
    toolCtx: { sql: string; tool: string; plotlySpec: unknown; meta?: Record<string, unknown> } | null,
  ) {
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
      tool: toolCtx?.tool || (event.resultType === "chart" ? "visualize_data" : "run_sql"),
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

  function bookmarkResult(sql: string, tool: string, name: string) {
    addBookmark(sql, tool, name);
  }

  function saveReportFromResult(
    sql: string,
    tool: string,
    name: string,
    plotlySpec?: unknown,
  ) {
    addReport(sql, tool, name, plotlySpec);
  }

  /** Check if a message is the active streaming assistant bubble. */
  function isStreamingBubble(event: ChatEvent, index: number): boolean {
    if (event.type !== "assistant_message") return false;
    if (!chatStore.isStreaming) return false;
    // It's the last assistant_message in the list
    for (let i = chatStore.messages.length - 1; i >= 0; i--) {
      if (chatStore.messages[i].type === "assistant_message") {
        return i === index;
      }
    }
    return false;
  }

  /** Get the last sql/tool info for action buttons. */
  function getLastToolContext(
    upToIndex: number,
  ): { sql: string; tool: string; plotlySpec: unknown; meta?: Record<string, unknown> } | null {
    let toolMeta: Record<string, unknown> | undefined;
    for (let i = upToIndex + 1; i < chatStore.messages.length; i++) {
      const msg = chatStore.messages[i];
      if (msg.type === "tool_done") {
        toolMeta = msg.meta as unknown as Record<string, unknown>;
        break;
      }
      if (msg.type === "tool_start" || msg.type === "user_message") break;
    }

    for (let i = upToIndex; i >= 0; i--) {
      const msg = chatStore.messages[i];
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
</script>

<div
  bind:this={messagesEl}
  class="flex-1 overflow-y-auto overflow-x-hidden min-w-0"
  style="padding: 28px 18px; scroll-behavior: smooth;"
>
  {#if chatStore.messages.length === 0}
    <!-- Welcome screen -->
    <div class="flex items-center justify-center h-full">
      <div
        class="flex flex-col items-center text-center text-text-secondary"
        style="max-width: 560px; padding: 48px 24px;"
      >
        <h2 class="text-teal" style="font-size: 1.5rem; font-weight: 600; margin-bottom: 8px;">
          Welcome to datasight
        </h2>
        <p style="font-size: 0.95rem; line-height: 1.5; margin-bottom: 20px;">
          Ask questions about your data in plain English. I can query the
          database, analyze results, and create visualizations.
        </p>
        <p style="font-size: 0.85rem; margin-bottom: 20px;">
          Browse tables in the sidebar, or try one of these:
        </p>
        <div class="flex flex-col w-full" style="gap: 8px;">
          <button
            class="w-full text-left bg-surface border border-border
              text-text-primary cursor-pointer transition-all
              hover:border-teal hover:shadow-[0_0_0_1px_var(--teal)]"
            style="padding: 10px 16px; border-radius: 8px; font-family: inherit; font-size: 0.88rem;"
            onclick={() => sendMessage("What tables are available and how many rows do they have?")}
          >
            What tables are available and how many rows do they have?
          </button>
          <button
            class="w-full text-left bg-surface border border-border
              text-text-primary cursor-pointer transition-all
              hover:border-teal hover:shadow-[0_0_0_1px_var(--teal)]"
            style="padding: 10px 16px; border-radius: 8px; font-family: inherit; font-size: 0.88rem;"
            onclick={() => summarizeDataset()}
          >
            Show me a summary of the data
          </button>
        </div>
      </div>
    </div>
  {/if}

  {#each chatStore.messages as event, idx (idx)}
    {@const turnIdx = turnIndices[idx]}
    {@const excluded = turnIdx >= 0 && excludeIndices.has(turnIdx)}
    {@const isTurnAnchor = event.type === "user_message"}
    <div
      class="flex items-start gap-2 transition-opacity"
      style:opacity={exportMode && excluded ? "0.4" : "1"}
    >
      {#if exportMode && isTurnAnchor}
        <input
          type="checkbox"
          checked={!excluded}
          title={excluded ? "Include turn in export" : "Exclude turn from export"}
          aria-label={excluded
            ? "Include this turn in export"
            : "Exclude this turn from export"}
          onchange={() => onToggleExclude?.(turnIdx)}
          class="cursor-pointer flex-shrink-0"
          style="margin-top: 14px; accent-color: var(--teal);
            width: 16px; height: 16px;"
        />
      {:else if exportMode}
        <div class="flex-shrink-0" style="width: 16px;" aria-hidden="true"></div>
      {/if}
      <div class="flex-1 min-w-0">
    {#if event.type === "user_message"}
      <MessageBubble
        role="user"
        content={event.content}
        onCopy={() => navigator.clipboard.writeText(event.content)}
        onDeleteBlock={() => deleteUserBlock(idx)}
        onEdit={(newText) => editAndReplay(idx, newText)}
      />
    {:else if event.type === "assistant_message"}
      <MessageBubble
        role="assistant"
        content={event.content}
        streaming={isStreamingBubble(event, idx)}
        onDelete={() => deleteMessage(idx)}
      />
    {:else if event.type === "tool_start"}
      <ToolIndicator
        tool={event.tool}
        sql={event.sql}
        onDelete={() => deleteMessage(idx)}
      />
    {:else if event.type === "tool_result"}
      {@const toolCtx = getLastToolContext(idx)}
      {#if event.resultType === "chart"}
        <ChartResult
          html={event.html}
          plotlySpec={event.plotlySpec}
          title={event.title}
          onPin={() => pinResult(event, toolCtx)}
          onBookmark={toolCtx
            ? () =>
                bookmarkResult(
                  toolCtx.sql,
                  "visualize_data",
                  event.title || "",
                )
            : undefined}
          onSaveReport={toolCtx
            ? () =>
                saveReportFromResult(
                  toolCtx.sql,
                  toolCtx.tool,
                  event.title || "",
                  toolCtx.plotlySpec,
                )
            : undefined}
          onDelete={() => deleteMessage(idx)}
        />
      {:else}
        <TableResult
          html={event.html}
          title={event.title}
          onPin={() => pinResult(event, toolCtx)}
          onBookmark={toolCtx
            ? () =>
                bookmarkResult(
                  toolCtx.sql,
                  "run_sql",
                  event.title || "",
                )
            : undefined}
          onSaveReport={toolCtx
            ? () =>
                saveReportFromResult(
                  toolCtx.sql,
                  toolCtx.tool,
                  event.title || "",
                )
            : undefined}
          onDelete={() => deleteMessage(idx)}
        />
      {/if}
    {:else if event.type === "sql_confirm"}
      <SqlConfirmDialog sql={event.sql} requestId={event.requestId} />
    {:else if event.type === "suggestions"}
      <SuggestionButtons suggestions={event.suggestions} />
    {:else if event.type === "clarify_options"}
      <ClarifyOptions options={event.options} />
    {:else if event.type === "starter_overview"}
      <StarterOverview kind={event.kind} overview={event.overview} />
    {:else if event.type === "provenance" && settingsStore.showProvenance}
      <RunDetails provenance={event.provenance} />
    {:else if event.type === "error"}
      <div class="px-4 py-3 rounded-lg border mb-4 w-full animate-fade-in"
        style="color: var(--orange); background: rgba(254,93,38,0.06); border-color: rgba(254,93,38,0.2); font-size: 0.85rem;">
        {event.error}
      </div>
    {:else if event.type === "typing"}
      <div class="w-full mb-4 animate-fade-in">
        <div class="inline-flex px-4 py-3">
          <div class="flex items-center" style="gap: 5px; height: 16px;">
            <span class="rounded-full bg-teal" style="width: 7px; height: 7px; animation: typing-bounce 1.4s ease-in-out infinite;"></span>
            <span class="rounded-full bg-teal" style="width: 7px; height: 7px; animation: typing-bounce 1.4s ease-in-out 0.2s infinite;"></span>
            <span class="rounded-full bg-teal" style="width: 7px; height: 7px; animation: typing-bounce 1.4s ease-in-out 0.4s infinite;"></span>
          </div>
        </div>
      </div>
    {/if}
      </div>
    </div>
  {/each}
</div>
