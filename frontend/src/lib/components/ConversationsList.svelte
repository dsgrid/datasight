<script lang="ts">
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { chatStore } from "$lib/stores/chat.svelte";
  import { sessionStore } from "$lib/stores/session.svelte";
  import {
    loadConversation,
    clearConversations,
  } from "$lib/api/saved";

  let loadingId = $state<string | null>(null);

  async function handleLoad(sessionId: string) {
    loadingId = sessionId;
    try {
      const data = await loadConversation(sessionId);
      chatStore.clear();

      // Replay events into chat store
      for (const event of data.events) {
        const ev = event as Record<string, unknown>;
        const type = ev.type as string;

        if (type === "user_message") {
          chatStore.pushMessage({
            type: "user_message",
            content: ev.content as string,
          });
        } else if (type === "assistant_message") {
          chatStore.pushMessage({
            type: "assistant_message",
            content: ev.content as string,
          });
        } else if (type === "tool_start") {
          chatStore.pushMessage({
            type: "tool_start",
            tool: ev.tool as string,
            sql: (ev.sql as string) || "",
          });
        } else if (type === "tool_result") {
          chatStore.pushMessage({
            type: "tool_result",
            html: ev.html as string,
            title: (ev.title as string) || "",
            resultType: ((ev.result_type as string) || "table") as "chart" | "table",
          });
        } else if (type === "tool_done") {
          chatStore.pushMessage({
            type: "tool_done",
            meta: (ev.meta as { sql: string; tool: string }) || {
              sql: "",
              tool: "",
            },
          });
        } else if (type === "suggestions") {
          chatStore.pushMessage({
            type: "suggestions",
            suggestions: ev.suggestions as string[],
          });
        }
      }

      // Update session ID to loaded conversation
      sessionStore.sessionId = sessionId;
    } finally {
      loadingId = null;
    }
  }
</script>

<div class="space-y-0.5">
  {#each sidebarStore.conversationsCache as conv (conv.session_id)}
    <button
      class="w-full text-left py-2 border-b border-border/30
        hover:bg-teal/[0.04] transition-colors duration-100 cursor-pointer
        {sessionStore.sessionId === conv.session_id
        ? 'bg-teal/8 border-l-2 border-l-teal'
        : ''}"
      style="padding-left: {sessionStore.sessionId === conv.session_id ? '14px' : '16px'}; padding-right: 16px;"
      onclick={() => handleLoad(conv.session_id)}
    >
      <div class="text-text-primary truncate" style="font-size: 0.82rem; line-height: 1.4;">{conv.title}</div>
      <div class="text-text-secondary" style="font-size: 0.7rem;">
        {conv.message_count} messages
        {#if loadingId === conv.session_id}
          <span class="text-teal ml-1">loading...</span>
        {/if}
      </div>
    </button>
  {/each}

  {#if sidebarStore.conversationsCache.length === 0}
    <div class="text-[11px] text-text-secondary py-2 text-center">
      No conversations saved
    </div>
  {:else}
    <button
      class="text-[10px] text-text-secondary hover:text-red-500
        transition-colors cursor-pointer mt-1"
      onclick={clearConversations}
    >
      Clear all
    </button>
  {/if}
</div>
