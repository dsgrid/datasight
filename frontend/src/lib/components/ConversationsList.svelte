<script lang="ts">
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { sessionStore } from "$lib/stores/session.svelte";
  import { clearConversations } from "$lib/api/saved";
  import { switchConversation } from "$lib/utils/conversation";

  let loadingId = $state<string | null>(null);

  async function handleLoad(sessionId: string) {
    loadingId = sessionId;
    try {
      await switchConversation(sessionId);
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
