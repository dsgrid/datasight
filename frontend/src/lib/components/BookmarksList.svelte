<script lang="ts">
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { deleteBookmark, clearBookmarks } from "$lib/api/saved";
  import { sendMessage } from "$lib/api/chat";

  function runBookmark(sql: string) {
    sendMessage(
      `Run this SQL query and display the results as a table:\n${sql}`,
    );
  }
</script>

<div class="space-y-1">
  {#each sidebarStore.bookmarksCache as bm (bm.id)}
    <div class="group relative">
      <button
        class="w-full text-left py-2 pr-6 border-b border-border/30
          hover:bg-teal/[0.04] transition-colors duration-100 cursor-pointer"
        style="padding-left: 16px; padding-right: 24px;"
        onclick={() => runBookmark(bm.sql)}
      >
        <div class="text-text-primary truncate" style="font-size: 0.82rem; line-height: 1.4;">
          {bm.name || bm.sql.slice(0, 60)}
        </div>
      </button>
      <button
        class="absolute right-1 top-1/2 -translate-y-1/2 text-text-secondary
          opacity-0 group-hover:opacity-60 hover:!opacity-100
          hover:text-red-500 text-xs cursor-pointer transition-opacity"
        onclick={() => deleteBookmark(bm.id)}
      >
        &times;
      </button>
    </div>
  {/each}

  {#if sidebarStore.bookmarksCache.length === 0}
    <div class="text-[11px] text-text-secondary py-2 text-center">
      No bookmarks saved
    </div>
  {:else}
    <button
      class="text-[10px] text-text-secondary hover:text-red-500
        transition-colors cursor-pointer mt-1"
      onclick={clearBookmarks}
    >
      Clear all
    </button>
  {/if}
</div>
