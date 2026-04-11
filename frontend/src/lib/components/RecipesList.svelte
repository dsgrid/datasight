<script lang="ts">
  import { schemaStore } from "$lib/stores/schema.svelte";
  import { sendMessage } from "$lib/api/chat";
</script>

<div class="space-y-1">
  {#each schemaStore.recipesCache as recipe, idx (idx)}
    <button
      class="w-full text-left py-2 border-b border-border/30
        hover:bg-teal/[0.04] transition-colors duration-100 cursor-pointer"
      style="padding-left: 16px; padding-right: 16px;"
      onclick={() => sendMessage(recipe.prompt)}
    >
      <div class="font-medium text-text-primary" style="font-size: 0.82rem; line-height: 1.4;">{recipe.title}</div>
      {#if recipe.category}
        <div class="text-text-secondary" style="font-size: 0.7rem;">{recipe.category}</div>
      {/if}
    </button>
  {/each}

  {#if schemaStore.recipesCache.length === 0}
    <div class="text-[11px] text-text-secondary py-2 text-center">
      No recipes available
    </div>
  {/if}
</div>
