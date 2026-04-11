<script lang="ts">
  import { sendMessage } from "$lib/api/chat";

  interface Props {
    suggestions: string[];
  }

  let { suggestions }: Props = $props();

  function handleClick(text: string) {
    sendMessage(text);
  }
</script>

<div class="flex flex-wrap w-full animate-fade-in"
  style="gap: 8px; padding: 4px 0 12px;">
  {#each suggestions as suggestion}
    <button
      class="bg-surface border border-border text-text-secondary cursor-pointer
        hover:text-teal hover:border-teal hover:bg-teal/[0.06] transition-all duration-150"
      style="border-radius: 16px; padding: 6px 14px; font-family: inherit; font-size: 0.82rem;"
      onclick={() => handleClick(suggestion)}
    >
      {suggestion}
    </button>
  {/each}
</div>
