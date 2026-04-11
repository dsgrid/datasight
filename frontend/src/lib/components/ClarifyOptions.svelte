<script lang="ts">
  import { sendMessage } from "$lib/api/chat";

  interface Props {
    options: string[];
  }

  let { options }: Props = $props();

  function handleClick(text: string) {
    sendMessage(text);
  }
</script>

<div class="flex flex-wrap max-w-[900px] mx-auto w-full animate-fade-in"
  style="gap: 8px; margin: 8px auto 4px;">
  {#each options as option}
    <button
      class="bg-surface text-teal font-medium cursor-pointer
        hover:bg-teal hover:text-white transition-all duration-150"
      style="border: 1.5px solid var(--teal); border-radius: 16px; padding: 6px 16px;
             font-family: inherit; font-size: 0.82rem;"
      onclick={() => handleClick(option)}
    >
      {option}
    </button>
  {/each}
</div>
