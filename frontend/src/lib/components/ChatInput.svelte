<script lang="ts">
  import { chatStore } from "$lib/stores/chat.svelte";
  import { sendMessage } from "$lib/api/chat";

  let inputValue = $state("");
  let textareaEl = $state<HTMLTextAreaElement | null>(null);

  function autoResize() {
    if (!textareaEl) return;
    textareaEl.style.height = "auto";
    textareaEl.style.height =
      Math.min(textareaEl.scrollHeight, window.innerHeight * 0.5) + "px";
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  }

  function handleSubmit() {
    const text = inputValue.trim();
    if (!text || chatStore.isStreaming) return;
    inputValue = "";
    if (textareaEl) {
      textareaEl.style.height = "auto";
    }
    sendMessage(text);
  }

  function handleStop() {
    chatStore.stopGeneration();
  }

  /** Allow external code to send a message (e.g., suggestion/example clicks). */
  export function sendText(text: string) {
    if (chatStore.isStreaming) return;
    sendMessage(text);
  }
</script>

<div class="shrink-0" style="padding: 12px 16px 16px; background: var(--bg); border-top: 1px solid var(--border);">
  <form
    class="flex items-end w-full transition-[border-color,box-shadow] duration-150
      focus-within:border-teal focus-within:shadow-[0_0_0_2px_rgba(21,168,168,0.15)]"
    style="gap: 8px; padding: 10px 10px 10px 16px; border-radius: 16px;
           background: color-mix(in srgb, var(--surface) 96%, var(--bg) 4%);
           border: 1px solid var(--border);
           box-shadow: 0 14px 30px rgba(2,61,96,0.08);"
    onsubmit={(e) => {
      e.preventDefault();
      handleSubmit();
    }}
  >
    <textarea
      bind:this={textareaEl}
      bind:value={inputValue}
      oninput={autoResize}
      onkeydown={handleKeydown}
      data-chat-input
      placeholder="Ask a question about your data..."
      rows="1"
      class="flex-1 bg-transparent text-text-primary resize-vertical
        focus:outline-none"
      style="font-size: 0.925rem; line-height: 1.5; max-height: 50vh; min-height: 24px;
             border: none; outline: none; font-family: inherit;"
    ></textarea>

    {#if chatStore.isStreaming}
      <button
        type="button"
        onclick={handleStop}
        class="flex items-center justify-center shrink-0 text-white cursor-pointer"
        style="width: 38px; height: 38px; border-radius: 8px; border: none;
               background: #dc3545; transition: background 0.15s;"
        title="Stop generating"
        onmouseenter={(e) => e.currentTarget.style.background = '#c82333'}
        onmouseleave={(e) => e.currentTarget.style.background = '#dc3545'}
      >
        <svg width="14" height="14" viewBox="0 0 16 16" fill="white"><rect x="4" y="4" width="8" height="8" rx="1" /></svg>
      </button>
    {:else}
      <button
        type="submit"
        disabled={!inputValue.trim()}
        class="flex items-center justify-center shrink-0 text-white cursor-pointer
          disabled:cursor-not-allowed disabled:opacity-50"
        style="width: 38px; height: 38px; border-radius: 8px; border: none;
               background: {inputValue.trim() ? 'var(--teal)' : 'var(--border)'};
               transition: background 0.15s;"
        title="Send message"
      >
        <svg width="18" height="18" viewBox="0 0 24 24" fill="white"><path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z"/></svg>
      </button>
    {/if}
  </form>
</div>
