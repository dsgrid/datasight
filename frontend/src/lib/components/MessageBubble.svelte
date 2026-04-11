<script lang="ts">
  import { renderMarkdown } from "$lib/utils/markdown";
  import { chatStore } from "$lib/stores/chat.svelte";
  import { tick } from "svelte";

  interface Props {
    role: "user" | "assistant";
    content: string;
    /** If true, this is the active streaming bubble — use currentAssistantText. */
    streaming?: boolean;
    onDelete?: () => void;
    onCopy?: () => void;
    onDeleteBlock?: () => void;
  }

  let {
    role,
    content,
    streaming = false,
    onDelete,
    onCopy,
    onDeleteBlock,
  }: Props = $props();

  let bubbleEl = $state<HTMLElement | null>(null);
  let copied = $state(false);

  let displayText = $derived(
    streaming ? chatStore.currentAssistantText : content,
  );
  let renderedHtml = $derived(
    role === "assistant" ? renderMarkdown(displayText || "") : "",
  );

  function handleCopy() {
    if (!onCopy) return;
    onCopy();
    copied = true;
    setTimeout(() => (copied = false), 1200);
  }

  /** Add copy buttons to code blocks after render. */
  function addCopyButtons(node: HTMLElement) {
    const observer = new MutationObserver(() => {
      node.querySelectorAll("pre").forEach((pre) => {
        if (pre.querySelector(".copy-code-btn")) return;
        const btn = document.createElement("button");
        btn.className = "copy-code-btn";
        btn.textContent = "Copy";
        btn.addEventListener("click", () => {
          const code = pre.querySelector("code");
          const text = code ? code.textContent : pre.textContent;
          navigator.clipboard.writeText(text || "").then(() => {
            btn.textContent = "Copied!";
            setTimeout(() => (btn.textContent = "Copy"), 1500);
          });
        });
        pre.style.position = "relative";
        pre.appendChild(btn);
      });
    });
    observer.observe(node, { childList: true, subtree: true });
    // Initial pass
    node.querySelectorAll("pre").forEach((pre) => {
      if (pre.querySelector(".copy-code-btn")) return;
      const btn = document.createElement("button");
      btn.className = "copy-code-btn";
      btn.textContent = "Copy";
      btn.addEventListener("click", () => {
        const code = pre.querySelector("code");
        const text = code ? code.textContent : pre.textContent;
        navigator.clipboard.writeText(text || "").then(() => {
          btn.textContent = "Copied!";
          setTimeout(() => (btn.textContent = "Copy"), 1500);
        });
      });
      pre.style.position = "relative";
      pre.appendChild(btn);
    });
    return { destroy: () => observer.disconnect() };
  }
</script>

<div class="message-row flex min-w-0 max-w-[900px] mx-auto w-full animate-fade-in {role === 'user' ? 'justify-end' : ''} group"
  style="margin-bottom: 18px;">
  {#if role === "user"}
    <!-- User actions -->
    <div class="flex items-start gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
      {#if onCopy}
        <button
          class="p-1 rounded hover:bg-surface-alt text-text-secondary cursor-pointer"
          title="Copy prompt"
          onclick={handleCopy}
        >
          {#if copied}
            <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M3.5 8.5l3 3 6-7" /></svg>
          {:else}
            <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="5" width="9" height="9" rx="1.5" /><path d="M5 11H3.5A1.5 1.5 0 0 1 2 9.5v-7A1.5 1.5 0 0 1 3.5 1h7A1.5 1.5 0 0 1 12 2.5V5" /></svg>
          {/if}
        </button>
      {/if}
      {#if onDeleteBlock}
        <button
          class="p-1 rounded hover:bg-surface-alt text-text-secondary cursor-pointer"
          title="Delete question and responses"
          onclick={onDeleteBlock}
        >
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M2 4h12M5.33 4V2.67a1.33 1.33 0 0 1 1.34-1.34h2.66a1.33 1.33 0 0 1 1.34 1.34V4M12.67 4v9.33a1.33 1.33 0 0 1-1.34 1.34H4.67a1.33 1.33 0 0 1-1.34-1.34V4" /></svg>
        </button>
      {/if}
    </div>

    <div class="message-bubble max-w-[85%] rounded-xl bg-user-bg text-user-text whitespace-pre-wrap break-words"
      style="padding: 12px 16px; border-bottom-right-radius: 4px; font-size: 0.925rem; line-height: 1.6;">
      {content}
    </div>
  {:else}
    <!-- Assistant bubble -->
    <div class="max-w-[85%] min-w-0 relative">
      {#if onDelete}
        <button
          class="absolute -left-6 top-1 p-1 rounded hover:bg-surface-alt text-text-secondary
            opacity-0 group-hover:opacity-100 transition-opacity cursor-pointer"
          title="Delete response"
          onclick={onDelete}
        >
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"><path d="M2 4h12M5.33 4V2.67a1.33 1.33 0 0 1 1.34-1.34h2.66a1.33 1.33 0 0 1 1.34 1.34V4M12.67 4v9.33a1.33 1.33 0 0 1-1.34 1.34H4.67a1.33 1.33 0 0 1-1.34-1.34V4" /></svg>
        </button>
      {/if}
      <div
        bind:this={bubbleEl}
        use:addCopyButtons
        class="message-bubble rounded-xl bg-assistant-bg text-assistant-text
          border border-border
          prose prose-sm max-w-none overflow-x-auto
          [&_pre]:bg-bg [&_pre]:border [&_pre]:border-border [&_pre]:rounded-lg [&_pre]:p-3 [&_pre]:overflow-x-auto [&_pre]:my-2
          [&_pre_code]:bg-transparent [&_pre_code]:p-0 [&_pre_code]:text-[0.82em] [&_pre_code]:leading-relaxed
          [&_code]:font-mono [&_code]:text-[0.85em] [&_code]:bg-bg [&_code]:px-1 [&_code]:py-0.5 [&_code]:rounded
          [&_table]:w-full [&_th]:text-left [&_th]:text-xs [&_th]:font-semibold
          [&_td]:text-xs [&_td]:py-1 [&_td]:border-b [&_td]:border-border
          [&_p]:mb-[0.6em] [&_p:last-child]:mb-0
          [&_ul]:my-1 [&_ul]:ml-5 [&_ol]:my-1 [&_ol]:ml-5
          [&_li]:mb-0.5 [&_strong]:font-semibold"
        style="padding: 12px 16px; border-bottom-left-radius: 4px; font-size: 0.925rem; line-height: 1.6;
               box-shadow: 0 10px 24px rgba(2,61,96,0.06);"
      >
        {@html renderedHtml}
      </div>
    </div>
  {/if}
</div>

<style>
  :global(.copy-code-btn) {
    position: absolute;
    top: 4px;
    right: 4px;
    padding: 2px 8px;
    font-size: 0.65rem;
    border-radius: 6px;
    background: var(--border);
    color: var(--text-secondary);
    border: none;
    cursor: pointer;
    opacity: 0;
    transition: opacity 0.15s;
  }
  :global(pre:hover .copy-code-btn) {
    opacity: 1;
  }
</style>
