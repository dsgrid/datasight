<script lang="ts">
  interface Props {
    open: boolean;
    onClose: () => void;
  }

  let { open, onClose }: Props = $props();

  const isMac =
    typeof navigator !== "undefined" && /Mac/.test(navigator.userAgent);
  const mod = isMac ? "⌘" : "Ctrl";

  const shortcuts = [
    { key: `${mod}+K`, desc: "Command palette" },
    { key: `${mod}+B`, desc: "Toggle sidebar" },
    { key: `${mod}+,`, desc: "Settings" },
    { key: "/", desc: "Focus chat input (or SQL editor in SQL view)" },
    { key: "?", desc: "Show shortcuts" },
    { key: "N", desc: "New conversation" },
    { key: "D", desc: "Toggle chat / dashboard" },
    { key: "S", desc: "Toggle chat / SQL editor" },
    { key: `${mod}+Enter`, desc: "Run SQL (in editor)" },
    { key: "Escape", desc: "Exit fullscreen / close modal" },
    { key: "↑ ↓ ← →", desc: "Navigate dashboard cards" },
    { key: "Enter", desc: "Fullscreen selected card" },
    { key: "Delete", desc: "Unpin selected card" },
  ];

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Escape") onClose();
  }
</script>

{#if open}
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-[90] flex items-center justify-center"
    onkeydown={handleKeydown}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div class="absolute inset-0 bg-black/40" onclick={onClose}></div>

    <div
      class="relative bg-surface rounded-xl shadow-xl border border-border
        w-full max-w-sm p-5"
    >
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-sm font-semibold text-text-primary">
          Keyboard Shortcuts
        </h2>
        <button
          class="text-text-secondary hover:text-text-primary cursor-pointer"
          onclick={onClose}
        >
          &times;
        </button>
      </div>

      <div class="space-y-1.5">
        {#each shortcuts as s}
          <div class="flex items-center justify-between">
            <span class="text-xs text-text-primary">{s.desc}</span>
            <kbd
              class="px-1.5 py-0.5 text-[10px] font-mono rounded border
                border-border bg-surface-alt text-text-secondary"
            >
              {s.key}
            </kbd>
          </div>
        {/each}
      </div>
    </div>
  </div>
{/if}
