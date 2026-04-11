<script lang="ts">
  import type { Snippet } from "svelte";

  interface Props {
    title: string;
    count?: number;
    id: string;
    children: Snippet;
  }

  let { title, count, id, children }: Props = $props();

  const STORAGE_KEY = "datasight-collapsed-sections";

  function loadCollapsed(): boolean {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const sections: string[] = JSON.parse(stored);
        return sections.includes(id);
      }
    } catch {
      // ignore
    }
    return false;
  }

  function saveCollapsed(collapsed: boolean) {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      let sections: string[] = stored ? JSON.parse(stored) : [];
      if (collapsed) {
        if (!sections.includes(id)) sections.push(id);
      } else {
        sections = sections.filter((s) => s !== id);
      }
      localStorage.setItem(STORAGE_KEY, JSON.stringify(sections));
    } catch {
      // ignore
    }
  }

  let collapsed = $state(loadCollapsed());

  function toggle() {
    collapsed = !collapsed;
    saveCollapsed(collapsed);
  }
</script>

<section>
  <button
    class="flex items-center w-full uppercase
      text-text-secondary hover:bg-surface-alt transition-colors duration-100
      cursor-pointer select-none"
    style="padding: 14px 16px 10px; font-size: 0.72rem; font-weight: 600; letter-spacing: 0.06em;
           border-bottom: 1px solid color-mix(in srgb, var(--border) 88%, transparent);"
    onclick={toggle}
  >
    <span
      class="inline-block transition-transform duration-200
        {collapsed ? '-rotate-90' : ''}"
      style="font-size: 0.6rem; margin-right: 4px;"
    >&#9660;</span>
    <span class="flex-1 text-left">{title}</span>
    {#if count !== undefined}
      <span class="font-normal text-text-secondary" style="font-size: 0.68rem;">{count}</span>
    {/if}
  </button>

  {#if !collapsed}
    <div>
      {@render children()}
    </div>
  {/if}
</section>
