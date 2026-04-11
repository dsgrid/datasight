<script lang="ts">
  import type { ViewMode } from "$lib/stores/dashboard.svelte";
  import { queriesStore } from "$lib/stores/queries.svelte";
  import { settingsStore } from "$lib/stores/settings.svelte";
  import { formatCost } from "$lib/utils/format";

  interface Props {
    theme: string;
    onToggleTheme: () => void;
    onToggleSettings?: () => void;
    onToggleProjects?: () => void;
    onToggleSidebar?: () => void;
    onToggleSqlPanel?: () => void;
    onToggleExport?: () => void;
    projectLoaded?: boolean;
    currentView?: ViewMode;
    onSwitchView?: (view: ViewMode) => void;
    isEphemeral?: boolean;
    projectName?: string | null;
    tableCount?: number;
    sqlPanelOpen?: boolean;
  }

  let {
    theme,
    onToggleTheme,
    onToggleSettings,
    onToggleProjects,
    onToggleSidebar,
    onToggleSqlPanel,
    onToggleExport,
    projectLoaded = false,
    currentView = "chat",
    onSwitchView,
    isEphemeral = false,
    projectName = null,
    tableCount = 0,
    sqlPanelOpen = false,
  }: Props = $props();

  let showCost = $derived(
    settingsStore.showCost && queriesStore.sessionTotalCost > 0,
  );
</script>

<header
  class="flex items-center justify-between text-cream z-20 shrink-0"
  style="padding: 14px 20px;
         background: linear-gradient(135deg, color-mix(in srgb, var(--navy) 94%, black 6%), var(--navy));
         border-bottom: 1px solid rgba(231,225,207,0.08);
         box-shadow: 0 10px 30px rgba(2,61,96,0.18);"
>
  <!-- Left -->
  <div class="flex items-center gap-3 min-w-0">
    <button
      class="btn-icon"
      title="Toggle sidebar"
      onclick={onToggleSidebar}
    >
      <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
        <rect x="1" y="2.5" width="14" height="1.5" rx="0.75" fill="currentColor" />
        <rect x="1" y="7.25" width="14" height="1.5" rx="0.75" fill="currentColor" />
        <rect x="1" y="12" width="14" height="1.5" rx="0.75" fill="currentColor" />
      </svg>
    </button>

    <button
      class="flex items-center cursor-pointer hover:opacity-80 transition-opacity"
      style="gap: 10px;"
      onclick={onToggleProjects}
      title="Switch project"
    >
      <img
        src="/datasight-icon.svg"
        alt="datasight"
        style="width: 24px; height: 24px; border-radius: 7px;
               box-shadow: 0 2px 10px rgba(2,61,96,0.22);"
      />
      <span class="font-bold" style="font-size: 1.35rem; letter-spacing: -0.02em;">
        data<span class="text-teal">sight</span>
      </span>
    </button>

    {#if onToggleProjects}
      <button
        class="btn-icon"
        title="Switch project"
        onclick={onToggleProjects}
      >
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
          <path d="M2 4h12M2 4l3-2M2 4l3 2" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round" />
          <path d="M14 12H2M14 12l-3-2M14 12l-3 2" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round" />
        </svg>
      </button>
    {/if}

    <!-- Session indicator -->
    {#if projectLoaded}
      {#if isEphemeral}
        <div
          class="flex items-center whitespace-nowrap"
          style="gap: 8px; padding: 4px 12px; border-radius: 6px; font-size: 0.75rem; font-weight: 500;
                 max-width: 300px; overflow: hidden;
                 background: color-mix(in srgb, var(--orange) 20%, transparent); color: var(--orange);"
        >
          <span style="text-transform: uppercase; font-size: 0.6rem; letter-spacing: 0.05em; opacity: 0.8;">Explore</span>
          <span style="overflow: hidden; text-overflow: ellipsis;">{tableCount} table(s)</span>
        </div>
      {:else if projectName}
        <div
          class="flex items-center whitespace-nowrap"
          style="gap: 8px; padding: 4px 12px; border-radius: 6px; font-size: 0.75rem; font-weight: 500;
                 max-width: 300px; overflow: hidden;
                 background: color-mix(in srgb, var(--teal) 15%, transparent); color: var(--teal);"
        >
          <span style="text-transform: uppercase; font-size: 0.6rem; letter-spacing: 0.05em; opacity: 0.8;">Project</span>
          <span style="overflow: hidden; text-overflow: ellipsis;">{projectName}</span>
        </div>
      {/if}
    {/if}
  </div>

  <!-- Center: View tabs -->
  {#if projectLoaded}
    <div class="flex items-center gap-1">
      <button
        class="font-medium transition-all duration-150 cursor-pointer"
        style="font-size: 0.78rem; padding: 5px 14px; border-radius: 5px;
               font-family: inherit;
               border: 1px solid {currentView === 'chat' ? 'var(--teal)' : 'rgba(231,225,207,0.2)'};
               background: {currentView === 'chat' ? 'rgba(21,168,168,0.2)' : 'transparent'};
               color: var(--cream);
               opacity: {currentView === 'chat' ? '1' : '0.65'};"
        onclick={() => onSwitchView?.("chat")}
      >
        Chat
      </button>
      <button
        class="font-medium transition-all duration-150 cursor-pointer"
        style="font-size: 0.78rem; padding: 5px 14px; border-radius: 5px;
               font-family: inherit;
               border: 1px solid {currentView === 'dashboard' ? 'var(--teal)' : 'rgba(231,225,207,0.2)'};
               background: {currentView === 'dashboard' ? 'rgba(21,168,168,0.2)' : 'transparent'};
               color: var(--cream);
               opacity: {currentView === 'dashboard' ? '1' : '0.65'};"
        onclick={() => onSwitchView?.("dashboard")}
      >
        Dashboard
      </button>
    </div>
  {/if}

  <!-- Right: Actions -->
  <div class="flex items-center" style="gap: 10px;">
    {#if showCost}
      <span class="text-xs" style="color: rgba(231,225,207,0.6); margin-right: 8px;">
        session {formatCost(queriesStore.sessionTotalCost)}
      </span>
    {/if}

    {#if projectLoaded}
      <div class="inline-flex items-center"
        style="gap: 8px; padding: 4px; border-radius: 12px;
               background: rgba(255,255,255,0.08);
               box-shadow: inset 0 1px 0 rgba(255,255,255,0.08);">
        <button
          class="btn-icon {sqlPanelOpen ? 'active' : ''}"
          title="SQL History"
          onclick={onToggleSqlPanel}
        >
          SQL
        </button>
        <button
          class="btn-icon"
          title="Export conversation"
          onclick={onToggleExport}
        >
          Export
        </button>
      </div>
    {/if}

    <div class="inline-flex items-center"
      style="gap: 8px; padding: 4px; border-radius: 12px;
             background: rgba(255,255,255,0.03);">
      <button
        class="btn-icon"
        title="Toggle theme"
        onclick={onToggleTheme}
      >
        {#if theme === "dark"}
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3">
            <circle cx="8" cy="8" r="3.5" />
            <path d="M8 1v2M8 13v2M1 8h2M13 8h2M3.05 3.05l1.41 1.41M11.54 11.54l1.41 1.41M3.05 12.95l1.41-1.41M11.54 4.46l1.41-1.41" stroke-linecap="round" />
          </svg>
        {:else}
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3">
            <path d="M13.36 10.06A6 6 0 015.94 2.64a6 6 0 107.42 7.42z" stroke-linejoin="round" />
          </svg>
        {/if}
      </button>

      {#if onToggleSettings}
        <button
          class="btn-icon"
          title="Settings"
          onclick={onToggleSettings}
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.3">
            <path d="M8 10a2 2 0 100-4 2 2 0 000 4z" />
            <path d="M13.5 8c0-.3 0-.5-.1-.8l1.4-1.1-1.5-2.6-1.7.7c-.4-.3-.8-.6-1.3-.8L10 1.6H7l-.3 1.8c-.5.2-.9.5-1.3.8l-1.7-.7-1.5 2.6 1.4 1.1c-.1.3-.1.5-.1.8s0 .5.1.8l-1.4 1.1 1.5 2.6 1.7-.7c.4.3.8.6 1.3.8l.3 1.8h3l.3-1.8c.5-.2.9-.5 1.3-.8l1.7.7 1.5-2.6-1.4-1.1c.1-.3.1-.5.1-.8z" stroke-linejoin="round" />
          </svg>
        </button>
      {/if}

      <a
        class="btn-icon text-cream no-underline"
        href="https://dsgrid.github.io/datasight/"
        target="_blank"
        rel="noopener"
        title="Documentation"
      >
        Docs
      </a>
    </div>
  </div>
</header>
