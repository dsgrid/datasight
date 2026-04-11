<script lang="ts">
  import { loadProject, loadRecentProjects } from "$lib/api/projects";
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { onMount } from "svelte";

  interface Props {
    open: boolean;
    onClose: () => void;
    onProjectLoaded: (path: string) => void;
  }

  let { open, onClose, onProjectLoaded }: Props = $props();

  let path = $state("");
  let loading = $state(false);
  let error = $state("");

  $effect(() => {
    if (open) {
      loadRecentProjects().catch(() => {});
    }
  });

  async function handleOpen(projectPath?: string) {
    error = "";
    const target = projectPath || path.trim();
    if (!target) {
      error = "Please enter a project path";
      return;
    }

    loading = true;
    try {
      await loadProject(target);
      path = "";
      onProjectLoaded(target);
      onClose();
    } catch (e) {
      error = e instanceof Error ? e.message : "Failed to load project";
    } finally {
      loading = false;
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Escape") onClose();
    if (e.key === "Enter") handleOpen();
  }
</script>

{#if open}
  <!-- Overlay -->
  <button
    class="fixed inset-0 bg-black/30 z-40 cursor-default"
    onclick={onClose}
    tabindex="-1"
    aria-label="Close projects"
  ></button>

  <!-- Panel -->
  <div
    class="fixed left-0 top-0 bottom-0 bg-surface border-r border-border
      z-50 flex flex-col"
    style="width: 360px; max-width: 90vw; box-shadow: 4px 0 20px rgba(0,0,0,0.15);"
  >
    <div class="flex items-center justify-between border-b border-border"
      style="padding: 16px 20px 14px;">
      <span class="font-semibold" style="font-size: 1rem;">Projects</span>
      <button
        onclick={onClose}
        class="p-1.5 rounded-lg hover:bg-surface-alt transition-colors cursor-pointer text-text-secondary"
        aria-label="Close"
      >
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round">
          <path d="M4 4l8 8M12 4l-8 8" />
        </svg>
      </button>
    </div>

    <div class="flex-1 overflow-y-auto" style="padding: 16px 20px;">
    <div class="space-y-5">
      <!-- Recent projects -->
      {#if sidebarStore.recentProjectsCache.length > 0}
        <div class="space-y-1">
          <div class="text-xs font-semibold uppercase tracking-wider text-text-secondary mb-2">
            Recent
          </div>
          {#each sidebarStore.recentProjectsCache as project}
            <button
              class="w-full text-left px-3 py-2.5 rounded-lg transition-colors cursor-pointer group
                {project.is_current
                ? 'bg-teal/5 border border-teal/30'
                : 'hover:bg-surface-alt'}"
              onclick={() => handleOpen(project.path)}
            >
              <span class="block text-sm font-medium text-text-primary group-hover:text-teal transition-colors">
                {project.name}
                {#if project.is_current}
                  <span class="text-xs text-teal ml-1">(current)</span>
                {/if}
              </span>
              <span class="block text-xs text-text-secondary truncate">
                {project.path}
              </span>
            </button>
          {/each}
        </div>
      {:else}
        <p class="text-xs text-text-secondary">No recent projects</p>
      {/if}

      <!-- Open by path -->
      <div class="space-y-2">
        <div class="text-xs font-semibold uppercase tracking-wider text-text-secondary">
          Open by path
        </div>
        <div class="flex" style="gap: 8px;">
          <input
            type="text"
            bind:value={path}
            onkeydown={handleKeydown}
            placeholder="Project directory..."
            class="flex-1 border border-border bg-bg text-text-primary
              focus:outline-none focus:border-teal"
            style="padding: 8px 10px; border-radius: 6px; font-family: inherit; font-size: 0.85rem;"
          />
          <button
            onclick={() => handleOpen()}
            disabled={loading}
            class="bg-teal text-white font-medium cursor-pointer
              hover:opacity-90 disabled:opacity-50 disabled:cursor-not-allowed"
            style="padding: 10px 20px; border: none; border-radius: 8px; font-family: inherit; font-size: 0.85rem;"
          >
            {loading ? "..." : "Open"}
          </button>
        </div>
        {#if error}
          <p style="font-size: 0.75rem; color: #e55; margin-top: 8px;">{error}</p>
        {/if}
      </div>
    </div>
    </div>
  </div>
{/if}
