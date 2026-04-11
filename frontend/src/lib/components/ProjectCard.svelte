<script lang="ts">
  import { loadProject, loadRecentProjects } from "$lib/api/projects";
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { onMount } from "svelte";

  interface Props {
    onProjectLoaded: (path: string) => void;
    onError: (msg: string) => void;
  }

  let { onProjectLoaded, onError }: Props = $props();

  let path = $state("");
  let loading = $state(false);
  let error = $state("");

  onMount(async () => {
    try {
      await loadRecentProjects();
    } catch {
      // ignore
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
      onProjectLoaded(target);
    } catch (e) {
      error = e instanceof Error ? e.message : "Failed to load project";
      onError(error);
    } finally {
      loading = false;
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") handleOpen();
  }
</script>

<div
  class="bg-surface border border-border"
  style="padding: 28px; border-radius: 18px;
         box-shadow: inset 0 1px 0 rgba(255,255,255,0.25), 0 14px 30px rgba(2,61,96,0.05);"
>
  <div
    class="inline-flex items-center text-teal font-bold uppercase"
    style="padding: 4px 9px; margin-bottom: 10px; border-radius: 999px;
           background: color-mix(in srgb, var(--teal) 14%, transparent);
           font-size: 0.72rem; letter-spacing: 0.05em;"
  >
    Saved setup
  </div>
  <div class="text-text-primary" style="font-size: 1.1rem; font-weight: 600; margin-bottom: 10px;">
    Open Project
  </div>
  <p class="text-text-secondary" style="font-size: 0.85rem; margin-bottom: 20px; line-height: 1.5;">
    Open a datasight project with schema descriptions and example queries.
  </p>

  {#if sidebarStore.recentProjectsCache.length > 0}
    <div
      class="flex flex-col"
      style="gap: 4px; margin-bottom: 12px; max-height: 180px; overflow-y: auto;"
    >
      {#each sidebarStore.recentProjectsCache.slice(0, 5) as project}
        <button
          class="flex items-center w-full text-left cursor-pointer
            border border-border hover:bg-bg transition-[background] duration-150"
          style="gap: 8px; padding: 8px 10px; border-radius: 6px; font-size: 0.8rem;"
          onclick={() => handleOpen(project.path)}
        >
          <span class="font-medium flex-1 truncate text-text-primary">
            {project.name}
          </span>
          <span
            class="text-text-secondary truncate"
            style="font-size: 0.7rem; font-family: 'JetBrains Mono', monospace; max-width: 200px;"
          >
            {project.path}
          </span>
        </button>
      {/each}
    </div>
  {:else}
    <div class="text-text-secondary" style="font-size: 0.8rem; margin-bottom: 12px;">
      No recent projects
    </div>
  {/if}

  <div class="flex" style="gap: 8px;">
    <input
      type="text"
      bind:value={path}
      onkeydown={handleKeydown}
      placeholder="Enter project path..."
      class="flex-1 border border-border bg-bg text-text-primary
        focus:outline-none focus:border-teal"
      style="padding: 10px 12px; border-radius: 10px;
             font-family: 'JetBrains Mono', monospace; font-size: 0.8rem;"
    />
    <button
      onclick={() => handleOpen()}
      disabled={loading}
      class="bg-teal text-white font-medium cursor-pointer
        hover:opacity-90 transition-opacity disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
      style="padding: 10px 20px; border: none; border-radius: 8px; font-family: inherit; font-size: 0.85rem;"
    >
      {loading ? "Loading..." : "Open"}
    </button>
  </div>
  {#if error}
    <p style="font-size: 0.75rem; color: #e55; margin-top: 8px;">{error}</p>
  {/if}
</div>
