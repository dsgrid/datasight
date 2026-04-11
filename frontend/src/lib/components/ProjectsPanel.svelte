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
  <!-- svelte-ignore a11y_click_events_have_key_events -->
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div class="projects-overlay" onclick={onClose}></div>

  <!-- Panel -->
  <aside class="projects-panel">
    <!-- Header -->
    <div class="projects-header">
      <span>Switch</span>
      <button class="projects-close" onclick={onClose} title="Close">&times;</button>
    </div>

    <!-- Recent Projects -->
    <div class="projects-list-section">
      <div class="projects-list-header">Recent Projects</div>
      <div class="projects-list">
        {#if sidebarStore.recentProjectsCache.length > 0}
          {#each sidebarStore.recentProjectsCache as project}
            <!-- svelte-ignore a11y_click_events_have_key_events -->
            <!-- svelte-ignore a11y_no_static_element_interactions -->
            <div
              class="project-item {project.is_current ? 'current' : ''}"
              onclick={() => { if (!project.is_current) handleOpen(project.path); }}
            >
              <div class="project-item-info">
                <div class="project-item-name">
                  {project.name}
                  {#if project.is_current}
                    <span class="project-item-current-tag">current</span>
                  {/if}
                </div>
                <div class="project-item-path">{project.path}</div>
              </div>
            </div>
          {/each}
        {:else}
          <div class="projects-empty">No recent projects</div>
        {/if}
      </div>
    </div>

    <!-- Open by path -->
    <div class="projects-add-section">
      <div class="projects-add-header">Open Project</div>
      <div class="projects-add-input-wrap">
        <input
          type="text"
          bind:value={path}
          onkeydown={handleKeydown}
          placeholder="Enter project path..."
        />
        <button
          onclick={() => handleOpen()}
          disabled={loading}
        >
          {loading ? "..." : "Open"}
        </button>
      </div>
      {#if error}
        <div class="projects-add-error">{error}</div>
      {/if}
    </div>
  </aside>
{/if}

<style>
  .projects-overlay {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.4);
    z-index: 99;
  }

  .projects-panel {
    position: fixed;
    left: 0;
    top: 0;
    bottom: 0;
    width: 360px;
    max-width: 90vw;
    background: var(--surface);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    z-index: 100;
    box-shadow: 4px 0 20px rgba(0,0,0,0.15);
  }

  .projects-header {
    padding: 16px 20px 14px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid var(--border);
    font-weight: 600;
    font-size: 0.88rem;
    color: var(--text);
  }

  .projects-close {
    background: none;
    border: none;
    color: var(--text-secondary);
    font-size: 1.4rem;
    cursor: pointer;
    line-height: 1;
    padding: 0 4px;
    opacity: 0.6;
    transition: opacity 0.15s;
  }
  .projects-close:hover { opacity: 1; }

  .projects-list-section {
    flex: 1;
    overflow-y: auto;
    padding: 12px 0;
  }

  .projects-list-header {
    padding: 8px 20px;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-secondary);
  }

  .projects-list {
    padding: 0 12px;
  }

  .projects-empty {
    padding: 8px 12px;
    font-size: 0.82rem;
    color: var(--text-secondary);
    font-style: italic;
  }

  .project-item {
    display: flex;
    align-items: center;
    padding: 10px 12px;
    border-radius: 8px;
    cursor: pointer;
    transition: background 0.15s;
    gap: 10px;
  }
  .project-item:hover { background: var(--bg); }
  .project-item.current {
    background: rgba(21,168,168,0.1);
    cursor: default;
  }

  .project-item-info {
    flex: 1;
    min-width: 0;
  }

  .project-item-name {
    font-weight: 500;
    font-size: 0.9rem;
    color: var(--text);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .project-item.current .project-item-name { color: var(--teal); }

  .project-item-current-tag {
    font-size: 0.65rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.03em;
    color: var(--teal);
    margin-left: 6px;
  }

  .project-item-path {
    font-size: 0.7rem;
    color: var(--text-secondary);
    font-family: 'JetBrains Mono', monospace;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .projects-add-section {
    padding: 16px 20px;
    border-top: 1px solid var(--border);
    background: var(--bg);
  }

  .projects-add-header {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-secondary);
    margin-bottom: 10px;
  }

  .projects-add-input-wrap {
    display: flex;
    gap: 8px;
  }

  .projects-add-input-wrap input {
    flex: 1;
    padding: 8px 12px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--surface);
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
  }
  .projects-add-input-wrap input:focus {
    outline: none;
    border-color: var(--teal);
  }

  .projects-add-input-wrap button {
    padding: 8px 16px;
    background: var(--teal);
    color: white;
    border: none;
    border-radius: 6px;
    font-family: inherit;
    font-size: 0.8rem;
    font-weight: 500;
    cursor: pointer;
    transition: opacity 0.15s;
  }
  .projects-add-input-wrap button:hover { opacity: 0.9; }
  .projects-add-input-wrap button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .projects-add-error {
    font-size: 0.75rem;
    color: #e55;
    margin-top: 8px;
  }
</style>
