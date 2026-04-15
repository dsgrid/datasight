<script lang="ts">
  import {
    checkProjectPath,
    saveExploreAsProject,
    generateProjectStream,
    getProjectStatus,
  } from "$lib/api/projects";
  import {
    loadSchema,
    loadQueries,
    loadRecipes,
  } from "$lib/api/schema";
  import { loadMeasureCatalog } from "$lib/api/measures";
  import { toastStore } from "$lib/stores/toast.svelte";
  import { sessionStore } from "$lib/stores/session.svelte";
  import { settingsStore } from "$lib/stores/settings.svelte";

  interface Props {
    open: boolean;
    onClose: () => void;
  }

  let { open, onClose }: Props = $props();

  let path = $state("");
  let name = $state("");
  let description = $state("");
  let error = $state("");
  let busy = $state(false);
  let status = $state("");

  $effect(() => {
    if (open) {
      error = "";
      status = "";
    }
  });

  async function refreshAfterSave() {
    const projectStatus = await getProjectStatus();
    sessionStore.isEphemeralSession = projectStatus.is_ephemeral;
    sessionStore.projectLoaded = projectStatus.loaded;
    sessionStore.currentProjectPath = projectStatus.path;
    if (projectStatus.tables) {
      sessionStore.ephemeralTablesInfo = projectStatus.tables;
    }
    await Promise.allSettled([
      loadSchema(),
      loadQueries(),
      loadRecipes(),
      loadMeasureCatalog(),
    ]);
  }

  function close() {
    path = "";
    name = "";
    description = "";
    error = "";
    status = "";
    onClose();
  }

  async function handleSave() {
    const projectPath = path.trim();
    const projectName = name.trim();
    if (!projectPath) {
      error = "Please enter a project directory path";
      return;
    }
    error = "";

    try {
      const check = await checkProjectPath(projectPath);
      if (check.exists && check.files.length > 0) {
        const ok = window.confirm(
          "The following files already exist and will be overwritten:\n\n" +
            check.files.join("\n") +
            "\n\nOverwrite?",
        );
        if (!ok) return;
      }
    } catch {
      // Proceed anyway if the check call failed
    }

    busy = true;

    if (settingsStore.llmConnected) {
      status = "Saving & generating documentation...";
      try {
        await generateProjectStream(
          projectPath,
          projectName || null,
          description.trim() || null,
          (event) => {
            if (event.type === "status") {
              status = String(event.data.message || "");
            } else if (event.type === "done") {
              const files = (event.data.files as string[]) || [];
              toastStore.show(
                "Project saved with documentation: " +
                  (files.length ? files.join(", ") : "done"),
                "success",
              );
            } else if (event.type === "error") {
              error = String(event.data.error || "Failed to save project");
            }
          },
        );
        if (!error) {
          await refreshAfterSave();
          close();
        }
      } catch (e) {
        error = (e as Error).message || "Failed to save project";
      } finally {
        busy = false;
        status = "";
      }
    } else {
      status = "Saving...";
      try {
        const result = await saveExploreAsProject(projectPath, projectName);
        if (!result.success) {
          error = result.error || "Failed to save project";
          return;
        }
        toastStore.show("Project saved to " + (result.path || projectPath), "success");
        await refreshAfterSave();
        close();
      } catch (e) {
        error = (e as Error).message || "Failed to save project";
      } finally {
        busy = false;
        status = "";
      }
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Escape" && !busy) close();
  }
</script>

{#if open}
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div class="fixed inset-0 z-[80] flex items-center justify-center" onkeydown={handleKeydown}>
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div class="absolute inset-0 bg-black/50" onclick={busy ? undefined : close}></div>

    <div
      class="relative bg-surface rounded-xl shadow-xl border border-border w-full max-w-md"
      style="padding: 20px;"
      role="dialog"
      aria-modal="true"
      aria-label="Save as project"
    >
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-sm font-semibold text-text-primary">Save as Project</h2>
        <button
          class="text-text-secondary hover:text-text-primary cursor-pointer"
          onclick={close}
          disabled={busy}
        >&times;</button>
      </div>

      <div class="flex flex-col gap-3">
        <label class="flex flex-col gap-1 text-xs text-text-secondary">
          <span>Project directory</span>
          <input
            type="text"
            bind:value={path}
            placeholder="/path/to/project"
            disabled={busy}
            class="w-full border border-border bg-bg text-text-primary
              focus:outline-none focus:border-teal disabled:opacity-50"
            style="padding: 8px 10px; border-radius: 6px;
                   font-family: 'JetBrains Mono', monospace; font-size: 0.78rem;"
          />
        </label>

        <label class="flex flex-col gap-1 text-xs text-text-secondary">
          <span>Project name (optional)</span>
          <input
            type="text"
            bind:value={name}
            placeholder="My Project"
            disabled={busy}
            class="w-full border border-border bg-bg text-text-primary
              focus:outline-none focus:border-teal disabled:opacity-50"
            style="padding: 8px 10px; border-radius: 6px; font-size: 0.82rem;"
          />
        </label>

        {#if settingsStore.llmConnected}
          <label class="flex flex-col gap-1 text-xs text-text-secondary">
            <span>Description (optional)</span>
            <textarea
              bind:value={description}
              rows="3"
              placeholder="What does this data represent? Key columns, domain terminology..."
              disabled={busy}
              class="w-full border border-border bg-bg text-text-primary
                focus:outline-none focus:border-teal disabled:opacity-50"
              style="padding: 8px 10px; border-radius: 6px; font-size: 0.82rem; resize: vertical;"
            ></textarea>
          </label>
          <div class="text-xs text-text-secondary" style="line-height: 1.4;">
            The AI will generate documentation and example queries from the
            schema and your description.
          </div>
        {:else}
          <div class="text-xs text-text-secondary" style="line-height: 1.4;">
            Project files will be saved without generated documentation.
            Configure an LLM in settings to enable docs generation.
          </div>
        {/if}

        {#if status}
          <div class="text-xs text-teal">{status}</div>
        {/if}
        {#if error}
          <div class="text-xs" style="color: #ef4444;">{error}</div>
        {/if}
      </div>

      <div class="flex justify-end gap-2 mt-5">
        <button
          onclick={close}
          disabled={busy}
          class="cursor-pointer border border-border bg-surface-alt text-text-primary
            hover:bg-surface disabled:opacity-50"
          style="padding: 6px 14px; border-radius: 6px; font-size: 0.82rem;"
        >Cancel</button>
        <button
          onclick={handleSave}
          disabled={busy || !path.trim()}
          class="cursor-pointer bg-teal text-white hover:opacity-90
            disabled:opacity-50 disabled:cursor-not-allowed"
          style="padding: 6px 14px; border: none; border-radius: 6px;
                 font-size: 0.82rem; font-weight: 600;"
        >{busy ? "Saving..." : "Save"}</button>
      </div>
    </div>
  </div>
{/if}
