<script lang="ts">
  import { addFiles } from "$lib/api/projects";
  import { loadSchema, loadRecipes } from "$lib/api/schema";
  import { toastStore } from "$lib/stores/toast.svelte";
  import { schemaStore } from "$lib/stores/schema.svelte";

  let path = $state("");
  let loading = $state(false);

  async function handleAdd() {
    const trimmed = path.trim();
    if (!trimmed || loading) return;

    loading = true;
    try {
      const result = await addFiles([trimmed]);
      if (result.success) {
        path = "";
        toastStore.show("Files added", "success");
        // Reload schema and recipes
        await Promise.allSettled([loadSchema(), loadRecipes()]);
        schemaStore.searchQuery = "";
      } else {
        toastStore.show(result.error || "Failed to add files", "error");
      }
    } catch (e) {
      toastStore.show("Failed to add files", "error");
    } finally {
      loading = false;
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Enter") handleAdd();
  }
</script>

<div class="flex" style="gap: 4px;">
  <input
    type="text"
    bind:value={path}
    onkeydown={handleKeydown}
    placeholder="Add file or directory..."
    disabled={loading}
    class="flex-1 min-w-0 border border-border bg-bg text-text-primary
      focus:outline-none focus:border-teal disabled:opacity-50"
    style="padding: 4px 8px; border-radius: 4px;
           font-family: 'JetBrains Mono', monospace; font-size: 0.7rem;"
  />
  <button
    onclick={handleAdd}
    disabled={loading || !path.trim()}
    class="bg-teal text-white cursor-pointer
      hover:opacity-90 transition-opacity
      disabled:opacity-50 disabled:cursor-not-allowed"
    style="padding: 4px 10px; border: none; border-radius: 4px;
           font-size: 0.8rem; font-weight: 600;"
  >
    Add
  </button>
</div>
