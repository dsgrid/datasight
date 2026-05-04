<script lang="ts">
  import { sessionStore } from "$lib/stores/session.svelte";
  import { toastStore } from "$lib/stores/toast.svelte";

  interface Props {
    open: boolean;
    excludeIndices: Set<number>;
    onCancel: () => void;
    onExported: () => void;
  }

  let { open, excludeIndices, onCancel, onExported }: Props = $props();

  type ExportFormat = "" | "html" | "py" | "bundle";
  type BundleInclude = "html" | "sql" | "python" | "csv" | "charts" | "metadata";

  const bundleLabels: Record<BundleInclude, string> = {
    html: "HTML report",
    sql: "SQL scripts",
    python: "Python script",
    csv: "CSV extracts",
    charts: "Chart specs",
    metadata: "Metadata",
  };

  let exporting = $state<ExportFormat>("");
  let bundleIncludes = $state<Record<BundleInclude, boolean>>({
    html: true,
    sql: true,
    python: true,
    csv: true,
    charts: true,
    metadata: true,
  });

  function selectedBundleIncludes(): BundleInclude[] {
    return (Object.entries(bundleIncludes) as [BundleInclude, boolean][])
      .filter(([, enabled]) => enabled)
      .map(([name]) => name);
  }

  function toggleBundleInclude(name: BundleInclude) {
    bundleIncludes = { ...bundleIncludes, [name]: !bundleIncludes[name] };
  }

  async function handleExport(format: Exclude<ExportFormat, "">) {
    const include = selectedBundleIncludes();
    if (format === "bundle" && include.length === 0) {
      toastStore.show("Select at least one bundle artifact", "error");
      return;
    }

    exporting = format;
    try {
      const res = await fetch(
        `/api/export/${encodeURIComponent(sessionStore.sessionId)}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            exclude_indices: Array.from(excludeIndices),
            format,
            ...(format === "bundle" ? { include } : {}),
          }),
        },
      );

      if (!res.ok) {
        toastStore.show("Export failed", "error");
        return;
      }

      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download =
        format === "py"
          ? "datasight-session.py"
          : format === "bundle"
            ? "datasight-bundle.zip"
            : "datasight-export.html";
      a.click();
      URL.revokeObjectURL(url);

      toastStore.show("Exported successfully", "success");
      onExported();
    } catch {
      toastStore.show("Export failed", "error");
    } finally {
      exporting = "";
    }
  }
</script>

{#if open}
  <div
    class="flex items-center justify-between px-4 py-2 border-t border-border
      bg-surface-alt gap-3 flex-wrap"
  >
    <div class="flex flex-col gap-2">
      <span class="text-xs text-text-secondary">
        Select messages to include in export
        {#if excludeIndices.size > 0}
          <span class="text-orange ml-1">
            ({excludeIndices.size} excluded)
          </span>
        {/if}
      </span>
      <div class="flex flex-wrap gap-2">
        {#each Object.entries(bundleLabels) as [name, label]}
          <label
            class="inline-flex items-center gap-1 rounded border border-border px-2 py-1
              text-[11px] text-text-secondary cursor-pointer hover:text-text-primary"
          >
            <input
              type="checkbox"
              checked={bundleIncludes[name as BundleInclude]}
              disabled={exporting !== ""}
              onchange={() => toggleBundleInclude(name as BundleInclude)}
            />
            <span>{label}</span>
          </label>
        {/each}
      </div>
    </div>
    <div class="flex gap-2">
      <button
        class="px-3 py-1 text-xs rounded border border-border
          text-text-secondary hover:text-text-primary transition-colors
          cursor-pointer"
        onclick={onCancel}
      >
        Cancel
      </button>
      <button
        class="px-3 py-1 text-xs rounded border border-border
          text-text-primary hover:bg-surface transition-colors
          cursor-pointer disabled:opacity-50"
        title="Runnable Python script with editable SQL constants"
        disabled={exporting !== ""}
        onclick={() => handleExport("py")}
      >
        {exporting === "py" ? "Exporting..." : "Export Python script"}
      </button>
      <button
        class="px-3 py-1 text-xs rounded bg-teal text-white
          hover:opacity-90 transition-opacity cursor-pointer
          disabled:opacity-50"
        disabled={exporting !== ""}
        onclick={() => handleExport("html")}
      >
        {exporting === "html" ? "Exporting..." : "Export HTML"}
      </button>
      <button
        class="px-3 py-1 text-xs rounded bg-orange text-white
          hover:opacity-90 transition-opacity cursor-pointer
          disabled:opacity-50"
        title="Zip archive with SQL, Python, HTML, CSV extracts, chart specs, and metadata"
        disabled={exporting !== ""}
        onclick={() => handleExport("bundle")}
      >
        {exporting === "bundle" ? "Exporting..." : "Export bundle"}
      </button>
    </div>
  </div>
{/if}
