<script lang="ts">
  import { dashboardStore } from "$lib/stores/dashboard.svelte";
  import { saveDashboard, clearDashboard } from "$lib/api/dashboard";
  import { toastStore } from "$lib/stores/toast.svelte";

  let exporting = $state(false);

  const COLUMN_OPTIONS = [
    { value: 0, label: "Auto" },
    { value: 1, label: "1" },
    { value: 2, label: "2" },
    { value: 3, label: "3" },
  ];

  function setColumns(cols: number) {
    dashboardStore.columns = cols;
    saveDashboard();
  }

  function addNote() {
    dashboardStore.addItem({
      type: "note",
      title: "",
      markdown: "",
    });
    saveDashboard();
  }

  function addSection() {
    dashboardStore.addItem({
      type: "section",
      title: "",
      markdown: "",
    });
    saveDashboard();
  }

  function syncScales() {
    // Collect all chart iframes and unify y-axis ranges
    const iframes = document.querySelectorAll<HTMLIFrameElement>(
      ".dashboard-chart-iframe",
    );
    if (iframes.length < 2) {
      toastStore.show("Need at least 2 charts to sync scales", "info");
      return;
    }

    let globalMin = Infinity;
    let globalMax = -Infinity;

    for (const iframe of iframes) {
      try {
        const win = iframe.contentWindow as Window & {
          Plotly?: { d3: unknown };
          document: Document;
        };
        const plotEl = win.document.querySelector(".js-plotly-plot") as HTMLElement & {
          data?: Array<{ y?: number[] }>;
        };
        if (plotEl?.data) {
          for (const trace of plotEl.data) {
            if (trace.y) {
              for (const v of trace.y) {
                if (typeof v === "number") {
                  globalMin = Math.min(globalMin, v);
                  globalMax = Math.max(globalMax, v);
                }
              }
            }
          }
        }
      } catch {
        // cross-origin or no Plotly
      }
    }

    if (globalMin === Infinity) return;

    const padding = (globalMax - globalMin) * 0.05;
    const range = [globalMin - padding, globalMax + padding];

    for (const iframe of iframes) {
      try {
        const win = iframe.contentWindow as Window & {
          Plotly?: { relayout: (el: HTMLElement, update: unknown) => void };
          document: Document;
        };
        const plotEl = win.document.querySelector(
          ".js-plotly-plot",
        ) as HTMLElement;
        if (plotEl && win.Plotly) {
          win.Plotly.relayout(plotEl, {
            "yaxis.range": range,
            "yaxis.autorange": false,
          });
        }
      } catch {
        // ignore
      }
    }

    toastStore.show("Y-axis scales synced", "success");
  }

  async function handleExportDashboard() {
    if (dashboardStore.pinnedItems.length === 0) {
      toastStore.show("No items to export", "info");
      return;
    }
    exporting = true;
    try {
      const res = await fetch("/api/dashboard/export", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          items: dashboardStore.pinnedItems,
          columns: dashboardStore.columns || 2,
          title: "datasight dashboard",
        }),
      });
      if (!res.ok) {
        toastStore.show("Export failed", "error");
        return;
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "datasight-dashboard.html";
      a.click();
      URL.revokeObjectURL(url);
      toastStore.show("Dashboard exported", "success");
    } catch {
      toastStore.show("Export failed", "error");
    } finally {
      exporting = false;
    }
  }

  async function handleSave() {
    await saveDashboard();
    toastStore.show("Dashboard saved", "success");
  }

  async function handleClear() {
    await clearDashboard();
    toastStore.show("Dashboard cleared", "success");
  }
</script>

<div
  class="flex items-center border border-border"
  style="gap: 6px; padding: 12px 14px; margin: 14px; border-radius: 16px;
         background: color-mix(in srgb, var(--surface) 90%, var(--bg) 10%);
         box-shadow: 0 10px 26px rgba(2,61,96,0.05);"
>
  <!-- Layout -->
  <span class="text-[10px] uppercase tracking-wider text-text-secondary font-semibold">
    Layout
  </span>
  <div class="flex gap-0.5">
    {#each COLUMN_OPTIONS as opt}
      <button
        class="px-2 py-0.5 text-xs rounded cursor-pointer transition-colors
          {dashboardStore.columns === opt.value
          ? 'bg-teal text-white'
          : 'bg-surface-alt text-text-secondary hover:text-text-primary border border-border'}"
        onclick={() => setColumns(opt.value)}
      >
        {opt.label}
      </button>
    {/each}
  </div>

  <div class="flex-1"></div>

  <!-- Actions -->
  <button
    class="px-2 py-1 text-xs rounded bg-surface-alt text-text-secondary
      border border-border hover:text-text-primary transition-colors cursor-pointer"
    onclick={addNote}
  >
    + Note
  </button>
  <button
    class="px-2 py-1 text-xs rounded bg-surface-alt text-text-secondary
      border border-border hover:text-text-primary transition-colors cursor-pointer"
    onclick={addSection}
  >
    + Section
  </button>
  <button
    class="px-2 py-1 text-xs rounded bg-surface-alt text-text-secondary
      border border-border hover:text-text-primary transition-colors cursor-pointer"
    onclick={syncScales}
  >
    Sync Scales
  </button>

  <div class="w-px h-4 bg-border"></div>

  <button
    class="px-2 py-1 text-xs rounded bg-teal text-white
      hover:opacity-90 transition-opacity cursor-pointer"
    onclick={handleSave}
  >
    Save
  </button>
  <button
    class="px-2 py-1 text-xs rounded bg-surface-alt text-text-secondary
      border border-border hover:text-text-primary transition-colors cursor-pointer
      disabled:opacity-50"
    onclick={handleExportDashboard}
    disabled={exporting}
  >
    {exporting ? "Exporting..." : "Export"}
  </button>
  <button
    class="px-2 py-1 text-xs rounded text-text-secondary
      hover:text-red-500 transition-colors cursor-pointer"
    onclick={handleClear}
  >
    Clear
  </button>
</div>
