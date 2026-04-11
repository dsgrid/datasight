<script lang="ts">
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { schemaStore } from "$lib/stores/schema.svelte";
  import {
    loadMeasureEditor,
    saveMeasureYaml,
    validateMeasureYaml,
    upsertMeasure,
    loadMeasureCatalog,
  } from "$lib/api/measures";
  import type { UpsertMeasureInput } from "$lib/api/measures";
  import { toastStore } from "$lib/stores/toast.svelte";

  interface Props {
    open: boolean;
    onClose: () => void;
  }

  let { open, onClose }: Props = $props();

  // Tabs
  type Tab = "structured" | "yaml";
  let activeTab = $state<Tab>("structured");

  // YAML editor state
  let yamlText = $state("");
  let yamlStatus = $state("");
  let yamlError = $state(false);
  let yamlPath = $state("");
  let yamlLoading = $state(false);

  // Structured form state
  type Mode = "physical" | "calculated";
  let mode = $state<Mode>("physical");
  let selectedTable = $state("");
  let selectedColumn = $state("");
  let measureName = $state("");
  let expression = $state("");
  let aggregation = $state("sum");
  let averageStrategy = $state("");
  let weightColumn = $state("");
  let displayName = $state("");
  let format = $state("");
  let chartTypes = $state<string[]>([]);

  // Derived
  let tables = $derived(schemaStore.schemaData.map((t) => t.name));
  let columns = $derived(
    schemaStore.schemaData.find((t) => t.name === selectedTable)?.columns || [],
  );
  let physicalMeasures = $derived(
    sidebarStore.measureEditorCatalog.filter(
      (m) => !m.is_calculated && (selectedTable ? m.table === selectedTable : true),
    ),
  );

  const AGGREGATIONS = ["sum", "avg", "count", "min", "max", "count_distinct"];
  const CHART_TYPES = ["bar", "line", "scatter", "pie", "heatmap", "histogram"];

  // Load YAML on open
  $effect(() => {
    if (open) {
      loadYaml();
    }
  });

  async function loadYaml() {
    yamlLoading = true;
    try {
      const data = await loadMeasureEditor();
      yamlText = data.text;
      yamlPath = data.path;
      yamlStatus = data.generated ? "Generated from schema" : "";
      yamlError = false;
    } catch (e) {
      yamlStatus = "Failed to load";
      yamlError = true;
    } finally {
      yamlLoading = false;
    }
  }

  async function handleValidate() {
    const result = await validateMeasureYaml(yamlText);
    if (result.ok) {
      yamlStatus = "Valid";
      yamlError = false;
    } else {
      yamlStatus = result.errors.join("; ");
      yamlError = true;
    }
    if (result.warnings.length > 0) {
      yamlStatus += " | Warnings: " + result.warnings.join("; ");
    }
  }

  async function handleSave() {
    const result = await saveMeasureYaml(yamlText);
    if (result.ok) {
      yamlStatus = "Saved";
      yamlError = false;
      toastStore.show("Measures saved", "success");
      await loadMeasureCatalog();
    } else {
      yamlStatus = result.error || "Save failed";
      yamlError = true;
    }
  }

  async function handleInsert() {
    const input: UpsertMeasureInput = {
      text: yamlText,
      table: selectedTable,
    };

    if (mode === "physical") {
      input.column = selectedColumn;
    } else {
      input.name = measureName;
      input.expression = expression;
    }

    if (aggregation) input.default_aggregation = aggregation;
    if (averageStrategy) input.average_strategy = averageStrategy;
    if (weightColumn) input.weight_column = weightColumn;
    if (displayName) input.display_name = displayName;
    if (format) input.format = format;
    if (chartTypes.length > 0) input.preferred_chart_types = chartTypes;

    const result = await upsertMeasure(input);
    if (result.ok) {
      yamlText = result.text;
      yamlStatus = "Inserted";
      yamlError = false;
      activeTab = "yaml";
    } else {
      yamlStatus = result.error || result.errors.join("; ");
      yamlError = true;
    }
  }

  function toggleChartType(ct: string) {
    if (chartTypes.includes(ct)) {
      chartTypes = chartTypes.filter((c) => c !== ct);
    } else {
      chartTypes = [...chartTypes, ct];
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Escape") onClose();
  }
</script>

{#if open}
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div
    class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
    onkeydown={handleKeydown}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div class="absolute inset-0" onclick={onClose}></div>

    <div
      class="relative bg-surface rounded-xl shadow-xl border border-border
        w-full max-w-2xl max-h-[80vh] flex flex-col"
    >
      <!-- Header -->
      <div
        class="flex items-center justify-between px-4 py-3 border-b border-border"
      >
        <h2 class="text-sm font-semibold text-text-primary">Measure Editor</h2>
        <button
          class="text-text-secondary hover:text-text-primary cursor-pointer"
          onclick={onClose}
        >
          &times;
        </button>
      </div>

      <!-- Tabs -->
      <div class="flex border-b border-border">
        <button
          class="px-4 py-2 text-xs font-medium cursor-pointer transition-colors
            {activeTab === 'structured'
            ? 'text-teal border-b-2 border-teal'
            : 'text-text-secondary hover:text-text-primary'}"
          onclick={() => (activeTab = "structured")}
        >
          Structured
        </button>
        <button
          class="px-4 py-2 text-xs font-medium cursor-pointer transition-colors
            {activeTab === 'yaml'
            ? 'text-teal border-b-2 border-teal'
            : 'text-text-secondary hover:text-text-primary'}"
          onclick={() => (activeTab = "yaml")}
        >
          YAML
        </button>
      </div>

      <!-- Content -->
      <div class="flex-1 overflow-y-auto p-4">
        {#if activeTab === "structured"}
          <div class="space-y-3">
            <!-- Mode toggle -->
            <div class="flex gap-2">
              <button
                class="px-3 py-1 text-xs rounded-md cursor-pointer transition-colors
                  {mode === 'physical'
                  ? 'bg-teal text-white'
                  : 'bg-surface-alt text-text-secondary border border-border'}"
                onclick={() => (mode = "physical")}
              >
                Physical
              </button>
              <button
                class="px-3 py-1 text-xs rounded-md cursor-pointer transition-colors
                  {mode === 'calculated'
                  ? 'bg-teal text-white'
                  : 'bg-surface-alt text-text-secondary border border-border'}"
                onclick={() => (mode = "calculated")}
              >
                Calculated
              </button>
            </div>

            <!-- Table select -->
            <label class="block">
              <span class="text-xs text-text-secondary">Table</span>
              <select
                bind:value={selectedTable}
                class="mt-0.5 w-full px-2 py-1.5 text-xs rounded-md border border-border
                  bg-bg text-text-primary focus:outline-none focus:ring-1 focus:ring-teal/40"
              >
                <option value="">Select table...</option>
                {#each tables as t}
                  <option value={t}>{t}</option>
                {/each}
              </select>
            </label>

            {#if mode === "physical"}
              <!-- Column select -->
              <label class="block">
                <span class="text-xs text-text-secondary">Column</span>
                <select
                  bind:value={selectedColumn}
                  class="mt-0.5 w-full px-2 py-1.5 text-xs rounded-md border border-border
                    bg-bg text-text-primary focus:outline-none focus:ring-1 focus:ring-teal/40"
                  disabled={!selectedTable}
                >
                  <option value="">Select column...</option>
                  {#each columns as col}
                    <option value={col.name}>{col.name} ({col.dtype})</option>
                  {/each}
                </select>
              </label>
            {:else}
              <!-- Calculated: name + expression -->
              <label class="block">
                <span class="text-xs text-text-secondary">Name</span>
                <input
                  type="text"
                  bind:value={measureName}
                  class="mt-0.5 w-full px-2 py-1.5 text-xs rounded-md border border-border
                    bg-bg text-text-primary focus:outline-none focus:ring-1 focus:ring-teal/40"
                  placeholder="e.g. revenue_per_user"
                />
              </label>
              <label class="block">
                <span class="text-xs text-text-secondary">Expression</span>
                <input
                  type="text"
                  bind:value={expression}
                  class="mt-0.5 w-full px-2 py-1.5 text-xs font-mono rounded-md border
                    border-border bg-bg text-text-primary
                    focus:outline-none focus:ring-1 focus:ring-teal/40"
                  placeholder="e.g. revenue / user_count"
                />
              </label>
            {/if}

            <!-- Options -->
            <div class="grid grid-cols-2 gap-3">
              <label class="block">
                <span class="text-xs text-text-secondary">Aggregation</span>
                <select
                  bind:value={aggregation}
                  class="mt-0.5 w-full px-2 py-1.5 text-xs rounded-md border border-border
                    bg-bg text-text-primary focus:outline-none focus:ring-1 focus:ring-teal/40"
                >
                  {#each AGGREGATIONS as agg}
                    <option value={agg}>{agg}</option>
                  {/each}
                </select>
              </label>

              <label class="block">
                <span class="text-xs text-text-secondary">Average Strategy</span>
                <select
                  bind:value={averageStrategy}
                  class="mt-0.5 w-full px-2 py-1.5 text-xs rounded-md border border-border
                    bg-bg text-text-primary focus:outline-none focus:ring-1 focus:ring-teal/40"
                >
                  <option value="">None</option>
                  <option value="simple">Simple</option>
                  <option value="weighted">Weighted</option>
                </select>
              </label>
            </div>

            {#if averageStrategy === "weighted"}
              <label class="block">
                <span class="text-xs text-text-secondary">Weight Column</span>
                <select
                  bind:value={weightColumn}
                  class="mt-0.5 w-full px-2 py-1.5 text-xs rounded-md border border-border
                    bg-bg text-text-primary focus:outline-none focus:ring-1 focus:ring-teal/40"
                  disabled={!selectedTable}
                >
                  <option value="">Select column...</option>
                  {#each columns as col}
                    <option value={col.name}>{col.name}</option>
                  {/each}
                </select>
              </label>
            {/if}

            <div class="grid grid-cols-2 gap-3">
              <label class="block">
                <span class="text-xs text-text-secondary">Display Name</span>
                <input
                  type="text"
                  bind:value={displayName}
                  class="mt-0.5 w-full px-2 py-1.5 text-xs rounded-md border border-border
                    bg-bg text-text-primary focus:outline-none focus:ring-1 focus:ring-teal/40"
                  placeholder="e.g. Total Revenue"
                />
              </label>
              <label class="block">
                <span class="text-xs text-text-secondary">Format</span>
                <input
                  type="text"
                  bind:value={format}
                  class="mt-0.5 w-full px-2 py-1.5 text-xs rounded-md border border-border
                    bg-bg text-text-primary focus:outline-none focus:ring-1 focus:ring-teal/40"
                  placeholder="e.g. $,.2f"
                />
              </label>
            </div>

            <!-- Chart types -->
            <div>
              <span class="text-xs text-text-secondary">Chart Types</span>
              <div class="flex flex-wrap gap-1 mt-1">
                {#each CHART_TYPES as ct}
                  <button
                    class="px-2 py-0.5 text-[10px] rounded-md cursor-pointer transition-colors
                      {chartTypes.includes(ct)
                      ? 'bg-teal text-white'
                      : 'bg-surface-alt text-text-secondary border border-border'}"
                    onclick={() => toggleChartType(ct)}
                  >
                    {ct}
                  </button>
                {/each}
              </div>
            </div>

            <!-- Insert button -->
            <button
              class="px-4 py-1.5 rounded-lg bg-teal text-white text-sm font-medium
                hover:opacity-90 transition-opacity cursor-pointer"
              onclick={handleInsert}
            >
              Insert into YAML
            </button>
          </div>
        {:else}
          <!-- YAML tab -->
          <div class="space-y-2">
            {#if yamlPath}
              <div class="text-[10px] text-text-secondary font-mono truncate">
                {yamlPath}
              </div>
            {/if}

            {#if yamlLoading}
              <div class="text-xs text-text-secondary py-4 text-center">
                Loading...
              </div>
            {:else}
              <textarea
                bind:value={yamlText}
                spellcheck="false"
                class="w-full h-64 px-3 py-2 rounded-lg bg-bg border border-border
                  text-text-primary text-xs font-mono resize-y
                  focus:outline-none focus:ring-2 focus:ring-teal/40"
              ></textarea>
            {/if}

            {#if yamlStatus}
              <div
                class="text-xs {yamlError ? 'text-red-500' : 'text-teal'}"
              >
                {yamlStatus}
              </div>
            {/if}

            <div class="flex gap-2">
              <button
                class="px-3 py-1.5 rounded-lg bg-surface-alt text-text-primary text-sm
                  border border-border hover:bg-teal/5 transition-colors cursor-pointer"
                onclick={handleValidate}
              >
                Validate
              </button>
              <button
                class="px-3 py-1.5 rounded-lg bg-teal text-white text-sm font-medium
                  hover:opacity-90 transition-opacity cursor-pointer"
                onclick={handleSave}
              >
                Save
              </button>
            </div>
          </div>
        {/if}
      </div>
    </div>
  </div>
{/if}
