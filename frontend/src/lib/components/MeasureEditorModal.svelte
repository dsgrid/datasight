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

  let scope = $derived(selectedTable || "dataset");
  let catalogCount = $derived(
    selectedTable
      ? sidebarStore.measureEditorCatalog.filter((m) => m.table === selectedTable).length
      : sidebarStore.measureEditorCatalog.length,
  );

  const AGGREGATIONS = ["sum", "avg", "count", "min", "max", "count_distinct"];
  const FORMATS = [
    { value: "", label: "format (optional)" },
    { value: "currency", label: "currency" },
    { value: "percent", label: "percent" },
    { value: "integer", label: "integer" },
    { value: "float", label: "float" },
    { value: "decimal", label: "decimal" },
    { value: "mw", label: "mw" },
    { value: "mwh", label: "mwh" },
    { value: "kwh", label: "kwh" },
  ];
  const CHART_TYPES = ["line", "area", "bar", "scatter", "heatmap"];

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
    } catch {
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
  <div class="fixed inset-0 z-50" onkeydown={handleKeydown}>
    <!-- Overlay -->
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div class="absolute inset-0" style="background: rgba(0,0,0,0.5);" onclick={onClose}></div>

    <!-- Modal -->
    <div
      class="measure-modal"
      role="dialog"
      aria-modal="true"
      aria-label="Measure override editor"
    >
      <!-- Header -->
      <div class="measure-modal-header">
        <div class="measure-header-copy">
          <h3>Measure Overrides</h3>
          <p>Work with inferred measures in a form first, then drop to raw <code>measures.yaml</code> only when needed.</p>
        </div>
        <button
          class="measure-close-btn"
          onclick={onClose}
          title="Close"
        >&times;</button>
      </div>

      <!-- Content -->
      <div class="measure-modal-content">
        <!-- Intro cards -->
        <div class="measure-intro">
          <div class="measure-intro-card">
            <span class="measure-intro-label">Catalog</span>
            <strong>{catalogCount} inferred measures</strong>
          </div>
          <div class="measure-intro-card">
            <span class="measure-intro-label">Current scope</span>
            <strong>{scope}</strong>
          </div>
        </div>

        <!-- Tabs -->
        <div class="measure-tabs" role="tablist" aria-label="Measure editor mode">
          <button
            class="measure-tab {activeTab === 'structured' ? 'active' : ''}"
            role="tab"
            aria-selected={activeTab === "structured"}
            onclick={() => (activeTab = "structured")}
          >Structured</button>
          <button
            class="measure-tab {activeTab === 'yaml' ? 'active' : ''}"
            role="tab"
            aria-selected={activeTab === "yaml"}
            onclick={() => (activeTab = "yaml")}
          >Raw YAML</button>
        </div>

        {#if activeTab === "structured"}
          <!-- Structured panel -->
          <div class="measure-form-grid">
            <label class="measure-field">
              <span>Mode</span>
              <select bind:value={mode} title="Measure type">
                <option value="physical">physical measure</option>
                <option value="calculated">calculated measure</option>
              </select>
            </label>

            <label class="measure-field">
              <span>Target table</span>
              <select bind:value={selectedTable} title="Target table">
                <option value="">Select table...</option>
                {#each tables as t}
                  <option value={t}>{t}</option>
                {/each}
              </select>
            </label>

            {#if mode === "physical"}
              <label class="measure-field measure-field-wide">
                <span>Inferred measure</span>
                <select bind:value={selectedColumn} title="Select inferred measure" disabled={!selectedTable}>
                  <option value="">Select inferred measure...</option>
                  {#each physicalMeasures as m}
                    <option value={m.column || m.name}>{m.table}.{m.column || m.name} ({m.aggregation})</option>
                  {/each}
                </select>
              </label>
            {:else}
              <label class="measure-field">
                <span>Calculated name</span>
                <input type="text" bind:value={measureName} placeholder="net_load_mw" />
              </label>
              <label class="measure-field measure-field-wide">
                <span>Expression</span>
                <input type="text" bind:value={expression} placeholder="load_mw - renewable_generation_mw" />
              </label>
            {/if}

            <label class="measure-field">
              <span>Default aggregation</span>
              <select bind:value={aggregation} title="Default aggregation">
                {#each AGGREGATIONS as agg}
                  <option value={agg}>{agg}</option>
                {/each}
              </select>
            </label>

            <label class="measure-field">
              <span>Average strategy</span>
              <select bind:value={averageStrategy} title="Average strategy">
                <option value="">avg</option>
                <option value="weighted_avg">weighted_avg</option>
              </select>
            </label>

            <label class="measure-field">
              <span>Weight column</span>
              <select bind:value={weightColumn} title="Weight column (optional)" disabled={!selectedTable}>
                <option value="">weight column (optional)</option>
                {#each columns as col}
                  <option value={col.name}>{col.name}</option>
                {/each}
              </select>
            </label>

            <label class="measure-field">
              <span>Display name</span>
              <input type="text" bind:value={displayName} placeholder="Net load" />
            </label>

            <label class="measure-field">
              <span>Format</span>
              <select bind:value={format} title="Format">
                {#each FORMATS as f}
                  <option value={f.value}>{f.label}</option>
                {/each}
              </select>
            </label>

            <label class="measure-field measure-field-wide">
              <span>Preferred chart types</span>
              <select
                multiple
                size="4"
                title="Preferred chart types"
                onchange={(e) => {
                  const sel = e.currentTarget as HTMLSelectElement;
                  chartTypes = Array.from(sel.selectedOptions, (o) => o.value);
                }}
              >
                {#each CHART_TYPES as ct}
                  <option value={ct} selected={chartTypes.includes(ct)}>{ct}</option>
                {/each}
              </select>
            </label>
          </div>

          <div class="measure-actions">
            <button class="measure-btn secondary" onclick={handleInsert}>Insert override</button>
            <button class="measure-btn secondary" onclick={handleValidate}>Validate YAML</button>
            <button class="measure-btn secondary" onclick={loadYaml}>Reload</button>
          </div>
        {:else}
          <!-- YAML panel -->
          <div class="measure-yaml-panel">
            <div class="measure-yaml-head">
              <span class="measure-yaml-title">Raw <code>measures.yaml</code></span>
              <span class="measure-yaml-hint">Edit directly when the structured form is too limiting.</span>
            </div>
            {#if yamlLoading}
              <div style="text-align: center; padding: 24px; font-size: 0.8rem; color: var(--text-secondary);">
                Loading...
              </div>
            {:else}
              <textarea
                bind:value={yamlText}
                spellcheck="false"
                rows="16"
                placeholder="# measures.yaml will load here"
              ></textarea>
            {/if}
          </div>

          {#if yamlStatus}
            <div class="measure-status {yamlError ? 'error' : 'success'}">
              {yamlStatus}
            </div>
          {/if}
        {/if}
      </div>

      <!-- Footer -->
      <div class="measure-modal-footer">
        <button class="measure-btn secondary" onclick={onClose}>Close</button>
        <button class="measure-btn primary" onclick={handleSave}>Save overrides</button>
      </div>
    </div>
  </div>
{/if}

<style>
  .measure-modal {
    position: fixed;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: min(900px, 92vw);
    max-height: 90vh;
    display: flex;
    flex-direction: column;
    border: 1px solid color-mix(in srgb, var(--teal) 16%, var(--border));
    border-radius: 20px;
    overflow: hidden;
    background:
      radial-gradient(circle at top left, color-mix(in srgb, var(--orange) 10%, transparent), transparent 34%),
      radial-gradient(circle at top right, color-mix(in srgb, var(--teal) 12%, transparent), transparent 36%),
      var(--surface);
    box-shadow: 0 24px 60px rgba(0,0,0,0.28);
    z-index: 60;
  }

  .measure-modal-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 16px;
    padding: 18px 20px;
    border-bottom: 1px solid var(--border);
    background:
      linear-gradient(180deg, color-mix(in srgb, var(--teal) 8%, var(--surface)), color-mix(in srgb, var(--surface) 96%, var(--bg)));
  }

  .measure-header-copy {
    display: grid;
    gap: 4px;
  }

  .measure-header-copy h3 {
    margin: 0;
    font-size: 1rem;
    color: var(--text);
  }

  .measure-header-copy p {
    margin: 0;
    max-width: 58ch;
    font-size: 0.8rem;
    line-height: 1.45;
    color: var(--text-secondary);
    font-weight: 400;
  }

  .measure-header-copy code {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.78rem;
  }

  .measure-close-btn {
    background: none;
    border: none;
    color: var(--text-secondary);
    font-size: 1.4rem;
    line-height: 1;
    cursor: pointer;
    padding: 4px;
    transition: color 0.15s;
  }
  .measure-close-btn:hover {
    color: var(--text);
  }

  .measure-modal-content {
    flex: 1;
    overflow-y: auto;
    display: grid;
    gap: 16px;
    padding: 18px 20px;
  }

  /* Intro cards */
  .measure-intro {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
  }

  .measure-intro-card {
    display: grid;
    gap: 4px;
    padding: 12px 14px;
    border: 1px solid color-mix(in srgb, var(--teal) 18%, var(--border));
    border-radius: 12px;
    background:
      linear-gradient(180deg, color-mix(in srgb, var(--teal) 7%, var(--surface)), color-mix(in srgb, var(--bg) 92%, var(--surface)));
    box-shadow: inset 0 1px 0 color-mix(in srgb, white 4%, transparent);
  }

  .measure-intro-label {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-secondary);
  }

  .measure-intro-card strong {
    font-size: 0.9rem;
    color: var(--text);
  }

  /* Tabs */
  .measure-tabs {
    display: inline-grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 6px;
    padding: 6px;
    border: 1px solid color-mix(in srgb, var(--teal) 14%, var(--border));
    border-radius: 14px;
    background: color-mix(in srgb, var(--bg) 88%, var(--surface));
  }

  .measure-tab {
    padding: 10px 14px;
    border: none;
    border-radius: 10px;
    background: transparent;
    color: var(--text-secondary);
    font: inherit;
    font-size: 0.84rem;
    font-weight: 600;
    cursor: pointer;
    transition: background 0.15s, color 0.15s, box-shadow 0.15s;
  }

  .measure-tab.active {
    background: linear-gradient(135deg, color-mix(in srgb, var(--teal) 82%, #1f8f88), color-mix(in srgb, var(--orange) 18%, var(--teal)));
    color: white;
    box-shadow: 0 8px 16px color-mix(in srgb, var(--teal) 20%, transparent);
  }

  /* Form grid */
  .measure-form-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 12px;
    padding: 14px;
    border: 1px solid color-mix(in srgb, var(--teal) 14%, var(--border));
    border-radius: 16px;
    background:
      linear-gradient(180deg, color-mix(in srgb, var(--surface) 96%, white), color-mix(in srgb, var(--bg) 94%, var(--surface)));
  }

  .measure-field {
    display: grid;
    gap: 6px;
    font-size: 0.78rem;
    color: var(--text-secondary);
  }

  .measure-field > span {
    font-size: 0.72rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: color-mix(in srgb, var(--teal) 58%, var(--text-secondary));
  }

  .measure-field-wide {
    grid-column: 1 / -1;
  }

  .measure-field input,
  .measure-field select {
    width: 100%;
    min-width: 0;
    padding: 11px 12px;
    border: 1px solid var(--border);
    border-radius: 10px;
    background: color-mix(in srgb, var(--bg) 94%, white);
    color: var(--text);
    font: inherit;
    font-size: 0.86rem;
    box-sizing: border-box;
    transition: border-color 0.15s, box-shadow 0.15s, background 0.15s;
  }

  .measure-field select[multiple] {
    min-height: 124px;
    padding: 8px;
  }

  .measure-field input:focus,
  .measure-field select:focus {
    outline: none;
    border-color: color-mix(in srgb, var(--teal) 55%, var(--border));
    box-shadow: 0 0 0 3px color-mix(in srgb, var(--teal) 16%, transparent);
  }

  .measure-field input:disabled,
  .measure-field select:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  /* Actions */
  .measure-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
  }

  .measure-btn {
    padding: 10px 18px;
    border: none;
    border-radius: 12px;
    font: inherit;
    font-size: 0.84rem;
    font-weight: 600;
    cursor: pointer;
    transition: opacity 0.15s, box-shadow 0.15s;
    min-width: 132px;
  }

  .measure-btn.secondary {
    background: color-mix(in srgb, var(--surface) 85%, var(--bg));
    border: 1px solid color-mix(in srgb, var(--teal) 18%, var(--border));
    color: var(--text);
  }
  .measure-btn.secondary:hover {
    background: color-mix(in srgb, var(--teal) 6%, var(--surface));
  }

  .measure-btn.primary {
    background: linear-gradient(135deg, color-mix(in srgb, var(--teal) 82%, #1f8f88), color-mix(in srgb, var(--orange) 26%, var(--teal)));
    color: white;
    box-shadow: 0 8px 18px color-mix(in srgb, var(--teal) 22%, transparent);
  }
  .measure-btn.primary:hover {
    opacity: 0.92;
  }

  /* YAML panel */
  .measure-yaml-panel {
    display: grid;
    gap: 12px;
    padding: 14px;
    border: 1px solid color-mix(in srgb, var(--orange) 18%, var(--border));
    border-radius: 16px;
    background:
      linear-gradient(180deg, color-mix(in srgb, var(--orange) 7%, var(--surface)), color-mix(in srgb, var(--bg) 95%, var(--surface)));
  }

  .measure-yaml-head {
    display: grid;
    gap: 4px;
  }

  .measure-yaml-title {
    font-size: 0.86rem;
    font-weight: 700;
    color: var(--text);
  }

  .measure-yaml-title code {
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
  }

  .measure-yaml-hint {
    font-size: 0.75rem;
    line-height: 1.4;
    color: var(--text-secondary);
  }

  .measure-yaml-panel textarea {
    min-height: 320px;
    width: 100%;
    padding: 14px;
    border: 1px solid color-mix(in srgb, var(--orange) 18%, var(--border));
    border-radius: 12px;
    background: color-mix(in srgb, var(--bg) 96%, black);
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.8rem;
    line-height: 1.55;
    box-sizing: border-box;
    resize: vertical;
  }

  .measure-yaml-panel textarea:focus {
    outline: none;
    border-color: color-mix(in srgb, var(--orange) 42%, var(--border));
    box-shadow: 0 0 0 3px color-mix(in srgb, var(--orange) 14%, transparent);
  }

  .measure-status {
    font-size: 0.78rem;
    padding: 8px 12px;
    border-radius: 8px;
  }
  .measure-status.success {
    color: var(--teal);
  }
  .measure-status.error {
    color: #ef4444;
  }

  /* Footer */
  .measure-modal-footer {
    display: flex;
    justify-content: flex-end;
    gap: 10px;
    padding: 14px 20px;
    border-top: 1px solid var(--border);
    background: color-mix(in srgb, var(--bg) 40%, var(--surface));
  }
</style>
