<script lang="ts">
  import type { ProvenanceData, ProvenanceTool } from "$lib/stores/chat.svelte";

  interface Props {
    provenance: ProvenanceData;
  }

  let { provenance }: Props = $props();
  let copied = $state(false);

  function formatValue(value: number | string | null | undefined): string {
    if (value === null || value === undefined || value === "") return "n/a";
    return String(value);
  }

  function toolSql(tool: ProvenanceTool): string {
    return tool.formatted_sql || tool.sql || "";
  }

  function provenanceJson(): string {
    return JSON.stringify(provenance, null, 2);
  }

  function fileSlug(value: string | undefined): string {
    const slug = (value || "query")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "")
      .slice(0, 60);
    return slug || "query";
  }

  async function copyProvenance(event: MouseEvent) {
    event.preventDefault();
    event.stopPropagation();
    await navigator.clipboard.writeText(provenanceJson());
    copied = true;
    setTimeout(() => (copied = false), 1200);
  }

  function exportProvenance(event: MouseEvent) {
    event.preventDefault();
    event.stopPropagation();
    const blob = new Blob([provenanceJson() + "\n"], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `datasight-provenance-${fileSlug(provenance.question)}.json`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  }
</script>

<div class="w-full mb-4 animate-fade-in">
  <details
    class="border border-border bg-surface overflow-hidden"
    style="border-radius: 8px; max-width: 760px;"
  >
    <summary class="summary-row text-text-secondary bg-surface-alt">
      Run details
    </summary>
    <div class="grid" style="gap: 14px; padding: 12px; font-size: 0.82rem;">
      <div class="actions">
        <button type="button" onclick={copyProvenance}>
          {copied ? "Copied" : "Copy JSON"}
        </button>
        <button type="button" onclick={exportProvenance}>Export JSON</button>
      </div>

      <dl class="run-meta">
        <dt>Model</dt><dd>{formatValue(provenance.model)}</dd>
        <dt>SQL dialect</dt><dd>{formatValue(provenance.dialect)}</dd>
        <dt>Project</dt><dd>{formatValue(provenance.project_dir)}</dd>
        <dt>LLM calls</dt><dd>{formatValue(provenance.llm?.api_calls)}</dd>
        <dt>Input tokens</dt><dd>{formatValue(provenance.llm?.input_tokens)}</dd>
        <dt>Output tokens</dt><dd>{formatValue(provenance.llm?.output_tokens)}</dd>
        <dt>Estimated cost</dt><dd>{formatValue(provenance.llm?.estimated_cost)}</dd>
      </dl>

      {#if provenance.warnings?.length}
        <div>
          <div class="section-label">Warnings</div>
          <ul class="warning-list">
            {#each provenance.warnings as warning}
              <li>{warning}</li>
            {/each}
          </ul>
        </div>
      {/if}

      {#each provenance.tools as tool}
        <section class="tool-block">
          <div class="section-label">{formatValue(tool.tool)}</div>
          <dl class="run-meta">
            <dt>Validation</dt><dd>{formatValue(tool.validation?.status)}</dd>
            <dt>Execution</dt><dd>{formatValue(tool.execution?.status)}</dd>
            <dt>Runtime</dt><dd>{formatValue(tool.execution?.execution_time_ms)} ms</dd>
            <dt>Rows</dt><dd>{formatValue(tool.execution?.row_count)}</dd>
            <dt>Columns</dt><dd>{formatValue(tool.execution?.column_count)}</dd>
            <dt>Result columns</dt>
            <dd>{tool.execution?.columns?.length ? tool.execution.columns.join(", ") : "n/a"}</dd>
            <dt>Error</dt><dd>{formatValue(tool.execution?.error)}</dd>
          </dl>

          {#if tool.validation?.errors?.length}
            <ul class="warning-list">
              {#each tool.validation.errors as error}
                <li>{error}</li>
              {/each}
            </ul>
          {/if}

          {#if toolSql(tool)}
            <pre class="sql-block"><code>{toolSql(tool)}</code></pre>
          {/if}
        </section>
      {/each}
    </div>
  </details>
</div>

<style>
  .run-meta {
    display: grid;
    grid-template-columns: minmax(110px, 150px) minmax(0, 1fr);
    gap: 4px 12px;
    align-items: start;
  }

  .summary-row {
    cursor: pointer;
    user-select: none;
    padding: 9px 12px;
    font-size: 0.82rem;
    font-weight: 600;
  }

  .actions {
    display: flex;
    justify-content: flex-end;
    gap: 6px;
  }

  .actions button {
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--surface);
    color: var(--text-primary);
    padding: 3px 8px;
    font: inherit;
    font-size: 0.76rem;
    cursor: pointer;
  }

  .actions button:hover {
    border-color: var(--teal);
  }

  .run-meta dt {
    color: var(--text-secondary);
    font-weight: 600;
  }

  .run-meta dd {
    min-width: 0;
    overflow-wrap: anywhere;
    color: var(--text-primary);
  }

  .section-label {
    margin-bottom: 6px;
    color: var(--text-primary);
    font-weight: 600;
  }

  .tool-block {
    border-top: 1px solid var(--border);
    padding-top: 12px;
  }

  .warning-list {
    margin: 0;
    padding-left: 18px;
    color: var(--orange);
  }

  .sql-block {
    margin: 10px 0 0;
    padding: 10px;
    max-height: 260px;
    overflow: auto;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--bg);
    color: var(--text-primary);
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono",
      "Courier New", monospace;
    font-size: 0.78rem;
    line-height: 1.5;
    white-space: pre;
  }
</style>
