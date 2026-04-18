<script lang="ts">
  import TableResult from "./TableResult.svelte";
  import CodeEditor from "./CodeEditor.svelte";
  import { sqlEditorStore } from "$lib/stores/sql_editor.svelte";
  import { queriesStore } from "$lib/stores/queries.svelte";
  import { schemaStore } from "$lib/stores/schema.svelte";
  import { sessionStore } from "$lib/stores/session.svelte";
  import { executeSql, validateSql, formatSql } from "$lib/api/sql";

  const editorSchema = $derived.by(() => {
    const result: Record<string, string[]> = {};
    for (const table of schemaStore.schemaData) {
      result[table.name] = table.columns.map((c) => c.name);
    }
    return result;
  });

  let insertRequest = $state<{ text: string; nonce: number } | null>(null);
  let formatting = $state(false);

  $effect(() => {
    const pending = sqlEditorStore.pendingInsert;
    if (pending) {
      insertRequest = { text: pending, nonce: Date.now() };
      sqlEditorStore.pendingInsert = null;
    }
  });

  async function runSql(sqlOverride?: string) {
    const sql = (sqlOverride ?? sqlEditorStore.sql).trim();
    if (!sql || sqlEditorStore.running) return;
    sqlEditorStore.running = true;
    sqlEditorStore.error = null;
    sqlEditorStore.validationErrors = [];
    try {
      const result = await executeSql(sql);
      sqlEditorStore.resultHtml = result.html;
      sqlEditorStore.rowCount = result.row_count;
      sqlEditorStore.elapsedMs = result.elapsed_ms;
      sqlEditorStore.error = result.error;
      queriesStore.addQuery({
        tool: "sql_editor",
        sql,
        timestamp: new Date().toISOString(),
        execution_time_ms: result.elapsed_ms,
        row_count: result.html ? result.row_count : undefined,
        error: result.error ?? undefined,
      });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      sqlEditorStore.error = msg;
      sqlEditorStore.resultHtml = null;
      queriesStore.addQuery({
        tool: "sql_editor",
        sql,
        timestamp: new Date().toISOString(),
        error: msg,
      });
    } finally {
      sqlEditorStore.running = false;
    }
  }

  async function validateOnly() {
    const sql = sqlEditorStore.sql.trim();
    if (!sql) return;
    try {
      const result = await validateSql(sql);
      sqlEditorStore.validationErrors = result.valid ? [] : result.errors;
      if (result.valid) sqlEditorStore.error = null;
    } catch (e) {
      sqlEditorStore.error = e instanceof Error ? e.message : String(e);
    }
  }

  async function formatCurrent() {
    const sql = sqlEditorStore.sql;
    if (!sql.trim() || formatting) return;
    formatting = true;
    try {
      const result = await formatSql(sql);
      if (result.error) {
        sqlEditorStore.error = result.error;
      } else if (result.formatted !== sql) {
        sqlEditorStore.sql = result.formatted;
      }
    } catch (e) {
      sqlEditorStore.error = e instanceof Error ? e.message : String(e);
    } finally {
      formatting = false;
    }
  }

</script>

<div
  class="flex flex-col flex-1 min-w-0 min-h-0 overflow-hidden"
  style="background: linear-gradient(180deg, color-mix(in srgb, var(--bg) 88%, var(--surface) 12%), var(--bg));"
>
  <!-- Editor -->
  <div
    class="border-b border-border bg-surface shrink-0"
    style="padding: 12px 16px;"
  >
    <div class="flex items-center justify-between" style="margin-bottom: 8px;">
      <div
        class="text-text-secondary"
        style="font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em;"
      >
        SQL Editor
      </div>
      <div class="flex items-center" style="gap: 8px;">
        <span
          class="text-text-secondary"
          style="font-size: 0.7rem;"
        >
          Run with <kbd style="font-family: inherit; padding: 1px 5px; border-radius: 3px; border: 1px solid var(--border); background: var(--bg);">⌘/Ctrl + Enter</kbd>
        </span>
        <button
          class="page-btn"
          onclick={() => sqlEditorStore.clearAll()}
          disabled={sqlEditorStore.running ||
            (!sqlEditorStore.sql && !sqlEditorStore.resultHtml && !sqlEditorStore.error)}
          title="Clear editor and results"
        >
          Clear
        </button>
        <button
          class="page-btn"
          onclick={formatCurrent}
          disabled={formatting || !sqlEditorStore.sql.trim()}
          title="Pretty-print SQL with sqlglot"
        >
          {formatting ? "Formatting..." : "Format"}
        </button>
        <button
          class="page-btn"
          onclick={validateOnly}
          disabled={sqlEditorStore.running || !sqlEditorStore.sql.trim()}
        >
          Validate
        </button>
        <button
          class="export-csv-btn"
          onclick={() => runSql()}
          disabled={sqlEditorStore.running || !sqlEditorStore.sql.trim()}
        >
          {sqlEditorStore.running ? "Running..." : "Run"}
        </button>
      </div>
    </div>
    <CodeEditor
      value={sqlEditorStore.sql}
      onChange={(v) => (sqlEditorStore.sql = v)}
      onRun={runSql}
      schema={editorSchema}
      dialect={sessionStore.sqlDialect}
      placeholder={"-- e.g. SELECT * FROM generation_fuel LIMIT 10"}
      {insertRequest}
      lintSql={validateSql}
    />
  </div>

  <!-- Results / errors -->
  <div class="flex-1 overflow-auto" style="padding: 16px;">
    {#if sqlEditorStore.validationErrors.length > 0}
      <div
        class="border border-border"
        style="padding: 10px 12px; border-radius: 8px; margin-bottom: 12px;
               background: color-mix(in srgb, var(--orange) 12%, var(--surface));
               color: var(--text-primary); font-size: 0.82rem;"
      >
        <div style="font-weight: 600; margin-bottom: 4px;">Validation errors</div>
        <ul style="margin: 0; padding-left: 18px;">
          {#each sqlEditorStore.validationErrors as err}
            <li>{err}</li>
          {/each}
        </ul>
      </div>
    {/if}

    {#if sqlEditorStore.error}
      <div
        class="border border-border"
        style="padding: 10px 12px; border-radius: 8px; margin-bottom: 12px;
               background: color-mix(in srgb, #d9534f 14%, var(--surface));
               color: var(--text-primary);
               font-family: 'JetBrains Mono', ui-monospace, monospace;
               font-size: 0.8rem; white-space: pre-wrap;"
      >
        {sqlEditorStore.error}
      </div>
    {/if}

    {#if sqlEditorStore.resultHtml}
      <div class="text-text-secondary" style="font-size: 0.72rem; margin-bottom: 6px;">
        {sqlEditorStore.rowCount} row{sqlEditorStore.rowCount === 1 ? "" : "s"}
        · {sqlEditorStore.elapsedMs.toFixed(0)} ms
      </div>
      <TableResult html={sqlEditorStore.resultHtml} title="sql-result" />
    {:else if !sqlEditorStore.error && !sqlEditorStore.running}
      <div
        class="text-text-secondary flex items-center justify-center"
        style="min-height: 120px; font-size: 0.85rem;"
      >
        Write a query and press Run to see results here.
      </div>
    {/if}
  </div>
</div>
