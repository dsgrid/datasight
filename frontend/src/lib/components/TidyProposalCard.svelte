<script lang="ts">
  import { tidyStore, type ProposalState } from "$lib/stores/tidy.svelte";
  import { renderTidySql, type TidyProposal } from "$lib/api/tidy";
  import TidyMeltDiagram from "./TidyMeltDiagram.svelte";

  interface Props {
    card: ProposalState;
  }

  let { card }: Props = $props();

  let showSql = $state(false);
  let renderedSql = $state<string | null>(null);
  let renderingSql = $state(false);
  let sqlError = $state<string | null>(null);

  // Build the proposal that the apply step would actually use — base
  // proposal merged with the user's current edits. Same merge as
  // tidyStore.applyAll, kept inline so the SQL preview tracks edits live.
  let editedProposal = $derived<TidyProposal>({
    ...card.proposal,
    target_object_name: card.edits.target_object_name,
    value_column: card.edits.value_column,
    id_columns: [...card.edits.id_columns],
    include_nulls: card.edits.include_nulls,
  });

  // Re-render SQL whenever the panel is open and any of (mode, edits)
  // changes. The pre-baked reshape_sql_view/reshape_sql_table fields on
  // the proposal are frozen at detect time and don't reflect edits, so
  // we fetch a fresh build from the server.
  $effect(() => {
    if (!showSql) return;
    // Capture the deps Svelte should track for change detection.
    const proposal = editedProposal;
    const mode = tidyStore.mode;
    let cancelled = false;

    renderingSql = true;
    sqlError = null;
    renderTidySql({ proposal, mode })
      .then((resp) => {
        if (cancelled) return;
        renderedSql = resp.sql || null;
        sqlError = resp.error;
      })
      .catch((err) => {
        if (cancelled) return;
        sqlError = (err as Error).message ?? "Failed to render SQL";
      })
      .finally(() => {
        if (!cancelled) renderingSql = false;
      });

    return () => {
      cancelled = true;
    };
  });

  let badgeLabel = $derived(
    card.proposal.source === "deterministic"
      ? "regex"
      : card.proposal.source === "llm"
        ? "LLM"
        : card.proposal.source,
  );

  let confidenceLabel = $derived(card.proposal.confidence);

  let mappingsCount = $derived(card.proposal.column_mappings.length);

  let dimensionsLabel = $derived(
    card.proposal.dimensions
      .map((d) => `${d.name} (${d.kind}, ${d.dtype})`)
      .join(", "),
  );

  let idColumnsText = $derived(card.edits.id_columns.join(", "));

  let appliedDetail = $derived(() => {
    const r = card.applyResult;
    if (!r) return "";
    return `${r.row_count_target.toLocaleString()} rows in ${r.final_target_name}`;
  });

  function handleIdColumnsChange(value: string) {
    const parts = value
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
    tidyStore.editProposal(card.id, { id_columns: parts });
  }

  async function handlePreviewClick() {
    if (card.preview.open && (card.preview.html || card.preview.error)) {
      tidyStore.togglePreviewOpen(card.id);
      return;
    }
    await tidyStore.loadPreview(card.id);
  }
</script>

<div
  class="card"
  class:skipped={card.skipped}
  class:applied={card.status === "applied"}
  class:apply-error={card.status === "apply_error"}
>
  <header class="card-head">
    <div class="card-title">
      <span class="badge badge-{card.proposal.source}">{badgeLabel}</span>
      <span class="badge badge-conf badge-conf-{confidenceLabel}">
        {confidenceLabel}
      </span>
      <strong>
        {card.proposal.table} → {card.edits.target_object_name}
      </strong>
    </div>
    {#if card.status !== "applied"}
      <button
        type="button"
        class="skip-btn"
        class:skip-btn-active={card.skipped}
        onclick={() => tidyStore.toggleSkip(card.id)}
        disabled={card.status === "applying"}
        title={card.skipped
          ? "This proposal will be skipped on Apply. Click to include it."
          : "Exclude this proposal from the next Apply."}
      >
        {card.skipped ? "Skipped" : "Skip"}
      </button>
    {/if}
  </header>

  {#if card.proposal.rationale}
    <p class="rationale">{card.proposal.rationale}</p>
  {/if}

  <div class="diagram-wrap">
    <TidyMeltDiagram proposal={card.proposal} />
  </div>

  <dl class="meta">
    <div>
      <dt>Pattern</dt>
      <dd>{card.proposal.pattern}</dd>
    </div>
    <div>
      <dt>Dimensions</dt>
      <dd>{dimensionsLabel}</dd>
    </div>
    <div>
      <dt>Mapped columns</dt>
      <dd>{mappingsCount}</dd>
    </div>
  </dl>

  <fieldset class="edits">
    <legend>Edits</legend>
    <label class="field">
      <span>Target name</span>
      <input
        type="text"
        value={card.edits.target_object_name}
        oninput={(e) =>
          tidyStore.editProposal(card.id, {
            target_object_name: e.currentTarget.value,
          })}
        disabled={card.status === "applied" || card.status === "applying"}
      />
    </label>
    <label class="field">
      <span>Value column</span>
      <input
        type="text"
        value={card.edits.value_column}
        oninput={(e) =>
          tidyStore.editProposal(card.id, {
            value_column: e.currentTarget.value,
          })}
        disabled={card.status === "applied" || card.status === "applying"}
      />
    </label>
    <label class="field field-wide">
      <span>Id columns (comma-separated)</span>
      <input
        type="text"
        value={idColumnsText}
        oninput={(e) => handleIdColumnsChange(e.currentTarget.value)}
        disabled={card.status === "applied" || card.status === "applying"}
        placeholder="region, country"
      />
    </label>
    <label class="field field-wide field-checkbox">
      <input
        type="checkbox"
        checked={card.edits.include_nulls}
        onchange={(e) =>
          tidyStore.editProposal(card.id, {
            include_nulls: e.currentTarget.checked,
          })}
        disabled={card.status === "applied" || card.status === "applying"}
      />
      <span>Keep rows with NULL values</span>
      <small>
        Off by default — wide-table NULLs are usually structural
        placeholders. Turn on for data where NULL is a real missing
        observation (sensor outage, optional answer) you want to keep.
      </small>
    </label>
  </fieldset>

  <div class="actions">
    <button class="btn ghost" onclick={handlePreviewClick}>
      {card.preview.open ? "Hide preview" : "Preview rows"}
    </button>
    <button class="btn ghost" onclick={() => (showSql = !showSql)}>
      {showSql ? "Hide SQL" : "Show SQL"}
    </button>
  </div>

  {#if card.preview.open}
    <div class="preview">
      {#if card.preview.loading}
        <p class="muted">Loading sample…</p>
      {:else if card.preview.error}
        <p class="error">Preview failed: {card.preview.error}</p>
      {:else if card.preview.html}
        <p class="muted">
          {card.preview.rowCount} sample row{card.preview.rowCount === 1
            ? ""
            : "s"}
        </p>
        <div class="preview-table">
          {@html card.preview.html}
        </div>
      {/if}
    </div>
  {/if}

  {#if showSql}
    <div class="sql-wrap">
      <div class="sql-head">
        <span class="sql-mode-badge">mode: {tidyStore.mode}</span>
        {#if renderingSql}
          <span class="sql-status">rendering…</span>
        {/if}
      </div>
      {#if sqlError}
        <p class="sql-error">SQL render failed: {sqlError}</p>
      {:else if renderedSql !== null}
        <pre class="sql">{renderedSql}</pre>
      {:else if !renderingSql}
        <p class="sql-status">No SQL yet — toggle Show SQL to render.</p>
      {/if}
    </div>
  {/if}

  {#if card.status === "applied"}
    <p class="status status-ok">Applied · {appliedDetail()}</p>
  {:else if card.status === "apply_error" && card.applyError}
    <p class="status status-err">Apply failed: {card.applyError}</p>
  {:else if card.status === "applying"}
    <p class="status status-pending">Applying…</p>
  {/if}
</div>

<style>
  .card {
    display: grid;
    gap: 12px;
    padding: 14px 16px;
    border: 1px solid var(--border);
    border-radius: 12px;
    background: var(--surface);
    transition: border-color 0.15s, background 0.15s;
  }
  .card.skipped {
    border-style: dashed;
    background: color-mix(in srgb, var(--bg) 70%, var(--surface));
    opacity: 0.65;
  }
  .card.applied {
    border-color: color-mix(in srgb, var(--teal) 70%, var(--border));
    background: color-mix(in srgb, var(--teal) 9%, var(--surface));
  }
  .card.apply-error {
    border-color: color-mix(in srgb, #ef4444 60%, var(--border));
    background: color-mix(in srgb, #ef4444 6%, var(--surface));
  }

  .card-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    flex-wrap: wrap;
  }

  .card-title {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.88rem;
    color: var(--text);
  }
  .card-title strong {
    font-weight: 600;
  }

  .badge {
    font-size: 0.62rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-weight: 700;
    padding: 2px 7px;
    border-radius: 999px;
  }
  .badge-deterministic {
    background: color-mix(in srgb, var(--text-secondary) 14%, transparent);
    color: var(--text-secondary);
  }
  .badge-llm {
    background: color-mix(in srgb, var(--teal) 22%, transparent);
    color: color-mix(in srgb, var(--teal) 80%, var(--text));
  }
  .badge-user {
    background: color-mix(in srgb, var(--orange) 22%, transparent);
    color: color-mix(in srgb, var(--orange) 78%, var(--text));
  }

  .badge-conf {
    background: color-mix(in srgb, var(--text-secondary) 10%, transparent);
    color: var(--text-secondary);
  }
  .badge-conf-high {
    background: color-mix(in srgb, var(--teal) 18%, transparent);
    color: color-mix(in srgb, var(--teal) 75%, var(--text));
  }
  .badge-conf-low {
    background: color-mix(in srgb, #ef4444 18%, transparent);
    color: color-mix(in srgb, #ef4444 80%, var(--text));
  }

  /* Skip toggle in the card header. Default state is a quiet ghost
     button that nudges away the active proposal; once skipped, the
     button takes a stronger style so it's obviously the toggle that
     re-enables the card. */
  .skip-btn {
    border: 1px solid var(--border);
    background: var(--surface);
    color: var(--text-secondary);
    font: inherit;
    font-size: 0.74rem;
    padding: 4px 10px;
    border-radius: 999px;
    cursor: pointer;
    transition: background 0.15s, color 0.15s, border-color 0.15s;
  }
  .skip-btn:hover {
    background: color-mix(in srgb, var(--text-secondary) 8%, var(--surface));
    color: var(--text);
  }
  .skip-btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
  .skip-btn-active {
    background: color-mix(in srgb, var(--text-secondary) 18%, transparent);
    border-color: color-mix(in srgb, var(--text-secondary) 36%, var(--border));
    color: var(--text);
    font-weight: 600;
  }

  .rationale {
    margin: 0;
    font-size: 0.78rem;
    color: var(--text-secondary);
    line-height: 1.5;
  }

  .diagram-wrap {
    display: flex;
    justify-content: center;
    padding: 4px 0;
    background: color-mix(in srgb, var(--bg) 60%, var(--surface));
    border-radius: 8px;
  }

  .meta {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 8px;
    margin: 0;
    padding: 0;
    font-size: 0.72rem;
  }
  .meta div {
    display: grid;
    gap: 2px;
  }
  .meta dt {
    color: var(--text-secondary);
    font-size: 0.66rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-weight: 700;
  }
  .meta dd {
    margin: 0;
    color: var(--text);
    font-size: 0.78rem;
    word-break: break-word;
  }

  .edits {
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 10px 12px 12px;
    margin: 0;
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
    background: color-mix(in srgb, var(--bg) 50%, var(--surface));
  }
  .edits legend {
    padding: 0 6px;
    font-size: 0.7rem;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-weight: 700;
  }

  .field {
    display: grid;
    gap: 4px;
    font-size: 0.74rem;
  }
  .field-wide {
    grid-column: 1 / -1;
  }
  .field span {
    color: var(--text-secondary);
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }
  .field input {
    width: 100%;
    box-sizing: border-box;
    padding: 6px 9px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: color-mix(in srgb, var(--bg) 94%, white);
    color: var(--text);
    font-family: "JetBrains Mono", monospace;
    font-size: 0.78rem;
  }
  .field input:focus {
    outline: none;
    border-color: color-mix(in srgb, var(--teal) 50%, var(--border));
    box-shadow: 0 0 0 2px color-mix(in srgb, var(--teal) 14%, transparent);
  }
  .field input:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  /* Checkbox-style edit row: the input is inline, the label and helper
     text wrap around it instead of stacking like the text inputs above. */
  .field-checkbox {
    display: grid;
    grid-template-columns: auto 1fr;
    align-items: center;
    gap: 4px 10px;
  }
  .field-checkbox input[type="checkbox"] {
    width: auto;
    grid-row: 1;
    grid-column: 1;
    accent-color: var(--teal);
    transform: scale(1.1);
    cursor: pointer;
  }
  .field-checkbox > span {
    grid-row: 1;
    grid-column: 2;
    font-size: 0.78rem;
    text-transform: none;
    letter-spacing: 0;
    color: var(--text);
    font-weight: 500;
  }
  .field-checkbox > small {
    grid-row: 2;
    grid-column: 1 / -1;
    font-size: 0.68rem;
    line-height: 1.4;
    color: var(--text-secondary);
  }

  .actions {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }

  .btn {
    padding: 6px 12px;
    border-radius: 6px;
    font: inherit;
    font-size: 0.76rem;
    cursor: pointer;
    border: 1px solid var(--border);
    transition: background 0.15s, border-color 0.15s;
  }
  .btn.ghost {
    background: var(--surface);
    color: var(--text);
  }
  .btn.ghost:hover {
    background: color-mix(in srgb, var(--teal) 6%, var(--surface));
    border-color: color-mix(in srgb, var(--teal) 30%, var(--border));
  }

  .preview {
    border: 1px solid var(--border);
    border-radius: 8px;
    background: color-mix(in srgb, var(--bg) 92%, var(--surface));
    padding: 10px;
    display: grid;
    gap: 6px;
    overflow: hidden;
  }
  .preview .muted {
    margin: 0;
    font-size: 0.72rem;
    color: var(--text-secondary);
  }
  .preview .error {
    margin: 0;
    font-size: 0.78rem;
    color: #ef4444;
  }
  .preview-table {
    overflow: auto;
    max-height: 240px;
    font-size: 0.72rem;
  }
  .preview-table :global(table) {
    width: 100%;
    border-collapse: collapse;
  }
  .preview-table :global(th),
  .preview-table :global(td) {
    padding: 4px 8px;
    border-top: 1px solid var(--border);
    text-align: left;
  }
  .preview-table :global(th) {
    background: var(--surface);
    font-weight: 600;
  }

  .sql-wrap {
    display: grid;
    gap: 6px;
  }
  .sql-head {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 0.7rem;
    color: var(--text-secondary);
  }
  .sql-mode-badge {
    text-transform: uppercase;
    letter-spacing: 0.05em;
    font-weight: 700;
    padding: 2px 7px;
    border-radius: 999px;
    background: color-mix(in srgb, var(--teal) 14%, transparent);
    color: color-mix(in srgb, var(--teal) 75%, var(--text));
    font-size: 0.64rem;
  }
  .sql-status {
    font-style: italic;
    color: var(--text-secondary);
    font-size: 0.7rem;
  }
  .sql-error {
    margin: 0;
    padding: 6px 10px;
    border-radius: 6px;
    background: color-mix(in srgb, #ef4444 10%, transparent);
    color: color-mix(in srgb, #ef4444 80%, var(--text));
    font-size: 0.74rem;
  }

  .sql {
    margin: 0;
    padding: 10px 12px;
    background: color-mix(in srgb, var(--bg) 98%, black);
    color: var(--text);
    font-family: "JetBrains Mono", monospace;
    font-size: 0.72rem;
    line-height: 1.55;
    border-radius: 8px;
    overflow-x: auto;
    border: 1px solid var(--border);
    white-space: pre;
  }

  .status {
    margin: 0;
    font-size: 0.78rem;
    padding: 6px 10px;
    border-radius: 6px;
  }
  .status-ok {
    background: color-mix(in srgb, var(--teal) 14%, transparent);
    color: color-mix(in srgb, var(--teal) 80%, var(--text));
  }
  .status-err {
    background: color-mix(in srgb, #ef4444 12%, transparent);
    color: color-mix(in srgb, #ef4444 84%, var(--text));
  }
  .status-pending {
    color: var(--text-secondary);
    font-style: italic;
  }
</style>
