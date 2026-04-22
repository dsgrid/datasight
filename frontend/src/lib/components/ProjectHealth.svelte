<script lang="ts">
  interface HealthCheck {
    name: string;
    ok: boolean;
    category: string;
    detail: string;
    remediation?: string;
  }

  interface Props {
    data: Record<string, unknown> | null;
  }

  let { data }: Props = $props();

  let checks = $derived<HealthCheck[]>(
    (data?.checks as HealthCheck[]) || [],
  );
  let summary = $derived(
    (data?.summary as { ok_count?: number; fail_count?: number }) || {},
  );
  let totalCount = $derived(checks.length);
  let healthy = $derived((summary.fail_count || 0) === 0);
  let failing = $derived(checks.filter((c) => !c.ok));
  let passing = $derived(checks.filter((c) => c.ok));
  let projectName = $derived(
    data?.project_loaded
      ? ((data?.project_dir as string) || "Project loaded").split("/").pop() ||
        "Project"
      : "No project loaded",
  );
  let dbMode = $derived((data?.db_mode as string) || null);
  let dbTarget = $derived((data?.db_target as string) || null);
  let llmProvider = $derived((data?.llm_provider as string) || null);

  // Show the OK rows only when the user opens the disclosure. Default
  // closed because in the healthy case they're just noise.
  let showAll = $state(false);
</script>

{#if !data}
  <p class="health-empty">Checking project health...</p>
{:else if checks.length === 0}
  <p class="health-empty">No health data available.</p>
{:else}
  <div class="health-grid">
    <!-- Identity rows: who/what is configured. Always visible. -->
    <dl class="identity">
      <div class="identity-row" title={data?.project_dir as string ?? ""}>
        <dt>Project</dt>
        <dd>{projectName}</dd>
      </div>
      {#if dbMode}
        <div class="identity-row" title={dbTarget ?? ""}>
          <dt>Database</dt>
          <dd>
            <span class="mode">{dbMode}</span>
            {#if dbTarget && dbTarget !== dbMode}
              <span class="target">{dbTarget}</span>
            {/if}
          </dd>
        </div>
      {/if}
      {#if llmProvider}
        <div class="identity-row">
          <dt>LLM</dt>
          <dd>{llmProvider}</dd>
        </div>
      {/if}
    </dl>

    <!-- Status pill: one line in the healthy case, click to expand. -->
    <button
      type="button"
      class="status-pill {healthy ? 'healthy' : 'has-failures'}"
      aria-expanded={showAll}
      onclick={() => (showAll = !showAll)}
    >
      <span class="status-text">
        {#if healthy}
          All {totalCount} checks passed
        {:else}
          {summary.fail_count} of {totalCount} checks failing
        {/if}
      </span>
      <span class="chev" aria-hidden="true">{showAll ? "▾" : "▸"}</span>
    </button>

    <!-- Failing checks + remediation hints — always visible when present. -->
    {#if failing.length > 0}
      <div class="failing-list">
        {#each failing as check}
          <div class="health-row fail">
            <span class="health-name">{check.name}</span>
            <span class="health-status fail">FAIL</span>
            {#if check.detail}
              <span class="health-detail">{check.detail}</span>
            {/if}
            {#if check.remediation}
              <span class="health-remediation">{check.remediation}</span>
            {/if}
          </div>
        {/each}
      </div>
    {/if}

    <!-- All passing checks behind the disclosure. -->
    {#if showAll && passing.length > 0}
      <div class="passing-list">
        {#each passing as check}
          <div class="health-row">
            <span class="health-name">{check.name}</span>
            <span class="health-status ok">OK</span>
            {#if check.detail}
              <span class="health-detail">{check.detail}</span>
            {/if}
          </div>
        {/each}
      </div>
    {/if}
  </div>
{/if}

<style>
  .health-grid {
    display: grid;
    gap: 10px;
  }

  .health-empty {
    font-size: 0.78rem;
    color: var(--text-secondary);
  }

  .identity {
    display: grid;
    gap: 4px;
    margin: 0;
    padding: 10px 12px;
    border-radius: 10px;
    border: 1px solid var(--border);
    background: color-mix(in srgb, var(--bg) 84%, var(--surface));
    font-size: 0.78rem;
  }

  .identity-row {
    display: grid;
    grid-template-columns: 70px minmax(0, 1fr);
    gap: 8px;
    align-items: baseline;
  }

  .identity-row dt {
    color: var(--text-secondary);
    font-weight: 600;
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }

  .identity-row dd {
    margin: 0;
    color: var(--text);
    word-break: break-word;
  }

  .identity-row .mode {
    font-weight: 600;
    margin-right: 6px;
  }

  .identity-row .target {
    color: var(--text-secondary);
    font-size: 0.72rem;
  }

  .status-pill {
    display: flex;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    padding: 8px 12px;
    border-radius: 999px;
    border: 1px solid var(--border);
    background: var(--bg);
    color: var(--text);
    font-size: 0.78rem;
    font-weight: 600;
    cursor: pointer;
    text-align: left;
  }

  .status-pill.healthy {
    border-color: color-mix(in srgb, var(--teal) 32%, var(--border));
    background: color-mix(in srgb, var(--teal) 6%, var(--bg));
  }

  .status-pill.has-failures {
    border-color: color-mix(in srgb, var(--orange) 34%, var(--border));
    background: color-mix(in srgb, var(--orange) 6%, var(--bg));
  }

  .status-pill .chev {
    color: var(--text-secondary);
    font-size: 0.78rem;
  }

  .failing-list,
  .passing-list {
    display: grid;
    gap: 8px;
  }

  .health-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: 4px 10px;
    padding: 10px 12px;
    border-radius: 10px;
    border: 1px solid var(--border);
    background: color-mix(in srgb, var(--bg) 84%, var(--surface));
  }

  .health-row.fail {
    border-color: color-mix(in srgb, var(--orange) 24%, var(--border));
    background: color-mix(in srgb, var(--orange) 5%, var(--bg));
  }

  .health-name {
    font-size: 0.78rem;
    font-weight: 600;
    color: var(--text);
  }

  .health-status {
    font-size: 0.68rem;
    font-weight: 700;
    letter-spacing: 0.04em;
  }

  .health-status.ok {
    color: var(--teal);
  }

  .health-status.fail {
    color: var(--orange);
  }

  .health-detail {
    grid-column: 1 / -1;
    color: var(--text-secondary);
    font-size: 0.74rem;
    word-break: break-word;
  }

  .health-remediation {
    grid-column: 1 / -1;
    color: var(--text);
    font-size: 0.74rem;
    word-break: break-word;
    padding-top: 2px;
    border-top: 1px solid color-mix(in srgb, var(--orange) 18%, transparent);
    margin-top: 4px;
  }
</style>
