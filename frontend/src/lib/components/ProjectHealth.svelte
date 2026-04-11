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
  let projectDir = $derived(
    data?.project_loaded
      ? (data?.project_dir as string) || "Project loaded"
      : "No project loaded",
  );
  let hints = $derived(
    checks.filter((c) => !c.ok && c.remediation),
  );
  let healthy = $derived((summary.fail_count || 0) === 0);
</script>

{#if !data}
  <p class="health-empty">Checking project health...</p>
{:else if checks.length === 0}
  <p class="health-empty">No health data available.</p>
{:else}
  <div class="health-grid">
    <!-- Summary -->
    <div class="health-summary {healthy ? 'healthy' : 'has-failures'}">
      <strong>{projectDir}</strong>
      <span>{summary.ok_count || 0} OK &bull; {summary.fail_count || 0} failing</span>
    </div>

    <!-- Remediation hints -->
    {#if hints.length > 0}
      <div class="health-hints">
        {#each hints as hint}
          <div class="health-hint">
            <strong>{hint.name}:</strong> {hint.remediation}
          </div>
        {/each}
      </div>
    {/if}

    <!-- Check rows -->
    {#each checks as check}
      <div class="health-row {check.ok ? '' : 'fail'}">
        <span class="health-name">{check.name}</span>
        <span class="health-status {check.ok ? 'ok' : 'fail'}">
          {check.ok ? "OK" : "FAIL"}
        </span>
        {#if check.detail}
          <span class="health-detail">{check.detail}</span>
        {:else}
          <span class="health-detail">{check.category}</span>
        {/if}
      </div>
    {/each}
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

  .health-summary {
    padding: 10px 12px;
    border-radius: 10px;
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text);
    font-size: 0.78rem;
    word-break: break-word;
    display: grid;
    gap: 4px;
  }

  .health-summary.healthy {
    border-color: color-mix(in srgb, var(--teal) 32%, var(--border));
    background: color-mix(in srgb, var(--teal) 6%, var(--bg));
  }

  .health-summary.has-failures {
    border-color: color-mix(in srgb, var(--orange) 34%, var(--border));
    background: color-mix(in srgb, var(--orange) 6%, var(--bg));
  }

  .health-hints {
    display: grid;
    gap: 8px;
  }

  .health-hint {
    padding: 10px 12px;
    border-radius: 10px;
    border: 1px solid color-mix(in srgb, var(--orange) 28%, var(--border));
    background: color-mix(in srgb, var(--orange) 6%, var(--bg));
    color: var(--text);
    font-size: 0.74rem;
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
</style>
