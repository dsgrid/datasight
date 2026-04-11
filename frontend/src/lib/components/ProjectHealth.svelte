<script lang="ts">
  import { escapeHtml } from "$lib/utils/search";

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
</script>

{#if !data}
  <p class="text-xs text-text-secondary">Checking project health...</p>
{:else if checks.length === 0}
  <p class="text-xs text-text-secondary">No health data available.</p>
{:else}
  <div class="space-y-3">
    <!-- Summary -->
    <div
      class="px-3 py-2 rounded-lg text-sm
        {(summary.fail_count || 0) > 0
        ? 'bg-red-50 text-red-800 dark:bg-red-900/20 dark:text-red-300'
        : 'bg-teal/5 text-teal'}"
    >
      <strong class="block text-xs">{projectDir}</strong>
      <span class="text-xs">
        {summary.ok_count || 0} OK &bull; {summary.fail_count || 0} failing
      </span>
    </div>

    <!-- Remediation hints -->
    {#if hints.length > 0}
      <div class="space-y-1">
        {#each hints as hint}
          <div class="text-xs px-3 py-1.5 rounded bg-orange/5 text-orange">
            <strong>{hint.name}:</strong>
            {hint.remediation}
          </div>
        {/each}
      </div>
    {/if}

    <!-- Check rows -->
    <div class="space-y-1">
      {#each checks as check}
        <div
          class="flex items-center gap-2 px-2 py-1.5 rounded text-xs
            {check.ok ? '' : 'bg-red-50/50 dark:bg-red-900/10'}"
        >
          <span
            class="w-8 text-center font-semibold rounded px-1
              {check.ok
              ? 'text-teal bg-teal/10'
              : 'text-red-600 bg-red-100 dark:bg-red-900/30'}"
          >
            {check.ok ? "OK" : "FAIL"}
          </span>
          <span class="font-medium text-text-primary flex-1 truncate">
            {check.name}
          </span>
          <span class="text-text-secondary truncate max-w-32">
            {check.detail || check.category}
          </span>
        </div>
      {/each}
    </div>
  </div>
{/if}
