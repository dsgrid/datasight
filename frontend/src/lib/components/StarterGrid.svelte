<script lang="ts">
  import { sidebarStore } from "$lib/stores/sidebar.svelte";
  import { sessionStore } from "$lib/stores/session.svelte";

  interface StarterConfig {
    id: string;
    kicker: string;
    title: string;
    description: string;
  }

  const BASE_STARTERS: StarterConfig[] = [
    {
      id: "profile",
      kicker: "Orientation",
      title: "Profile this dataset",
      description:
        "Get a structured overview of tables, date coverage, measures, and likely dimensions.",
    },
    {
      id: "measures",
      kicker: "Measures",
      title: "Inspect key measures",
      description:
        "Identify likely energy measures and the safest default aggregations for each one.",
    },
    {
      id: "dimensions",
      kicker: "Discovery",
      title: "Find key dimensions",
      description:
        "Surface the categories, codes, and breakdowns worth exploring first.",
    },
    {
      id: "trend",
      kicker: "Visualization",
      title: "Build a trend chart",
      description:
        "Start with the most likely time-series view in the data.",
    },
    {
      id: "quality",
      kicker: "Quality",
      title: "Audit nulls and outliers",
      description:
        "Look for missingness, suspicious ranges, and columns that need scrutiny.",
    },
  ];

  const TIMESERIES_STARTER: StarterConfig = {
    id: "timeseries",
    kicker: "Completeness",
    title: "Check time series",
    description:
      "Detect gaps, duplicates, and DST issues in declared hourly time arrays.",
  };

  let STARTERS = $derived(
    sessionStore.hasTimeSeries
      ? [...BASE_STARTERS, TIMESERIES_STARTER]
      : BASE_STARTERS,
  );

  function selectStarter(id: string) {
    sidebarStore.pendingStarterAction = id;
  }
</script>

<div
  style="margin-bottom: 26px; padding: 22px;
         border: 1px solid color-mix(in srgb, var(--teal) 16%, var(--border));
         border-radius: 22px;
         background: linear-gradient(180deg, color-mix(in srgb, var(--surface) 90%, var(--teal) 10%), color-mix(in srgb, var(--surface) 97%, var(--bg)));
         box-shadow: 0 18px 48px rgba(2,61,96,0.08);"
>
  <div style="margin-bottom: 14px;">
    <div
      class="inline-flex items-center text-teal font-bold uppercase"
      style="padding: 4px 9px; margin-bottom: 10px; border-radius: 999px;
             background: color-mix(in srgb, var(--teal) 14%, transparent);
             font-size: 0.72rem; letter-spacing: 0.05em;"
    >
      Usage starters
    </div>
    <p class="text-text-secondary" style="font-size: 0.96rem; line-height: 1.55; max-width: 58ch;">
      Choose a concrete analysis path. After you open a file or project below,
      datasight will run it immediately.
    </p>
  </div>

  <div
    class="grid grid-cols-1 sm:grid-cols-2"
    style="gap: 14px;"
  >
    {#each STARTERS as starter}
      {@const isActive = sidebarStore.pendingStarterAction === starter.id}
      <button
        class="flex flex-col items-start text-left text-text-primary cursor-pointer landing-starter"
        class:active={isActive}
        style="gap: 8px; padding: 18px 20px; border: 1px solid {isActive ? 'var(--teal)' : 'var(--border)'};
               border-radius: 16px;
               background: linear-gradient(180deg, {isActive
                 ? 'color-mix(in srgb, var(--surface) 76%, var(--teal) 24%), var(--surface)'
                 : 'color-mix(in srgb, var(--surface) 92%, var(--cream) 8%), var(--surface)'});
               {isActive ? 'box-shadow: 0 0 0 1px color-mix(in srgb, var(--teal) 40%, transparent), 0 16px 36px rgba(21, 168, 168, 0.12);' : ''}"
        onclick={() => selectStarter(starter.id)}
      >
        <span class="font-bold uppercase text-orange" style="font-size: 0.7rem; letter-spacing: 0.04em;">
          {starter.kicker}
        </span>
        <strong style="font-size: 1rem; line-height: 1.25;">
          {starter.title}
        </strong>
        <span class="text-text-secondary" style="font-size: 0.84rem; line-height: 1.5;">
          {starter.description}
        </span>
      </button>
    {/each}
  </div>

  {#if sidebarStore.pendingStarterAction}
    <div
      class="text-text-secondary"
      style="margin-top: 14px; padding: 14px 16px;
             border: 1px solid color-mix(in srgb, var(--teal) 24%, var(--border));
             border-radius: 12px;
             background: color-mix(in srgb, var(--teal) 8%, var(--surface));
             font-size: 0.86rem; line-height: 1.5;"
    >
      Selections <strong class="text-text-primary">{STARTERS.find((s) => s.id === sidebarStore.pendingStarterAction)?.title}</strong>.
      Open a file or project below to start with a structured overview.
    </div>
  {/if}
</div>
