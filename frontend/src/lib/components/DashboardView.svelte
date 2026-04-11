<script lang="ts">
  import DashboardToolbar from "./DashboardToolbar.svelte";
  import DashboardGrid from "./DashboardGrid.svelte";
</script>

<div
  class="flex flex-col flex-1 min-w-0 min-h-0"
  style="background: linear-gradient(180deg, color-mix(in srgb, var(--bg) 88%, var(--surface) 12%), var(--bg));"
>
  <DashboardToolbar />
  <DashboardGrid />
</div>
