<script lang="ts">
  import { settingsStore } from "$lib/stores/settings.svelte";
  import { saveSettings, loadProjectHealth } from "$lib/api/settings";
  import LlmConfigForm from "./LlmConfigForm.svelte";
  import ProjectHealth from "./ProjectHealth.svelte";
  import { onMount } from "svelte";

  interface Props {
    open: boolean;
    onClose: () => void;
  }

  let { open, onClose }: Props = $props();

  let healthData = $state<Record<string, unknown> | null>(null);
  let healthError = $state("");

  async function toggleSetting(
    key:
      | "confirm_sql"
      | "explain_sql"
      | "clarify_sql"
      | "show_cost"
      | "show_provenance",
    currentValue: boolean,
  ) {
    // Optimistic update
    const newValue = !currentValue;
    switch (key) {
      case "confirm_sql":
        settingsStore.confirmSql = newValue;
        break;
      case "explain_sql":
        settingsStore.explainSql = newValue;
        break;
      case "clarify_sql":
        settingsStore.clarifySql = newValue;
        break;
      case "show_cost":
        settingsStore.showCost = newValue;
        break;
      case "show_provenance":
        settingsStore.showProvenance = newValue;
        break;
    }

    try {
      await saveSettings({ [key]: newValue });
    } catch {
      // Revert on failure
      switch (key) {
        case "confirm_sql":
          settingsStore.confirmSql = currentValue;
          break;
        case "explain_sql":
          settingsStore.explainSql = currentValue;
          break;
        case "clarify_sql":
          settingsStore.clarifySql = currentValue;
          break;
        case "show_cost":
          settingsStore.showCost = currentValue;
          break;
        case "show_provenance":
          settingsStore.showProvenance = currentValue;
          break;
      }
    }
  }

  async function refreshHealth() {
    healthError = "";
    try {
      healthData = await loadProjectHealth();
    } catch {
      healthError = "Failed to load project health.";
    }
  }

  $effect(() => {
    if (open) {
      refreshHealth();
    }
  });

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Escape") onClose();
  }
</script>

{#if open}
  <!-- Overlay -->
  <button
    class="fixed inset-0 bg-black/30 z-40 cursor-default"
    onclick={onClose}
    onkeydown={handleKeydown}
    tabindex="-1"
    aria-label="Close settings"
  ></button>

  <!-- Panel -->
  <div
    class="fixed right-0 top-0 bottom-0 bg-surface border-l border-border
      z-50 overflow-y-auto flex flex-col"
    style="width: 340px; max-width: 90vw; box-shadow: -4px 0 20px rgba(0,0,0,0.15);"
  >
    <!-- Header -->
    <div class="flex items-center justify-between border-b border-border"
      style="padding: 16px 20px 14px;">
      <span class="font-semibold" style="font-size: 1rem;">Settings</span>
      <button
        onclick={onClose}
        class="p-1.5 rounded-lg hover:bg-surface-alt transition-colors cursor-pointer text-text-secondary"
        aria-label="Close"
      >
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round">
          <path d="M4 4l8 8M12 4l-8 8" />
        </svg>
      </button>
    </div>

    <div class="flex-1 overflow-y-auto" style="padding: 16px 20px;">
    <div class="space-y-6">
      <!-- Query Behavior -->
      <div class="space-y-3">
        <h3 class="text-sm font-semibold text-text-primary">Query Behavior</h3>

        <label class="flex items-center gap-3 cursor-pointer">
          <input
            type="checkbox"
            checked={settingsStore.confirmSql}
            onchange={() => toggleSetting("confirm_sql", settingsStore.confirmSql)}
            class="w-4 h-4 rounded accent-teal"
          />
          <div>
            <div class="text-sm text-text-primary">Confirm SQL</div>
            <div class="text-xs text-text-secondary">
              Review SQL before execution
            </div>
          </div>
        </label>

        <label class="flex items-center gap-3 cursor-pointer">
          <input
            type="checkbox"
            checked={settingsStore.explainSql}
            onchange={() => toggleSetting("explain_sql", settingsStore.explainSql)}
            class="w-4 h-4 rounded accent-teal"
          />
          <div>
            <div class="text-sm text-text-primary">Explain SQL</div>
            <div class="text-xs text-text-secondary">
              Show natural language explanation before results
            </div>
          </div>
        </label>

        <label class="flex items-center gap-3 cursor-pointer">
          <input
            type="checkbox"
            checked={settingsStore.clarifySql}
            onchange={() => toggleSetting("clarify_sql", settingsStore.clarifySql)}
            class="w-4 h-4 rounded accent-teal"
          />
          <div>
            <div class="text-sm text-text-primary">Clarify</div>
            <div class="text-xs text-text-secondary">
              Ask clarifying questions instead of guessing
            </div>
          </div>
        </label>

        <label class="flex items-center gap-3 cursor-pointer">
          <input
            type="checkbox"
            checked={settingsStore.showCost}
            onchange={() => toggleSetting("show_cost", settingsStore.showCost)}
            class="w-4 h-4 rounded accent-teal"
          />
          <div>
            <div class="text-sm text-text-primary">Show Cost</div>
            <div class="text-xs text-text-secondary">
              Display estimated token cost per turn
            </div>
          </div>
        </label>

        <label class="flex items-center gap-3 cursor-pointer">
          <input
            type="checkbox"
            checked={settingsStore.showProvenance}
            onchange={() => toggleSetting("show_provenance", settingsStore.showProvenance)}
            class="w-4 h-4 rounded accent-teal"
          />
          <div>
            <div class="text-sm text-text-primary">Show Run Details</div>
            <div class="text-xs text-text-secondary">
              Show copyable query provenance after results
            </div>
          </div>
        </label>
      </div>

      <!-- LLM Configuration -->
      <div class="space-y-3">
        <div class="flex items-center gap-2">
          <h3 class="text-sm font-semibold text-text-primary">
            LLM Configuration
          </h3>
          <span
            class="text-xs px-2 py-0.5 rounded-full font-medium
              {settingsStore.llmConnected
              ? 'bg-teal/10 text-teal'
              : 'bg-red-100 text-red-600'}"
          >
            {settingsStore.llmConnected ? "Connected" : "Not configured"}
          </span>
        </div>
        <LlmConfigForm compact />
      </div>

      <!-- Project Health -->
      <div class="space-y-3">
        <h3 class="text-sm font-semibold text-text-primary">Project Health</h3>
        {#if healthError}
          <p class="text-xs text-text-secondary">{healthError}</p>
        {:else}
          <ProjectHealth data={healthData} />
        {/if}
      </div>
    </div>
    </div>
  </div>
{/if}
