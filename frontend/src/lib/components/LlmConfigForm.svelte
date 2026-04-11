<script lang="ts">
  import { saveLlmConfig } from "$lib/api/settings";
  import { settingsStore } from "$lib/stores/settings.svelte";

  interface Props {
    onConnected?: () => void;
    compact?: boolean;
  }

  let { onConnected, compact = false }: Props = $props();

  const MODEL_DEFAULTS: Record<string, string> = {
    anthropic: "claude-haiku-4-5-20251001",
    ollama: "qwen3.5:35b-a3b",
    github: "gpt-4o",
  };

  let provider = $state(settingsStore.llmConfig?.provider || "anthropic");
  let apiKey = $state("");
  let model = $state(settingsStore.llmConfig?.model || "");
  let baseUrl = $state(settingsStore.llmConfig?.base_url || "");
  let error = $state("");
  let saving = $state(false);

  let envKeys = $derived(settingsStore.llmConfig?.env_keys || {});
  let envModels = $derived(settingsStore.llmConfig?.env_models || {});

  let providerHasEnvKey = $derived(envKeys[provider] || false);
  let providerEnvModel = $derived(envModels[provider] || "");

  let modelPlaceholder = $derived(
    providerEnvModel || MODEL_DEFAULTS[provider] || "Model name",
  );

  // Reset form fields when provider changes
  let prevProvider = $state(provider);
  $effect(() => {
    if (provider !== prevProvider) {
      model = "";
      apiKey = "";
      baseUrl = "";
      error = "";
      prevProvider = provider;
    }
  });

  let showApiKey = $derived(provider === "anthropic" || provider === "github");
  let showBaseUrl = $derived(provider === "ollama");

  async function handleConnect() {
    error = "";
    saving = true;

    try {
      const data = await saveLlmConfig({
        provider,
        api_key: apiKey || undefined,
        model: model || providerEnvModel || MODEL_DEFAULTS[provider] || "",
        base_url: baseUrl || undefined,
      });

      if (data.connected) {
        apiKey = "";
        onConnected?.();
      } else {
        error = data.error || "Failed to connect. Check your API key and settings.";
      }
    } catch {
      error = "Connection failed";
    } finally {
      saving = false;
    }
  }
</script>

<div class={compact ? "space-y-3" : "space-y-4"}>
  <label class="block" style="font-size: 0.8rem; color: var(--text-secondary); margin-bottom: 12px;">
    <span class="block" style="margin-bottom: 4px;">Provider</span>
    <select
      bind:value={provider}
      class="w-full border border-border bg-bg text-text-primary focus:outline-none focus:border-teal"
      style="display: block; margin-top: 4px; padding: 8px 10px; border-radius: 6px;
             font-family: inherit; font-size: 0.85rem;"
    >
      <option value="anthropic">Anthropic</option>
      <option value="ollama">Ollama</option>
      <option value="github">GitHub Models</option>
    </select>
  </label>

  {#if showApiKey}
    <label class="block" style="font-size: 0.8rem; color: var(--text-secondary); margin-bottom: 12px;">
      <span class="block" style="margin-bottom: 4px;">API Key</span>
      <input
        type="password"
        bind:value={apiKey}
        placeholder={providerHasEnvKey
          ? "Set from environment"
          : "Enter API key..."}
        class="w-full border border-border bg-bg text-text-primary focus:outline-none focus:border-teal"
        style="display: block; margin-top: 4px; padding: 8px 10px; border-radius: 6px;
               font-family: inherit; font-size: 0.85rem;"
      />
    </label>
  {/if}

  <label class="block" style="font-size: 0.8rem; color: var(--text-secondary); margin-bottom: 12px;">
    <span class="block" style="margin-bottom: 4px;">Model</span>
    <input
      type="text"
      bind:value={model}
      placeholder={modelPlaceholder}
      class="w-full border border-border bg-bg text-text-primary focus:outline-none focus:border-teal"
      style="display: block; margin-top: 4px; padding: 8px 10px; border-radius: 6px;
             font-family: inherit; font-size: 0.85rem;"
    />
  </label>

  {#if showBaseUrl}
    <label class="block" style="font-size: 0.8rem; color: var(--text-secondary); margin-bottom: 12px;">
      <span class="block" style="margin-bottom: 4px;">Base URL</span>
      <input
        type="text"
        bind:value={baseUrl}
        placeholder="http://localhost:11434/v1"
        class="w-full border border-border bg-bg text-text-primary focus:outline-none focus:border-teal"
        style="display: block; margin-top: 4px; padding: 8px 10px; border-radius: 6px;
               font-family: inherit; font-size: 0.85rem;"
      />
    </label>
  {/if}

  <button
    onclick={handleConnect}
    disabled={saving}
    class="w-full bg-teal text-white font-medium cursor-pointer
      hover:opacity-90 transition-opacity disabled:opacity-50 disabled:cursor-not-allowed"
    style="padding: 10px 20px; border: none; border-radius: 8px; font-family: inherit; font-size: 0.85rem;"
  >
    {saving ? "Connecting..." : "Connect"}
  </button>

  {#if error}
    <p style="font-size: 0.75rem; color: #e55; margin-top: 8px;">{error}</p>
  {/if}
</div>
