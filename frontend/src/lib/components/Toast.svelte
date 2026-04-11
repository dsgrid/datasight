<script lang="ts">
  import { toastStore } from "$lib/stores/toast.svelte";
</script>

{#if toastStore.toasts.length > 0}
  <div class="fixed flex flex-col" style="bottom: 24px; right: 24px; z-index: 200; gap: 8px;">
    {#each toastStore.toasts as toast (toast.id)}
      <div
        class="bg-surface border border-border text-text-primary animate-fade-in"
        style="border-radius: 8px; padding: 12px 20px;
               box-shadow: 0 4px 20px rgba(0,0,0,0.12);
               font-size: 0.85rem; max-width: 360px;
               border-left: 3px solid {toast.type === 'error'
                 ? '#e55'
                 : toast.type === 'success'
                   ? 'var(--teal)'
                   : 'var(--navy)'};"
      >
        {toast.text}
      </div>
    {/each}
  </div>
{/if}
