<script lang="ts">
  import { respondSqlConfirm } from "$lib/api/chat";

  interface Props {
    sql: string;
    requestId: string;
  }

  let { sql, requestId }: Props = $props();

  // svelte-ignore state_referenced_locally
  let editedSql = $state(sql);
  let responded = $state(false);
  let responseLabel = $state("");

  async function handleAction(action: "approve" | "edit" | "reject") {
    responded = true;
    responseLabel =
      action === "approve"
        ? "Approved"
        : action === "edit"
          ? "Approved (edited)"
          : "Rejected";
    await respondSqlConfirm(requestId, action, editedSql);
  }
</script>

<div
  class="bg-surface max-w-[900px] mx-auto w-full animate-fade-in"
  style="border: 1px solid var(--yellow, #e8a317); border-radius: var(--radius);
         padding: 12px 16px; margin: 8px auto;"
>
  <div class="flex items-center text-text-primary" style="gap: 8px; font-size: 0.85rem; font-weight: 500; margin-bottom: 8px;">
    <span class="w-2 h-2 rounded-full bg-orange"></span>
    Review SQL before execution
  </div>

  <textarea
    bind:value={editedSql}
    disabled={responded}
    spellcheck="false"
    class="w-full bg-bg border border-border text-text-primary
      focus:outline-none focus:border-teal disabled:opacity-60"
    style="min-height: 80px; max-height: 300px; padding: 10px 12px; border-radius: 6px;
           font-family: 'JetBrains Mono', monospace; font-size: 0.82rem; line-height: 1.5;
           resize: vertical; box-sizing: border-box;"
  ></textarea>

  {#if responded}
    <div
      class="font-medium"
      style="font-size: 0.85rem; margin-top: 8px;
             color: {responseLabel === 'Rejected' ? '#e74c3c' : 'var(--teal)'};"
    >
      {responseLabel}
    </div>
  {:else}
    <div class="flex items-center" style="gap: 8px; margin-top: 8px;">
      <button
        class="bg-teal text-white cursor-pointer border border-teal
          hover:opacity-90 transition-opacity"
        style="padding: 5px 14px; border-radius: 6px; font-family: inherit;
               font-size: 0.8rem; font-weight: 500;"
        onclick={() => handleAction("approve")}
      >
        Approve
      </button>
      <button
        class="bg-transparent text-text-primary border border-border cursor-pointer
          hover:border-teal transition-colors"
        style="padding: 5px 14px; border-radius: 6px; font-family: inherit;
               font-size: 0.8rem; font-weight: 500;"
        onclick={() => handleAction("edit")}
      >
        Approve with edits
      </button>
      <button
        class="bg-transparent border cursor-pointer transition-all sql-reject-btn"
        style="padding: 5px 14px; border-radius: 6px; font-family: inherit;
               font-size: 0.8rem; font-weight: 500;
               color: #e74c3c; border-color: #e74c3c;"
        onclick={() => handleAction("reject")}
      >
        Reject
      </button>
    </div>
  {/if}
</div>
