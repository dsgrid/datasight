<script lang="ts">
  import { tidyStore } from "$lib/stores/tidy.svelte";
  import { toastStore } from "$lib/stores/toast.svelte";
  import { schemaStore, type TableInfo } from "$lib/stores/schema.svelte";
  import { groundingStore } from "$lib/stores/grounding.svelte";
  import TidyProposalCard from "./TidyProposalCard.svelte";

  let renameValid = $derived(
    tidyStore.dispositionMode !== "rename" ||
      tidyStore.dispositionRenameTo.trim().length > 0,
  );

  let canApply = $derived(
    tidyStore.pendingCount > 0 &&
      renameValid &&
      tidyStore.status !== "loading_deterministic" &&
      tidyStore.status !== "loading_llm",
  );

  let isDetectLoading = $derived(
    tidyStore.status === "loading_deterministic",
  );
  let isLlmLoading = $derived(tidyStore.status === "loading_llm");
  let isLoading = $derived(isDetectLoading || isLlmLoading);
  let canRunAgent = $derived(
    tidyStore.table !== null &&
      !isLoading &&
      tidyStore.status !== "error",
  );
  let llmAlreadyRan = $derived(
    tidyStore.status === "loaded_with_llm" ||
      tidyStore.proposals.some((p) => p.proposal.source === "llm"),
  );

  let dispositionDisabledForViewMode = $derived(tidyStore.mode === "view");

  // Reload the schema sidebar after each successful apply so the new
  // long-form object shows up immediately. Wired here (not in the store)
  // to avoid a circular import between tidyStore and schemaStore.
  $effect(() => {
    tidyStore.setAppliedHook((schemaInfo) => {
      if (Array.isArray(schemaInfo)) {
        schemaStore.schemaData = schemaInfo as TableInfo[];
      }
    });
    return () => tidyStore.setAppliedHook(null);
  });

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === "Escape") {
      tidyStore.close();
    }
  }

  async function handleApply() {
    const { applied, failed } = await tidyStore.applyAll();
    if (applied > 0) {
      toastStore.show(
        `Applied ${applied} proposal${applied === 1 ? "" : "s"}`,
        "success",
      );
      // Apply changed the schema; refresh the always-on header pill so
      // it reflects the new drift state (typically: drift just appeared).
      groundingStore.check();
    }
    if (failed > 0) {
      toastStore.show(
        `${failed} proposal${failed === 1 ? "" : "s"} failed to apply`,
        "error",
      );
    }
  }

  async function handleRepairGrounding() {
    await tidyStore.runGroundingRepair();
    if (tidyStore.repairStatus === "success") {
      const written = tidyStore.repairSummary?.files_written ?? [];
      const msg = written.length > 0
        ? `Rewrote ${written.join(", ")}`
        : "Grounding files were already up to date";
      toastStore.show(msg, "success");
    } else if (tidyStore.repairStatus === "error") {
      toastStore.show(
        tidyStore.repairError ?? "Grounding repair failed",
        "error",
      );
    }
    // Repair (success or partial-failure) changed something on disk;
    // re-poll so the header pill matches what the user just did.
    groundingStore.check();
  }

  let isRepairing = $derived(tidyStore.repairStatus === "running");

  function handleSampleRowsBlur(value: string) {
    const n = parseInt(value, 10);
    tidyStore.sampleRows = Number.isFinite(n) ? n : 0;
  }

  async function handleRunAgent() {
    await tidyStore.runAgent();
  }
</script>

{#if tidyStore.open}
  <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
  <div
    class="drawer-root"
    role="dialog"
    aria-modal="true"
    aria-label="Tidy review"
    tabindex="-1"
    onkeydown={handleKeydown}
  >
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div class="backdrop" onclick={() => tidyStore.close()}></div>

    <aside class="panel">
      <header class="panel-head">
        <div>
          <h3>Tidy review</h3>
          <p>
            {#if tidyStore.table}
              <code>{tidyStore.table}</code>
            {:else}
              &nbsp;
            {/if}
          </p>
        </div>
        <button class="close-btn" onclick={() => tidyStore.close()} title="Close (Esc)">
          ×
        </button>
      </header>

      <div class="panel-body">
        <!-- Grounding repair prompt: appears after apply when the static
             drift check found stale references in queries.yaml /
             schema_description.md / time_series.yaml. The LLM rewrite
             is slow so it's user-triggered, not automatic. -->
        {#if tidyStore.groundingDrift?.needs_repair}
          <section class="repair-panel" class:repair-panel-running={isRepairing}>
            <div class="repair-head">
              <div>
                <h4>
                  {#if isRepairing}
                    <span class="dot-flash" aria-hidden="true"></span>
                    Repairing grounding files…
                  {:else}
                    Grounding may be stale
                  {/if}
                </h4>
                <p>
                  {#if isRepairing}
                    The agent is rewriting queries.yaml,
                    schema_description.md, and time_series.yaml against
                    the new schema. Local models can take a few minutes.
                  {:else}
                    The reshape changed the schema and the static check
                    found {tidyStore.groundingDrift.drift_items} stale
                    reference{tidyStore.groundingDrift.drift_items === 1
                      ? ""
                      : "s"} in your grounding files. Run the LLM
                    repair to rewrite them, or skip if you'd rather edit
                    by hand.
                  {/if}
                </p>
              </div>
              {#if !isRepairing}
                <div class="repair-actions">
                  <button
                    class="btn ghost"
                    onclick={() => tidyStore.dismissGroundingPrompt()}
                  >
                    Skip
                  </button>
                  <button class="btn primary" onclick={handleRepairGrounding}>
                    Repair grounding
                  </button>
                </div>
              {/if}
            </div>
          </section>
        {/if}

        <!-- Last grounding-repair result. Stays visible after success
             so the user can see which files changed; hidden once they
             open the drawer for a new table. -->
        {#if tidyStore.repairStatus === "success" && tidyStore.repairSummary}
          <p class="banner banner-ok">
            {#if tidyStore.repairSummary.applied}
              Rewrote {tidyStore.repairSummary.files_written.join(", ")}.
            {:else if tidyStore.repairSummary.skipped === "no_drift"}
              Grounding files were already up to date.
            {:else if tidyStore.repairSummary.skipped === "no_llm_changes"}
              The agent didn't propose any changes.
            {:else}
              Grounding repair finished.
            {/if}
          </p>
        {/if}
        {#if tidyStore.repairStatus === "error"}
          <p class="banner banner-err">
            Grounding repair failed: {tidyStore.repairError ?? "unknown error"}
          </p>
        {/if}

        <!-- Run-agent panel: configure + trigger the LLM advisor -->
        <section class="agent-panel" class:agent-panel-running={isLlmLoading}>
          <div class="agent-head">
            <div>
              <h4>
                {#if isLlmLoading}
                  <span class="dot-flash" aria-hidden="true"></span>
                  Agent is reviewing the schema…
                {:else if llmAlreadyRan}
                  Agent reviewed the schema
                {:else}
                  Run the LLM agent for more candidates
                {/if}
              </h4>
              <p>
                {#if isLlmLoading}
                  This usually takes 5–30 seconds. You can cancel without
                  losing the deterministic results below.
                {:else if llmAlreadyRan}
                  Re-run if you tweak the sample-rows count or want a
                  different set of suggestions.
                {:else}
                  The deterministic detector ran instantly. Click Run to
                  ask the configured LLM for additional reshape candidates
                  the regex misses (fuel-as-column, geography-as-column,
                  multi-axis pivots).
                {/if}
              </p>
            </div>
            {#if isLlmLoading}
              <button class="btn ghost" onclick={() => tidyStore.cancel()}>
                Cancel
              </button>
            {:else}
              <button
                class="btn primary"
                disabled={!canRunAgent}
                onclick={handleRunAgent}
              >
                {llmAlreadyRan ? "Run again" : "Run agent"}
              </button>
            {/if}
          </div>
          <label class="agent-sample">
            <span>Sample rows sent to LLM</span>
            <input
              type="number"
              min="0"
              max="500"
              value={tidyStore.sampleRows}
              onblur={(e) => handleSampleRowsBlur(e.currentTarget.value)}
              disabled={isLoading}
              title="Values are sent over the network. 0 = schema-only."
            />
            <small>Values are sent over the network. 0 = schema-only.</small>
          </label>
        </section>

        <!-- Status banner: errors only (loading is shown in the agent panel) -->
        {#if isDetectLoading}
          <p class="banner">Detecting untidy patterns…</p>
        {:else if tidyStore.status === "error"}
          <p class="banner banner-err">
            {tidyStore.errorMessage ?? "Failed to load proposals"}
          </p>
        {/if}

        {#if tidyStore.parseWarnings.length > 0}
          <ul class="warnings">
            {#each tidyStore.parseWarnings as w}
              <li>{w}</li>
            {/each}
          </ul>
        {/if}

        <!-- Proposal cards -->
        {#if tidyStore.proposals.length === 0 && (tidyStore.status === "loaded_deterministic" || tidyStore.status === "loaded_with_llm")}
          <p class="empty">
            {tidyStore.status === "loaded_with_llm"
              ? "Agent found no additional candidates. The deterministic detector also came up empty."
              : "No deterministic patterns detected. Try Run agent for LLM-derived candidates."}
          </p>
        {:else}
          <div class="cards">
            {#each tidyStore.proposals as p (p.id)}
              <TidyProposalCard card={p} />
            {/each}
            {#if isLlmLoading}
              <div class="thinking-card" aria-live="polite">
                <span class="dot-flash" aria-hidden="true"></span>
                Agent is thinking…
              </div>
            {/if}
          </div>
        {/if}
      </div>

      <!-- Footer with batch controls -->
      <footer class="panel-foot">
        <div class="foot-row">
          <fieldset class="seg">
            <legend>Materialize as</legend>
            <label>
              <input
                type="radio"
                name="tidy-mode"
                value="view"
                checked={tidyStore.mode === "view"}
                onchange={() => (tidyStore.mode = "view")}
              />
              <span>View</span>
            </label>
            <label>
              <input
                type="radio"
                name="tidy-mode"
                value="table"
                checked={tidyStore.mode === "table"}
                onchange={() => (tidyStore.mode = "table")}
              />
              <span>Table</span>
            </label>
          </fieldset>

          <fieldset
            class="seg"
            class:disabled={dispositionDisabledForViewMode}
            title={dispositionDisabledForViewMode
              ? "Rename, Replace, and Drop require Materialize as Table — a view references its source by name."
              : ""}
          >
            <legend>Source</legend>
            <label title="Source and long form coexist (default).">
              <input
                type="radio"
                name="tidy-disp"
                value="keep"
                checked={tidyStore.dispositionMode === "keep"}
                onchange={() => (tidyStore.dispositionMode = "keep")}
              />
              <span>Keep</span>
            </label>
            <label title="Rename the source; long form lives at the target name.">
              <input
                type="radio"
                name="tidy-disp"
                value="rename"
                disabled={dispositionDisabledForViewMode}
                checked={tidyStore.dispositionMode === "rename"}
                onchange={() => (tidyStore.dispositionMode = "rename")}
              />
              <span>Rename</span>
            </label>
            <label title="Drop the source; long form takes over the source's name. Downstream code that referenced the source keeps working.">
              <input
                type="radio"
                name="tidy-disp"
                value="replace"
                disabled={dispositionDisabledForViewMode}
                checked={tidyStore.dispositionMode === "replace"}
                onchange={() => (tidyStore.dispositionMode = "replace")}
              />
              <span>Replace</span>
            </label>
            <label title="Drop the source; long form keeps its target name. Downstream references to the source name will break.">
              <input
                type="radio"
                name="tidy-disp"
                value="drop"
                disabled={dispositionDisabledForViewMode}
                checked={tidyStore.dispositionMode === "drop"}
                onchange={() => (tidyStore.dispositionMode = "drop")}
              />
              <span>Drop</span>
            </label>
          </fieldset>
        </div>

        {#if tidyStore.dispositionMode === "rename"}
          <label class="rename-input">
            <span>Rename source to</span>
            <input
              type="text"
              value={tidyStore.dispositionRenameTo}
              oninput={(e) =>
                (tidyStore.dispositionRenameTo = e.currentTarget.value)}
              placeholder="e.g. sales_wide_raw"
            />
          </label>
        {/if}

        <div class="apply-row">
          <span class="apply-count">
            {tidyStore.pendingCount} to apply
          </span>
          <button class="btn primary" disabled={!canApply} onclick={handleApply}>
            Apply
          </button>
        </div>
      </footer>
    </aside>
  </div>
{/if}

<style>
  .drawer-root {
    position: fixed;
    inset: 0;
    z-index: 60;
    display: flex;
    justify-content: flex-end;
  }

  .backdrop {
    position: absolute;
    inset: 0;
    background: rgba(0, 0, 0, 0.45);
    cursor: pointer;
  }

  .panel {
    position: relative;
    width: min(720px, 96vw);
    height: 100%;
    background: var(--surface);
    border-left: 1px solid color-mix(in srgb, var(--teal) 20%, var(--border));
    display: flex;
    flex-direction: column;
    box-shadow: -16px 0 40px rgba(0, 0, 0, 0.28);
    animation: slide-in 0.18s ease-out;
  }

  @keyframes slide-in {
    from {
      transform: translateX(8%);
      opacity: 0.6;
    }
    to {
      transform: translateX(0);
      opacity: 1;
    }
  }

  .panel-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
    padding: 16px 18px;
    border-bottom: 1px solid var(--border);
    background:
      linear-gradient(180deg,
        color-mix(in srgb, var(--teal) 10%, var(--surface)),
        color-mix(in srgb, var(--surface) 96%, var(--bg)));
  }
  .panel-head h3 {
    margin: 0 0 2px;
    font-size: 1rem;
    color: var(--text);
  }
  .panel-head p {
    margin: 0;
    font-size: 0.74rem;
    color: var(--text-secondary);
  }
  .panel-head code {
    font-family: "JetBrains Mono", monospace;
    font-size: 0.78rem;
    color: var(--text);
  }

  .close-btn {
    border: none;
    background: none;
    color: var(--text-secondary);
    font-size: 1.5rem;
    line-height: 1;
    cursor: pointer;
    padding: 2px 8px;
    border-radius: 6px;
    transition: background 0.15s, color 0.15s;
  }
  .close-btn:hover {
    background: color-mix(in srgb, var(--bg) 60%, var(--surface));
    color: var(--text);
  }

  .panel-body {
    flex: 1;
    overflow-y: auto;
    padding: 14px 18px;
    display: grid;
    gap: 12px;
  }

  .agent-panel {
    display: grid;
    gap: 10px;
    padding: 12px 14px;
    border: 1px solid color-mix(in srgb, var(--teal) 22%, var(--border));
    border-radius: 10px;
    background: color-mix(in srgb, var(--teal) 5%, var(--surface));
    transition: background 0.2s, border-color 0.2s;
  }
  .agent-panel-running {
    border-color: color-mix(in srgb, var(--orange) 50%, var(--border));
    background: color-mix(in srgb, var(--orange) 7%, var(--surface));
  }
  .agent-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
  }
  .agent-head h4 {
    margin: 0 0 4px;
    font-size: 0.86rem;
    color: var(--text);
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }
  .agent-head p {
    margin: 0;
    max-width: 52ch;
    font-size: 0.74rem;
    line-height: 1.5;
    color: var(--text-secondary);
  }
  .agent-sample {
    display: grid;
    grid-template-columns: auto 72px;
    align-items: center;
    gap: 6px 10px;
    font-size: 0.74rem;
    color: var(--text-secondary);
  }
  .agent-sample > span {
    grid-column: 1;
  }
  .agent-sample > input {
    grid-column: 2;
    width: 72px;
    padding: 4px 6px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--surface);
    color: var(--text);
    font-family: "JetBrains Mono", monospace;
    font-size: 0.78rem;
  }
  .agent-sample > input:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }
  .agent-sample > small {
    grid-column: 1 / -1;
    font-size: 0.68rem;
    color: var(--text-secondary);
    opacity: 0.85;
  }

  /* Pulsing-dot indicator used inside the agent panel header and the
     thinking-placeholder card while the LLM call is in flight. */
  .dot-flash {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 999px;
    background: color-mix(in srgb, var(--orange) 80%, var(--teal));
    box-shadow: 0 0 0 0 color-mix(in srgb, var(--orange) 40%, transparent);
    animation: dot-flash-pulse 1.1s ease-in-out infinite;
  }
  @keyframes dot-flash-pulse {
    0% {
      transform: scale(0.85);
      box-shadow: 0 0 0 0 color-mix(in srgb, var(--orange) 50%, transparent);
    }
    70% {
      transform: scale(1.1);
      box-shadow: 0 0 0 8px color-mix(in srgb, var(--orange) 0%, transparent);
    }
    100% {
      transform: scale(0.85);
      box-shadow: 0 0 0 0 color-mix(in srgb, var(--orange) 0%, transparent);
    }
  }

  .thinking-card {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 14px 16px;
    border: 1px dashed color-mix(in srgb, var(--orange) 50%, var(--border));
    border-radius: 12px;
    background: color-mix(in srgb, var(--orange) 5%, var(--surface));
    color: var(--text-secondary);
    font-size: 0.84rem;
    font-style: italic;
  }

  .banner {
    margin: 0;
    padding: 8px 12px;
    border-radius: 8px;
    background: color-mix(in srgb, var(--teal) 8%, var(--surface));
    border: 1px solid color-mix(in srgb, var(--teal) 26%, var(--border));
    font-size: 0.78rem;
    color: var(--text);
  }
  .banner-err {
    background: color-mix(in srgb, #ef4444 8%, var(--surface));
    border-color: color-mix(in srgb, #ef4444 36%, var(--border));
    color: color-mix(in srgb, #ef4444 80%, var(--text));
  }
  .banner-ok {
    background: color-mix(in srgb, #22c55e 8%, var(--surface));
    border-color: color-mix(in srgb, #22c55e 36%, var(--border));
    color: color-mix(in srgb, #16a34a 80%, var(--text));
  }

  /* Grounding-repair prompt: same visual language as .agent-panel so
     it reads as a peer prompt rather than an inline notice. Orange tint
     to flag that action is needed; flips to a more urgent border while
     the LLM call is in flight. */
  .repair-panel {
    display: grid;
    gap: 10px;
    padding: 12px 14px;
    border: 1px solid color-mix(in srgb, var(--orange) 32%, var(--border));
    border-radius: 10px;
    background: color-mix(in srgb, var(--orange) 6%, var(--surface));
    transition: background 0.2s, border-color 0.2s;
  }
  .repair-panel-running {
    border-color: color-mix(in srgb, var(--orange) 60%, var(--border));
    background: color-mix(in srgb, var(--orange) 10%, var(--surface));
  }
  .repair-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 12px;
  }
  .repair-head h4 {
    margin: 0 0 4px;
    font-size: 0.86rem;
    color: var(--text);
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }
  .repair-head p {
    margin: 0;
    max-width: 52ch;
    font-size: 0.74rem;
    line-height: 1.5;
    color: var(--text-secondary);
  }
  .repair-actions {
    display: inline-flex;
    gap: 8px;
    flex-shrink: 0;
  }

  .warnings {
    margin: 0;
    padding: 8px 16px 8px 28px;
    border-radius: 8px;
    background: color-mix(in srgb, var(--orange) 8%, var(--surface));
    border: 1px solid color-mix(in srgb, var(--orange) 30%, var(--border));
    font-size: 0.74rem;
    color: var(--text);
  }
  .warnings li {
    margin: 0;
    padding: 1px 0;
  }

  .empty {
    text-align: center;
    color: var(--text-secondary);
    font-size: 0.84rem;
    font-style: italic;
    padding: 28px 0;
  }

  .cards {
    display: grid;
    gap: 12px;
  }

  .panel-foot {
    border-top: 1px solid var(--border);
    padding: 12px 18px 14px;
    display: grid;
    gap: 10px;
    background: color-mix(in srgb, var(--bg) 36%, var(--surface));
  }

  .foot-row {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
  }

  .seg {
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 4px 10px 8px;
    margin: 0;
    display: inline-flex;
    flex-direction: column;
    gap: 2px;
    background: var(--surface);
  }
  .seg.disabled {
    opacity: 0.55;
  }
  .seg legend {
    padding: 0 4px;
    font-size: 0.66rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--text-secondary);
    font-weight: 700;
  }
  .seg label {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    font-size: 0.78rem;
    color: var(--text);
    cursor: pointer;
  }
  .seg input[type="radio"] {
    accent-color: var(--teal);
    cursor: pointer;
  }
  .seg input[type="radio"]:disabled + span {
    color: var(--text-secondary);
    cursor: not-allowed;
  }
  .seg :global(label:has(input:disabled)) {
    cursor: not-allowed;
  }

  .rename-input {
    display: grid;
    gap: 4px;
    font-size: 0.74rem;
  }
  .rename-input span {
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.04em;
    font-size: 0.66rem;
    font-weight: 700;
  }
  .rename-input input {
    padding: 6px 9px;
    border: 1px solid var(--border);
    border-radius: 6px;
    font-family: "JetBrains Mono", monospace;
    font-size: 0.8rem;
    background: var(--surface);
    color: var(--text);
  }
  .rename-input input:focus {
    outline: none;
    border-color: color-mix(in srgb, var(--teal) 50%, var(--border));
    box-shadow: 0 0 0 2px color-mix(in srgb, var(--teal) 14%, transparent);
  }

  .apply-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  .apply-count {
    font-size: 0.8rem;
    color: var(--text-secondary);
  }

  .btn {
    padding: 8px 14px;
    border-radius: 8px;
    font: inherit;
    font-size: 0.84rem;
    font-weight: 600;
    border: 1px solid var(--border);
    cursor: pointer;
    transition: background 0.15s, border-color 0.15s, opacity 0.15s;
  }
  .btn.ghost {
    background: var(--surface);
    color: var(--text);
  }
  .btn.ghost:hover {
    background: color-mix(in srgb, var(--teal) 6%, var(--surface));
    border-color: color-mix(in srgb, var(--teal) 30%, var(--border));
  }
  .btn.primary {
    background: linear-gradient(
      135deg,
      color-mix(in srgb, var(--teal) 84%, #1f8f88),
      color-mix(in srgb, var(--orange) 22%, var(--teal))
    );
    border-color: transparent;
    color: white;
    box-shadow: 0 6px 14px color-mix(in srgb, var(--teal) 22%, transparent);
  }
  .btn.primary:hover {
    opacity: 0.95;
  }
  .btn:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
</style>
