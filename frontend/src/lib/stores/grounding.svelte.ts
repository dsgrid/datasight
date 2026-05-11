/** Always-on grounding-drift state for the header pill.
 *
 * Distinct from ``tidyStore``: tidyStore tracks the in-flight tidy
 * review drawer (its grounding banner is a one-shot post-apply
 * surface). This store tracks the project-wide truth — "is the
 * grounding currently stale?" — so the header pill can advertise
 * drift any time the user is in the project, not just after they
 * applied something.
 *
 * Both surfaces call the same backend repair endpoint, but they own
 * their own UI state: the post-apply banner clears when the user
 * dismisses or repairs from the drawer; the header pill clears when
 * the next status check returns clean.
 */

import {
  fetchGroundingStatus,
  repairGrounding,
  type TidyGroundingRepairSummary,
} from "$lib/api/tidy";

export type GroundingStatus =
  | "idle"
  | "checking"
  | "clean"
  | "drift"
  | "unavailable"
  | "error";

export type GroundingRepairStatus = "idle" | "running" | "success" | "error";

function createGroundingStore() {
  let status = $state<GroundingStatus>("idle");
  let driftItems = $state(0);
  let hasSnapshot = $state(false);
  // ``unavailableReason`` is the server's hint about why the check
  // doesn't apply (no_project / non_duckdb / etc). Surfaced to the
  // pill tooltip so a confused user can see why no chip is rendered.
  let unavailableReason = $state<string | null>(null);
  let lastError = $state<string | null>(null);

  let repairStatus = $state<GroundingRepairStatus>("idle");
  let repairSummary = $state<TidyGroundingRepairSummary | null>(null);
  let repairError = $state<string | null>(null);

  return {
    get status() {
      return status;
    },
    get driftItems() {
      return driftItems;
    },
    get hasSnapshot() {
      return hasSnapshot;
    },
    get unavailableReason() {
      return unavailableReason;
    },
    get lastError() {
      return lastError;
    },
    get repairStatus() {
      return repairStatus;
    },
    get repairSummary() {
      return repairSummary;
    },
    get repairError() {
      return repairError;
    },
    /** True when the pill should be visible. Covers the only case the
     * user can act on — known drift in a real project. */
    get pillVisible() {
      return status === "drift";
    },

    /** Re-poll /api/grounding/status. Cheap (no LLM), safe to call on
     * project load and after any apply/repair. Sets status="checking"
     * during the call so the UI can debounce duplicate triggers. */
    async check(): Promise<void> {
      if (status === "checking") return;
      status = "checking";
      lastError = null;
      try {
        const resp = await fetchGroundingStatus();
        if (!resp.available) {
          status = "unavailable";
          unavailableReason = resp.reason ?? null;
          driftItems = 0;
          hasSnapshot = false;
          return;
        }
        unavailableReason = null;
        driftItems = resp.drift_items ?? 0;
        hasSnapshot = resp.has_snapshot ?? false;
        status = resp.needs_repair ? "drift" : "clean";
      } catch (err) {
        status = "error";
        lastError = (err as Error).message ?? "Failed to check grounding status";
      }
    },

    /** Clear all state — call when switching projects so a stale
     * drift count from project A doesn't leak into project B. */
    reset(): void {
      status = "idle";
      driftItems = 0;
      hasSnapshot = false;
      unavailableReason = null;
      lastError = null;
      repairStatus = "idle";
      repairSummary = null;
      repairError = null;
    },

    /** Trigger the LLM grounding repair from the header pill. Mirrors
     * tidyStore.runGroundingRepair but updates this store's repair
     * state. On success, re-checks status so the pill goes from
     * "drift" → "clean" if the rewrite resolved everything. */
    async runRepair(): Promise<void> {
      if (repairStatus === "running") return;
      repairStatus = "running";
      repairSummary = null;
      repairError = null;
      try {
        const resp = await repairGrounding();
        if (!resp.success) {
          repairStatus = "error";
          repairError = resp.error ?? "Grounding repair failed";
          return;
        }
        const summary = resp.grounding_repair ?? null;
        repairSummary = summary;
        if (summary?.applied) {
          repairStatus = "success";
        } else if (summary?.error) {
          repairStatus = "error";
          repairError = summary.error;
        } else if (summary?.validation_errors?.length) {
          repairStatus = "error";
          repairError = summary.validation_errors.join("; ");
        } else {
          repairStatus = "success";
        }
        // Either way, re-check the canonical status — drift may be
        // gone (success) or partially gone (some files written). The
        // pill should reflect what the file system actually shows now.
        await this.check();
      } catch (err) {
        repairStatus = "error";
        repairError = (err as Error).message ?? "Grounding repair failed";
      }
    },

    /** Dismiss a one-shot success/error toast without touching the
     * underlying status. The pill stays visible if drift remains. */
    dismissRepairResult(): void {
      repairStatus = "idle";
      repairSummary = null;
      repairError = null;
    },
  };
}

export const groundingStore = createGroundingStore();
