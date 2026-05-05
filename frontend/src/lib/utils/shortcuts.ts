/**
 * Pure key-matching for global keyboard shortcuts.
 *
 * Returns a {@link ShortcutAction} name (or `null` for "let the browser/focused
 * element handle this"). Side effects — focus, view changes, store mutations —
 * are dispatched by the caller. This split keeps the matcher unit-testable
 * without a Svelte runtime.
 */
import type { ViewMode } from "$lib/stores/dashboard.svelte";

export type ShortcutAction =
  // Mod-chords
  | "open-settings"
  | "toggle-palette"
  | "toggle-sidebar"
  // Escape
  | "escape"
  // Plain keys (non-input)
  | "focus-input"
  | "toggle-shortcuts"
  | "new-chat"
  | "toggle-chat-dashboard"
  | "toggle-chat-sql"
  | "view-chat"
  | "view-dashboard"
  | "view-sql"
  | "toggle-export"
  | "toggle-history"
  | "toggle-pin"
  | "fullscreen-selected"
  | "next-message"
  | "prev-message"
  | "next-conversation"
  | "prev-conversation"
  | "focus-schema"
  // Dashboard nav
  | "dashboard-up"
  | "dashboard-down"
  | "dashboard-left"
  | "dashboard-right"
  | "dashboard-enter"
  | "dashboard-delete";

export interface ShortcutContext {
  isMac: boolean;
  isInput: boolean;
  view: ViewMode;
}

/**
 * Matches a keyboard event to a {@link ShortcutAction}.
 *
 * Returns `null` when the event is not a shortcut, or when the matcher
 * deliberately yields control (e.g. Ctrl-k / Ctrl-b inside an input on
 * non-Mac so readline-style chords work in the focused element).
 */
export function matchShortcut(
  e: KeyboardEvent,
  ctx: ShortcutContext,
): ShortcutAction | null {
  const { isMac, isInput, view } = ctx;
  const modDown = isMac ? e.metaKey : e.ctrlKey;

  // Mod-chord shortcuts (Cmd/Ctrl + key, no Alt/Shift)
  if (modDown && !e.altKey && !e.shiftKey) {
    // Non-Mac: let inputs keep readline-style Ctrl-k/Ctrl-b chords.
    if (!isMac && isInput && (e.key === "k" || e.key === "b")) return null;
    if (e.key === ",") return "open-settings";
    if (e.key === "k") return "toggle-palette";
    if (e.key === "b") return "toggle-sidebar";
    return null;
  }

  // Escape works regardless of focus
  if (e.key === "Escape") return "escape";

  // Plain keys must not be held with Cmd/Ctrl/Alt (Cmd+Shift+K shouldn't
  // accidentally fire the J/K message-navigation shortcuts). Shift is allowed
  // because some shortcuts are inherently shifted ("?") or accept either case
  // ("N" / "n").
  if (e.metaKey || e.ctrlKey || e.altKey) return null;

  // Remaining shortcuts only when not in an input
  if (isInput) return null;

  // Plain-key shortcuts (case-insensitive for letters)
  switch (e.key) {
    case "/":
      return "focus-input";
    case "?":
      return "toggle-shortcuts";
    case "n":
    case "N":
      return "new-chat";
    case "d":
    case "D":
      return "toggle-chat-dashboard";
    case "s":
    case "S":
      return "toggle-chat-sql";
    case "1":
      return "view-chat";
    case "2":
      return "view-dashboard";
    case "3":
      return "view-sql";
    case "e":
    case "E":
      return "toggle-export";
    case "h":
    case "H":
      return "toggle-history";
    case "p":
    case "P":
      return "toggle-pin";
    case "f":
    case "F":
      return view === "dashboard" ? "fullscreen-selected" : null;
    case "j":
    case "J":
      return view === "dashboard" ? "dashboard-down" : "next-message";
    case "k":
    case "K":
      return view === "dashboard" ? "dashboard-up" : "prev-message";
    case "[":
      return "prev-conversation";
    case "]":
      return "next-conversation";
    case "g":
    case "G":
      return "focus-schema";
  }

  // Dashboard arrow/Enter/Delete navigation
  if (view === "dashboard") {
    switch (e.key) {
      case "ArrowUp":
        return "dashboard-up";
      case "ArrowDown":
        return "dashboard-down";
      case "ArrowLeft":
        return "dashboard-left";
      case "ArrowRight":
        return "dashboard-right";
      case "Enter":
        return "dashboard-enter";
      case "Delete":
      case "Backspace":
        return "dashboard-delete";
    }
  }

  return null;
}
