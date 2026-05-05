import { describe, it, expect } from "vitest";
import { matchShortcut, type ShortcutContext } from "$lib/utils/shortcuts";

function key(
  k: string,
  opts: { meta?: boolean; ctrl?: boolean; shift?: boolean; alt?: boolean } = {},
): KeyboardEvent {
  return new KeyboardEvent("keydown", {
    key: k,
    metaKey: opts.meta,
    ctrlKey: opts.ctrl,
    shiftKey: opts.shift,
    altKey: opts.alt,
    cancelable: true,
  });
}

const macChat: ShortcutContext = { isMac: true, isInput: false, view: "chat" };
const macInput: ShortcutContext = { isMac: true, isInput: true, view: "chat" };
const linuxInput: ShortcutContext = {
  isMac: false,
  isInput: true,
  view: "chat",
};
const dash: ShortcutContext = {
  isMac: true,
  isInput: false,
  view: "dashboard",
};
const sql: ShortcutContext = { isMac: true, isInput: false, view: "sql" };

describe("matchShortcut — mod chords", () => {
  it("Cmd+K opens the palette on Mac", () => {
    expect(matchShortcut(key("k", { meta: true }), macChat)).toBe(
      "toggle-palette",
    );
  });

  it("Ctrl+K opens the palette on non-Mac", () => {
    expect(
      matchShortcut(key("k", { ctrl: true }), {
        ...macChat,
        isMac: false,
      }),
    ).toBe("toggle-palette");
  });

  it("Cmd+, opens settings", () => {
    expect(matchShortcut(key(",", { meta: true }), macChat)).toBe(
      "open-settings",
    );
  });

  it("Cmd+B toggles sidebar", () => {
    expect(matchShortcut(key("b", { meta: true }), macChat)).toBe(
      "toggle-sidebar",
    );
  });

  it("non-Mac Ctrl-k inside an input yields to readline (returns null)", () => {
    expect(matchShortcut(key("k", { ctrl: true }), linuxInput)).toBeNull();
    expect(matchShortcut(key("b", { ctrl: true }), linuxInput)).toBeNull();
  });

  it("Mac Cmd-k inside an input still toggles the palette (Cmd is not used as a chord prefix)", () => {
    expect(matchShortcut(key("k", { meta: true }), macInput)).toBe(
      "toggle-palette",
    );
  });

  it("ignores Cmd+Shift+K (mod with shift)", () => {
    expect(
      matchShortcut(key("k", { meta: true, shift: true }), macChat),
    ).toBeNull();
  });
});

describe("matchShortcut — escape and input gating", () => {
  it("Escape works regardless of input focus", () => {
    expect(matchShortcut(key("Escape"), macChat)).toBe("escape");
    expect(matchShortcut(key("Escape"), macInput)).toBe("escape");
  });

  it("plain keys are ignored when typing in an input", () => {
    expect(matchShortcut(key("e"), macInput)).toBeNull();
    expect(matchShortcut(key("/"), macInput)).toBeNull();
    expect(matchShortcut(key("?"), macInput)).toBeNull();
    expect(matchShortcut(key("j"), macInput)).toBeNull();
  });
});

describe("matchShortcut — plain-key shortcuts", () => {
  it.each([
    ["/", "focus-input"],
    ["?", "toggle-shortcuts"],
    ["n", "new-chat"],
    ["N", "new-chat"],
    ["d", "toggle-chat-dashboard"],
    ["s", "toggle-chat-sql"],
    ["1", "view-chat"],
    ["2", "view-dashboard"],
    ["3", "view-sql"],
    ["e", "toggle-export"],
    ["E", "toggle-export"],
    ["h", "toggle-history"],
    ["p", "toggle-pin"],
    ["g", "focus-schema"],
    ["[", "prev-conversation"],
    ["]", "next-conversation"],
  ])("%s → %s", (k, expected) => {
    expect(matchShortcut(key(k), macChat)).toBe(expected);
  });

  it("J / K scroll messages in chat view", () => {
    expect(matchShortcut(key("j"), macChat)).toBe("next-message");
    expect(matchShortcut(key("k"), macChat)).toBe("prev-message");
  });

  it("J / K navigate dashboard cards in dashboard view", () => {
    expect(matchShortcut(key("j"), dash)).toBe("dashboard-down");
    expect(matchShortcut(key("k"), dash)).toBe("dashboard-up");
  });

  it("F triggers fullscreen only on dashboard view", () => {
    expect(matchShortcut(key("f"), dash)).toBe("fullscreen-selected");
    expect(matchShortcut(key("f"), macChat)).toBeNull();
    expect(matchShortcut(key("f"), sql)).toBeNull();
  });
});

describe("matchShortcut — dashboard navigation", () => {
  it.each([
    ["ArrowUp", "dashboard-up"],
    ["ArrowDown", "dashboard-down"],
    ["ArrowLeft", "dashboard-left"],
    ["ArrowRight", "dashboard-right"],
    ["Enter", "dashboard-enter"],
    ["Delete", "dashboard-delete"],
    ["Backspace", "dashboard-delete"],
  ])("%s → %s on dashboard", (k, expected) => {
    expect(matchShortcut(key(k), dash)).toBe(expected);
  });

  it("arrow keys do nothing in chat view", () => {
    expect(matchShortcut(key("ArrowUp"), macChat)).toBeNull();
    expect(matchShortcut(key("Enter"), macChat)).toBeNull();
  });
});

describe("matchShortcut — unmapped keys", () => {
  it("returns null for unmapped keys", () => {
    expect(matchShortcut(key("z"), macChat)).toBeNull();
    expect(matchShortcut(key("y"), macChat)).toBeNull();
    expect(matchShortcut(key("\\"), macChat)).toBeNull();
  });
});
