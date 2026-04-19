import { describe, it, expect, beforeEach } from "vitest";
import { handleEmacsKey } from "$lib/utils/emacs-keys";

function makeTextarea(value = "", caret = 0): HTMLTextAreaElement {
  const ta = document.createElement("textarea");
  document.body.appendChild(ta);
  ta.value = value;
  ta.setSelectionRange(caret, caret);
  return ta;
}

function key(k: string): KeyboardEvent {
  return new KeyboardEvent("keydown", {
    key: k,
    ctrlKey: true,
    cancelable: true,
  });
}

describe("handleEmacsKey", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  it("Ctrl-a moves to the start of the current line", () => {
    const ta = makeTextarea("hello world", 6);
    const e = key("a");
    expect(handleEmacsKey(e, ta)).toBe(true);
    expect(ta.selectionStart).toBe(0);
    expect(e.defaultPrevented).toBe(true);
  });

  it("Ctrl-a respects line boundaries in multi-line text", () => {
    const ta = makeTextarea("first line\nsecond line", 16);
    handleEmacsKey(key("a"), ta);
    expect(ta.selectionStart).toBe(11);
  });

  it("Ctrl-e moves to the end of the current line", () => {
    const ta = makeTextarea("hello\nworld", 2);
    handleEmacsKey(key("e"), ta);
    expect(ta.selectionStart).toBe(5);
  });

  it("Ctrl-k deletes from cursor to end of line", () => {
    const ta = makeTextarea("hello world", 5);
    handleEmacsKey(key("k"), ta);
    expect(ta.value).toBe("hello");
    expect(ta.selectionStart).toBe(5);
  });

  it("Ctrl-k at end-of-line eats the newline", () => {
    const ta = makeTextarea("one\ntwo", 3);
    handleEmacsKey(key("k"), ta);
    expect(ta.value).toBe("onetwo");
  });

  it("Ctrl-k deletes the active selection when one exists", () => {
    const ta = makeTextarea("hello world", 0);
    ta.setSelectionRange(0, 5);
    handleEmacsKey(key("k"), ta);
    expect(ta.value).toBe(" world");
  });

  it("Ctrl-u deletes from cursor back to start of line", () => {
    const ta = makeTextarea("hello world", 6);
    handleEmacsKey(key("u"), ta);
    expect(ta.value).toBe("world");
    expect(ta.selectionStart).toBe(0);
  });

  it("Ctrl-u in multi-line only deletes the current line's prefix", () => {
    const ta = makeTextarea("line one\nline two", 13);
    handleEmacsKey(key("u"), ta);
    expect(ta.value).toBe("line one\n two");
  });

  it("Ctrl-b and Ctrl-f move one character", () => {
    const ta = makeTextarea("abcdef", 3);
    handleEmacsKey(key("b"), ta);
    expect(ta.selectionStart).toBe(2);
    handleEmacsKey(key("f"), ta);
    expect(ta.selectionStart).toBe(3);
  });

  it("Ctrl-d deletes forward; Ctrl-h deletes backward", () => {
    const ta = makeTextarea("abc", 1);
    handleEmacsKey(key("d"), ta);
    expect(ta.value).toBe("ac");
    handleEmacsKey(key("h"), ta);
    expect(ta.value).toBe("c");
  });

  it("fires an input event so bound values stay in sync", () => {
    const ta = makeTextarea("hello", 0);
    let fired = 0;
    ta.addEventListener("input", () => fired++);
    handleEmacsKey(key("k"), ta);
    expect(fired).toBe(1);
  });

  it("ignores modified chords (Ctrl-Shift-a, Cmd-a, Alt-a)", () => {
    const ta = makeTextarea("hello", 3);
    const shift = new KeyboardEvent("keydown", {
      key: "a",
      ctrlKey: true,
      shiftKey: true,
      cancelable: true,
    });
    expect(handleEmacsKey(shift, ta)).toBe(false);
    expect(ta.selectionStart).toBe(3);

    const meta = new KeyboardEvent("keydown", {
      key: "a",
      metaKey: true,
      cancelable: true,
    });
    expect(handleEmacsKey(meta, ta)).toBe(false);
  });

  it("returns false for unrelated keys", () => {
    const ta = makeTextarea("hi", 0);
    expect(handleEmacsKey(key("z"), ta)).toBe(false);
  });
});
