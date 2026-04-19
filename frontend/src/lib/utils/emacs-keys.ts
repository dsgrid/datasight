/**
 * Readline-style keybindings for plain `<textarea>` / `<input>` elements.
 *
 * CodeMirror already ships an emacs-style keymap — this handler exists so the
 * same bindings work in the chat input (and any other textarea we wire it to),
 * where macOS-native bindings cover some keys (Ctrl-a/e/k) but not Ctrl-u.
 */

function lineStart(value: string, pos: number): number {
  const nl = value.lastIndexOf("\n", pos - 1);
  return nl === -1 ? 0 : nl + 1;
}

function lineEnd(value: string, pos: number): number {
  const nl = value.indexOf("\n", pos);
  return nl === -1 ? value.length : nl;
}

function fireInput(el: HTMLTextAreaElement | HTMLInputElement): void {
  el.dispatchEvent(new Event("input", { bubbles: true }));
}

function replaceRange(
  el: HTMLTextAreaElement | HTMLInputElement,
  from: number,
  to: number,
  text: string,
): void {
  if (typeof el.setRangeText === "function") {
    el.setRangeText(text, from, to, "start");
  } else {
    const v = el.value;
    el.value = v.slice(0, from) + text + v.slice(to);
    el.setSelectionRange(from + text.length, from + text.length);
  }
  fireInput(el);
}

/**
 * Handle a readline-style chord on the given element. Returns `true` when the
 * event was consumed (in which case the handler has already called
 * `preventDefault`).
 */
export function handleEmacsKey(
  e: KeyboardEvent,
  el: HTMLTextAreaElement | HTMLInputElement,
): boolean {
  if (!e.ctrlKey || e.metaKey || e.altKey || e.shiftKey) return false;
  const key = e.key.toLowerCase();

  const start = el.selectionStart ?? 0;
  const end = el.selectionEnd ?? 0;
  const val = el.value;

  switch (key) {
    case "a": {
      e.preventDefault();
      const pos = lineStart(val, start);
      el.setSelectionRange(pos, pos);
      return true;
    }
    case "e": {
      e.preventDefault();
      const pos = lineEnd(val, start);
      el.setSelectionRange(pos, pos);
      return true;
    }
    case "b": {
      e.preventDefault();
      const pos = Math.max(0, start - 1);
      el.setSelectionRange(pos, pos);
      return true;
    }
    case "f": {
      e.preventDefault();
      const pos = Math.min(val.length, start + 1);
      el.setSelectionRange(pos, pos);
      return true;
    }
    case "k": {
      e.preventDefault();
      if (start !== end) {
        replaceRange(el, start, end, "");
      } else {
        const eol = lineEnd(val, start);
        const to = eol === start ? Math.min(val.length, start + 1) : eol;
        if (to > start) replaceRange(el, start, to, "");
      }
      return true;
    }
    case "u": {
      e.preventDefault();
      if (start !== end) {
        replaceRange(el, start, end, "");
      } else {
        const bol = lineStart(val, start);
        if (bol < start) replaceRange(el, bol, start, "");
      }
      return true;
    }
    case "d": {
      e.preventDefault();
      if (start !== end) {
        replaceRange(el, start, end, "");
      } else if (start < val.length) {
        replaceRange(el, start, start + 1, "");
      }
      return true;
    }
    case "h": {
      e.preventDefault();
      if (start !== end) {
        replaceRange(el, start, end, "");
      } else if (start > 0) {
        replaceRange(el, start - 1, start, "");
      }
      return true;
    }
    default:
      return false;
  }
}
