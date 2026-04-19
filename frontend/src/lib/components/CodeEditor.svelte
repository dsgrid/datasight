<script lang="ts">
  import { onMount, onDestroy } from "svelte";
  import { EditorView, keymap, placeholder as placeholderExt } from "@codemirror/view";
  import { EditorState, Compartment } from "@codemirror/state";
  import {
    HighlightStyle,
    syntaxHighlighting,
  } from "@codemirror/language";
  import { tags as t } from "@lezer/highlight";
  import { sql, StandardSQL, PostgreSQL, SQLite } from "@codemirror/lang-sql";
  import { linter, type Diagnostic } from "@codemirror/lint";
  import {
    autocompletion,
    acceptCompletion,
    startCompletion,
    completionStatus,
  } from "@codemirror/autocomplete";
  import { basicSetup } from "codemirror";
  import { contextualColumnSource } from "$lib/utils/sql-completion";

  interface InsertRequest {
    text: string;
    nonce: number;
  }

  interface Props {
    value: string;
    schema?: Record<string, string[]>;
    dialect?: string;
    placeholder?: string;
    onChange: (value: string) => void;
    onRun?: (selectedText?: string) => void;
    minHeight?: string;
    insertRequest?: InsertRequest | null;
    lintSql?: (sql: string) => Promise<{ valid: boolean; errors: string[] }>;
  }

  let {
    value,
    schema = {},
    dialect = "duckdb",
    placeholder = "",
    onChange,
    onRun,
    minHeight = "180px",
    insertRequest = null,
    lintSql,
  }: Props = $props();

  let container: HTMLDivElement | null = $state(null);
  let view: EditorView | null = null;
  const langCompartment = new Compartment();
  const lintCompartment = new Compartment();
  const placeholderCompartment = new Compartment();

  const highlightStyle = HighlightStyle.define([
    { tag: t.keyword, color: "var(--teal)", fontWeight: "600" },
    { tag: [t.string, t.special(t.string)], color: "var(--orange)" },
    { tag: [t.number, t.bool, t.null], color: "var(--magenta)" },
    { tag: [t.comment, t.lineComment, t.blockComment], color: "var(--text-secondary)", fontStyle: "italic" },
    { tag: t.operator, color: "var(--text-secondary)" },
    { tag: t.punctuation, color: "var(--text-secondary)" },
    { tag: [t.typeName, t.atom], color: "var(--navy)" },
    { tag: t.variableName, color: "var(--text)" },
  ]);

  function dialectFor(d: string) {
    if (d === "postgres") return PostgreSQL;
    if (d === "sqlite") return SQLite;
    return StandardSQL;
  }

  function buildSqlExtension() {
    const support = sql({
      dialect: dialectFor(dialect),
      schema,
      upperCaseKeywords: true,
    });
    return [
      support,
      support.language.data.of({
        autocomplete: contextualColumnSource(() => schema),
      }),
    ];
  }

  function buildLinter() {
    if (!lintSql) return [];
    return linter(
      async (v): Promise<Diagnostic[]> => {
        const doc = v.state.doc.toString();
        if (!doc.trim()) return [];
        try {
          const result = await lintSql(doc);
          if (result.valid) return [];
          return result.errors.map((msg) => mapErrorToDiagnostic(doc, msg));
        } catch {
          return [];
        }
      },
      { delay: 750 },
    );
  }

  function mapErrorToDiagnostic(doc: string, message: string): Diagnostic {
    const quoted = message.match(/['"`]([A-Za-z_][\w.]*)['"`]/);
    if (quoted) {
      const needle = quoted[1];
      const idx = doc.indexOf(needle);
      if (idx >= 0) {
        return {
          from: idx,
          to: idx + needle.length,
          severity: "error",
          message,
        };
      }
    }
    const firstNewline = doc.indexOf("\n");
    const end = firstNewline === -1 ? doc.length : firstNewline;
    return { from: 0, to: end, severity: "error", message };
  }

  onMount(() => {
    if (!container) return;

    const theme = EditorView.theme({
      "&": {
        fontSize: "0.85rem",
        backgroundColor: "var(--bg)",
      },
      "&.cm-focused": { outline: "none" },
      ".cm-scroller": {
        fontFamily: "'JetBrains Mono', ui-monospace, monospace",
        lineHeight: "1.5",
      },
      ".cm-content": { minHeight, padding: "10px 0" },
      ".cm-gutters": {
        backgroundColor: "var(--surface-alt)",
        color: "var(--text-secondary)",
        borderRight: "1px solid var(--border)",
      },
      ".cm-activeLine": {
        backgroundColor: "color-mix(in srgb, var(--teal) 4%, transparent)",
      },
      ".cm-activeLineGutter": {
        backgroundColor: "color-mix(in srgb, var(--teal) 8%, transparent)",
        color: "var(--text)",
      },
      ".cm-selectionBackground, &.cm-focused .cm-selectionBackground, ::selection": {
        backgroundColor: "rgba(21, 168, 168, 0.25) !important",
      },
      ".cm-cursor": { borderLeftColor: "var(--teal)" },
      ".cm-tooltip": {
        backgroundColor: "var(--surface)",
        border: "1px solid var(--border)",
        borderRadius: "6px",
        color: "var(--text)",
      },
      ".cm-tooltip.cm-tooltip-autocomplete > ul": {
        fontFamily: "'JetBrains Mono', ui-monospace, monospace",
        maxHeight: "14em",
      },
      ".cm-tooltip-autocomplete ul li[aria-selected]": {
        backgroundColor: "color-mix(in srgb, var(--teal) 18%, var(--surface))",
        color: "var(--text)",
      },
      ".cm-completionLabel": { color: "var(--text)" },
      ".cm-completionDetail": { color: "var(--text-secondary)", fontStyle: "normal" },
      ".cm-diagnostic-error": {
        borderLeftColor: "var(--orange)",
      },
      ".cm-lintRange-error": {
        backgroundImage:
          "linear-gradient(45deg, transparent 65%, var(--orange) 80%, transparent 90%), linear-gradient(135deg, transparent 65%, var(--orange) 80%, transparent 90%)",
      },
    });

    const runKeymap = keymap.of([
      {
        key: "Mod-Enter",
        preventDefault: true,
        run: (target) => {
          const sel = target.state.selection.main;
          const selected = sel.empty
            ? undefined
            : target.state.sliceDoc(sel.from, sel.to);
          onRun?.(selected);
          return true;
        },
      },
      {
        key: "Tab",
        run: (target) => {
          if (completionStatus(target.state) === "active") {
            return acceptCompletion(target);
          }
          return startCompletion(target);
        },
      },
    ]);

    const state = EditorState.create({
      doc: value,
      extensions: [
        runKeymap,
        autocompletion({ activateOnTyping: true, activateOnTypingDelay: 75 }),
        basicSetup,
        langCompartment.of(buildSqlExtension()),
        lintCompartment.of(buildLinter()),
        syntaxHighlighting(highlightStyle),
        theme,
        placeholderCompartment.of(placeholder ? placeholderExt(placeholder) : []),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            const next = update.state.doc.toString();
            if (next !== value) onChange(next);
          }
        }),
      ],
    });

    view = new EditorView({ state, parent: container });
  });

  $effect(() => {
    if (!view) return;
    view.dispatch({
      effects: langCompartment.reconfigure(buildSqlExtension()),
    });
  });

  $effect(() => {
    // Reconfigure linter when lintSql or dialect changes.
    void dialect;
    void lintSql;
    if (!view) return;
    view.dispatch({
      effects: lintCompartment.reconfigure(buildLinter()),
    });
  });

  $effect(() => {
    if (!view) return;
    const current = view.state.doc.toString();
    if (current !== value) {
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: value },
      });
    }
  });

  $effect(() => {
    if (!view) return;
    view.dispatch({
      effects: placeholderCompartment.reconfigure(
        placeholder ? placeholderExt(placeholder) : [],
      ),
    });
  });

  $effect(() => {
    if (!view || !insertRequest) return;
    const text = insertRequest.text;
    void insertRequest.nonce;
    const pos = view.state.selection.main.head;
    const needsSpaceBefore = pos > 0 && /\S/.test(view.state.sliceDoc(pos - 1, pos));
    const insert = (needsSpaceBefore ? " " : "") + text;
    view.dispatch({
      changes: { from: pos, insert },
      selection: { anchor: pos + insert.length },
    });
    view.focus();
  });

  onDestroy(() => {
    view?.destroy();
    view = null;
  });
</script>

<div
  bind:this={container}
  class="border border-border focus-within:border-teal"
  style="border-radius: 8px; overflow: hidden; background: var(--bg);
         --focus-shadow: 0 0 0 1px rgba(21, 168, 168, 0.2);"
></div>
