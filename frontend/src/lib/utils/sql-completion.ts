/**
 * Context-aware SQL column completion.
 *
 * `@codemirror/lang-sql` only offers unqualified column completions when a
 * single `defaultTable` is configured. This source fills that gap: it scans
 * the current statement's FROM/JOIN clauses, resolves the referenced tables
 * against the configured schema, and offers their columns at bare identifier
 * positions (e.g. `SELECT <cursor>` or `WHERE col<cursor>`). Qualified access
 * like `t.col` is already handled by the built-in completer, so we bail out
 * when the previous character is a dot.
 */
import type {
  Completion,
  CompletionContext,
  CompletionResult,
  CompletionSource,
} from "@codemirror/autocomplete";

export interface TableRef {
  table: string;
  alias: string | null;
}

const ALIAS_STOP_WORDS = new Set([
  "where",
  "on",
  "using",
  "join",
  "inner",
  "left",
  "right",
  "full",
  "outer",
  "cross",
  "natural",
  "lateral",
  "group",
  "order",
  "having",
  "limit",
  "offset",
  "window",
  "qualify",
  "union",
  "intersect",
  "except",
  "returning",
  "for",
  "tablesample",
  "with",
  "as",
]);

function stripNoiseBeforeParse(sql: string): string {
  // Mask string literals before stripping comments so a `--` inside a string
  // doesn't get treated as a line comment that swallows the rest of the line.
  return sql
    .replace(/'(?:[^'\\]|\\.)*'/g, "''")
    .replace(/--[^\n]*/g, " ")
    .replace(/\/\*[\s\S]*?\*\//g, " ");
}

/**
 * Extract `{table, alias}` entries from FROM/JOIN clauses.
 * Handles comma-separated lists, `AS` aliases, implicit aliases, and quoted
 * identifiers. Subqueries in FROM (e.g. `FROM (SELECT ...) sub`) are ignored
 * because their columns aren't in the passed-in schema anyway.
 */
export function extractFromTables(sql: string): TableRef[] {
  const refs: TableRef[] = [];
  const cleaned = stripNoiseBeforeParse(sql);

  const tableRe =
    /\b(?:from|join)\b\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)*|"[^"]+"(?:\.[a-zA-Z_][\w]*)?)(?:\s+(?:as\s+)?([a-zA-Z_]\w*))?/gi;

  let m: RegExpExecArray | null;
  while ((m = tableRe.exec(cleaned)) !== null) {
    const rawTable = m[1];
    const rawAlias = m[2] ?? null;

    const table = rawTable.replace(/"/g, "");
    let alias: string | null = rawAlias;
    if (alias && ALIAS_STOP_WORDS.has(alias.toLowerCase())) {
      alias = null;
    }
    refs.push({ table, alias });

    // Continue through comma-separated FROM lists: `FROM a, b c, d`.
    let tailStart = tableRe.lastIndex;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const tail = cleaned.slice(tailStart);
      const commaMatch = tail.match(
        /^\s*,\s*([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)*|"[^"]+"(?:\.[a-zA-Z_][\w]*)?)(?:\s+(?:as\s+)?([a-zA-Z_]\w*))?/i,
      );
      if (!commaMatch) break;
      const cmTable = commaMatch[1].replace(/"/g, "");
      let cmAlias: string | null = commaMatch[2] ?? null;
      if (cmAlias && ALIAS_STOP_WORDS.has(cmAlias.toLowerCase())) {
        cmAlias = null;
      }
      refs.push({ table: cmTable, alias: cmAlias });
      tailStart += commaMatch[0].length;
    }
    tableRe.lastIndex = tailStart;
  }
  return refs;
}

function resolveTableKey(
  schema: Record<string, string[]>,
  tableRef: string,
): string | null {
  const keys = Object.keys(schema);
  const exact = keys.find((k) => k === tableRef);
  if (exact) return exact;
  const ci = keys.find((k) => k.toLowerCase() === tableRef.toLowerCase());
  if (ci) return ci;
  // Schema-qualified reference: "public.plants" → match on trailing segment.
  if (tableRef.includes(".")) {
    const tail = tableRef.split(".").pop()!;
    const tailMatch = keys.find((k) => k.toLowerCase() === tail.toLowerCase());
    if (tailMatch) return tailMatch;
  }
  return null;
}

/**
 * Build a CompletionSource that offers unqualified columns pulled from the
 * tables referenced in the current statement's FROM/JOIN clauses. Safe to
 * install alongside the built-in `@codemirror/lang-sql` completer — we bail
 * out when the cursor is after a `.` so qualified completion keeps working.
 */
export function contextualColumnSource(
  getSchema: () => Record<string, string[]>,
): CompletionSource {
  return (context: CompletionContext): CompletionResult | null => {
    const word = context.matchBefore(/\w*/);
    if (!word) return null;
    if (word.from === word.to && !context.explicit) return null;

    const prevChar = context.state.doc.sliceString(
      Math.max(0, word.from - 1),
      word.from,
    );
    if (prevChar === ".") return null;

    const schema = getSchema();
    if (!schema || Object.keys(schema).length === 0) return null;

    // Scan the entire current statement (bounded by `;`s on either side of
    // the cursor) so completions also work when the cursor sits in the SELECT
    // list before the FROM clause has been fully typed/parsed.
    const fullDoc = context.state.doc.sliceString(0);
    const before = fullDoc.lastIndexOf(";", word.from - 1);
    const after = fullDoc.indexOf(";", word.from);
    const stmtStart = before === -1 ? 0 : before + 1;
    const stmtEnd = after === -1 ? fullDoc.length : after;
    const stmt = fullDoc.slice(stmtStart, stmtEnd);
    const tables = extractFromTables(stmt);
    if (tables.length === 0) return null;

    const seenCols = new Set<string>();
    const options: Completion[] = [];

    for (const ref of tables) {
      const key = resolveTableKey(schema, ref.table);
      if (!key) continue;
      for (const col of schema[key]) {
        const dedupeKey = col.toLowerCase();
        if (seenCols.has(dedupeKey)) continue;
        seenCols.add(dedupeKey);
        options.push({
          label: col,
          type: "property",
          detail: ref.alias ?? key,
          boost: 1,
        });
      }
      if (ref.alias) {
        options.push({
          label: ref.alias,
          type: "variable",
          detail: `alias · ${key}`,
        });
      }
    }

    if (options.length === 0) return null;

    return {
      from: word.from,
      to: word.to,
      options,
      validFor: /^\w*$/,
    };
  };
}
