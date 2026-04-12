# Dashboard Filter Roadmap

These are follow-up ideas for making datasight dashboards feel more like
Tableau while staying aligned with datasight's SQL-first, LLM-assisted design.

## Status notes

- **Per-filter scope (done):** Each filter chip has an "Applies to"
  menu with `All applicable cards` (default) and `Selected cards`. The
  column picker shows the union of card columns, not the intersection.
  Cards where a filter does not apply display a `filter not applied`
  badge explaining whether the column is missing or scope excludes the
  card.
- **Filter enable/disable (done):** Each chip's scope menu has an
  `Enabled` checkbox. Disabled filters render muted and strikethrough
  and are skipped during rerun.
- **Card empty/error states (partial):** Cards now show a `No rows
  after filter` overlay when active filters excluded every row, and a
  `Rerun failed` overlay when SQL rerun errored. Toolbar-level summary
  and alias-mismatch state are still future work (items 6 and 10).
- **Filter-aware export (partial):** Exported dashboards render a chip
  strip listing each active filter (column, operator, value, and scope
  when it targets specific cards) plus a `post-aggregation` note.
  Filter presets (item 8) are still future work.

## 1. Filter Shelves

Add a visible filter shelf to the dashboard toolbar. The shelf would show each
active filter as a structured control with:

- column name
- operator
- selected values
- enabled/disabled state
- scope

Scope should be explicit. Good first options:

- all runnable dashboard cards
- selected card only
- all cards using a specific result column

This would make filters easier to inspect and edit than the current chip-only
display.

## 2. Domain-Aware Value Pickers

After a user chooses a filter column, populate allowed values from the pinned
card result columns instead of requiring free-form value entry.

Examples:

- `state`: checkbox list such as `CA`, `TX`, `CO`
- `fuel_type_code_agg`: checkbox list such as `NG`, `SUN`, `WND`, `COL`
- `report_year`: range slider or min/max inputs
- `report_date`: date range picker

This should use the wrapped card SQL or cached result metadata, not LLM guesses.
For large domains, cap the initial list and provide search.

## 3. Cross-Highlight Before Cross-Filter

Support clicking a chart mark to highlight related marks across the dashboard
without immediately changing the SQL result set.

This is lower risk than cross-filtering because it does not rerun queries. It
also gives users a reversible exploratory action:

- click once to highlight `fuel_type_code_agg = NG`
- click again or clear selection to remove the highlight
- promote the highlight to a persistent filter if desired

## 4. Pre-Aggregation Filters

The current implementation applies filters after the saved card SQL has run by
wrapping it as a subquery. That is deterministic and safe, but it is not always
what users expect.

A future version could support pre-aggregation filters when datasight can prove
the target column exists in the underlying source query before grouping.

Required guardrails:

- parse SQL with `sqlglot`
- identify source tables, aliases, projections, and grouping boundaries
- only inject predicates when the target column is unambiguous
- preserve the original query for auditability
- show whether a filter is pre-aggregation or post-aggregation

If datasight cannot prove the rewrite is safe, it should fall back to
post-aggregation filtering or ask the user to rerun the prompt.

## 5. Filter Dependency Graph

Track which dashboard cards can accept each filter and why.

For each filter/card pair, record:

- applicable or not applicable
- matching result column
- matched alias, if any
- filter mode: post-aggregation, pre-aggregation, highlight-only
- last rerun status and error

This would make failures explainable instead of surprising. It would also
enable a UI affordance like "applies to 3 of 4 cards".

## 6. Alias Registry

The current alias handling is heuristic. A stronger approach would maintain a
registry of column aliases per card:

- original SQL projection name
- result column name
- LLM-provided label/name
- normalized key
- semantic role, if known from measures or schema metadata

This would let `fuel_type` map to `fuel_type_code_agg` when that relationship is
unambiguous, while refusing ambiguous matches.

## 7. Dashboard Parameters

> Not the same thing as per-filter scope (now shipped). Parameters are
> abstract, typed, named values referenced explicitly by card SQL —
> distinct from column-bound filter predicates.

Add Tableau-like parameters that are not tied to one specific column.

Examples:

- selected state
- selected fuel category
- comparison year
- top N
- aggregation mode

Parameters would be explicitly referenced by rerunnable cards. The first
implementation could support parameters only in saved SQL templates generated
by datasight, then expand later.

## 8. Filter Presets

Allow saving named filter states for a dashboard:

- "California renewables"
- "Texas thermal generation"
- "Post-2020"
- "Coal and gas only"

Presets should be stored with the dashboard snapshot and restored when loading
the corresponding conversation.

## 9. Filter-Aware Dashboard Export

Exported dashboards should preserve:

- active filter chips
- current filtered card outputs
- filter preset name, if selected
- metadata explaining whether filters were post-aggregation or pre-aggregation

If the export is static HTML, it should not imply that filters remain rerunnable
unless the SQL engine is available.

## 10. Better Empty And Error States

When a filter produces no rows or cannot apply to a card, the card should show a
specific reason:

- no rows after filter
- filter column not present in result
- alias was ambiguous
- SQL rerun failed
- card is not rerunnable

The dashboard toolbar should summarize these conditions so users do not need to
inspect individual cards.
