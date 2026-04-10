# Use the web UI

The datasight web UI provides a chat-based interface for exploring your data.
Beyond asking questions, the UI has features for organizing results, tracking
queries, and navigating your database schema.

## Landing page

When you open datasight with no project loaded, you see a landing page with
guided starters, quick-open paths, and recent projects.

### Guided starters

The landing page leads with four starter workflows:

- **Profile this dataset** — deterministic overview of table sizes, date
  coverage, measure candidates, and likely dimensions
- **Find key dimensions** — likely grouping columns, suggested breakdowns,
  and join hints
- **Build a trend chart** — candidate date/measure pairs plus starter chart
  recommendations
- **Audit nulls and outliers** — null-heavy columns, suspicious numeric
  ranges, and quick QA notes

Choose a starter first, then open files or a project. datasight runs the
selected starter immediately after the data loads.

The starter results include follow-up actions, so you can move from the
overview into a real question without rewriting the prompt from scratch.

Examples include:

- **Inspect Top Dimension**
- **Build a First Trend**
- **Profile `<table>`**

These buttons send a concrete follow-up prompt into the normal chat workflow.

Beyond starters, the landing page has up to three sections:

### LLM configuration

Shown only if no LLM provider is detected from environment variables. Select
a provider (Anthropic, Ollama, or GitHub Models), enter your API key and
model, and click **Connect**. This section disappears once connected.

You can also configure the LLM from the **Settings** panel (gear icon in the
header) at any time. Settings configured via environment variables
(`ANTHROPIC_API_KEY`, etc.) are detected automatically.

### Explore Files

Enter the path to a data file or directory:

- **Single file** — `.csv`, `.parquet`, or `.duckdb`
- **Parquet directory** — a folder containing `*.parquet` files (hive-partitioned datasets)
- **DuckDB database** — opens directly with all its tables

Click **Explore** to load the data instantly. No project setup, no
configuration files — just your data and the chat interface.

### Open Project

Shows your recent projects (click to open) and a path input for opening a
project directory manually. A project directory must contain
`schema_description.md`.

## Explore mode

After exploring files, the header shows an **Explore** indicator with a table
count and a **Save** button.

### Adding files

Use the input at the top of the sidebar (below the **Tables** header) to add
more files to your session. This works in both explore mode and project mode.

### Saving as a project

Click **Save** in the header to persist your session. Enter a directory path
and optional name. If an LLM is configured, datasight automatically generates
`schema_description.md` and `queries.yaml` in the background. It also seeds a
`measures.yaml` scaffold from the inferred semantic measures when possible. You can also
provide a description of your data to improve the generated documentation.

After saving, the indicator switches to **Project** mode and future sessions
can reload the project from the landing page.

## Switch between projects

Click the **datasight** logo or the switch icon in the header to open the
project switcher panel. The panel shows:

- **Quick Explore** — enter a file path to start a new explore session
- **Recent projects** — previously opened projects (click to switch)
- **Open project** — enter a path to open a new project

### What is a project?

A project directory contains:

- `schema_description.md` — required, describes your data for the AI
- `.env` — optional, database connection and settings
- `queries.yaml` — optional, example queries
- `measures.yaml` — optional, measure aggregation overrides
- `.datasight/` — auto-created, stores conversations and bookmarks

### Switching projects

When you switch projects, datasight:

1. Loads the new project's `.env` file
2. Connects to the new database
3. Loads the new schema and example queries
4. Clears the current chat (previous conversations remain in History)

!!! note "Configuration inheritance"
    **LLM settings** (API keys, model) can come from either the startup
    directory or the project's `.env`. If you start datasight without an API
    key, the project's `.env` must provide one.
    
    **Database settings** always come from the project's `.env`.

## Explore the sidebar

The left sidebar shows your database tables, example queries, bookmarks, and
conversation history. Toggle it with the hamburger button in the header.

### Table schema

Click a table name to expand it and see all columns with their types.

**Schema search:** the search box above the table list filters both table
names and column names. Matching column hits auto-expand their parent
tables, and matching text is highlighted inline.

When the underlying dataset changes, the filter resets automatically so the
sidebar does not stay stuck in a stale filtered state.

**Preview rows:** click the "Preview rows" button at the top of the expanded
column list to see the first 10 rows inline. Click again to hide.

**Column statistics:** click any column name to see quick stats: distinct
count, null count, min, max, and average (for numeric columns). Click again
to collapse.

Both table previews and column stats are cached in the browser and server, so
reopening the same item is usually instant.

### Example queries

If your project has a `queries.yaml` file, the example queries appear below
the tables. Click one to populate the chat input. When a table is selected,
only queries relevant to that table are shown.

### Recipes

The **Recipes** section provides reusable prompt recipes generated from the
loaded schema. These are deterministic project-specific prompts such as:

- profile the biggest tables
- break down by a likely dimension
- start with a trend analysis
- summarize a likely measure

Click a recipe to load its prompt into the chat input, then edit or send it.

### Inspect

The **Inspect** section gives you explicit access to the same deterministic
inspection flows that power the guided starters:

- **Profile dataset**
- **Key measures**
- **Find dimensions**
- **Quality audit**
- **Trend ideas**

These are useful after a project is already loaded, because you can rerun the
deterministic inspection tools without going back to the landing page.

### Key measures

Use **Key measures** when you want datasight to classify likely metrics before
you ask a question. This view shows:

- the measure role, such as `energy`, `power`, `capacity`, `rate`, or `ratio`
- the default aggregation datasight will prefer
- weighted-average guidance
- suggested SQL rollup formulas
- configured display metadata such as display name, format, and preferred chart type

This is especially useful for energy datasets where `MWh`, `MW`, rates, and
factors should not be rolled up the same way.

### Measure Overrides

The **Measure Overrides** section lets you edit `measures.yaml` without
leaving the app.

You can use it to:

- change the default aggregation for a physical column
- set a weight column for weighted-average rollups
- set a display name or format
- set preferred chart types
- create calculated measures with a name and SQL expression

Recommended workflow:

1. Run **Key measures**.
2. Click **Edit override** on a measure card if the default behavior is wrong.
3. Adjust aggregation, weighting, display metadata, or chart preferences.
4. Save overrides and rerun your question or deterministic inspect flow.

For calculated measures:

1. Open **Measure Overrides**.
2. Switch the builder to **calculated measure**.
3. Choose a target table.
4. Enter the measure name and SQL expression.
5. Set aggregation, display name, format, and preferred chart types.
6. Save the generated YAML.

Datasight reloads the semantic layer after save, so the updated measure config
immediately affects inspect tools, recipes, and prompt guidance.

See [Semantic measures](measures.md) for a full explanation of
`measures.yaml`, weighted averages, calculated measures, and display metadata.

If you select a table in the sidebar first, the same section also offers
table-scoped actions:

- **Profile selected table**
- **Dimensions on table**
- **Quality on table**
- **Trends on table**

The scope label at the top of the section shows whether the action will run on
the full dataset or the selected table.

## Work with results

### Tables

Result tables support:

- **Sort** — click any column header to sort ascending/descending
- **Filter** — type in the filter input above the table to narrow rows
- **Pagination** — large results are paginated (25 rows per page)
- **CSV export** — click "Download CSV" to download the visible data. The
  filename is derived from your question.

### Charts

Charts render as interactive Plotly.js plots. The toolbar at the top of each
chart provides:

- **Save Plotly JSON** (download icon) — saves the chart specification as a
  JSON file, useful for embedding in notebooks or other tools
- **Chart controls** (gear icon) — opens a control bar to change the chart
  type (bar, line, scatter, pie), title, and axis labels in place

See [Create visualizations](visualizations.md) for more on chart types.

### Result provenance

Each live result card includes a **Source** disclosure with:

- the originating question
- tool type
- row and column counts
- execution time
- chart type
- generated SQL

Pinned dashboard cards preserve this metadata and exported dashboards include
it as well.

## Pin results to the dashboard

Each result (table or chart) shows a **Pin** button on hover. Clicking it
copies the result to the dashboard.

Switch between **Chat** and **Dashboard** using the tabs in the header. The
dashboard tab shows a badge with the number of pinned items. Pinned items
are arranged in a responsive grid and each has an **Unpin** button to remove
it.

Pinned items persist across chat clears — clicking "New chat" does not
remove items from the dashboard.

### Notes and sections

The dashboard is not limited to pinned tables and charts. You can also add:

- **Notes** — short markdown commentary between cards
- **Sections** — headings and short intros that group the cards that follow

Use the dashboard toolbar to add them, or use the insert controls between
cards. This makes exports read like analysis documents instead of a loose grid
of results.

### Dashboard layout

The dashboard toolbar lets you choose a column layout:

- **Auto** — responsive grid that fills available space
- **1 / 2 / 3** — fixed column count

Drag cards by their handle (visible on hover) to reorder them. Charts
automatically resize to fit their container when the layout changes.

The dashboard toolbar also supports:

- **Add note**
- **Add section**
- **Sync scales** for chart comparison
- **Export dashboard** as a standalone HTML page

## Bookmark queries

To save a query for later reuse, hover over any result and click
**★ Bookmark**. You can also bookmark from the query history panel (the ★
button on each query card).

Bookmarks appear in the **Bookmarks** section of the sidebar. Click a
bookmark to populate the input with its SQL. Bookmarks are stored in
`.datasight/bookmarks.json` and persist across server restarts.

To clear all bookmarks, click "Clear" in the Bookmarks section header.

## Save reports

Reports let you save a query and re-run it against fresh data without
involving the AI. Unlike bookmarks (which populate the chat input), reports
execute immediately and display the result inline.

### Save a report

Hover over any result (table or chart) and click **Save Report**. The
report captures the SQL, tool type, chart specification (if applicable), and
the title.

### Run a report

Click a report in the **Reports** sidebar section. datasight re-executes
the saved SQL and renders the result directly in the chat — no LLM call, no
token cost.

### Manage reports

- **Delete** — click the **x** button on any report in the sidebar
- **Clear all** — click "Clear" in the Reports section header

Reports are stored in `.datasight/reports.json` and persist across server
restarts.

## Conversation history

Chat conversations are saved automatically and persist across page reloads
and server restarts. Conversations are stored as JSON files in
`.datasight/conversations/`.

The **History** section in the sidebar lists past conversations. Click one
to switch to it — the full chat (messages, tool results, charts) is
replayed. You can continue the conversation where you left off.

**New chat** starts a fresh conversation. The old one remains in the history.

To clear all history, click "Clear" in the History section header.

## Suggested follow-ups

After each response, the AI suggests 2–3 follow-up questions as clickable
buttons below the message. Click one to send it as your next question.

## Query history panel

Click the **SQL** button in the header to open the query history panel on
the right. It shows every SQL query executed in the current session with:

- Tool type (SQL or Chart)
- Execution time and row count
- Expandable SQL with syntax highlighting
- Copy and Rerun buttons
- ★ button to bookmark the query

Failed queries are highlighted with an orange border.

## Settings and project health

Open the **Settings** panel from the header to configure:

- LLM provider, model, API key, and base URL
- query behavior toggles such as SQL confirmation and explanations
- display options like cost visibility

The same panel includes a **Project Health** section that checks:

- `.env`
- LLM settings validity
- database configuration
- `schema_description.md`
- `queries.yaml`
- `.datasight` writability
- live database connectivity

The health panel refreshes automatically after major project/explore
transitions.

## Command palette

Press **Cmd/Ctrl+K** to open the command palette. It supports:

- view switching and panel toggles
- project switching
- schema navigation
- table preview and column stats actions
- starters
- recipes
- bookmarks
- saved reports
- conversation history
- dashboard composition actions

## Export a conversation

Click the **export** button (download icon) in the header to enter export
mode. Each question gets a trash button — click it to exclude that entire
Q&A turn (question, SQL, results, and answer) from the export. Then click
**Export HTML**. The browser downloads a self-contained HTML page with all
selected content, SQL, tables, and interactive charts.

You can also export from the command line:

```bash
# List available sessions
datasight export --list-sessions

# Export a session
datasight export <session-id> -o my-analysis.html

# Exclude specific turns by index (0-based, each turn is a Q&A pair)
datasight export <session-id> --exclude 2,3
```

## Keyboard shortcuts

Press **?** to see the shortcuts overlay. All shortcuts:

| Shortcut | Action |
|----------|--------|
| `Cmd/Ctrl+K` | Open command palette |
| `/` | Focus question input |
| `Cmd/Ctrl+B` | Toggle sidebar |
| `N` | New conversation |
| `D` | Toggle chat/dashboard view |
| `Escape` | Close modal / blur input |
| `?` | Show shortcuts help |

On macOS, shortcuts use `Cmd`; on Windows/Linux, they use `Ctrl`.
