# Use the web UI

The datasight web UI provides a chat-based interface for exploring your data.
Beyond asking questions, the UI has features for organizing results, tracking
queries, and navigating your database schema.

## Landing page

When you open datasight with no project loaded, you see a landing page with
up to three sections:

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
`schema_description.md` and `queries.yaml` in the background. You can also
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

**Preview rows:** click the "Preview rows" button at the top of the expanded
column list to see the first 10 rows inline. Click again to hide.

**Column statistics:** click any column name to see quick stats: distinct
count, null count, min, max, and average (for numeric columns). Click again
to collapse.

### Example queries

If your project has a `queries.yaml` file, the example queries appear below
the tables. Click one to populate the chat input. When a table is selected,
only queries relevant to that table are shown.

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

## Pin results to the dashboard

Each result (table or chart) shows a **Pin** button on hover. Clicking it
copies the result to the dashboard.

Switch between **Chat** and **Dashboard** using the tabs in the header. The
dashboard tab shows a badge with the number of pinned items. Pinned items
are arranged in a responsive grid and each has an **Unpin** button to remove
it.

Pinned items persist across chat clears — clicking "New chat" does not
remove items from the dashboard.

### Dashboard layout

The dashboard toolbar lets you choose a column layout:

- **Auto** — responsive grid that fills available space
- **1 / 2 / 3** — fixed column count

Drag cards by their handle (visible on hover) to reorder them. Charts
automatically resize to fit their container when the layout changes.

## Bookmark queries

To save a query for later reuse, hover over any result and click
**★ Bookmark**. You can also bookmark from the query history panel (the ★
button on each query card).

Bookmarks appear in the **Bookmarks** section of the sidebar. Click a
bookmark to populate the input with its SQL. Bookmarks are stored in
`.datasight/bookmarks.json` and persist across server restarts.

To clear all bookmarks, click "Clear" in the Bookmarks section header.

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
| `/` or `Cmd/Ctrl+K` | Focus question input |
| `Cmd/Ctrl+B` | Toggle sidebar |
| `N` | New conversation |
| `Escape` | Close modal / blur input |
| `?` | Show shortcuts help |

On macOS, shortcuts use `Cmd`; on Windows/Linux, they use `Ctrl`.
