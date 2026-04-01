# Use the web UI

The datasight web UI provides a chat-based interface for exploring your data.
Beyond asking questions, the UI has features for organizing results, tracking
queries, and navigating your database schema.

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
