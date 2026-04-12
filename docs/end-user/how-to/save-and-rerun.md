# Save and rerun results

datasight offers three ways to return to past work: bookmarks (reusable
prompts), reports (saved SQL that re-runs without the AI), and conversation
history (full chat replays).

## Bookmark a query

To save a query for later reuse, hover over any result and click
**★ Bookmark**. You can also bookmark from the query history panel (the ★
button on each query card).

Bookmarks appear in the **Bookmarks** sidebar section. Click a bookmark to
populate the chat input with its SQL. Bookmarks are stored in
`.datasight/bookmarks.json` and persist across server restarts.

To clear all bookmarks, click **Clear** in the Bookmarks section header.

## Save a report

Reports save a query *and* re-run it against fresh data without involving
the AI. Unlike bookmarks (which only populate the chat input), reports
execute immediately and display the result inline.

### Save

Hover over any result (table or chart) and click **Save Report**. The
report captures the SQL, tool type, chart specification (if applicable),
and the title.

### Run

Click a report in the **Reports** sidebar section. datasight re-executes
the saved SQL and renders the result directly in the chat — no LLM call,
no token cost.

### Manage

- **Delete** — click the **x** button on any report in the sidebar
- **Clear all** — click **Clear** in the Reports section header

Reports are stored in `.datasight/reports.json` and persist across server
restarts.

### Run reports from the CLI

```bash
datasight report list
datasight report run 1
datasight report run 1 --format csv
datasight report run 2 --chart-format html -o trend.html
datasight report delete 1
```

## Conversation history

Chat conversations are saved automatically and persist across page reloads
and server restarts. They're stored as JSON files in
`.datasight/conversations/`.

The **History** sidebar section lists past conversations. Click one to
switch to it — the full chat (messages, tool results, charts) is replayed,
and you can continue where you left off.

**New Chat** in the header clears the current chat screen, SQL history,
and dashboard, then starts a fresh working session. Saved conversations
remain available in History. The `N` keyboard shortcut does the same
thing.

To clear all history, click **Clear** in the History section header.

## Export a conversation as HTML

Click the **export** button (download icon) in the header to enter export
mode. Each question gets a trash button — click it to exclude that Q&A
turn (question, SQL, results, and answer). Then click **Export HTML** to
download a self-contained HTML page.

You can also export from the command line:

```bash
# List available sessions
datasight export --list-sessions

# Export a session
datasight export <session-id> -o my-analysis.html

# Exclude specific turns by index (0-based)
datasight export <session-id> --exclude 2,3
```
