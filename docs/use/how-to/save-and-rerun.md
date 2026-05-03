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

## Export a session as a Python script

When you want a runnable, hand-editable record of an analysis — to
audit the SQL, share with colleagues who don't run datasight, or wire
into a pipeline — export the conversation as a Python script. From
export mode, click **Export Python script** instead of **Export HTML**.

The downloaded `datasight-session.py` contains:

- A short docstring with the session title and what each section means.
- The project's database path baked in as `DEFAULT_DB_PATH`, with a
  `--db` flag to override at runtime.
- One labelled section per turn — the user question as a `# ─── Turn N ───`
  header, the SQL as an editable `SQL_N = """..."""` constant, the chart
  spec as `CHART_N_SPEC = json.loads(...)` fed into a Plotly `go.Figure`,
  and the assistant's narrative preserved as `# Assistant: ...` comments.

Run the script standalone:

```bash
python datasight-session.py
python datasight-session.py --db /path/to/other.duckdb --output-dir charts/
python datasight-session.py --help
```

Edit any `SQL_N` constant to tweak filters, group-bys, or aggregations,
then re-run — no AI in the loop, no datasight installation required
(only `duckdb`/`sqlite3`, `pandas`, and `plotly`). Charts are written
to the current directory by default; use `--output-dir` to redirect.

You can also export from the CLI:

```bash
datasight export <session-id> --format py -o my-analysis.py
datasight export <session-id> --format bundle -o my-analysis.zip
datasight export <session-id> --format bundle --include html,sql,python,csv,charts,metadata
```

Bundle exports package the session into a portable zip archive with a
versioned `manifest.json`. Depending on `--include`, the archive can contain
the HTML report, runnable Python script, SQL scripts, CSV result extracts,
Plotly chart specs, and structured metadata for provenance and reruns.

DuckDB and SQLite sessions get a fully-runnable connection block.
Sessions backed by PostgreSQL or Flight SQL produce a script with a
clearly-marked `run_sql` scaffold for you to wire up your own driver —
the script still parses and shows you exactly where the change goes.
