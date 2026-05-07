---
hide:
  - navigation
  - toc
---

# datasight

**Ask questions about your data in plain English.** datasight translates your question
into SQL, runs it, and gives you a table or interactive chart — no SQL knowledge needed.

![datasight chat view](assets/screenshots/02-chat-view.png)

## What you need to get started

- A terminal (macOS Terminal, Linux shell, or Windows PowerShell)
- Python 3.11+ (check with `python --version`) — or use [uv](use/how-to/install.md),
  which manages this for you
- **An API key** from one AI provider — or a free GitHub account. See
  [Choosing an LLM](use/concepts/choosing-an-llm.md) if you're not sure which to pick.
  Not sure what an API key is? See the [Glossary](glossary.md).

## Try it in five minutes

```bash
# 1. Install datasight
uv tool install datasight
# (Don't have uv? See https://docs.astral.sh/uv/getting-started/installation/)

# 2. Download a demo dataset (US power plant data, ~50 MB)
datasight demo eia-generation ./eia-project
cd eia-project

# 3. Add your API key to .env
#    Open .env in any text editor and paste in your key, e.g.:
#    ANTHROPIC_API_KEY=sk-ant-...

# 4. Launch the web UI
datasight run
# Open http://localhost:8084 and start asking questions
```

The sidebar shows your tables and suggested starter questions. Type anything in the
chat input — *"What are the top 10 power plants by total generation?"* — and datasight
does the rest.

!!! tip "No project setup needed"
    You can also point datasight directly at a CSV, Parquet, Excel, or DuckDB file —
    no project directory required. Run `datasight run`, enter a file path on the landing
    page, and start asking questions immediately.

## Where to go next

<div class="grid cards" markdown>

-   :material-account:{ .lg .middle } **[Use datasight](use/how-to/install.md)**

    ---

    Install, configure an AI provider, ask questions in the web UI or CLI, build
    dashboards, audit data quality, and more.

    [:octicons-arrow-right-24: Five-minute tutorial (EIA generation data)](use/tutorials/getting-started.md)
    [:octicons-arrow-right-24: Choosing an LLM](use/concepts/choosing-an-llm.md)
    [:octicons-arrow-right-24: What the AI sees](use/concepts/what-the-ai-sees.md)

-   :material-database-cog:{ .lg .middle } **[Project Setup](project-setup/tutorials/set-up-project.md)**

    ---

    Connect your own database, write schema descriptions, define semantic
    measures, declare validation rules, and verify queries.

    [:octicons-arrow-right-24: Set up your first project](project-setup/tutorials/set-up-project.md)
    [:octicons-arrow-right-24: Verify queries across models](project-setup/how-to/verification.md)

-   :material-code-braces:{ .lg .middle } **[Design](design/architecture.md)**

    ---

    Architecture, internals, and contributor notes for the datasight
    codebase itself.

    [:octicons-arrow-right-24: Architecture](design/architecture.md)

</div>
