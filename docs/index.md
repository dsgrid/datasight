---
hide:
  - navigation
  - toc
---

# datasight

**AI-powered data exploration with natural language.** Point datasight at
your CSV, Parquet, Excel, or DuckDB files and start asking questions — minimal setup
required. Or create a curated project with schema descriptions and example
queries for your team. Start with guided starter workflows in the web UI or
use deterministic CLI inspection commands before involving the LLM.

![datasight chat view](assets/screenshots/02-chat-view.png)

```bash
uv tool install datasight

# Explore files instantly — no project setup needed
datasight run
# Open http://localhost:8084, enter a file path, and start asking questions

# Or inspect a configured project without the web UI
datasight profile

# Or try a built-in demo dataset
datasight demo eia-generation ./my-project    # US power plants
cd my-project
# Edit .env with your API key (see Set up your first project)
datasight run
```

## Where to go next

<div class="grid cards" markdown>

-   :material-account:{ .lg .middle } **[Use datasight](use/how-to/install.md)**

    ---

    Install, choose an LLM, ask questions in the web UI or CLI, build
    dashboards, audit data quality, and run on HPC or Spark.

    [:octicons-arrow-right-24: Explore US electricity generation](use/tutorials/getting-started.md)
    [:octicons-arrow-right-24: Choosing an LLM](use/concepts/choosing-an-llm.md)

-   :material-database-cog:{ .lg .middle } **[Project Setup](project-setup/tutorials/set-up-project.md)**

    ---

    Connect a database, write schema descriptions, define semantic
    measures, declare validation rules, and verify queries across
    models.

    [:octicons-arrow-right-24: Set up your first project](project-setup/tutorials/set-up-project.md)
    [:octicons-arrow-right-24: Verify queries across models](project-setup/how-to/verification.md)

-   :material-code-braces:{ .lg .middle } **[Design](design/architecture.md)**

    ---

    Architecture, internals, and contributor notes for the datasight
    codebase itself.

    [:octicons-arrow-right-24: Architecture](design/architecture.md)

</div>
