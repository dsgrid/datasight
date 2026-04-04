---
hide:
  - navigation
  - toc
---

# datasight

**AI-powered database exploration with natural language.** Connect an AI agent to
your database (DuckDB, PostgreSQL, SQLite, or Flight SQL) and explore data
through a web UI or CLI — ask questions in plain English, get SQL queries,
interactive charts, and tabular results.

```bash
pip install git+https://github.com/dsgrid/datasight.git

# Try with the built-in EIA energy demo dataset
datasight demo ./my-project
cd my-project
# Edit .env with your API key (see Getting Started)
datasight run
```

## Documentation by role

datasight has three types of users. Pick the section that matches how you use
the tool — or start with [Users and roles](concepts/users-and-roles.md) for an
overview.

<div class="grid cards" markdown>

-   :material-account:{ .lg .middle } **End user**

    ---

    Get started, explore data through the web UI, ask questions, view charts,
    and review SQL.

    [:octicons-arrow-right-24: Getting started](end-user/getting-started.md)

-   :material-database-cog:{ .lg .middle } **Dataset developer**

    ---

    Set up a datasight project for your team. Connect a database, write schema
    descriptions, curate example queries, and verify results across models.

    [:octicons-arrow-right-24: Schema description](dataset-developer/schema-description.md)

-   :material-code-braces:{ .lg .middle } **Tool developer**

    ---

    Contribute to datasight itself. Understand the architecture, LLM agent
    loop, and module structure.

    [:octicons-arrow-right-24: Architecture](tool-developer/architecture.md)

</div>
