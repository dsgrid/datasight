# Explore US electricity generation (EIA)

This tutorial walks you through installing datasight, loading US power
plant generation data from the [PUDL project](https://catalyst.coop/pudl/),
and asking your first question. Allow about five minutes.

For other datasets, see [Explore EV charging demand with
TEMPO](tempo.md) or [Try the time-validation demo](time-validation.md).

## 1. Install

```bash
pip install git+https://github.com/dsgrid/datasight.git
```

This includes DuckDB, SQLite, PostgreSQL, Anthropic, GitHub Models, and
Ollama — no extras needed.

## 2. Download a demo dataset

The `eia-generation` demo downloads US power plant generation data (~50 MB)
from the [PUDL project](https://catalyst.coop/pudl/):

```bash
datasight demo eia-generation ./eia-project
cd eia-project
```

This creates a project directory with a DuckDB database, a schema
description, and example queries already wired up.

## 3. Add an API key

Edit `.env` in the project directory and add your Anthropic key:

```bash
ANTHROPIC_API_KEY=sk-ant-...
```

Using GitHub Models or a local Ollama instance instead? See
[Set up a project](../../project-developer/set-up-project.md#configure) for the alternative
`.env` configurations.

## 4. Launch the web UI

```bash
datasight run
```

Open <http://localhost:8084>. The sidebar shows the loaded tables, example
queries, and deterministic inspection tools.

## 5. Ask your first question

Type a question in the chat input — try one of these:

- *What are the top 10 power plants by total generation?*
- *Show me the monthly trend of wind generation*
- *Compare coal vs natural gas generation over time*

datasight writes SQL, runs it, and returns a table or an interactive chart.
Click **Pin** on any result to add it to the dashboard.

## What's next

- **Explore more questions.** [Ask questions in the web UI](../how-to/ask-in-web-ui.md)
  covers follow-ups, clarifying prompts, and the schema sidebar.
- **Build a dashboard.** [Build a dashboard](../how-to/build-a-dashboard.md)
  shows how to pin results, apply cross-card filters, and export.
- **Use your own data.** [Set up a project](../../project-developer/set-up-project.md)
  walks through connecting your own database or files.
- **Explore files directly.** [Explore files without a project](../how-to/explore-files.md)
  skips project setup entirely — point datasight at a CSV, Parquet, or DuckDB
  file and start asking.

!!! note "PNG chart export"
    The `datasight ask --chart-format png` CLI command needs an additional
    package: `pip install 'datasight[export]'`. The web UI uses interactive
    HTML charts and does not need this.
