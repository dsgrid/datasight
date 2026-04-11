# Getting started

Install datasight and start exploring data in under five minutes.

## Install

```bash
pip install git+https://github.com/dsgrid/datasight.git
```

This includes support for DuckDB, SQLite, PostgreSQL, Anthropic, GitHub
Models, and Ollama — no extras needed.

!!! note "PNG chart export"
    The `datasight ask --chart-format png` CLI command requires an additional
    package: `pip install 'datasight[export]'`. The web UI uses interactive
    HTML charts and does not need this.

## Choose your path

datasight supports three workflows depending on what you need:

<div class="grid cards" markdown>

-   **Inspect files**

    ---

    Have CSV, Parquet, or DuckDB files? Get a complete overview — profile,
    measures, dimensions, trends, and suggested prompts — with a single
    command. No project setup, no LLM, no files written.

    ```bash
    datasight inspect generation.parquet
    ```

    [:octicons-arrow-right-24: Inspect files](inspect-files.md)

-   **Audit data quality**

    ---

    Run a structured quality audit: find nulls, suspicious ranges, date
    coverage gaps, and dimension breakdowns. Use individual commands for
    a focused check, or run `datasight inspect` for everything at once.

    ```bash
    datasight quality --table generation_fuel
    ```

    [:octicons-arrow-right-24: Audit data quality](data-quality.md)

-   **Full project with AI**

    ---

    Set up a curated project with schema descriptions, example queries, and
    semantic measures. Use the web UI or CLI to ask questions in natural
    language, generate visualizations, and build dashboards.

    ```bash
    datasight init ./my-project
    datasight run
    ```

    [:octicons-arrow-right-24: Set up a project](project-setup.md)

</div>

## Try a demo

datasight includes several demo datasets you can download and explore
immediately:

```bash
# US power plant generation data (EIA, ~50 MB)
datasight demo eia-generation ./my-project

# EV charging demand projections (NLR TEMPO, ~19 MB)
datasight demo dsgrid-tempo ./my-project

cd my-project
# Edit .env with your API key (see Project setup)
datasight run
```

Open <http://localhost:8084> and start asking questions. See
[Try the demo datasets](demo-dataset.md) for details about each dataset.
