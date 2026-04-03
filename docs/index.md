# datasight

```{rst-class} hero-tagline
AI-powered database exploration with natural language. Connect an AI agent to
your DuckDB database and explore data through a web UI — ask questions in plain
English, get SQL queries, interactive charts, and tabular results.
```

````{div} install-block
```bash
pip install git+https://github.com/dsgrid/datasight.git

# Try with the built-in EIA energy demo dataset
datasight demo ./my-project
cd my-project
# Edit .env with your API key (see Getting Started)
datasight run
```
````

## Documentation by role

datasight has three types of users. Pick the section that matches how you use
the tool — or start with [Users and roles](concepts/users-and-roles.md) for an
overview.

::::{grid} 1 1 3 3
:gutter: 3

:::{grid-item-card} End user
:link: end-user/getting-started
:link-type: doc

Get started, explore data through the web UI, ask questions, view charts,
and review SQL.
:::

:::{grid-item-card} Dataset developer
:link: dataset-developer/schema-description
:link-type: doc

Set up a datasight project for your team. Connect a database, write schema
descriptions, curate example queries, and verify results across models.
:::

:::{grid-item-card} Tool developer
:link: tool-developer/architecture
:link-type: doc

Contribute to datasight itself. Understand the architecture, LLM agent
loop, and module structure.
:::

::::

```{toctree}
:hidden:
:caption: End user

end-user/getting-started
end-user/demo-dataset
end-user/quickstart
end-user/web-ui
end-user/visualizations
end-user/query-confidence
end-user/query-log
end-user/remote-hpc
```

```{toctree}
:hidden:
:caption: Dataset developer

dataset-developer/schema-description
dataset-developer/example-queries
dataset-developer/verification
dataset-developer/configuration
dataset-developer/cli
```

```{toctree}
:hidden:
:caption: Tool developer

tool-developer/architecture
tool-developer/contributing
```

```{toctree}
:hidden:
:caption: Core concepts

concepts/users-and-roles
```
