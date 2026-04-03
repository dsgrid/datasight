# datasight

**AI-powered database exploration with natural language.**

datasight connects an AI agent to your DuckDB database (local or remote via
Flight SQL) and provides a web UI where you can ask questions in plain English.
The agent writes SQL, runs queries, and generates interactive Plotly
visualizations.

## Get started

```bash
pip install git+https://github.com/dsgrid/datasight.git

# Try with the built-in EIA energy demo dataset
datasight demo ./my-project
cd my-project
# Edit .env with your ANTHROPIC_API_KEY
datasight run
```

---

```{toctree}
:maxdepth: 2
:caption: End user

end-user/web-ui
end-user/visualizations
end-user/query-confidence
```

```{toctree}
:maxdepth: 2
:caption: Dataset developer

dataset-developer/quickstart
dataset-developer/demo-dataset
dataset-developer/schema-description
dataset-developer/example-queries
dataset-developer/verification
dataset-developer/query-log
dataset-developer/remote-hpc
dataset-developer/configuration
```

```{toctree}
:maxdepth: 2
:caption: Tool developer

tool-developer/cli
```

```{toctree}
:maxdepth: 2
:caption: Core concepts

concepts/users-and-roles
concepts/architecture
```
