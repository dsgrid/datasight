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
:caption: Tutorials

tutorials/quickstart
tutorials/demo-dataset
```

```{toctree}
:maxdepth: 2
:caption: How-to guides

how-to/schema-description
how-to/example-queries
how-to/visualizations
how-to/remote-hpc
```

```{toctree}
:maxdepth: 2
:caption: Reference

reference/cli
reference/configuration
```

```{toctree}
:maxdepth: 2
:caption: Explanation

explanation/architecture
```
