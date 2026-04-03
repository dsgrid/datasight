# Contributing

## Development setup

```bash
git clone https://github.com/dsgrid/datasight.git
cd datasight
pip install -e ".[dev]"
```

## Project structure

```
src/datasight/
├── cli.py              # Click CLI commands
├── config.py           # Configuration helpers
├── schema.py           # Database introspection
├── llm.py              # LLM client abstraction
├── chart.py            # Plotly chart generator
├── runner.py           # SQL execution backends
├── verify.py           # Query verification engine
├── demo.py             # Demo dataset generator
└── web/
    ├── app.py          # FastAPI server + SSE streaming
    ├── static/
    │   ├── app.js      # Frontend JavaScript
    │   └── style.css   # Frontend styles
    └── templates/
        └── index.html  # Single-page HTML template
```

## Running locally

```bash
# Start with a demo project
datasight demo ./dev-project
cd dev-project
# Edit .env with your API key (Anthropic, GitHub token, or Ollama)
datasight run -v
```

The `-v` flag enables debug logging, which shows the full LLM request/response
cycle including tool calls.

## Pre-commit hooks

The project uses [pre-commit](https://pre-commit.com/) to run checks
automatically on every commit. Install the hooks after cloning:

```bash
pre-commit install
```

Hooks run ruff (lint + format), ESLint (JavaScript), and ty (type checking).
If a hook fails, it will either auto-fix the file (ruff format) or show you
what to fix. Stage the fixes and commit again.

To run all hooks manually against every file:

```bash
pre-commit run --all-files
```

## Code style

The project uses [ruff](https://docs.astral.sh/ruff/) for linting and
formatting, and [ty](https://docs.astral.sh/ty/) for type checking.

```bash
# Run manually
ruff check src/
ruff format src/
```

## Frontend

The frontend is vanilla HTML/CSS/JS with no build step. Edit files in
`src/datasight/web/static/` and `src/datasight/web/templates/` directly.
Refresh the browser to see changes (the static files are served by FastAPI,
not bundled).

## Testing

```bash
# Run the verification suite against a demo project
datasight demo ./test-project
cd test-project
datasight verify -v
```

## Documentation

Docs use Sphinx with MyST (Markdown) and the Furo theme.

```bash
pip install -e ".[docs]"
cd docs
make html
open _build/html/index.html
```
