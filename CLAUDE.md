# CLAUDE.md

## Project overview

datasight is an AI-powered database exploration tool. Users ask questions in natural language, an LLM agent translates them to SQL, executes queries, and returns results as tables or interactive Plotly charts. It supports a web UI (`datasight run`) and a headless CLI (`datasight ask`).

## Tech stack

- **Python 3.13+**, built with Hatchling (`pyproject.toml`)
- **FastAPI + uvicorn** for the web server with SSE streaming
- **Vanilla HTML/CSS/JS** frontend — no build step, no npm, no React
- **LLM backends**: Anthropic (default), GitHub Models, Ollama — all via a common `LLMClient` abstraction in `datasight.llm`
- **Database backends**: DuckDB (default), SQLite, PostgreSQL, Flight SQL — all via `SqlRunner` implementations in `datasight.runner`
- **Click CLI** with commands: `run`, `ask`, `init`, `demo`, `verify`, `export`, `log`

## Repository layout

```
src/datasight/
├── cli.py          # Click CLI commands
├── agent.py        # Shared agent loop (web + CLI)
├── config.py       # Configuration helpers, normalize_db_mode()
├── schema.py       # Database introspection
├── llm.py          # LLM client abstraction
├── chart.py        # Plotly chart generator
├── runner.py       # SQL execution backends
├── prompts.py      # System prompt builder
├── sql_validation.py # SQL validation with sqlglot
├── export.py       # Session-to-HTML export
├── verify.py       # Query verification engine
├── demo.py         # Demo dataset generator
└── web/
    ├── app.py      # FastAPI server + SSE streaming
    ├── static/     # split vanilla JS modules, CSS, icons
    └── templates/  # index.html
tests/              # pytest + pytest-asyncio
docs/               # Zensical documentation
mkdocs.yml          # Zensical config (MkDocs-compatible)
```

## Development commands

```bash
# Primary workflow: stdlib venv + editable install
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"

# Run pre-commit hooks (ruff, eslint, ty)
pre-commit run --all-files

# Build docs
zensical serve      # dev server at localhost:8000
zensical build

# Run tests
pytest

# Run frontend structure tests
node --test tests/test_web_helpers.js tests/test_web_ui_refactor.js

# Run web UI smoke tests
pytest -q tests/test_web_ui_smoke.py
```

## Pre-commit hooks

Hooks run automatically on commit. Don't skip them — fix issues instead.

- **ruff** — Python lint + format. Auto-fixes on failure; re-stage and commit.
- **ruff-format** — Python formatting.
- **eslint** — JavaScript lint for `src/datasight/web/static/*.js`. Uses flat config (`eslint.config.js`). Browser globals like `ResizeObserver` and `alert` must be accessed via `window.` prefix.
- **ty** — Python type checking. Excludes `src/datasight/llm.py`. Optional imports (like `psycopg`) need `# ty: ignore[unresolved-import]`.

## Code conventions

- **DB_MODE values**: `duckdb`, `sqlite`, `postgres`, `flightsql`. The legacy value `local` is accepted as a silent alias for `duckdb` via `normalize_db_mode()` in `config.py`.
- **SQL dialect**: Tracked as `sql_dialect` (values: `duckdb`, `sqlite`, `postgres`). Mapped from `db_mode` via `_db_mode_dialects` dicts.
- **No framework for frontend**: All JS is vanilla and split across `src/datasight/web/static/app_*.js`. Keep new behavior in the matching domain file and route click/change handling through `app_events.js`. ESLint `no-undef` is strict — add new browser APIs to `eslint.config.js` globals or use `window.` prefix.
- **Keyboard shortcuts**: Must not conflict with browser shortcuts. Use plain keys (like `n`, `/`, `?`) guarded by `isInput` check, or `Cmd/Ctrl+key` combos that browsers don't claim.
- **Type annotations**: Use modern Python syntax — `list[str]`, `dict[str, Any]`, `str | None`. Do not use `List`, `Dict`, `Optional`, `Union`, or other imports from `typing` when a built-in equivalent exists.

## Documentation

Docs use **Zensical** (successor to MkDocs Material). Key details:

- Config: `mkdocs.yml` (Zensical reads MkDocs config natively)
- Content: `docs/` directory, standard Markdown
- Mermaid diagrams: use ` ```mermaid ` fences
- Admonitions: use `!!! tip`, `!!! warning`
- CLI reference: uses `mkdocs-click` markdown extension (`::: mkdocs-click` blocks)
- Icons: use `:material-icon-name:` and `:octicons-icon-name:` (requires `pymdownx.emoji` extension)
- CI: `.github/workflows/gh-pages.yml` deploys to GitHub Pages on push to main

## Testing

- **pytest + pytest-asyncio** with `asyncio_mode = "auto"`
- **Node test runner** is used for frontend structure tests: `node --test tests/test_web_helpers.js tests/test_web_ui_refactor.js`
- **Web UI smoke tests** live in `tests/test_web_ui_smoke.py` and exercise the rendered FastAPI app without a browser build step
- Integration tests (marked `@pytest.mark.integration`) require a running Ollama instance
- Test fixtures in `tests/conftest.py` create a temporary DuckDB project directory
