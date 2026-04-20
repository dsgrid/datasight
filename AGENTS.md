# AGENTS.md

## Project overview

datasight is an AI-powered data exploration tool. Users ask questions in natural language, an LLM agent translates them to SQL, executes queries, and returns results as tables or interactive Plotly charts. It supports a web UI (`datasight run`) and a headless CLI (`datasight ask`).

## Tech stack

- **Python 3.11+**, built with Hatchling (`pyproject.toml`). CI tests on 3.13 only.
- **FastAPI + uvicorn** for the web server with SSE streaming
- **Svelte 5 + TypeScript + Tailwind CSS** frontend — built with Vite, served by FastAPI
- **LLM backends**: Anthropic (default), GitHub Models, Ollama — all via a common `LLMClient` abstraction in `datasight.llm`
- **Database backends**: DuckDB (default), SQLite, PostgreSQL, Flight SQL — all via `SqlRunner` implementations in `datasight.runner`
- **Click CLI** with commands: `run`, `ask`, `init`, `demo`, `generate`, `verify`, `profile`, `quality`, `doctor`, `export`, `log`

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
    ├── static/     # Generated Vite build output (ignored by git)
    └── templates/  # Generated index.html (ignored by git)
frontend/               # Svelte 5 + TypeScript + Tailwind source
├── src/
│   ├── App.svelte      # Root component
│   ├── main.ts         # Entry point
│   ├── app.css         # Tailwind + design tokens
│   └── lib/
│       ├── stores/     # Svelte 5 rune-based stores (8 modules)
│       ├── api/        # Typed API client functions (9 modules)
│       ├── components/ # ~40 Svelte components
│       └── utils/      # Search, format, markdown utilities
├── tests/              # Vitest unit tests
├── e2e/                # Playwright E2E tests
└── scripts/
    └── build-frontend.sh  # Build + copy to FastAPI dirs
tests/              # pytest + pytest-asyncio
docs/               # Zensical documentation
mkdocs.yml          # Zensical config
```

## Development commands

```bash
# Primary workflow: uv creates .venv and installs project + dev extras
uv sync --extra dev
. .venv/bin/activate

# Run prek hooks (ruff, ruff-format, ty, docs CLI reference drift)
prek run --all-files

# Build docs
zensical serve      # dev server at localhost:8000
zensical build

# Regenerate the static CLI reference after CLI changes
python scripts/generate_cli_reference.py

# Run Python tests
pytest

# Run CI-safe Python tests, excluding local Ollama integration tests
pytest -m "not integration"

# Run web UI smoke tests
pytest -q tests/test_web_ui_smoke.py

# Frontend development
cd frontend
npm install
npm run dev           # Vite dev server on :5173 (proxies /api to :8084)
npm run check         # Svelte + TypeScript checks
npm test              # Vitest unit tests
npm run build         # Production build

# Build frontend for FastAPI serving after a clean checkout or frontend changes
bash scripts/build-frontend.sh
```

## Pre-commit hooks

Hooks run automatically on commit. Don't skip them — fix issues instead.

- **ruff** — Python lint + format. Auto-fixes on failure; re-stage and commit.
- **ruff-format** — Python formatting.
- **ty** — Python type checking. Optional imports (like `psycopg`) need `# ty: ignore[unresolved-import]`.
- **docs CLI reference drift** — Ensures `docs/reference/cli.md` stays aligned with the current Click command tree.

## Code conventions

- **DB_MODE values**: `duckdb`, `sqlite`, `postgres`, `flightsql`. The legacy value `local` is accepted as a silent alias for `duckdb` via `normalize_db_mode()` in `config.py`.
- **SQL dialect**: Tracked as `sql_dialect` (values: `duckdb`, `sqlite`, `postgres`). Mapped from `db_mode` via `_db_mode_dialects` dicts.
- **Frontend**: Svelte 5 with TypeScript in `frontend/`. Components use runes API (`$state`, `$derived`, `$effect`, `$props`). Stores are factory functions with getter/setter pairs wrapping `$state()`. Tailwind CSS for styling with design tokens in `app.css`. Build with `scripts/build-frontend.sh` which copies Vite output to FastAPI's `static/` and `templates/` dirs. Generated files in `src/datasight/web/static/` and `src/datasight/web/templates/index.html` are ignored by git; run the script after a clean checkout before serving the production UI with `datasight run`.

## CI and release workflows

- `.github/workflows/ci.yml` runs a Ruff/ty/frontend type-check lint job, builds the frontend, runs Vitest, runs Playwright E2E tests against `datasight run`, runs pytest with coverage, and uploads `coverage.xml` to Codecov.
- `.github/workflows/gh-pages.yml` builds and deploys Zensical documentation to GitHub Pages on pushes to `main`.
- `.github/workflows/release.yml` runs on `v*` tags, builds frontend assets, builds Python sdist/wheel distributions, creates a GitHub release, and publishes to PyPI. Hatch includes the generated frontend files as build artifacts, so do not commit bundled frontend output.
- **Keyboard shortcuts**: Must not conflict with browser shortcuts. Use plain keys (like `n`, `/`, `?`) guarded by `isInput` check, or `Cmd/Ctrl+key` combos that browsers don't claim.
- **Type annotations**: Use modern Python syntax — `list[str]`, `dict[str, Any]`, `str | None`. Do not use `List`, `Dict`, `Optional`, `Union`, or other imports from `typing` when a built-in equivalent exists.

## Documentation domain

datasight is built for energy research. Documentation examples, CLI help text, and sample prompts should use energy-domain language (generation, fuel types, plants, MWh, capacity) — not generic retail/sales/orders examples. When writing new examples or updating existing ones, use tables like `generation_fuel`, `plants`, columns like `net_generation_mwh`, `report_date`, `energy_source_code`, and questions about electricity generation, capacity, or fuel consumption.

## Documentation

Docs use **Zensical**. Key details:

- Config: `mkdocs.yml`
- Source content lives in `docs/` as standard Markdown
- Serve docs locally with `zensical serve`
- Build docs with `zensical build`
- Mermaid diagrams: use ` ```mermaid ` fences
- Admonitions: use `!!! tip`, `!!! warning`
- Icons: use `:material-icon-name:` and `:octicons-icon-name:` (requires `pymdownx.emoji` extension)
- `docs/reference/cli.md` is generated from the Click command tree with `python scripts/generate_cli_reference.py`
- CI: `.github/workflows/gh-pages.yml` builds the site with `zensical build` and deploys to GitHub Pages on push to main

## Testing

- **pytest + pytest-asyncio** with `asyncio_mode = "auto"`
- **Integration marker**: tests marked `integration` require a running local Ollama instance with `qwen2.5:7b` available and are excluded in CI with `pytest -m "not integration"`. New tests that call a live LLM provider must be marked `integration`; prefer fake/stub LLM clients for deterministic unit tests.
- **Vitest** for frontend unit tests (`frontend/tests/`). Run with `cd frontend && npm test`.
- **Playwright** for frontend E2E tests (`frontend/e2e/`). Requires `datasight run` to be running. Run with `cd frontend && npm run test:e2e`.
- **Web UI smoke tests** live in `tests/test_web_ui_smoke.py` and exercise the rendered FastAPI app without a browser build step
- Integration tests (marked `@pytest.mark.integration`) require a running Ollama instance
- Test fixtures in `tests/conftest.py` create a temporary DuckDB project directory
