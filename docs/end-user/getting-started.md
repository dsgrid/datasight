# Getting started

Install datasight and start exploring data in under five minutes.

## Install

```bash
pip install git+https://github.com/dsgrid/datasight.git
```

Optional extras for different databases and LLM providers:

```bash
# PostgreSQL support
pip install "datasight[postgres] @ git+https://github.com/dsgrid/datasight.git"

# GitHub Models (included with Copilot subscriptions)
pip install "datasight[github] @ git+https://github.com/dsgrid/datasight.git"

# Ollama (free, local models)
pip install "datasight[ollama] @ git+https://github.com/dsgrid/datasight.git"

# PNG chart export (for datasight ask --chart-format png)
pip install "datasight[export] @ git+https://github.com/dsgrid/datasight.git"
```

SQLite support is built in (no extra install needed).

## Set up your API key

datasight needs an LLM to translate your questions into SQL. Choose one:

**Option A — Anthropic (recommended)**

1. Create an account at [console.anthropic.com](https://console.anthropic.com/)
2. Go to **API Keys** and create a new key
3. Add it to the `.env` file in your project directory:

```bash
ANTHROPIC_API_KEY=sk-ant-...
```

**Option B — GitHub Models (uses your Copilot subscription)**

No per-token billing — uses your existing GitHub Copilot subscription. Get your
token from [github.com/settings/tokens](https://github.com/settings/tokens) or
run `gh auth token` if you use the GitHub CLI.

```bash
LLM_PROVIDER=github
GITHUB_TOKEN=ghp_...
GITHUB_MODELS_MODEL=gpt-4o
```

**Option C — Ollama (free, runs locally)**

No API key needed. Install [Ollama](https://ollama.com/), pull a model with
tool-calling support, and configure `.env`:

```bash
ollama pull qwen3.5:35b-a3b
```

```bash
LLM_PROVIDER=ollama
OLLAMA_MODEL=qwen3.5:35b-a3b
```

## Try the demo

The fastest way to see datasight in action:

```bash
datasight demo ./my-project
cd my-project
# Edit .env with your API key (see above)
datasight run
```

Open <http://localhost:8084> and start asking questions. See
[Try the demo dataset](demo-dataset.md) for details about the included
EIA energy data.

## Use your own database

If you have a DuckDB database file ready:

```bash
datasight init ./my-project
cd my-project
# Edit .env with your API key and DB_PATH
datasight generate   # auto-generate schema docs and example queries
datasight run
```

`datasight generate` connects to your database and uses the LLM to draft
`schema_description.md` and `queries.yaml` so you don't have to write them
from scratch. See [Quickstart](quickstart.md) for a full walkthrough.
