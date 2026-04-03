# Getting started

Install datasight and start exploring data in under five minutes.

## Install

```bash
pip install git+https://github.com/dsgrid/datasight.git
```

To use Ollama (free, local models) instead of Anthropic:

```bash
pip install "datasight[ollama] @ git+https://github.com/dsgrid/datasight.git"
```

## Set up your API key

datasight needs an LLM to translate your questions into SQL. Choose one:

**Option A — Anthropic (recommended)**

1. Create an account at [console.anthropic.com](https://console.anthropic.com/)
2. Go to **API Keys** and create a new key
3. Add it to the `.env` file in your project directory:

```bash
ANTHROPIC_API_KEY=sk-ant-...
```

**Option B — Ollama (free, runs locally)**

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
datasight run
```

See [Quickstart](quickstart.md) for a full walkthrough of connecting your own
data and writing schema descriptions.
