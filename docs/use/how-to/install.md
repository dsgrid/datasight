# Install datasight

datasight is distributed as a Python CLI on PyPI. The recommended installer is
[uv](https://docs.astral.sh/uv/), which handles Python version management and installs
datasight as a global tool. If you already have a Python toolchain you prefer, `pip`
works too.

=== "uv (recommended)"

    ```bash
    # Install uv (one-time — see https://docs.astral.sh/uv/getting-started/installation/)
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Install datasight
    uv tool install datasight
    ```

=== "pip"

    ```bash
    pip install --user datasight
    ```

Both options include support for DuckDB, SQLite, PostgreSQL, and all AI providers
(Anthropic, OpenAI, GitHub Models, Ollama) — no extra packages needed.

## Configure an AI provider

datasight needs an AI provider to translate your questions into SQL. **If you're
choosing for the first time, start with Anthropic** — it's the default, the models are
good, and you can get a key in about two minutes. GitHub Models is a free alternative
if you have a GitHub account.

Run `datasight config init` to create a shared credentials file that every project on
this machine will pick up automatically:

```bash
datasight config init
# Creates ~/.config/datasight/.env — edit it to add your key
```

Alternatively, paste the key directly into a project's `.env` file instead.

=== "Anthropic (recommended starting point)"

    1. Go to [console.anthropic.com](https://console.anthropic.com) → **API Keys** →
       **Create Key**.
    2. Copy the key (starts with `sk-ant-`).
    3. Add it to your `.env`:

    ```bash
    ANTHROPIC_API_KEY=sk-ant-...
    ```

    No other settings are needed — `anthropic` is the default provider and
    Claude Haiku is the default model, which handles SQL generation well at low cost.

=== "OpenAI"

    ```bash
    LLM_PROVIDER=openai
    OPENAI_API_KEY=sk-...
    OPENAI_MODEL=gpt-4o-mini
    ```

    For Azure OpenAI or a corporate gateway, also set `OPENAI_BASE_URL`.

=== "GitHub Models (free tier)"

    GitHub Models provides free access to GPT and other models using your GitHub
    account — no billing setup required.

    ```bash
    LLM_PROVIDER=github
    GITHUB_TOKEN=ghp_...
    GITHUB_MODELS_MODEL=gpt-4o
    ```

    To get a `GITHUB_TOKEN`:

    - **GitHub CLI (quickest):** run `gh auth token` and paste the output.
    - **Personal access token:** go to GitHub → Settings → Developer settings →
      Personal access tokens → Fine-grained tokens → Generate new token. Grant the
      **Models: read** account permission. Classic tokens do not work.

    !!! note "Free tier context limit"
        GitHub Models caps requests at 8,000 tokens. Databases with more than ~20
        tables can exceed this. If you hit "request too large" errors, see
        [Limit schema sent to the LLM](../../project-setup/how-to/schema-config.md).

=== "Ollama (local, no API key)"

    Ollama runs AI models on your own hardware — nothing leaves your machine and there's
    no per-query cost. Use it when data sensitivity or offline use requires it; for most
    users, a hosted provider gives better results with less setup.

    Install [Ollama](https://ollama.com/), then pull a tool-calling model:

    ```bash
    ollama pull qwen2.5:7b
    ```

    Then configure `.env`:

    ```bash
    LLM_PROVIDER=ollama
    OLLAMA_MODEL=qwen2.5:7b
    ```

    `qwen2.5:7b` works well for CLI queries (`datasight ask`). For the web UI with
    chart generation, `qwen2.5:14b` handles the more complex interactions better.
    See [Choosing an LLM](../concepts/choosing-an-llm.md) for hardware sizing guidance.

See the [Configuration reference](../../reference/configuration.md) for every supported
variable.

!!! note "PNG chart export"
    `datasight ask --chart-format png` needs the optional export extra.
    Reinstall with `uv tool install "datasight[export]"` or
    `pip install --user "datasight[export]"`.
    The web UI does not need it.
