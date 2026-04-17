# Install datasight

```bash
pip install git+https://github.com/dsgrid/datasight.git
```

This includes DuckDB, SQLite, PostgreSQL, Anthropic, OpenAI, GitHub
Models, and Ollama support.

## Configure an LLM provider

Set one of these in your shell or in the project `.env`:

=== "Anthropic"

    ```bash
    ANTHROPIC_API_KEY=sk-ant-...
    ```

=== "OpenAI"

    ```bash
    LLM_PROVIDER=openai
    OPENAI_API_KEY=sk-...
    OPENAI_MODEL=gpt-4o-mini
    ```

    For Azure OpenAI or a corporate gateway, also set `OPENAI_BASE_URL`.

=== "GitHub Models"

    ```bash
    LLM_PROVIDER=github
    GITHUB_TOKEN=ghp_...
    GITHUB_MODELS_MODEL=gpt-4o
    ```

=== "Ollama (local)"

    Install [Ollama](https://ollama.com/), pull a tool-calling model, then:

    ```bash
    ollama pull qwen3:8b

    LLM_PROVIDER=ollama
    OLLAMA_MODEL=qwen3:8b
    ```

See the [Configuration reference](../../reference/configuration.md) for
every supported variable.

!!! note "PNG chart export"
    `datasight ask --chart-format png` needs the optional export extra:
    `pip install 'datasight[export]'`. The web UI does not need it.
