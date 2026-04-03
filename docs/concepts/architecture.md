# Architecture

datasight uses a FastAPI web server, a pluggable LLM backend, and a
lightweight HTML/CSS/JS frontend. There are no heavy frameworks — the LLM
agent loop, tool execution, and streaming are all implemented in plain Python.

## LLM backends

datasight supports multiple LLM providers through a common `LLMClient`
abstraction (defined in `datasight.llm`). The backend is selected via the
`LLM_PROVIDER` environment variable:

- **`anthropic`** (default) — uses the Anthropic SDK to call Claude models via
  the cloud API. Requires an `ANTHROPIC_API_KEY`.
- **`ollama`** — uses Ollama's OpenAI-compatible API to run models locally. No
  API key required. Install with `pip install "datasight[ollama] @ git+https://github.com/dsgrid/datasight.git"`.

Both backends support the same tool-calling interface (`run_sql` and
`visualize_data`), so the rest of the application is provider-agnostic.

## System overview

```{mermaid}
flowchart TB
    subgraph ui ["Web UI (FastAPI + SSE)"]
        WEB[FastAPI server]
        HTML[HTML/JS frontend]
    end

    subgraph agent ["LLM Agent Loop"]
        LLM[LLMClient]
        ANTH[Anthropic API]
        OLL[Ollama local]
        SQL[run_sql tool]
        VIZ[visualize_data tool]
        LLM -.-> ANTH
        LLM -.-> OLL
        SQL --> RUNNER[SqlRunner]
        VIZ --> CHART[Interactive<br>ChartGenerator]
    end

    subgraph db ["Database"]
        DUCK[DuckDB<br>local file]
        FLIGHT[Flight SQL<br>remote server]
    end

    HTML <-->|SSE stream| WEB
    WEB --> LLM
    LLM --> SQL
    LLM --> VIZ
    RUNNER --> DUCK
    RUNNER --> FLIGHT

    style WEB fill:#15a8a8,stroke:#023d60,color:#fff
    style HTML fill:#15a8a8,stroke:#023d60,color:#fff
    style LLM fill:#023d60,stroke:#023d60,color:#fff
    style ANTH fill:#023d60,stroke:#023d60,color:#fff
    style OLL fill:#023d60,stroke:#023d60,color:#fff
    style SQL fill:#023d60,stroke:#023d60,color:#fff
    style VIZ fill:#023d60,stroke:#023d60,color:#fff
    style RUNNER fill:#bf1363,stroke:#023d60,color:#fff
    style CHART fill:#bf1363,stroke:#023d60,color:#fff
    style DUCK fill:#fe5d26,stroke:#023d60,color:#fff
    style FLIGHT fill:#fe5d26,stroke:#023d60,color:#fff
    style ui fill:#f0fafa,stroke:#15a8a8
    style agent fill:#f0f0fa,stroke:#023d60
    style db fill:#fff5f0,stroke:#fe5d26
```

## Request flow

```{mermaid}
sequenceDiagram
    participant U as User
    participant W as FastAPI
    participant A as Claude API
    participant D as Database

    U->>W: POST /api/chat (SSE)
    activate W
    W->>A: messages.create (tools)
    activate A
    A->>A: Write SQL query
    A-->>W: tool_use: run_sql
    deactivate A
    W->>D: Execute SQL
    activate D
    D-->>W: DataFrame
    deactivate D
    W-->>U: SSE: tool_result (HTML table)
    W->>A: tool_result + continue
    activate A
    A-->>W: text response
    deactivate A
    W-->>U: SSE: token stream
    W-->>U: SSE: done
    deactivate W
```

## Modules

`datasight.cli`
: Click CLI with `init`, `demo`, and `run` commands.

`datasight.config`
: Configuration helpers — loads schema descriptions, example queries, and
  creates SQL runners from environment settings.

`datasight.schema`
: Database introspection. Discovers tables, columns, and row counts using
  multiple strategies (DuckDB `SHOW TABLES`, `INFORMATION_SCHEMA`, SQLite).

`datasight.llm`
: LLM client abstraction with implementations for Anthropic and Ollama.
  Converts between provider-specific message and tool formats.

`datasight.chart`
: Interactive Plotly chart generator with chart-type switching buttons.

`datasight.runner`
: SQL execution backends — `DuckDBRunner` for local files and
  `FlightSqlRunner` for remote databases via Arrow gRPC.

`datasight.web.app`
: FastAPI application with SSE streaming, tool execution, and REST API
  endpoints for schema and query browsing.

`datasight.demo`
: Downloads cleaned EIA energy data from PUDL's public S3 bucket and creates
  a ready-to-use demo project.

## Schema context injection

The AI receives database context at startup, which is included in every
system prompt:

```{mermaid}
flowchart LR
    A[Auto-discovered schema<br>tables, columns, types, rows] --> D[System prompt]
    B[schema_description.md<br>domain context] --> D
    C[queries.yaml<br>few-shot examples] --> D
    D --> E[Claude]

    style A fill:#023d60,stroke:#023d60,color:#fff
    style B fill:#e7e1cf,stroke:#023d60,color:#023d60
    style C fill:#e7e1cf,stroke:#023d60,color:#023d60
    style D fill:#15a8a8,stroke:#023d60,color:#fff
    style E fill:#fe5d26,stroke:#023d60,color:#fff
```

The schema is introspected once at startup. The description and example queries
are loaded from disk and formatted into the prompt. Together, they give the AI
enough context to write accurate SQL without needing to explore the database
itself.

## Web UI

The frontend is a single HTML file with vanilla JavaScript — no build step,
no npm, no React. It features:

- **Sidebar** with a table browser (expandable columns) and example queries
  that filter by selected table
- **SSE streaming** for real-time token-by-token responses
- **Inline tool results** — data tables and interactive Plotly charts rendered
  directly in the chat
- **Markdown rendering** with syntax-highlighted SQL code blocks
