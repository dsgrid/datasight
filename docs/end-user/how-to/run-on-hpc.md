# Run datasight on an HPC compute node

If your data lives on an HPC filesystem, the simplest setup is to run
datasight itself on a compute node and query it from your laptop. No
Flight SQL server, no extra infrastructure — just install datasight on
the HPC and an SSH tunnel if you want the browser UI.

For a multi-user shared backend or a non-DuckDB engine, see
[Connect to a remote Flight SQL backend](connect-flight-sql.md)
instead.

Because datasight itself runs on the compute node in this setup, **the LLM
call originates from the compute node too**. That's the right fit if you
want to use a local model on the compute node's GPU (e.g. Ollama on a GPU
node), or if a hosted API key configured on the compute node is fine. If
policy or preference requires the LLM to run from your laptop — including
using your laptop's GPU — use the Flight SQL setup instead, which keeps
the agent client-side.

## 1. Install datasight on the HPC

From the login node, create a [uv](https://docs.astral.sh/uv/)
environment and install:

```bash
uv venv ~/datasight-env
source ~/datasight-env/bin/activate
uv pip install git+https://github.com/dsgrid/datasight.git
```

If uv isn't available, install it first with
`curl -LsSf https://astral.sh/uv/install.sh | sh`. Install once from
the login node; compute nodes share the same filesystem.

## 2. Allocate a compute node

```bash
salloc --time=4:00:00 --mem=240G --cpus-per-task=104 --account <your-account>
```

DuckDB benefits from plenty of memory and cores for large aggregations.
Adjust to match your dataset.

## 3. Point datasight at your data

You have two low-friction options on the compute node:

=== "Inspect files directly"

    `datasight inspect` runs deterministic profile/quality/measures/
    dimensions analyses on raw files — no project or LLM required:

    ```bash
    source ~/datasight-env/bin/activate
    datasight inspect /scratch/project/data/*.parquet
    ```

=== "Use a project directory"

    Create a project directory (on the login node is fine — it's a
    one-time setup) with a `.duckdb` file of views over your parquet:

    ```bash
    duckdb /scratch/project/mydata.duckdb
    ```

    ```sql
    CREATE VIEW generation AS
    SELECT * FROM read_parquet('/scratch/project/data/generation/**/*.parquet');
    CREATE VIEW stations AS
    SELECT * FROM read_parquet('/scratch/project/data/stations.parquet');
    ```

    Then point `.env` at it:

    ```bash
    DB_MODE=duckdb
    DB_PATH=/scratch/project/mydata.duckdb
    ```

    See [Set up a project](../../project-developer/set-up-project.md) for
    schema descriptions and example queries.

## 4. Pick a workflow

=== "CLI only (no tunnel needed)"

    For headless runs, scripts, and batch work, stay in the compute node
    shell:

    ```bash
    datasight ask "Monthly generation trend" --format csv -o trend.csv
    datasight ask --file questions.txt --output-dir batch-output
    datasight profile
    datasight audit-report -o audit.html
    ```

    See [Ask questions from the CLI](ask-from-cli.md) for the full CLI
    reference.

=== "Web UI via SSH tunnel"

    Start the server on the compute node (it binds to `0.0.0.0` by
    default so the login node can reach it):

    ```bash
    datasight run
    ```

    From your laptop, tunnel port 8084 through the login node. Replace
    `compute-node-42` with your actual hostname (`hostname` on the compute
    node, or `squeue --me --format="%N" --noheader`):

    ```bash
    ssh -N -L 8084:compute-node-42:8084 user@hpc-login-node
    ```

    Open <http://localhost:8084> in your browser.

## Tips

- **Your LLM key still travels from the laptop environment if you set it
  there** — but since datasight is running on the compute node in this
  setup, set `ANTHROPIC_API_KEY` (or equivalent) in the compute node
  shell or the project's `.env`.
- **Pre-aggregate in views** for common queries. A `daily_summary` view
  is much faster than scanning raw parquet every time.
- **Write a `schema_description.md`** — the AI discovers table structure
  automatically, but domain context dramatically improves SQL quality.
- **Watch the Slurm time limit.** When the job ends, the datasight server
  stops and the tunnel breaks. Use `salloc` for interactive sessions you
  may want to extend.
