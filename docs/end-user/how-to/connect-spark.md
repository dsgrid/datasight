# Connect to an Apache Spark backend

This guide covers configuring datasight to talk to an Apache Spark cluster
over [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html).
Use this when your data is too large to fit on a single machine — typically
multi-terabyte Parquet, Delta, or Iceberg tables managed by Spark.

## When to use this

Reach for the Spark backend when:

- **Your data is on a Spark cluster.** Generation data partitioned across
  years of `report_date`, plant-level fuel consumption at sub-hourly
  resolution, or any other multi-TB table set that a single laptop cannot
  scan.
- **Someone else runs the cluster.** A data platform team at your
  organization already operates Spark with Spark Connect enabled — you just
  need a URI and credentials.
- **You want aggregations, not row dumps.** datasight's agent is steered
  (via the system prompt) to always aggregate, always filter on partition
  columns, and never `SELECT *` on Spark tables. The results come back as
  small summaries suitable for a chart, not multi-gigabyte exports.

If your data fits on one machine, prefer DuckDB — it will be much faster
for interactive exploration.

## Overview

Spark Connect decouples the client from the cluster. datasight (the web UI
and LLM agent) runs on your laptop; the Spark driver and executors run on
your cluster. Your laptop never materializes more than a bounded slice of
any result.

```mermaid
flowchart LR
    subgraph laptop ["Your laptop"]
        DS[datasight web UI<br>+ LLM agent]
    end
    subgraph cluster ["Spark cluster"]
        SC[Spark Connect server]
        DR[Spark driver<br>+ executors]
        DATA["Parquet / Delta / Iceberg<br>multi-TB tables"]
        SC --> DR --> DATA
    end
    DS <-->|"SQL over<br>gRPC + Arrow"| SC

    style DS fill:#15a8a8,stroke:#023d60,color:#fff
    style SC fill:#fe5d26,stroke:#c44a1e,color:#fff
    style DR fill:#2e7ebb,stroke:#1a5c8a,color:#fff
    style DATA fill:#8a7d55,stroke:#6b6040,color:#fff
    style laptop fill:#1a8a8a,stroke:#15a8a8,color:#fff
    style cluster fill:#c44a1e,stroke:#fe5d26,color:#fff
```

Queries travel as Spark logical plans over gRPC. Results stream back as
Arrow record batches. datasight stops reading the stream once the
accumulated result exceeds `SPARK_MAX_RESULT_BYTES` (default 100 MiB) and
surfaces a truncation warning in the UI and to the LLM.

## Prerequisites

- A Spark cluster running **Spark 3.5 or newer** with the Spark Connect
  server enabled.
- The **Spark Connect URI** for that cluster — typically `sc://host:15002`.
  Ask your data platform team if you don't know it.
- A **bearer token** if the cluster requires authentication.

!!! tip
    If you're not sure whether Spark Connect is enabled, ask for the URI.
    Clusters running Databricks, EMR, or self-managed Spark with the
    `spark.api.mode=connect` config have a Connect endpoint.

## Step 1: Install the Spark extra

The Spark Connect client isn't bundled by default because it pulls in
pyspark. Install the extra on your laptop:

```bash
pip install 'datasight[spark]'
```

This adds `pyspark[connect]>=3.5` to your environment. No Java is
required — the Connect client is pure Python and talks to the cluster
over gRPC.

## Step 2: Configure the connection

In your project's `.env`:

```bash
# Use any LLM provider (see Getting Started for options)
ANTHROPIC_API_KEY=sk-ant-...

DB_MODE=spark
SPARK_REMOTE=sc://spark-connect.example.com:15002
SPARK_TOKEN=your_bearer_token            # optional, only if the cluster requires auth
```

### Tuning the byte cap

datasight caps client-side result size to protect your laptop (and, if you
host the web UI for others, the web server) from OOMing when the agent
writes a query that returns too much data. The default is 100 MiB on the
wire, which inflates to roughly 250–500 MiB as a pandas DataFrame.

Raise or lower the cap to match your environment:

```bash
# Default — safe for most laptops and small web deployments
SPARK_MAX_RESULT_BYTES=104857600         # 100 MiB

# Lower for memory-constrained environments
SPARK_MAX_RESULT_BYTES=26214400          # 25 MiB

# Higher if you trust the queries and have the RAM
SPARK_MAX_RESULT_BYTES=524288000         # 500 MiB
```

When a result is truncated, the UI shows a banner and the LLM sees a
partial-result warning in its tool output — so the agent can say "showing
the first 2M rows; add aggregation for the full answer" instead of
silently misreporting truncated data.

## Step 3: Describe the schema for multi-TB scale

datasight introspects tables automatically, but for Spark it deliberately
**skips row counts** — a naive `SELECT COUNT(*)` on a partitioned
multi-TB table kicks off a full-cluster job every time the project loads.

To help the agent write cheap queries, document your partition columns
explicitly in `schema_description.md`:

```markdown
## generation_fuel

Daily net generation and fuel consumption by plant, aggregated from the
EIA-923 hourly feed. Approximately 4 billion rows.

**Partition columns:** `report_date` (daily), `energy_source_code`.
Always include a `report_date` filter — e.g. `WHERE report_date >=
'2024-01-01'` — otherwise the query scans the full history.

**Columns:**
- `plant_id` (int) — EIA plant identifier
- `report_date` (date) — partition key
- `energy_source_code` (varchar) — partition key; 'NG', 'COL', 'NUC', 'WND', etc.
- `net_generation_mwh` (double) — MWh produced in the period
- `fuel_consumed_units` (double) — physical units of fuel consumed
- `fuel_consumed_mmbtu` (double) — heat content consumed
```

Partition hints in the schema description flow into the system prompt, so
the agent learns to write `WHERE report_date >= '2024-01-01' AND
energy_source_code = 'NG'` instead of full-table scans.

## Step 4: Run datasight

```bash
datasight run
```

datasight connects to the Spark Connect server via `spark.catalog.listTables()`,
pulls column metadata for each table, and starts the web UI at
<http://localhost:8084>. Ask a question and the agent writes Spark SQL
against your tables.

### File-based commands also route through Spark

When `DB_MODE=spark` is set in your `.env`, commands that accept explicit
file arguments — `datasight inspect foo.parquet`, `datasight generate
--files`, `datasight trends --files`, and the web UI's "explore files"
flow — register the given Parquet/CSV paths as Spark temp views and run
introspection on the cluster instead of opening a local in-memory
DuckDB. Your `SPARK_REMOTE` appears in the startup log so you can
confirm Spark was used:

```text
INFO  File inspection routed through Spark Connect: sc://localhost:15002
INFO  Connected to Spark Connect: sc://localhost:15002
INFO  Spark session info: ...
```

The file paths you pass must be reachable from the Spark workers at the
same absolute path — Spark does not upload local files to the cluster.
On HPC this is usually fine because `/scratch` / `/projects` are shared;
on a laptop-only Spark-Connect setup it won't work, so use DuckDB mode
in that case.

### Verifying you're actually distributed

On startup, datasight logs the Spark session details it sees from the
Connect server. Look for a block like this in the terminal output:

```text
INFO  Spark session info:
  version                                   = 3.5.1
  spark.master                              = spark://master-node:7077
  spark.app.name                            = datasight
  spark.app.id                              = app-20260422-0001
  spark.default.parallelism                 = 208
  spark.sql.shuffle.partitions              = 200
  spark.executor.instances                  = 2
  spark.executor.cores                      = 104
  spark.executor.memory                     = 200g
  spark.dynamicAllocation.enabled           = false
```

The field to check first is **`spark.master`**:

- `spark://host:7077` — connected to a standalone cluster. ✓ distributed
- `yarn` / `k8s://...` — connected to a YARN / Kubernetes cluster. ✓
- `local` or `local[*]` — **the Connect server is running all work in
  one JVM on the driver node.** No executors on other nodes are used.
  This is almost always the reason only one node shows CPU activity.

If you see `local[*]` and you expected distribution, datasight also emits
a warning pointing at the fix:

```text
WARNING Spark master is 'local[*]' — the Connect server is running all
work in one JVM on the driver node. If you expected distributed
execution, set spark.master on the Connect server (e.g.
spark://master:7077, yarn, or k8s://...) and restart it.
```

`spark.master` is set on the **Connect server** side when it starts, not
on the datasight client. If you launched Spark with
`start-connect-server.sh --master local[104]`, that's where the setting
lives — restart the server with the correct master URL.

Also useful to check:

- `spark.executor.instances` — should be ≥ 1 for a real cluster. `None`
  or missing means dynamic allocation, in which case look at
  `spark.dynamicAllocation.minExecutors` / `maxExecutors`.
- `spark.default.parallelism` — roughly `executor_count × cores_per_executor`.
  If this number looks like a single machine's core count, you're local.

## Running Spark in the cloud

Most managed Spark services expose a Spark Connect endpoint — all
datasight needs is the `sc://…` URI and (if auth is enabled) a bearer
token.

**Databricks** — connection strings embed the PAT and target cluster
right in the URI. Datasight forwards this as-is:

```bash
DB_MODE=spark
SPARK_REMOTE=sc://<workspace>.cloud.databricks.com:443/;token=<personal-access-token>;x-databricks-cluster-id=<cluster-id>
```

**Self-managed on K8s / EKS / GKE / AKS / Dataproc / EMR** — you or
your team started a Spark Connect server on the cluster. The URI is
the service hostname and port, TLS if configured, plus optional token:

```bash
DB_MODE=spark
SPARK_REMOTE=sc://spark-connect.example.com:15002
# Or with TLS + bearer auth:
# SPARK_REMOTE=sc://spark-connect.example.com:443/;use_ssl=true
SPARK_TOKEN=<bearer-token>
```

No tunnel needed in either case — datasight connects directly to the
public endpoint. If your cluster is VPC-private, the usual options
apply: VPN, bastion tunnel, Cloud IAP, etc. — all of which terminate
at a local port that `SPARK_REMOTE=sc://localhost:<port>` points at.

## Running Spark on an HPC compute node

If your organization doesn't have a shared Spark cluster but does have
HPC, you can start a short-lived Spark cluster on Slurm-allocated
compute nodes and tunnel to it from your laptop.

The cleanest path is **[sparkctl](https://github.com/NatLabRockies/sparkctl)**
— an NLR-developed tool that orchestrates a Spark standalone cluster
across Slurm-allocated compute nodes for you. It handles the master,
workers, and Connect server in a handful of commands. Fall back to the
hand-rolled single-node variant below only if sparkctl isn't available
on your cluster.

!!! important
    **Do not run Spark on the login node.** Spark drivers and executors
    can consume significant memory and CPU. Always allocate compute
    nodes with Slurm first, then start the services on those nodes.

### Recommended: sparkctl (multi-node standalone)

sparkctl stands up a Spark standalone cluster across the compute nodes
in your Slurm allocation — one head (master + driver) and N workers —
and starts the Connect server on the head node.

**Step 1: Install sparkctl** (one-time, on a login node)

```bash
module load python
python -m venv ~/python-envs/sparkctl
source ~/python-envs/sparkctl/bin/activate
pip install sparkctl
```

See the [sparkctl install guide](https://natlabrockies.github.io/sparkctl/)
for supported Spark versions and OS notes.

**Step 2: Allocate compute nodes**

sparkctl uses the Slurm env vars to discover the allocation. This
example reserves 1 node for the master + driver + your interactive
session, and 2 full nodes for workers:

```bash
salloc -t 04:00:00 -n4 --partition=shared --mem=30G : \
       -N2 --account=<your-account> --mem=240G
```

**Step 3: Configure and start the cluster**

```bash
source ~/python-envs/sparkctl/bin/activate
sparkctl configure           # inspects the allocation, writes ./conf
sparkctl start               # starts master, workers, and Connect server

# Set the env vars sparkctl prints so subsequent commands use this cluster.
export SPARK_CONF_DIR=$(pwd)/conf
export JAVA_HOME=<path printed by sparkctl>
```

The Connect server binds to port 15002 on the head node (the box your
interactive session is on). Verify with `hostname` and pick that up as
`$HEAD_NODE` for the next step.

**Step 4: SSH tunnel from your laptop**

```bash
ssh -N -L 15002:<head-node-hostname>:15002 user@hpc-login-node
```

**Step 5: Configure datasight**

```bash
DB_MODE=spark
SPARK_REMOTE=sc://localhost:15002
```

**Step 6: When you're done**

```bash
sparkctl stop
```

Shutting down cleanly returns cluster resources to the scheduler and
avoids leaked JVMs.

See the [sparkctl Spark Connect tutorial](https://github.com/NatLabRockies/sparkctl/blob/main/docs/tutorials/run_python_spark_jobs_spark_connect.md)
for the full reference, including how to tune worker memory and
customize the Spark config before `sparkctl start`.

### Fallback: hand-rolled single-node driver (local mode)

Use this only if sparkctl isn't installed on your cluster and you need
a quick smoke test. Everything runs in one JVM on one node, so there's
no cross-node parallelism — good for datasets that fit in a single
compute node's memory, bad for real multi-TB workloads.

```mermaid
flowchart LR
    subgraph laptop ["Your laptop"]
        DS[datasight web UI<br>+ LLM agent]
    end
    subgraph login ["Login node"]
        SSH[SSH tunnel<br>passthrough only]
    end
    subgraph compute ["Compute node · Slurm-allocated"]
        SC[Spark Connect server]
        DR[Spark driver<br>+ local executors]
        PQ["/scratch parquet files"]
        SC --> DR --> PQ
    end
    DS <-->|"SQL over<br>gRPC + Arrow"| SSH <-->|port 15002| SC

    style DS fill:#15a8a8,stroke:#023d60,color:#fff
    style SSH fill:#bf1363,stroke:#8a0d42,color:#fff
    style SC fill:#fe5d26,stroke:#c44a1e,color:#fff
    style DR fill:#2e7ebb,stroke:#1a5c8a,color:#fff
    style PQ fill:#8a7d55,stroke:#6b6040,color:#fff
    style laptop fill:#1a8a8a,stroke:#15a8a8,color:#fff
    style login fill:#9e1050,stroke:#bf1363,color:#fff
    style compute fill:#c44a1e,stroke:#fe5d26,color:#fff
```

**Step A1: Allocate one compute node**

```bash
salloc --time=4:00:00 --mem=240G --cpus-per-task=104 --account <your-account>
hostname     # note the compute node hostname (e.g. compute-node-42)
```

**Step A2: Start Spark Connect on the compute node**

Spark 3.5+ ships a helper script that launches a local driver with the
Connect server enabled:

```bash
# On the compute node, inside your allocation
$SPARK_HOME/sbin/start-connect-server.sh \
    --master "local[104]" \
    --packages org.apache.spark:spark-connect_2.13:3.5.1 \
    --conf spark.driver.memory=200g \
    --conf spark.sql.execution.arrow.pyspark.enabled=true
```

- `--master local[104]` runs driver and executors in one JVM using all
  allocated cores (tune to `--cpus-per-task`).
- `--packages` pulls the Connect server jar matching your Spark version.
  Some Spark distributions bundle it already — if so, omit this flag.
- `--conf spark.driver.memory=...` must fit inside your Slurm `--mem`
  allocation with headroom for the Python gRPC process and OS cache.

The script binds the Connect gRPC server to `0.0.0.0:15002` by default.
Override with `--conf spark.connect.grpc.binding.port=<port>` if 15002
is taken. Check the driver log — it prints the exact bound address.

**Step A3: SSH tunnel from your laptop**

```bash
ssh -N -L 15002:<compute-node-hostname>:15002 user@hpc-login-node
```

Leave this running in a separate terminal.

**Step A4: Configure datasight**

```bash
DB_MODE=spark
SPARK_REMOTE=sc://localhost:15002
# No SPARK_TOKEN needed — the SSH tunnel already authenticates you
```

### HPC gotchas

- **Use the compute node hostname in the `-L` target, not the login
  node.** Spark is on the compute node; the login node is a passthrough.
  If you tunnel only to the login node, you'll hit `connection refused`.
- **Watch for port collisions.** 15002 is the Spark Connect default;
  if a previous job on the same node is still bound to it, startup
  will fail. Either pick a different port (`spark.connect.grpc.binding.port`)
  or kill the old job.
- **The tunnel dies when the allocation ends.** Request generous
  `--time`, or use `salloc` so you can extend interactively.
- **Package resolution requires login-node internet.** The first
  `--packages` invocation downloads jars to `~/.ivy2`. If your compute
  nodes lack outbound network, pre-resolve on the login node first, or
  install the Connect jar into `$SPARK_HOME/jars`.
- **Shared filesystem paths must match on every node** — workers
  executing the query need the parquet data at the exact same path the
  query references. sparkctl handles this for you; if you're hand-rolling,
  prefer `/scratch` or `/projects` (identically mounted on every node)
  over per-user paths.

## Tips

- **Let the agent aggregate.** Questions like "total wind generation by
  month in 2024" return a tiny result (12 rows) even though the underlying
  table is terabytes. Questions like "show me every generation record in
  2024" will hit the byte cap — the agent will be told, and should
  re-plan as an aggregation.
- **Document partition columns** explicitly in `schema_description.md`.
  This is the single highest-leverage thing you can do to make Spark
  queries fast.
- **Watch for truncation banners** in the UI. If they appear on
  aggregated queries, your aggregation isn't grouping enough — the result
  is still too wide. Either narrow the time range or bucket more
  aggressively.
- **Server-side cancellation works.** If a query hits the `LLM_TIMEOUT`
  or you kill the session, datasight calls Spark Connect's `interruptTag`
  API so the cluster stops executing the job — cluster resources are
  freed, not left running in the background.
- **Keep credentials out of git.** `.env` should be in `.gitignore`
  (datasight adds it by default).
