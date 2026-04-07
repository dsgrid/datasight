# Database connections

datasight supports four database backends. This guide explains when to use
each one and how to configure the connection.

## Choosing a database

| | DuckDB | SQLite | PostgreSQL | Flight SQL |
|---|---|---|---|---|
| **Best for** | Local analytics on Parquet/CSV files | Existing SQLite databases from other apps | Production databases, multi-user access | Remote HPC or distributed query engines |
| **Install** | Built in | Built in | Built in | Built in |
| **DB_MODE** | `duckdb` | `sqlite` | `postgres` | `flightsql` |
| **Connection** | Local file path | Local file path | Host/port or connection string | gRPC URI |
| **Concurrent users** | Single process | Single process | Multi-user | Multi-user |
| **SQL dialect** | DuckDB SQL (Postgres-like) | SQLite SQL | PostgreSQL | Depends on server |

**DuckDB** is the default and recommended for most use cases. It is an
embedded OLAP (Online Analytical Processing) database designed for
analytical queries — aggregations, joins, window functions, and scans
over large datasets are significantly faster than in SQLite or a
typical PostgreSQL configuration. It reads Parquet and CSV files
natively and requires no external server.

**SQLite** is useful when you already have a `.sqlite` or `.db` file from
another application (Django, mobile apps, embedded systems) and want to
explore it without converting to another format.

**PostgreSQL** is for connecting to an existing Postgres server — production
databases, data warehouses, or managed services like RDS or Cloud SQL.

**Flight SQL** is for remote query engines that speak the Arrow Flight SQL
protocol, such as [GizmoSQL](https://github.com/gizmodata/gizmosql) on an
HPC cluster. See [Connect to a remote HPC](../end-user/remote-hpc.md) for a
full walkthrough.

## DuckDB

DuckDB is the default — no extra install or configuration needed beyond
pointing to a database file.

```bash
DB_MODE=duckdb
DB_PATH=./my_database.duckdb
```

### Why DuckDB for data exploration

DuckDB is purpose-built for OLAP workloads — the kind of analytical
queries that datasight generates (aggregations, GROUP BY, joins across
large tables, window functions). Compared to row-oriented databases like
SQLite and PostgreSQL:

- **Columnar storage** — reads only the columns a query needs, so
  `SELECT state, SUM(mwh) FROM generation GROUP BY state` scans far less
  data than a row store would.
- **Vectorized execution** — processes data in batches using SIMD
  instructions, making aggregations and scans significantly faster.
- **Zero configuration** — no server process, no connection management,
  no tuning. Just a file.
- **Native file format support** — queries Parquet, CSV, and JSON files
  directly via SQL without an import step.
- **Rich SQL dialect** — supports `DATE_TRUNC`, `UNNEST`, `PIVOT`,
  window functions, CTEs, and other analytical SQL features out of the
  box.

For datasets up to tens of gigabytes on a single machine, DuckDB will
typically outperform PostgreSQL for analytical queries without any tuning.

### Querying files directly

DuckDB can query Parquet and CSV files directly using SQL — no import
step, no data duplication. Create lightweight views that point at your
files and datasight treats them like regular tables.

See [Query CSV and Parquet files](querying-files.md) for a full guide
covering globs, Hive partitioning, and tips.

## SQLite

SQLite support is built in — no extra install needed.

```bash
DB_MODE=sqlite
DB_PATH=./my_database.sqlite
```

### Limitations compared to DuckDB

- No `DATE_TRUNC`, `EXTRACT`, or other advanced date functions — SQLite
  stores dates as text, so date-based queries may need `strftime()` instead.
- No window function support before SQLite 3.25.
- No native Parquet or CSV reading.

!!! tip
    If your schema description mentions date columns, note the storage
    format (e.g. "ISO 8601 text") and any preferred date functions. This
    helps the AI write correct date queries for SQLite.

## PostgreSQL

### Connect with individual fields

```bash
DB_MODE=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=mydb
POSTGRES_USER=datasight
POSTGRES_PASSWORD=secret
```

### Connect with a URL

A connection string takes precedence over individual fields:

```bash
DB_MODE=postgres
POSTGRES_URL=postgresql://datasight:secret@localhost:5432/mydb
```

### SSL configuration

| `POSTGRES_SSLMODE` | Behavior |
|---|---|
| `disable` | No SSL |
| `prefer` (default) | Use SSL if available, fall back to plain |
| `require` | Require SSL, don't verify certificate |
| `verify-ca` | Require SSL, verify the server certificate is signed by a trusted CA |
| `verify-full` | Require SSL, verify CA and that the server hostname matches the certificate |

!!! warning
    For production databases, use `POSTGRES_SSLMODE=verify-full`. The
    default `prefer` mode does not protect against man-in-the-middle attacks.

### Managed PostgreSQL services

For AWS RDS, Google Cloud SQL, Azure Database, and similar managed services,
you typically need:

- The connection hostname from your provider's console
- SSL mode set to `require` or `verify-full`
- The CA certificate bundle (usually provided by the service)

```bash
DB_MODE=postgres
POSTGRES_URL=postgresql://datasight:secret@mydb.abc123.us-east-1.rds.amazonaws.com:5432/mydb
POSTGRES_SSLMODE=verify-full
```

### Read-only access

datasight only runs `SELECT` queries — it never writes to your database.
For production safety, connect with a read-only database user:

```sql
CREATE USER datasight WITH PASSWORD 'secret';
GRANT CONNECT ON DATABASE mydb TO datasight;
GRANT USAGE ON SCHEMA public TO datasight;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datasight;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datasight;
```

## Flight SQL

Flight SQL connects to remote query engines over gRPC. See
[Connect to a remote HPC](../end-user/remote-hpc.md) for a detailed guide
using GizmoSQL.

```bash
DB_MODE=flightsql
FLIGHT_SQL_URI=grpc://localhost:31337
FLIGHT_SQL_USERNAME=gizmosql_user
FLIGHT_SQL_PASSWORD=your_password
```

For TLS-enabled servers, use `grpc+tls://` as the URI scheme:

```bash
FLIGHT_SQL_URI=grpc+tls://flight.example.com:31337
FLIGHT_SQL_TOKEN=your_bearer_token
```

## All environment variables

See the [Configuration reference](../reference/configuration.md) for a
complete list of database-related environment variables.
