# dbt-to-metabase

Migrate [dbt](https://www.getdbt.com/) models into [Metabase Transforms](https://www.metabase.com/docs/latest/data-studio/transforms) automatically.

This tool reads your dbt project, compiles it with `dbt compile`, and creates Metabase transforms from the compiled SQL — complete with dependency ordering, schema mapping, tagging, and job scheduling.

## How it works

```
dbt project ──► dbt compile ──► manifest.json ──► schema remapping ──► Metabase Transforms API
```

The tool leverages **dbt's own compiler** to resolve all Jinja templating (`ref()`, `source()`, `var()`, macros, conditionals, packages like `dbt_utils`, etc.) into plain SQL. This means every dbt feature works out of the box — no need for regex-based rewriting or manual macro translation.

### What gets migrated (and what doesn't)

| dbt concept | What happens |
|---|---|
| **Models** (table, view, incremental, ephemeral) | Converted to Metabase transforms |
| **Sources** | Referenced as-is in compiled SQL (no transform created) |
| **Seeds** | Referenced as-is in compiled SQL (no transform created) |
| **Snapshots** (SCD2) | Registered in dependency graph but **not migrated** — dbt must keep running `dbt snapshot` |

## Requirements

- **Python** >= 3.9
- **dbt-core** >= 1.4 (with your database adapter, e.g. `dbt-postgres`)
- **git** (if cloning from GitHub)
- **Metabase** with Transforms support (Metabase 50+)

A **live database connection** is required for `dbt compile` to resolve introspective queries. You can provide credentials inline in the config or point to an existing `profiles.yml`.

## Installation

```bash
pip install .
```

For development:

```bash
pip install -e ".[dev]"
```

## Quick start

1. Copy the example config:

```bash
cp config.example.yaml config.yaml
```

2. Edit `config.yaml` with your dbt project source, database credentials, and Metabase connection.

3. Dry run to preview the migration plan:

```bash
dbt-to-metabase plan --config config.yaml --stdout
```

4. Execute the migration:

```bash
dbt-to-metabase migrate --config config.yaml
```

## Configuration

The config file has three main sections: **dbt project source**, **Metabase connection**, and **migration settings**.

### dbt project source

Three input modes are supported:

#### A) Local dbt project

```yaml
dbt:
  project_path: "/path/to/your/dbt/project"
  target: "prod"
  profiles_dir: "~/.dbt"          # OR provide inline credentials below
```

#### B) GitHub repository (cloned automatically)

```yaml
dbt:
  github_repo: "your-org/your-dbt-repo"
  github_branch: "main"
  # github_token: "ghp_..."       # Or set GITHUB_TOKEN env var
  # github_subdirectory: ""        # For monorepos
  target: "prod"
  db_type: "postgres"
  db_host: "localhost"
  db_port: 5432
  db_user: "analytics"
  db_name: "warehouse"
  db_schema: "public"
```

#### C) Pre-built manifest (skip compilation)

If you already run `dbt compile` in CI/CD, pass the manifest directly:

```yaml
dbt:
  manifest_path: "/path/to/target/manifest.json"
```

Or via CLI:

```bash
dbt-to-metabase migrate --config config.yaml --manifest-path ./target/manifest.json
```

### Database credentials

`dbt compile` requires a live database connection. You can provide credentials in two ways:

**Inline credentials** (generates `profiles.yml` automatically):

```yaml
dbt:
  db_type: "postgres"
  db_host: "localhost"
  db_port: 5432
  db_user: "analytics"
  # db_password: ""              # Or set DBT_DB_PASSWORD env var
  db_name: "warehouse"
  db_schema: "public"
```

**Existing profiles.yml**:

```yaml
dbt:
  profiles_dir: "~/.dbt"
  target: "prod"
```

### Schema remapping

dbt's compiled SQL contains literal schema references (e.g. `staging.stg_orders`). To avoid Metabase transforms overwriting your original dbt tables, you remap schemas to a `transforms_*` namespace:

```yaml
transform_schema_prefix: "transforms_"

schema_remap:
  staging: transforms_staging
  intermediate: transforms_intermediate
  marts: transforms_marts
  analytics: transforms_analytics
```

This rewrites `SELECT * FROM staging.stg_orders` to `SELECT * FROM transforms_staging.stg_orders` in the generated transform queries.

> **Note:** Do not add your snapshot schema (e.g. `dbt_models_snapshot`) or your database name to `schema_remap`. Snapshot tables are managed by dbt and should be referenced by their original schema. The database name is not a schema — adding it causes three-part references like `"database"."schema"."table"` to be incorrectly rewritten.

### Checkpoint columns (incremental transforms)

Metabase [incremental query transforms](https://www.metabase.com/docs/latest/data-studio/transforms/query-transforms#incremental-query-transforms) use a **checkpoint column** to track which rows have already been processed. On each run, Metabase only processes rows where the checkpoint column value is greater than the last-seen value — similar to dbt's incremental materialization.

Since `dbt compile` evaluates `is_incremental()` as `False` (the model doesn't exist yet at compile time), the incremental block is compiled away and the checkpoint column cannot be auto-detected. You must declare these mappings explicitly:

```yaml
checkpoint_columns:
  - model: "fct_orders"
    column: "updated_at"
  - model: "fct_events"
    column: "event_timestamp"
```

**How it works:** Transforms are initially created as non-incremental and run once to bootstrap the target table. After the first successful run, you must set the checkpoint column manually in the Metabase UI (Data Studio → Transforms → *transform name* → Settings → "Column to check for new values"). The Metabase API for incremental strategy is currently unstable and may change — setting it via the UI is the reliable path.

### dbt snapshots (SCD2 tables)

dbt [snapshots](https://docs.getdbt.com/docs/build/snapshots) are SCD2 (Slowly Changing Dimension Type 2) tables that track row-level changes over time. They add `dbt_valid_from`, `dbt_valid_to`, and `dbt_scd_id` columns to capture historical state.

**Metabase transforms cannot replicate SCD2 logic.** Transforms either overwrite the entire table (full refresh) or append new rows via a checkpoint column. There is no merge/upsert capability needed for change tracking.

This tool handles snapshots by registering them in the dependency graph but **skipping them during transform creation**. They appear in the migration plan under `skipped_models` with the reason "Snapshot (SCD2) — managed by dbt, not migrated". Downstream models that `ref()` a snapshot will still work — the compiled SQL references the snapshot table directly (e.g. `"analytics"."dbt_models_snapshot"."snapshot_account"`), and the Metabase transform reads from that existing table.

**Important: you still need `dbt snapshot` running on a schedule.** Snapshots are not migrated to Metabase — dbt continues to own them. If you stop running `dbt snapshot`, the snapshot tables will go stale and downstream transforms will read outdated data.

A typical setup after migration:

```
dbt snapshot (cron)  ──► snapshot tables (SCD2, managed by dbt)
                              │
                              ▼
Metabase transforms  ──► read from snapshot tables via SQL
```

**Config note:** do not add your snapshot schema (e.g. `dbt_models_snapshot`) to `schema_remap`. That schema should remain untouched so transforms read from the same tables that dbt writes to.

If your project uses snapshots and you want to eventually remove dbt entirely, you have a few options:

- **Metabase Python transforms** — write the SCD2 merge logic manually in a Python transform
- **Freeze the history** — stop `dbt snapshot`, keep existing tables as-is, history stops accumulating
- **Minimal dbt** — strip your dbt project down to just the `snapshots/` folder and a cron job
- **Upstream CDC** — move change tracking to your ETL tool (Fivetran, Airbyte history mode) so snapshots become unnecessary

### Metabase connection

```yaml
metabase:
  url: "https://metabase.example.com"
  api_key: "mb_..."                # Or set METABASE_API_KEY env var
  # username: "admin@example.com"  # Or set METABASE_USERNAME env var
  # password: "..."                # Or set METABASE_PASSWORD env var
  database_id: 1
  default_schema: "analytics"
```

### Full config reference

See [`config.example.yaml`](config.example.yaml) for a complete annotated configuration file.

## Commands

### `migrate`

Run the full migration pipeline:

```bash
dbt-to-metabase migrate --config config.yaml
```

Options:
- `--dry-run` — generate plan without executing
- `--output plan.json` — export plan to JSON
- `--validate` — run data validation after transforms complete
- `--manifest-path PATH` — use a pre-compiled manifest.json
- `--dbt-project-path PATH` — override dbt project location
- `--dbt-target NAME` — override dbt target

### `plan`

Generate a migration plan without executing it:

```bash
dbt-to-metabase plan --config config.yaml --stdout
```

### `check`

Validate configuration and connectivity:

```bash
dbt-to-metabase check --config config.yaml
```

This verifies that dbt can compile successfully, dependencies resolve, and Metabase is reachable.

### `validate`

Compare transform output tables against the original dbt tables:

```bash
dbt-to-metabase validate --config config.yaml
```

### `remap`

Remap existing Metabase cards/dashboards from dbt tables to transform tables:

```bash
dbt-to-metabase remap --config config.yaml
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    DbtCompiler                           │
│  git clone (or local path) → dbt deps → dbt compile     │
│  Output: target/manifest.json                           │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│                  ManifestParser                          │
│  Read manifest.json → DbtProject with compiled SQL       │
│  (models, sources, seeds, snapshots, dependencies)      │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│              TransformSqlAdapter                         │
│  Remap schema references in compiled SQL                 │
│  staging.orders → transforms_staging.orders              │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│                    Migrator                              │
│  Dependency ordering → Metabase API calls                │
│  Create transforms, tags, jobs, run in order             │
└─────────────────────────────────────────────────────────┘
```

## Environment variables

| Variable | Description |
|---|---|
| `GITHUB_TOKEN` | GitHub personal access token for private repos |
| `DBT_DB_PASSWORD` | Database password for dbt compile |
| `DBT_DB_USER` | Database user for dbt compile |
| `DBT_DB_HOST` | Database host for dbt compile |
| `DBT_DB_NAME` | Database name for dbt compile |
| `METABASE_API_KEY` | Metabase API key |
| `METABASE_USERNAME` | Metabase username |
| `METABASE_PASSWORD` | Metabase password |

## Migrating from v0.1

v0.1 used the `github:` config key and a regex-based SQL rewriter. v0.2 uses `dbt compile` for accurate SQL generation.

**Config changes:**

```yaml
# v0.1 (still supported with deprecation warning)
github:
  repo: "org/repo"
  branch: "main"

# v0.2 (recommended)
dbt:
  github_repo: "org/repo"
  github_branch: "main"
  target: "prod"
  db_host: "localhost"
  db_user: "analytics"
  db_name: "warehouse"
```

**New requirements:**
- `dbt-core` must be installed (`pip install dbt-postgres` or your adapter)
- `git` must be available if cloning from GitHub
- Database credentials are needed for `dbt compile`
- Checkpoint columns must be declared explicitly in config (no longer auto-detected)
- **`dbt snapshot` must continue running** if your project uses dbt snapshots — they are not migrated to Metabase transforms

**What you can remove:**
- No more `sql_rewriter` warnings about unsupported Jinja
- No more `NULL` replacements for unrecognized macros
- No more passthrough queries for complex models

## Example project

For a working example, see [dbt-metabase-postgres-example](https://github.com/ignacio-mb/dbt-metabase-postgres-example) which includes a dbt project, Postgres setup, and Docker Compose for local testing.

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Lint
ruff check .
```