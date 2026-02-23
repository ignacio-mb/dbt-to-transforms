# dbt-to-metabase
> [!WARNING]
> **Do not use this in production without extensive testing.**
> **Back up your Metabase application database before using this tool.**

Migrate dbt models into **Metabase Transforms** — a new Metabase feature that lets you write query results back to your database and reuse them as data sources.

This tool reads your dbt project from GitHub, rewrites the Jinja-templated SQL into plain SQL, resolves the dependency graph, and creates Metabase transforms (with tags and scheduled jobs) via the Metabase API.

## Features

- **GitHub-native** — reads your dbt project directly from a GitHub repository (no local clone needed; supports monorepos)
- **Full dependency resolution** — topologically sorts models so transforms are created in the correct order, and Metabase's built-in dependency tracking takes over at runtime
- **SQL rewriting** — converts `{{ ref() }}`, `{{ source() }}`, `{{ var() }}`, `{{ config() }}`, and `{% if is_incremental() %}` blocks into Metabase-compatible plain SQL
- **Seed resolution** — discovers dbt seeds from your `seeds/` directory so `ref('my_seed')` resolves correctly without creating unnecessary transforms
- **Graceful macro fallback** — unsupported Jinja expressions (custom macros, `dbt_utils`) are replaced with `NULL` to keep SQL valid; when that produces structurally broken SQL, the tool falls back to a passthrough `SELECT * FROM schema.table` that reads from the table dbt already created
- **Incremental support** — maps dbt `incremental` materialization to Metabase's incremental transforms via the `PUT ee/transform/{id}` API with `source-incremental-strategy`
- **Folder structure preservation** — mirrors your dbt `models/` directory hierarchy as Metabase folders
- **Schema mapping** — configurable rules to route dbt model folders to specific Metabase target schemas
- **Tag and job creation** — maps dbt tags to Metabase transform tags and creates scheduled jobs
- **Remote sync integration** — triggers Metabase's remote sync to serialize transforms to git
- **Dry-run and plan export** — preview the full migration plan as JSON before executing
- **Conflict handling** — configurable behavior (skip / replace / error) when transforms already exist

## Installation
```bash
pip install .
# or for development:
pip install -e ".[dev]"
```

## Quick start
```bash
# 1. Copy and edit the config
cp config.example.yaml config.yaml
# Edit config.yaml with your GitHub repo, Metabase URL, credentials, etc.

# 2. Validate everything connects
dbt-to-metabase validate --config config.yaml

# 3. Preview the migration plan
dbt-to-metabase plan --config config.yaml --output plan.json

# 4. Run the migration
dbt-to-metabase migrate --config config.yaml

# 4.1 Run the migraiton and validate the data after migration
dbt-to-metabase migrate --config config.yaml --validate

# 5. Remap content to use transforms tables instead
dbt-to-metabase remap --config config.yaml

# Or do a dry run first:
dbt-to-metabase migrate --config config.yaml --dry-run --output plan.json


# optional: after making a change to a file, run
pip install . --force-reinstall --no-deps
dbt-to-metabase migrate --config config.yaml
```

## Configuration

See [`config.example.yaml`](config.example.yaml) for the full annotated configuration. Key sections:

### GitHub connection
```yaml
github:
  repo: "your-org/your-dbt-repo"
  branch: "main"
  subdirectory: "dbt"  # for monorepos
  # token via env var GITHUB_TOKEN
```

### Metabase connection
```yaml
metabase:
  url: "https://metabase.example.com"
  database_id: 1
  default_schema: "analytics"
  # api_key via env var METABASE_API_KEY
```

### Schema mappings

Route dbt folders to Metabase schemas. A `transform_schema_prefix` (default: `"transforms_"`) is automatically prepended to keep transform tables separate from the original dbt-produced tables:

```yaml
transform_schema_prefix: "transforms_"   # default; set to "" to disable

schema_mappings:
  - dbt_folder_pattern: "staging/*"
    metabase_schema: "staging"       # → transforms_staging
  - dbt_folder_pattern: "marts/finance"
    metabase_schema: "marts_finance" # → transforms_marts_finance
```

With the default prefix, the resulting Postgres schemas are:

| dbt schema | Transform schema |
|---|---|
| `staging` | `transforms_staging` |
| `intermediate` | `transforms_intermediate` |
| `marts` | `transforms_marts` |

> **Seeds are not prefixed.** Seeds are pre-existing raw tables (created by `dbt seed`) and are never recreated as transforms. When a transform SQL references a seed via `ref('my_seed')`, it resolves to the seed's original schema (e.g. `raw.payment_methods`), not `transforms_raw.payment_methods`. This ensures transforms can read from seed data without requiring a copy.

### Tag mappings and jobs
```yaml
tag_mappings:
  - dbt_tag: "nightly"
    metabase_tag: "daily"

default_tags: ["daily"]

jobs:
  - name: "Nightly transforms"
    schedule: "0 0 * * *"
    tags: ["daily"]
```

### Model filters
```yaml
include_models: ["stg_*", "fct_*", "dim_*"]
exclude_models: ["*_tmp", "*_deprecated"]
```

## How it works
```
┌─────────────┐    ┌──────────────┐    ┌────────────────┐    ┌──────────────┐
│   GitHub     │    │  dbt Parser  │    │  SQL Rewriter  │    │  Metabase    │
│  dbt repo    │───▶│  + Resolver  │───▶│  (Jinja→SQL)   │───▶│  API Client  │
└─────────────┘    └──────────────┘    └────────────────┘    └──────────────┘
                          │                                          │
                   ┌──────┴──────┐                          ┌───────┴───────┐
                   │ Dependency  │                          │ Creates:      │
                   │ DAG / topo  │                          │ - Transforms  │
                   │ sort        │                          │ - Tags        │
                   └─────────────┘                          │ - Jobs        │
                                                            │ - Folders     │
                                                            └───────────────┘
```

### SQL rewriting rules

| dbt construct | Metabase output |
|---|---|
| `{{ ref('stg_orders') }}` | `analytics.stg_orders` (schema resolved from config) |
| `{{ ref('payment_methods') }}` (seed) | `raw.payment_methods` (schema resolved from seed config) |
| `{{ source('raw', 'orders') }}` | `raw_data.orders` (schema from sources.yml) |
| `{{ config(...) }}` | Stripped entirely |
| `{{ var('name') }}` | Literal value from `dbt_project.yml` vars |
| `{# comment #}` | Stripped |
| `{% if is_incremental() %} WHERE col > ... {% endif %}` | `[[WHERE col > {{checkpoint}}::timestamp]]` |
| `{{ custom_macro(...) }}` | Replaced with `NULL`; falls back to passthrough if SQL breaks |
| `{{ dbt_utils.date_spine(...) }}` | Falls back to `SELECT * FROM schema.model` (untranslatable) |

### Materialization mapping

| dbt materialization | Metabase behavior |
|---|---|
| `table` | Standard transform (DROP + CREATE on each run) |
| `view` | Converted to table transform (with warning) |
| `incremental` | Incremental transform via `source-incremental-strategy` API |
| `ephemeral` | Materialized as a table transform (with warning). In dbt these are inlined CTEs, but Metabase requires physical tables so they are persisted. |
| `seed` | **Not a transform.** Seeds are pre-existing raw tables; they are registered so `ref('seed_name')` resolves to the seed's original schema (e.g. `raw.seed_name`), but no transform is created. |

## Remote sync

When `remote_sync.enabled: true`, the tool triggers Metabase's serialization export after migration. This pushes transform definitions as YAML files to your configured GitHub repo, enabling:

- Version-controlled transform definitions
- Code review workflows for transform changes
- Environment promotion (dev → staging → prod)

To set up remote sync in Metabase: Admin Settings → General → Remote Sync → toggle "Transforms" on.

## CLI reference
```
dbt-to-metabase migrate  -c config.yaml [--dry-run] [-o plan.json]
dbt-to-metabase plan     -c config.yaml [-o plan.json] [--stdout]
dbt-to-metabase validate -c config.yaml
```

---

## dbt Cloud Runner vs. Metabase Jobs & Runs: A Comparison

When migrating from dbt to Metabase Transforms, it's worth understanding how the scheduling and execution models compare.

### Scheduling model

| Aspect | dbt Cloud | Metabase Transforms |
|---|---|---|
| **Scheduler** | dbt Cloud Jobs with cron or interval schedules | Metabase Jobs with cron schedules |
| **Grouping** | Jobs run a set of dbt commands (e.g. `dbt run --select tag:nightly`) | Jobs run all transforms that match one or more **tags** |
| **Granularity** | Can run individual models, selectors, tags, or full project | Tag-based: all transforms with a given tag run together |
| **Model selection** | Rich selector syntax: `dbt run --select stg_orders+`, `tag:nightly`, `path:marts/` | Tags only. No graph selectors — but Metabase auto-resolves dependencies |

### Dependency handling

| Aspect | dbt Cloud | Metabase Transforms |
|---|---|---|
| **DAG awareness** | Full. dbt resolves the DAG and runs models in order within a job | Full. Metabase tracks transform dependencies and runs them in order |
| **Cross-job deps** | Manual. You chain jobs using "Run after" triggers or API orchestration | **Automatic**. If Transform B depends on A, any job that runs B will also run A — even if A isn't tagged for that job |
| **Failure handling** | Configurable: fail-fast or continue. Downstream models skip on upstream failure | Run-level: a failed transform stops dependent transforms in the same job run |

### Execution environment

| Aspect | dbt Cloud | Metabase Transforms |
|---|---|---|
| **SQL execution** | dbt compiles Jinja → SQL, sends to warehouse | Metabase sends plain SQL to warehouse (no Jinja compilation) |
| **Python support** | dbt Python models (via dbt-snowflake, dbt-bigquery, etc.) | Metabase Python transforms (dedicated execution environment) |
| **Compute** | Runs on dbt Cloud infrastructure or self-hosted runners | SQL runs on your database; Python runs in Metabase's environment |
| **Incremental** | Rich incremental strategies (append, merge, delete+insert, microbatch) | Append-only incremental via checkpoint column |

### Observability

| Aspect | dbt Cloud | Metabase Transforms |
|---|---|---|
| **Run history** | Full run history with model-level timing, logs, and artifacts | Run history per transform with status, duration, and error logs |
| **Lineage** | dbt Docs lineage graph, dbt Explorer | Transform dependency graph (Pro/Enterprise) in Data Studio |
| **Alerts** | Slack/email notifications on failure, webhooks | Visible in Runs view; alerting depends on Metabase plan |
| **Freshness** | Source freshness checks (`dbt source freshness`) | No equivalent — freshness is implied by transform run schedule |

### Key differences summary

**dbt Cloud** is purpose-built for data transformation orchestration with sophisticated features like model selectors, incremental strategies (merge, delete+insert), source freshness, and environment management. It's the right tool for complex transformation pipelines.

**Metabase Transforms** are designed to bring lightweight transformation directly into the BI layer. The killer feature is that **transforms are first-class citizens in Metabase** — the output tables are immediately available for questions, dashboards, and other transforms without any external tooling. The dependency auto-resolution across jobs is also more forgiving than dbt Cloud's manual job chaining.

**When to keep dbt Cloud alongside Metabase Transforms**: If you have complex incremental strategies (merge, snapshot), heavy use of dbt macros/packages, or need dbt's testing framework. You can use dbt for the heavy ETL and Metabase Transforms for "last-mile" transformations closer to the BI layer.

**When Metabase Transforms can replace dbt**: If your dbt project is primarily `SELECT`-based transformations (staging, marts) without complex Jinja macros, and you want a simpler operational model where your BI tool owns the full data-to-dashboard pipeline.

## Utilities

### Wiping transforms

To delete all transforms from both Metabase and the database before re-running a migration:
```bash
chmod +x scripts/wipe-transforms.sh
./scripts/wipe-transforms.sh
```

The script removes transforms via the Metabase API, drops the corresponding tables from Postgres (scanning `transforms_staging`, `transforms_intermediate`, `transforms_marts`, `staging`, `intermediate`, and `marts` schemas), and triggers a Metabase database sync. Configure with environment variables:
```bash
METABASE_URL=http://localhost:3000 \
DB_HOST=localhost DB_PORT=5433 \
./scripts/wipe-transforms.sh
```

## Limitations

- **Jinja macros**: Custom macros, `dbt_utils` macros, and complex Jinja logic cannot be automatically translated. Unsupported expressions are replaced with `NULL`. If that makes the SQL structurally invalid (e.g. a CTE body that becomes `NULL`), the tool generates a passthrough `SELECT *` from the dbt-created table and logs a warning. These passthrough transforms work correctly but won't reflect future dbt logic changes until manually updated in Metabase.
- **Incremental strategies**: dbt supports merge, delete+insert, and microbatch. Metabase only supports append-style incremental via `source-incremental-strategy`. Models with `incremental_strategy: merge` need manual review.
- **Tests**: dbt tests are not migrated. Consider using Metabase's data quality features or keeping dbt tests running separately.
- **Snapshots**: dbt snapshots (SCD Type 2) have no Metabase equivalent and are excluded.
- **Seeds**: Seeds are registered for `ref()` resolution but are not created as transforms (they already exist as database tables from `dbt seed`). If your seeds target a custom schema, configure it in `dbt_project.yml` under the `seeds:` key.
- **Cross-database**: Metabase transforms write to the same database they read from. Cross-database refs won't work.

## Development
```bash
pip install -e ".[dev]"
pytest tests/ -v
```
