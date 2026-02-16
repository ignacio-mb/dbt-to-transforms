# dbt-to-metabase

Migrate dbt models into **Metabase Transforms** — a new Metabase feature that lets you write query results back to your database and reuse them as data sources.

This tool reads your dbt project from GitHub, rewrites the Jinja-templated SQL into plain SQL, resolves the dependency graph, and creates Metabase transforms (with tags and scheduled jobs) via the Metabase API.

## Features

- **GitHub-native** — reads your dbt project directly from a GitHub repository (no local clone needed; supports monorepos)
- **Full dependency resolution** — topologically sorts models so transforms are created in the correct order, and Metabase's built-in dependency tracking takes over at runtime
- **SQL rewriting** — converts `{{ ref() }}`, `{{ source() }}`, `{{ var() }}`, `{{ config() }}`, and `{% if is_incremental() %}` blocks into Metabase-compatible plain SQL
- **Incremental support** — maps dbt `incremental` materialization to Metabase's incremental transforms with `{{checkpoint}}` syntax
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

Route dbt folders to Metabase schemas:

```yaml
schema_mappings:
  - dbt_folder_pattern: "staging/*"
    metabase_schema: "staging"
  - dbt_folder_pattern: "marts/finance"
    metabase_schema: "marts_finance"
```

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
| `{{ source('raw', 'orders') }}` | `raw_data.orders` (schema from sources.yml) |
| `{{ config(...) }}` | Stripped entirely |
| `{{ var('name') }}` | Literal value from `dbt_project.yml` vars |
| `{# comment #}` | Stripped |
| `{% if is_incremental() %} WHERE col > ... {% endif %}` | `[[WHERE col > {{checkpoint}}::timestamp]]` |

### Materialization mapping

| dbt materialization | Metabase behavior |
|---|---|
| `table` | Standard transform (DROP + CREATE on each run) |
| `view` | Converted to table transform (with warning) |
| `incremental` | Incremental transform with `{{checkpoint}}` |
| `ephemeral` | Skipped (these are CTEs inlined by dbt, not persisted) |

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

## Limitations

- **Jinja macros**: Custom macros, `dbt_utils` macros, and complex Jinja logic cannot be automatically converted. The tool strips unrecognized Jinja and logs warnings.
- **Incremental strategies**: dbt supports merge, delete+insert, and microbatch. Metabase only supports append-style incremental. Models with `incremental_strategy: merge` need manual review.
- **Tests**: dbt tests are not migrated. Consider using Metabase's data quality features or keeping dbt tests running separately.
- **Snapshots**: dbt snapshots (SCD Type 2) have no Metabase equivalent and are excluded.
- **Seeds**: dbt seeds are not migrated. Upload them as tables directly.
- **Cross-database**: Metabase transforms write to the same database they read from. Cross-database refs won't work.

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## License

MIT
