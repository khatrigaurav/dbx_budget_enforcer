# Budget Enforcer — Databricks Asset Bundle

Automated workspace budget enforcement that monitors monthly spend and takes protective action when a configurable threshold is exceeded.

Repo Structure : 
 - Budget Enforcer.py : Notebook to run directly
 - dab-budget-enforcer/ : Folder to be used to deploy the DAB directly. This enables a Databricks Jobs with a Budget Enforcer and Budget Resumer Job.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Budget Enforcer Job                        │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────┐  │
│  │ CheckBudget  │───>│ IsOverBudget │───>│  StopActivity  │  │
│  │   (SQL)      │    │ (Condition)  │    │  (Notebook)    │  │
│  └──────────────┘    └──────────────┘    └───────────────┘  │
│                             │                                │
│                        if false:                             │
│                      job completes                           │
│                      (no action)                             │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                    Budget Resumer Job                         │
│                    (manual trigger)                           │
│                                                              │
│               ┌─────────────────────┐                        │
│               │   ResumeActivity    │                        │
│               │    (Notebook)       │                        │
│               └─────────────────────┘                        │
└──────────────────────────────────────────────────────────────┘
```

## Usage Options

### Option A: Databricks Asset Bundle (Recommended)

Deploy as two automated jobs using `databricks bundle deploy`. See [Deployment](#deployment) below.

### Option B: Single Notebook (Manual)

For ad-hoc use without jobs or DABs, run `Budget Enforcer.py` directly in a Databricks notebook. It contains the full flow in one file:
1. Budget check SQL query
2. Stop all resources (jobs, clusters, warehouses, apps)
3. Resume logic

Located at: `BudgetEnforcer/Budget Enforcer.py`

## How It Works

### Job 1: Budget Enforcer

Runs on a schedule (or manually) to check if the workspace has exceeded its monthly budget.

**Task 1 — CheckBudget (SQL)**

Queries `system.billing.usage` joined with `system.billing.list_prices` to calculate total spend for the current month. Returns:
- `budget_status`: `OVER_BUDGET` or `WITHIN_BUDGET`
- `current_spend`: the actual dollar amount spent

**Task 2 — IsOverBudget (Condition)**

Evaluates the output of CheckBudget. If `budget_status == OVER_BUDGET`, the workflow proceeds to StopActivity. Otherwise, the job completes with no further action.

**Task 3 — StopActivity (Notebook)**

Executes six actions in sequence:

1. **Pause all jobs** — Iterates through all workspace jobs and pauses them (scheduled, continuous, trigger-based, and on-demand). Skips the enforcer job itself and any exempt job IDs.
2. **Cancel active runs** — Cancels all currently running job executions.
3. **Terminate clusters** — Terminates all running, resizing, or pending clusters (except the one running this script).
4. **Stop SQL warehouses** — Stops all running or starting SQL warehouses.
5. **Stop apps** — Stops all running Databricks Apps.
6. **Record state** — Writes an audit table (`paused_jobs`) to Unity Catalog so the resumer knows which resources to restore.

### Job 2: Budget Resumer

Triggered manually when the new billing cycle starts or when budget is replenished.

**Task 1 — ResumeActivity (Notebook)**

1. Reads the `paused_jobs` audit table
2. Restores each resource based on its type:
   - `JOB` — Unpauses the job (scheduled, continuous, trigger) or removes the placeholder schedule (on-demand)
   - `CLUSTER` — Starts the cluster
   - `WAREHOUSE` — Starts the SQL warehouse
   - `APP` — Starts the Databricks App
3. Truncates the audit table after all resources are resumed

## Resources Managed

| Resource | Stop Action | Resume Action |
|----------|-------------|---------------|
| Scheduled Jobs | Set `pause_status` to `PAUSED` | Set `pause_status` to `UNPAUSED` |
| Continuous Jobs | Set `pause_status` to `PAUSED` | Set `pause_status` to `UNPAUSED` |
| Trigger Jobs | Set `pause_status` to `PAUSED` | Set `pause_status` to `UNPAUSED` |
| On-Demand Jobs | Assign dummy 2099 schedule | Remove placeholder schedule |
| Active Runs | Cancel via API | N/A (not resumable) |
| Clusters | Terminate | Start |
| SQL Warehouses | Stop | Start |
| Databricks Apps | Stop | Start |

## Configuration

All configuration is managed through bundle variables in `databricks.yml`:

| Variable | Description | Default |
|----------|-------------|---------|
| `catalog` | Unity Catalog for tracking tables | `serverless_stable_7n3jad_catalog` |
| `schema` | Schema for tracking tables | `budget_control` |
| `budget_threshold` | Monthly spend limit in dollars | `50000` |

### Selective Kill Switch

By default, the enforcer stops **all** resources of each type. Use the `target_*` variables to restrict enforcement to specific resources.

| Variable | Resource Type | ID Format | How to find the ID | Default |
|----------|--------------|-----------|-------------------|---------|
| `target_clusters` | All-purpose compute | Cluster ID (e.g. `0410-123456-abc123`) | Compute page or `databricks clusters list` | `ALL` |
| `target_jobs` | Job compute / scheduled jobs | Job ID (e.g. `230001808620939`) | Jobs page URL or `databricks jobs list` | `ALL` |
| `target_warehouses` | SQL Warehouses | Warehouse ID (e.g. `8c622aa24ea3a6da`) | SQL Warehouses page or `databricks warehouses list` | `ALL` |
| `target_apps` | Databricks Apps | App name (e.g. `my-dashboard-app`) | Apps page or `databricks apps list` | `ALL` |

#### Behavior

| Setting | Behavior |
|---------|----------|
| `"ALL"` (default) | Stops **every** resource of that type (except enforcer/resumer) |
| `"id1,id2,id3"` | Stops **only** the listed resources; all others are left untouched |
| Not set / empty | Same as `"ALL"` |

> **Important:** Always quote numeric IDs in YAML (e.g. `target_jobs: "230001808620939"`). Unquoted numbers can be mangled by YAML parsing (scientific notation, float conversion). The code handles this defensively, but quoting is the safest practice.

> **Protection:** The enforcer job and resumer job are always exempt regardless of these settings. The enforcer skips itself via `current_job_id`, and the resumer is skipped via `resumer_job_id`. Active runs belonging to exempt or non-targeted jobs are also never canceled.

#### Example

```yaml
targets:
  prod:
    variables:
      # Only stop these specific clusters
      target_clusters: "0410-123456-abc123,0410-789012-def456"
      # Only stop this specific job
      target_jobs: "230001808620939"
      # Only stop these specific SQL warehouses
      target_warehouses: "8c622aa24ea3a6da,a1b2c3d4e5f6"
      # Only stop these specific apps
      target_apps: "my-dashboard-app,data-explorer"
      # Stop ALL clusters (default behavior — same as omitting the variable)
      # target_clusters: "ALL"
```

### Targets

| Target | Budget Threshold | Mode |
|--------|-----------------|------|
| `dev` | $50,000 | development |
| `prod` | $100,000 | production (runs as service principal) |

## Deployment

```bash
# Validate the bundle
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```

## File Structure

```
BudgetEnforcer/
├── BudgetEnforcer/
│   ├── Budget Enforcer.py    # Standalone single-notebook (manual use)
│   ├── ActivityStopper.py    # Source notebook: stops all resources
│   ├── ActivityResume.py     # Source notebook: resumes all resources
│   └── README.md             # Workspace-level README
│
└── dab-budget-enforcer/
    ├── databricks.yml        # Bundle definition (jobs, variables, targets)
    ├── README.md             # This file
    └── src/
        ├── activity_stopper.py   # DAB notebook: parameterized stopper
        └── activity_resumer.py   # DAB notebook: parameterized resumer
```

## Audit Table Schema

The `paused_jobs` table is created automatically by the stopper notebook:

| Column | Type | Description |
|--------|------|-------------|
| `resource_type` | STRING | `JOB`, `CLUSTER`, `WAREHOUSE`, or `APP` |
| `action` | STRING | Action taken (`PAUSED`, `STOPPED`) |
| `resource_id` | STRING | Databricks resource ID or name |
| `resource_name` | STRING | Human-readable resource name |
| `timestamp` | TIMESTAMP | When the action was taken |
| `acted_by` | STRING | Always `budget_enforcer` |
| `month` | STRING | Year-month when action taken (e.g., `2026-04`) |

## Important Notes

- The enforcer **skips itself** — it will not pause its own job or terminate its own cluster.
- **On-demand jobs** (no schedule/trigger) are paused by assigning a dummy schedule set to year 2099. The resumer removes this placeholder.
- **SQL Warehouses** will auto-start on next query if not explicitly resumed — the resumer starts them proactively.
- **Apps** that were stopped will remain stopped until the resumer runs or they are manually started.
- The resumer **truncates** the audit table after restoring all resources, so it is safe to run multiple times.
- For production, configure a service principal with workspace admin permissions in the `prod` target.
