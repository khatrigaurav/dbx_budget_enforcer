# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Activity Stopper
# MAGIC Pauses all jobs, cancels active runs, terminates clusters, stops SQL warehouses, and stops apps when budget is exceeded.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "", "Schema Name")
dbutils.widgets.text("table", "paused_jobs", "Tracking Table")
dbutils.widgets.text("current_job_id", "", "Current Job ID (auto-populated by workflow)")
dbutils.widgets.text("resumer_job_id", "", "Resumer Job ID (auto-populated by workflow)")
dbutils.widgets.text("target_clusters", "ALL", "Clusters to stop: ALL or comma-separated IDs")
dbutils.widgets.text("target_jobs", "ALL", "Jobs to stop: ALL or comma-separated IDs")
dbutils.widgets.text("target_warehouses", "ALL", "SQL Warehouses to stop: ALL or comma-separated IDs")
dbutils.widgets.text("target_apps", "ALL", "Apps to stop: ALL or comma-separated names")

catalog_name = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table")
resumer_job_id = dbutils.widgets.get("resumer_job_id")

# Selective kill switch: "ALL" targets everything, otherwise a set of IDs/names
def normalize_id(value):
    """Normalize an ID to handle YAML numeric coercion (e.g. 7.114e+14 -> 711400000000000)."""
    try:
        return str(int(float(value)))
    except (ValueError, OverflowError):
        return value

def parse_target_list(raw):
    raw = str(raw).strip()
    if raw.upper() == "ALL" or raw == "":
        return None  # None means target all
    return {normalize_id(item.strip()) for item in raw.split(",") if item.strip()}

TARGET_CLUSTERS = parse_target_list(dbutils.widgets.get("target_clusters"))
TARGET_JOBS = parse_target_list(dbutils.widgets.get("target_jobs"))
TARGET_WAREHOUSES = parse_target_list(dbutils.widgets.get("target_warehouses"))
TARGET_APPS = parse_target_list(dbutils.widgets.get("target_apps"))

print(f"Target clusters:   {'ALL' if TARGET_CLUSTERS is None else TARGET_CLUSTERS}")
print(f"Target jobs:       {'ALL' if TARGET_JOBS is None else TARGET_JOBS}")
print(f"Target warehouses: {'ALL' if TARGET_WAREHOUSES is None else TARGET_WAREHOUSES}")
print(f"Target apps:       {'ALL' if TARGET_APPS is None else TARGET_APPS}")

# COMMAND ----------

from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, CronSchedule, Continuous, TriggerSettings, PauseStatus
from databricks.sdk.service.compute import State

w = WorkspaceClient()

current_cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", None)

# Job ID is passed as a parameter from the workflow using {{job.id}}
current_job_id = dbutils.widgets.get("current_job_id")
if not current_job_id:
    current_job_id = None

print(f"Current job ID: {current_job_id} (will be excluded from pausing)")

# Ensure schema and audit table exist before stopping anything
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (
        resource_type STRING,
        action STRING,
        resource_id STRING,
        resource_name STRING,
        timestamp TIMESTAMP,
        acted_by STRING,
        month STRING
    )
""")
print(f"Verified audit table: {catalog_name}.{schema_name}.{table_name}")

current_month = datetime.now().strftime("%Y-%m")
records = []

def log(resource_type, action, resource_id, resource_name):
    records.append((
        resource_type,
        action,
        str(resource_id),
        resource_name,
        datetime.now(),
        "budget_enforcer",
        current_month
    ))

# COMMAND ----------

# =========================
# 1. PAUSE ALL JOBS
# =========================
for job in w.jobs.list():
    settings = job.settings

    if str(job.job_id) == str(current_job_id) or str(job.job_id) == str(resumer_job_id):
        continue
    if not settings:
        continue
    if TARGET_JOBS is not None and str(job.job_id) not in TARGET_JOBS:
        continue

    try:
        new_settings = JobSettings()
        already_paused = False

        if settings.schedule:
            if settings.schedule.pause_status == PauseStatus.PAUSED:
                already_paused = True
            else:
                new_settings.schedule = CronSchedule(
                    quartz_cron_expression=settings.schedule.quartz_cron_expression,
                    timezone_id=settings.schedule.timezone_id,
                    pause_status=PauseStatus.PAUSED
                )

        elif settings.continuous:
            if settings.continuous.pause_status == PauseStatus.PAUSED:
                already_paused = True
            else:
                new_settings.continuous = Continuous(
                    pause_status=PauseStatus.PAUSED
                )

        elif settings.trigger:
            if settings.trigger.pause_status == PauseStatus.PAUSED:
                already_paused = True
            else:
                new_settings.trigger = TriggerSettings(
                    file_arrival=settings.trigger.file_arrival,
                    table_update=settings.trigger.table_update,
                    pause_status=PauseStatus.PAUSED
                )

        else:
            new_settings.schedule = CronSchedule(
                quartz_cron_expression="0 0 0 1 1 ? 2099",
                timezone_id="UTC",
                pause_status=PauseStatus.PAUSED
            )

        if already_paused:
            continue

        w.jobs.update(job_id=job.job_id, new_settings=new_settings)

        log("JOB", "STOPPED", job.job_id, settings.name)
        print(f"Paused job {job.job_id} ({settings.name})")

    except Exception as e:
        print(f"Skipping job {job.job_id}: {e}")

# COMMAND ----------

# =========================
# 2. CANCEL ACTIVE RUNS
# =========================
for run in w.jobs.list_runs(active_only=True):
    if str(run.job_id) == str(current_job_id) or str(run.job_id) == str(resumer_job_id):
        continue
    if TARGET_JOBS is not None and str(run.job_id) not in TARGET_JOBS:
        continue
    try:
        w.jobs.cancel_run(run_id=run.run_id)
        print(f"Canceled run {run.run_id}")
    except Exception as e:
        print(f"Failed cancel {run.run_id}: {e}")

# COMMAND ----------

# =========================
# 3. TERMINATE CLUSTERS
# =========================
active_states = [State.RUNNING, State.RESIZING, State.PENDING]

for cluster in w.clusters.list():
    if cluster.cluster_id == current_cluster_id:
        continue
    if TARGET_CLUSTERS is not None and cluster.cluster_id not in TARGET_CLUSTERS:
        continue

    if cluster.state in active_states:
        try:
            w.clusters.terminate(cluster_id=cluster.cluster_id)
            log("CLUSTER", "STOPPED", cluster.cluster_id, cluster.cluster_name)
            print(f"Stopped cluster {cluster.cluster_name}")
        except Exception as e:
            print(f"Cluster fail {cluster.cluster_id}: {e}")

# COMMAND ----------

# =========================
# 4. STOP SQL WAREHOUSES
# =========================
for wh in w.warehouses.list():
    if TARGET_WAREHOUSES is not None and wh.id not in TARGET_WAREHOUSES:
        continue
    try:
        if wh.state.value in ["RUNNING", "STARTING"]:
            w.warehouses.stop(id=wh.id)
            log("WAREHOUSE", "STOPPED", wh.id, wh.name)
            print(f"Stopped warehouse {wh.name}")
    except Exception as e:
        print(f"Warehouse fail {wh.id}: {e}")

# COMMAND ----------

# =========================
# 5. STOP APPS
# =========================
try:
    for app in w.apps.list():
        if TARGET_APPS is not None and app.name not in TARGET_APPS:
            continue
        try:
            w.apps.stop(name=app.name)
            log("APP", "STOPPED", app.name, app.name)
            print(f"Stopped app {app.name}")
        except Exception as e:
            print(f"App fail {app.name}: {e}")
except Exception:
    pass

# COMMAND ----------

# =========================
# 6. WRITE AUDIT TABLE
# =========================
if records:
    cols = [
        "resource_type",
        "action",
        "resource_id",
        "resource_name",
        "timestamp",
        "acted_by",
        "month"
    ]

    df = spark.createDataFrame(records, cols)

    df.write.mode("append").saveAsTable(
        f"{catalog_name}.{schema_name}.{table_name}"
    )

    print(f"\nRecorded {len(records)} actions")
else:
    print("No actions recorded.")
