# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Activity Stopper
# MAGIC Pauses all jobs, cancels active runs, terminates clusters, stops SQL warehouses, and stops apps when budget is exceeded.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "", "Schema Name")
dbutils.widgets.text("table", "paused_jobs", "Tracking Table")
dbutils.widgets.text("exempt_job_ids", "", "Exempt Job IDs (comma-separated)")
dbutils.widgets.text("current_job_id", "", "Current Job ID (auto-populated by workflow)")
dbutils.widgets.text("resumer_job_id", "", "Resumer Job ID (auto-populated by workflow)")

catalog_name = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
table_name = dbutils.widgets.get("table")
exempt_raw = dbutils.widgets.get("exempt_job_ids")

EXEMPT_JOB_IDS = [int(x.strip()) for x in exempt_raw.split(",") if x.strip()]

# Always exempt the resumer job so it can resume everything later
resumer_job_id = dbutils.widgets.get("resumer_job_id")
if resumer_job_id:
    EXEMPT_JOB_IDS.append(int(resumer_job_id))

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

current_month = datetime.now().strftime("%Y-%m")
records = []

def log(resource_type, action, resource_id, resource_name):
    records.append((
        resource_type,
        action,
        resource_id,
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

    if job.job_id in EXEMPT_JOB_IDS or str(job.job_id) == str(current_job_id):
        continue
    if not settings:
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
    if str(run.job_id) == str(current_job_id):
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
