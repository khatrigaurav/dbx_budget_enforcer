# Databricks notebook source
# MAGIC %md
# MAGIC Step 1 : SQL to check the budget status

# COMMAND ----------


spark.sql("""
          SELECT CASE
         WHEN Sum(u.usage_quantity * lp.pricing.effective_list.default) > 50000
       THEN
         'OVER_BUDGET'
         ELSE 'WITHIN_BUDGET'
       END                                                       AS
       budget_status,
       Sum(u.usage_quantity * lp.pricing.effective_list.default) AS
       current_spend
FROM   system.billing.usage u
       JOIN system.billing.list_prices lp
         ON lp.sku_name = u.sku_name
            AND u.usage_end_time >= lp.price_start_time
            AND ( lp.price_end_time IS NULL
                   OR u.usage_end_time < lp.price_end_time )
WHERE  u.usage_date >= Date_trunc('month', CURRENT_DATE()) 

""").display()


# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Stopping all the jobs, compute

# COMMAND ----------

catalog_name = 'serverless_stable_5rb6za_catalog'
schema_name = 'budget_control'
table_name = 'paused_jobs'

# COMMAND ----------

from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, CronSchedule, Continuous, TriggerSettings, PauseStatus
from databricks.sdk.service.compute import State

w = WorkspaceClient()

current_cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", None)
try:
    current_job_id = spark.conf.get("spark.databricks.job_id")
except Exception:
    current_job_id = None

current_month = datetime.now().strftime("%Y-%m")
records = []
EXEMPT_JOB_IDS = []

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

# =========================
# 1. PAUSE ALL JOBS
# =========================
for job in w.jobs.list():
    settings = job.settings

    if job.job_id in EXEMPT_JOB_IDS or str(job.job_id) == current_job_id:
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

        log("JOB", "PAUSED", job.job_id, settings.name)
        print(f"Paused job {job.job_id} ({settings.name})")

    except Exception as e:
        print(f"Skipping job {job.job_id}: {e}")

# =========================
# 2. CANCEL ACTIVE RUNS
# =========================
for run in w.jobs.list_runs(active_only=True):
    if str(run.job_id) == current_job_id:
        continue
    try:
        w.jobs.cancel_run(run_id=run.run_id)
        print(f"Canceled run {run.run_id}")
    except Exception as e:
        print(f"Failed cancel {run.run_id}: {e}")

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


# Apps (if enabled)
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

    df.write.mode("overwrite").saveAsTable(
        f"{catalog_name}.{schema_name}.{table_name}"
    )

    print(f"\nRecorded {len(records)} actions")
else:
    print("No actions recorded.")

# COMMAND ----------

from databricks.sdk.service.jobs import JobSettings, CronSchedule, TriggerSettings, PauseStatus

# Query the tracking table for jobs paused last month
paused_df = spark.sql(f"""
  SELECT DISTINCT job_id, job_name
  FROM {catalog_name}.{schema_name}.{table_name}
  WHERE acted_by = 'budget_enforcer'
""")


failed_jobs = []
resumed_jobs = []

for row in paused_df.collect():
    try:
        job = w.jobs.get(row.job_id)
        settings = job.settings

        # Resume scheduled jobs
        if settings and settings.schedule:
            if settings.schedule.pause_status == PauseStatus.PAUSED:
                w.jobs.update(
                    job_id=row.job_id,
                    new_settings=JobSettings(
                        schedule=CronSchedule(
                            quartz_cron_expression=settings.schedule.quartz_cron_expression,
                            timezone_id=settings.schedule.timezone_id,
                            pause_status=PauseStatus.UNPAUSED
                        )
                    )
                )
                resumed_jobs.append(row.job_name)

        # Resume trigger-based jobs (e.g., File Arrival Triggers)
        if settings and settings.trigger:
            if settings.trigger.pause_status == PauseStatus.PAUSED:
                w.jobs.update(
                    job_id=row.job_id,
                    new_settings=JobSettings(
                        trigger=TriggerSettings(
                            pause_status=PauseStatus.UNPAUSED
                        )
                    )
                )
                resumed_jobs.append(row.job_name)

    except Exception as e:
        failed_jobs.append((row.job_name, str(e)))


#cleanup
spark.sql(f"DELETE FROM {catalog_name}.{schema_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3 : Resume required jobs

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, CronSchedule, Continuous, TriggerSettings, PauseStatus

w = WorkspaceClient()

# =========================
# JOB RESUME LOGIC
# =========================
def resume_job(job_id):
    job = w.jobs.get(job_id=job_id)
    s = job.settings
    new_settings = JobSettings()

    if s.schedule:
        new_settings.schedule = CronSchedule(
            quartz_cron_expression=s.schedule.quartz_cron_expression,
            timezone_id=s.schedule.timezone_id,
            pause_status=PauseStatus.UNPAUSED
        )

    elif s.continuous:
        new_settings.continuous = Continuous(
            pause_status=PauseStatus.UNPAUSED
        )

    elif s.trigger:
        new_settings.trigger = TriggerSettings(
            file_arrival=s.trigger.file_arrival,
            table_update=s.trigger.table_update,
            pause_status=PauseStatus.UNPAUSED
        )

    else:
        # ON_DEMAND → remove placeholder schedule
        w.jobs.update(job_id=job_id, new_settings=JobSettings(schedule=None))
        print(f"Restored manual job: {job_id}")
        return

    w.jobs.update(job_id=job_id, new_settings=new_settings)
    print(f"Resumed job: {job_id}")


# =========================
# CLUSTER RESUME
# =========================
def resume_cluster(cluster_id):
    try:
        w.clusters.start(cluster_id=cluster_id)
        print(f"🟢 Started cluster: {cluster_id}")
    except Exception as e:
        print(f"Cluster resume failed {cluster_id}: {e}")


# =========================
# WAREHOUSE RESUME
# =========================
def resume_warehouse(warehouse_id):
    try:
        w.warehouses.start(id=warehouse_id)
        print(f"🟢 Started warehouse: {warehouse_id}")
    except Exception as e:
        print(f"Warehouse resume failed {warehouse_id}: {e}")


# =========================
# APP RESUME
# =========================
def resume_app(name):
    try:
        w.apps.start(name=name)
        print(f"🟢 Started app: {name}")
    except Exception as e:
        print(f"App resume failed {name}: {e}")


# =========================
# MAIN EXECUTION
# =========================
print(f"--- Starting Workspace Resume from {CATALOG}.{SCHEMA}.{TABLE} ---")

try:
    df = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE}")
    rows = df.collect()

    if not rows:
        print("No resources to resume.")
    else:
        for row in rows:
            try:
                rt = row.resource_type
                rid = row.resource_id

                if rt == "JOB":
                    resume_job(rid)

                elif rt == "CLUSTER":
                    resume_cluster(rid)

                elif rt == "WAREHOUSE":
                    resume_warehouse(rid)

                elif rt == "APP":
                    resume_app(rid)

                else:
                    print(f"Unknown resource type: {rt}")

            except Exception as e:
                print(f"❌ Failed for {row.resource_type} {row.resource_id}: {e}")

        # =========================
        # CLEANUP
        # =========================
        spark.sql(f"TRUNCATE TABLE {CATALOG}.{SCHEMA}.{TABLE}")
        print(f"\n--- Resume complete: {len(rows)} resources processed ---")

except Exception as e:
    print(f"Error accessing audit table: {e}")
