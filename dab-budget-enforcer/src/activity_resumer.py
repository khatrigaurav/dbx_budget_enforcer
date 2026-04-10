# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Activity Resumer
# MAGIC Resumes all resources (jobs, clusters, warehouses, apps) that were stopped by the Budget Enforcer.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "", "Schema Name")
dbutils.widgets.text("table", "paused_jobs", "Tracking Table")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TABLE = dbutils.widgets.get("table")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, CronSchedule, Continuous, TriggerSettings, PauseStatus

w = WorkspaceClient()

# =========================
# JOB RESUME LOGIC
# =========================
def resume_job(job_id):
    job = w.jobs.get(job_id=int(job_id))
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
        # ON_DEMAND -> remove placeholder schedule
        w.jobs.update(job_id=int(job_id), new_settings=JobSettings(schedule=None))
        print(f"Restored manual job: {job_id}")
        return

    w.jobs.update(job_id=int(job_id), new_settings=new_settings)
    print(f"Resumed job: {job_id}")


# =========================
# CLUSTER RESUME
# =========================
def resume_cluster(cluster_id):
    try:
        w.clusters.start(cluster_id=cluster_id)
        print(f"Started cluster: {cluster_id}")
    except Exception as e:
        print(f"Cluster resume failed {cluster_id}: {e}")


# =========================
# WAREHOUSE RESUME
# =========================
def resume_warehouse(warehouse_id):
    try:
        w.warehouses.start(id=warehouse_id)
        print(f"Started warehouse: {warehouse_id}")
    except Exception as e:
        print(f"Warehouse resume failed {warehouse_id}: {e}")


# =========================
# APP RESUME
# =========================
def resume_app(name):
    try:
        w.apps.start(name=name)
        print(f"Started app: {name}")
    except Exception as e:
        print(f"App resume failed {name}: {e}")

# COMMAND ----------

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
                print(f"Failed for {row.resource_type} {row.resource_id}: {e}")

        # =========================
        # CLEANUP
        # =========================
        spark.sql(f"TRUNCATE TABLE {CATALOG}.{SCHEMA}.{TABLE}")
        print(f"\n--- Resume complete: {len(rows)} resources processed ---")

except Exception as e:
    print(f"Error accessing audit table: {e}")
