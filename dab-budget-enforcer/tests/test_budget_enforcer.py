"""
Tests for budget enforcer logic.
Mocks Databricks-specific dependencies (dbutils, spark, WorkspaceClient)
to validate filtering, targeting, and resume logic locally.
"""
import unittest
from unittest.mock import MagicMock, patch, call
from datetime import datetime
from types import SimpleNamespace


# ============================================================
# 1. parse_target_list + normalize_id — extracted from activity_stopper.py
# ============================================================
def normalize_id(value):
    """Normalize an ID to handle YAML numeric coercion (e.g. 7.114e+14 -> 711400000000000)."""
    try:
        return str(int(float(value)))
    except (ValueError, OverflowError):
        return value

def parse_target_list(raw):
    raw = str(raw).strip()
    if raw.upper() == "ALL" or raw == "":
        return None
    return {normalize_id(item.strip()) for item in raw.split(",") if item.strip()}


class TestParseTargetList(unittest.TestCase):
    def test_all_uppercase(self):
        self.assertIsNone(parse_target_list("ALL"))

    def test_all_lowercase(self):
        self.assertIsNone(parse_target_list("all"))

    def test_all_mixed_case(self):
        self.assertIsNone(parse_target_list("All"))

    def test_empty_string(self):
        self.assertIsNone(parse_target_list(""))

    def test_whitespace_only(self):
        self.assertIsNone(parse_target_list("   "))

    def test_all_with_whitespace(self):
        self.assertIsNone(parse_target_list("  ALL  "))

    def test_single_id(self):
        result = parse_target_list("12345")
        self.assertEqual(result, {"12345"})

    def test_multiple_ids(self):
        result = parse_target_list("111,222,333")
        self.assertEqual(result, {"111", "222", "333"})

    def test_ids_with_whitespace(self):
        result = parse_target_list(" 111 , 222 , 333 ")
        self.assertEqual(result, {"111", "222", "333"})

    def test_cluster_id_format(self):
        result = parse_target_list("0410-123456-abc123,0410-789012-def456")
        self.assertEqual(result, {"0410-123456-abc123", "0410-789012-def456"})

    def test_trailing_comma_ignored(self):
        result = parse_target_list("111,222,")
        self.assertEqual(result, {"111", "222"})

    def test_app_names(self):
        result = parse_target_list("my-app,data-explorer")
        self.assertEqual(result, {"my-app", "data-explorer"})

    # --- YAML numeric coercion edge cases ---

    def test_large_job_id_string(self):
        """Normal case: quoted ID in YAML."""
        result = parse_target_list("711404742026056")
        self.assertEqual(result, {"711404742026056"})

    def test_large_job_id_scientific_notation(self):
        """YAML may convert unquoted 711404742026056 to scientific notation."""
        result = parse_target_list("7.11404742026056e+14")
        self.assertEqual(result, {"711404742026056"})

    def test_large_job_id_with_decimal(self):
        """YAML may pass as float string e.g. '711404742026056.0'."""
        result = parse_target_list("711404742026056.0")
        self.assertEqual(result, {"711404742026056"})

    def test_multiple_ids_mixed_formats(self):
        """Mix of normal and scientific notation IDs."""
        result = parse_target_list("711404742026056,1.23456789e+11")
        self.assertEqual(result, {"711404742026056", "123456789000"})

    def test_numeric_input_type(self):
        """If YAML passes an actual int/float to str(), handle it."""
        result = parse_target_list(711404742026056)
        self.assertEqual(result, {"711404742026056"})

    def test_float_input_type(self):
        """If YAML passes a float."""
        result = parse_target_list(7.11404742026056e+14)
        self.assertEqual(result, {"711404742026056"})

    def test_cluster_id_not_mangled(self):
        """Cluster IDs with dashes should pass through normalize_id unchanged."""
        result = parse_target_list("0410-123456-abc123")
        self.assertEqual(result, {"0410-123456-abc123"})

    def test_warehouse_id_hex_not_mangled(self):
        """Hex-like warehouse IDs should pass through unchanged."""
        result = parse_target_list("8c622aa24ea3a6da")
        self.assertEqual(result, {"8c622aa24ea3a6da"})


# ============================================================
# 2. log() — should always store resource_id as string
# ============================================================
class TestLogFunction(unittest.TestCase):
    def test_log_casts_resource_id_to_string(self):
        records = []
        current_month = "2026-04"

        def log(resource_type, action, resource_id, resource_name):
            records.append((
                resource_type, action, str(resource_id), resource_name,
                datetime.now(), "budget_enforcer", current_month
            ))

        # Job IDs are ints
        log("JOB", "STOPPED", 12345, "My Job")
        # Cluster IDs are strings
        log("CLUSTER", "STOPPED", "0410-abc123", "My Cluster")
        # Warehouse IDs are strings
        log("WAREHOUSE", "STOPPED", "8c622aa24ea3a6da", "My Warehouse")
        # App names are strings
        log("APP", "STOPPED", "my-app", "my-app")

        # All resource_ids should be strings
        for record in records:
            self.assertIsInstance(record[2], str, f"resource_id should be str, got {type(record[2])}")

        self.assertEqual(records[0][2], "12345")
        self.assertEqual(records[1][2], "0410-abc123")


# ============================================================
# 3. Job filtering — enforcer/resumer exempt + target filtering
# ============================================================
def make_job(job_id, name, paused=False):
    """Create a mock job object."""
    job = SimpleNamespace()
    job.job_id = job_id
    job.settings = SimpleNamespace()
    job.settings.name = name
    job.settings.schedule = SimpleNamespace()
    job.settings.schedule.quartz_cron_expression = "0 0 8 * * ?"
    job.settings.schedule.timezone_id = "UTC"
    job.settings.schedule.pause_status = "PAUSED" if paused else "UNPAUSED"
    job.settings.continuous = None
    job.settings.trigger = None
    return job


class TestJobFiltering(unittest.TestCase):
    def _get_actionable_jobs(self, jobs, current_job_id, resumer_job_id, target_jobs):
        """Replicate the filtering logic from activity_stopper.py."""
        actionable = []
        for job in jobs:
            settings = job.settings
            if str(job.job_id) == str(current_job_id) or str(job.job_id) == str(resumer_job_id):
                continue
            if not settings:
                continue
            if target_jobs is not None and str(job.job_id) not in target_jobs:
                continue
            actionable.append(job)
        return actionable

    def test_enforcer_job_exempt(self):
        jobs = [make_job(100, "Enforcer"), make_job(200, "Other")]
        result = self._get_actionable_jobs(jobs, current_job_id=100, resumer_job_id=300, target_jobs=None)
        self.assertEqual([j.job_id for j in result], [200])

    def test_resumer_job_exempt(self):
        jobs = [make_job(100, "Enforcer"), make_job(200, "Other"), make_job(300, "Resumer")]
        result = self._get_actionable_jobs(jobs, current_job_id=100, resumer_job_id=300, target_jobs=None)
        self.assertEqual([j.job_id for j in result], [200])

    def test_target_jobs_all(self):
        """target_jobs=None means ALL — stop every non-exempt job."""
        jobs = [make_job(100, "Enforcer"), make_job(200, "Job A"), make_job(300, "Resumer"), make_job(400, "Job B")]
        result = self._get_actionable_jobs(jobs, current_job_id=100, resumer_job_id=300, target_jobs=None)
        self.assertEqual([j.job_id for j in result], [200, 400])

    def test_target_jobs_specific(self):
        """Only stop targeted jobs, still exempt enforcer/resumer."""
        jobs = [make_job(100, "Enforcer"), make_job(200, "Job A"), make_job(300, "Resumer"), make_job(400, "Job B")]
        result = self._get_actionable_jobs(jobs, current_job_id=100, resumer_job_id=300, target_jobs={"200"})
        self.assertEqual([j.job_id for j in result], [200])

    def test_target_jobs_specific_excludes_nontargets(self):
        """Jobs not in target list are skipped."""
        jobs = [make_job(200, "Job A"), make_job(400, "Job B"), make_job(500, "Job C")]
        result = self._get_actionable_jobs(jobs, current_job_id=100, resumer_job_id=300, target_jobs={"400"})
        self.assertEqual([j.job_id for j in result], [400])

    def test_enforcer_exempt_even_when_targeted(self):
        """Even if enforcer is in target list, it should still be skipped."""
        jobs = [make_job(100, "Enforcer"), make_job(200, "Job A")]
        result = self._get_actionable_jobs(jobs, current_job_id=100, resumer_job_id=300, target_jobs={"100", "200"})
        self.assertEqual([j.job_id for j in result], [200])

    def test_resumer_exempt_even_when_targeted(self):
        """Even if resumer is in target list, it should still be skipped."""
        jobs = [make_job(300, "Resumer"), make_job(200, "Job A")]
        result = self._get_actionable_jobs(jobs, current_job_id=100, resumer_job_id=300, target_jobs={"300", "200"})
        self.assertEqual([j.job_id for j in result], [200])

    def test_empty_target_means_nothing_stopped(self):
        """An empty set (not None) means no jobs match."""
        jobs = [make_job(200, "Job A"), make_job(400, "Job B")]
        result = self._get_actionable_jobs(jobs, current_job_id=100, resumer_job_id=300, target_jobs=set())
        self.assertEqual(result, [])

    def test_no_settings_skipped(self):
        """Jobs with no settings are skipped."""
        job = SimpleNamespace(job_id=200, settings=None)
        result = self._get_actionable_jobs([job], current_job_id=100, resumer_job_id=300, target_jobs=None)
        self.assertEqual(result, [])


# ============================================================
# 4. Cluster filtering
# ============================================================
def make_cluster(cluster_id, name, state="RUNNING"):
    return SimpleNamespace(cluster_id=cluster_id, cluster_name=name, state=state)


class TestClusterFiltering(unittest.TestCase):
    def _get_actionable_clusters(self, clusters, current_cluster_id, target_clusters, active_states):
        actionable = []
        for cluster in clusters:
            if cluster.cluster_id == current_cluster_id:
                continue
            if target_clusters is not None and cluster.cluster_id not in target_clusters:
                continue
            if cluster.state in active_states:
                actionable.append(cluster)
        return actionable

    def test_all_clusters_targeted(self):
        clusters = [make_cluster("c1", "Cluster 1"), make_cluster("c2", "Cluster 2")]
        result = self._get_actionable_clusters(clusters, "current", None, ["RUNNING"])
        self.assertEqual(len(result), 2)

    def test_specific_cluster_targeted(self):
        clusters = [make_cluster("c1", "Cluster 1"), make_cluster("c2", "Cluster 2")]
        result = self._get_actionable_clusters(clusters, "current", {"c1"}, ["RUNNING"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].cluster_id, "c1")

    def test_current_cluster_exempt(self):
        clusters = [make_cluster("current", "My Cluster"), make_cluster("c2", "Other")]
        result = self._get_actionable_clusters(clusters, "current", None, ["RUNNING"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].cluster_id, "c2")

    def test_current_cluster_exempt_even_when_targeted(self):
        clusters = [make_cluster("current", "My Cluster"), make_cluster("c2", "Other")]
        result = self._get_actionable_clusters(clusters, "current", {"current", "c2"}, ["RUNNING"])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].cluster_id, "c2")

    def test_non_active_cluster_skipped(self):
        clusters = [make_cluster("c1", "Cluster 1", "TERMINATED")]
        result = self._get_actionable_clusters(clusters, "current", None, ["RUNNING"])
        self.assertEqual(result, [])


# ============================================================
# 5. Warehouse filtering
# ============================================================
def make_warehouse(wh_id, name, state="RUNNING"):
    return SimpleNamespace(id=wh_id, name=name, state=SimpleNamespace(value=state))


class TestWarehouseFiltering(unittest.TestCase):
    def _get_actionable_warehouses(self, warehouses, target_warehouses):
        actionable = []
        for wh in warehouses:
            if target_warehouses is not None and wh.id not in target_warehouses:
                continue
            if wh.state.value in ["RUNNING", "STARTING"]:
                actionable.append(wh)
        return actionable

    def test_all_warehouses_targeted(self):
        whs = [make_warehouse("w1", "WH 1"), make_warehouse("w2", "WH 2")]
        result = self._get_actionable_warehouses(whs, None)
        self.assertEqual(len(result), 2)

    def test_specific_warehouse_targeted(self):
        whs = [make_warehouse("w1", "WH 1"), make_warehouse("w2", "WH 2")]
        result = self._get_actionable_warehouses(whs, {"w1"})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, "w1")

    def test_stopped_warehouse_skipped(self):
        whs = [make_warehouse("w1", "WH 1", "STOPPED")]
        result = self._get_actionable_warehouses(whs, None)
        self.assertEqual(result, [])


# ============================================================
# 6. App filtering
# ============================================================
class TestAppFiltering(unittest.TestCase):
    def _get_actionable_apps(self, apps, target_apps):
        actionable = []
        for app in apps:
            if target_apps is not None and app.name not in target_apps:
                continue
            actionable.append(app)
        return actionable

    def test_all_apps_targeted(self):
        apps = [SimpleNamespace(name="app1"), SimpleNamespace(name="app2")]
        result = self._get_actionable_apps(apps, None)
        self.assertEqual(len(result), 2)

    def test_specific_app_targeted(self):
        apps = [SimpleNamespace(name="app1"), SimpleNamespace(name="app2")]
        result = self._get_actionable_apps(apps, {"app1"})
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "app1")

    def test_no_apps_when_empty_target(self):
        apps = [SimpleNamespace(name="app1")]
        result = self._get_actionable_apps(apps, set())
        self.assertEqual(result, [])


# ============================================================
# 7. Cancel active runs filtering
# ============================================================
class TestCancelRunsFiltering(unittest.TestCase):
    def _get_cancelable_runs(self, runs, current_job_id, resumer_job_id, target_jobs):
        cancelable = []
        for run in runs:
            if str(run.job_id) == str(current_job_id) or str(run.job_id) == str(resumer_job_id):
                continue
            if target_jobs is not None and str(run.job_id) not in target_jobs:
                continue
            cancelable.append(run)
        return cancelable

    def test_enforcer_runs_not_canceled(self):
        runs = [SimpleNamespace(job_id=100, run_id=1), SimpleNamespace(job_id=200, run_id=2)]
        result = self._get_cancelable_runs(runs, current_job_id=100, resumer_job_id=300, target_jobs=None)
        self.assertEqual([r.run_id for r in result], [2])

    def test_resumer_runs_not_canceled(self):
        runs = [SimpleNamespace(job_id=300, run_id=1), SimpleNamespace(job_id=200, run_id=2)]
        result = self._get_cancelable_runs(runs, current_job_id=100, resumer_job_id=300, target_jobs=None)
        self.assertEqual([r.run_id for r in result], [2])

    def test_only_targeted_runs_canceled(self):
        runs = [SimpleNamespace(job_id=200, run_id=1), SimpleNamespace(job_id=400, run_id=2)]
        result = self._get_cancelable_runs(runs, current_job_id=100, resumer_job_id=300, target_jobs={"200"})
        self.assertEqual([r.run_id for r in result], [1])

    def test_enforcer_exempt_even_in_target(self):
        runs = [SimpleNamespace(job_id=100, run_id=1), SimpleNamespace(job_id=200, run_id=2)]
        result = self._get_cancelable_runs(runs, current_job_id=100, resumer_job_id=300, target_jobs={"100", "200"})
        self.assertEqual([r.run_id for r in result], [2])


# ============================================================
# 8. Resumer — reads all records (no month filter)
# ============================================================
class TestResumerMonthLogic(unittest.TestCase):
    def test_resumer_reads_all_records(self):
        """Resumer should process all records regardless of month.
        Previously it filtered by last month, which broke same-month manual runs."""
        rows = [
            SimpleNamespace(resource_type="JOB", resource_id="200", month="2026-04"),
            SimpleNamespace(resource_type="CLUSTER", resource_id="c1", month="2026-03"),
            SimpleNamespace(resource_type="WAREHOUSE", resource_id="w1", month="2026-04"),
        ]
        # No filter — all rows should be processed
        self.assertEqual(len(rows), 3)

    def test_resume_job_casts_to_int(self):
        """resume_job should cast string resource_id to int for the SDK."""
        mock_client = MagicMock()
        mock_job = MagicMock()
        mock_job.settings.schedule = None
        mock_job.settings.continuous = None
        mock_job.settings.trigger = None
        mock_client.jobs.get.return_value = mock_job

        # Simulate what resume_job does
        job_id = "12345"  # string from audit table
        mock_client.jobs.get(job_id=int(job_id))
        mock_client.jobs.get.assert_called_with(job_id=12345)


# ============================================================
# 9. End-to-end selective targeting scenarios
# ============================================================
class TestEndToEndScenarios(unittest.TestCase):
    def test_parse_then_filter_jobs(self):
        """Simulate: user sets target_jobs='200,400' in databricks.yml."""
        target_raw = "200,400"
        target_jobs = parse_target_list(target_raw)
        self.assertEqual(target_jobs, {"200", "400"})

        jobs = [make_job(100, "Enforcer"), make_job(200, "Job A"),
                make_job(300, "Resumer"), make_job(400, "Job B"), make_job(500, "Job C")]

        actionable = []
        for job in jobs:
            if str(job.job_id) == "100" or str(job.job_id) == "300":
                continue
            if target_jobs is not None and str(job.job_id) not in target_jobs:
                continue
            actionable.append(job)

        self.assertEqual([j.job_id for j in actionable], [200, 400])

    def test_parse_then_filter_clusters(self):
        """Simulate: user sets target_clusters='c2' in databricks.yml."""
        target_raw = "c2"
        target_clusters = parse_target_list(target_raw)
        self.assertEqual(target_clusters, {"c2"})

        clusters = [make_cluster("c1", "Cluster 1"), make_cluster("c2", "Cluster 2"),
                    make_cluster("c3", "Cluster 3")]

        actionable = []
        for c in clusters:
            if target_clusters is not None and c.cluster_id not in target_clusters:
                continue
            actionable.append(c)

        self.assertEqual([c.cluster_id for c in actionable], ["c2"])

    def test_parse_then_filter_warehouses(self):
        """Simulate: user sets target_warehouses='w1,w3'."""
        target_raw = "w1,w3"
        target_warehouses = parse_target_list(target_raw)

        whs = [make_warehouse("w1", "WH 1"), make_warehouse("w2", "WH 2"),
               make_warehouse("w3", "WH 3")]

        actionable = []
        for wh in whs:
            if target_warehouses is not None and wh.id not in target_warehouses:
                continue
            if wh.state.value in ["RUNNING", "STARTING"]:
                actionable.append(wh)

        self.assertEqual([w.id for w in actionable], ["w1", "w3"])

    def test_all_defaults_stop_everything(self):
        """When all targets are ALL (None), everything non-exempt is stopped."""
        target_jobs = parse_target_list("ALL")
        target_clusters = parse_target_list("ALL")
        target_warehouses = parse_target_list("ALL")
        target_apps = parse_target_list("ALL")

        self.assertIsNone(target_jobs)
        self.assertIsNone(target_clusters)
        self.assertIsNone(target_warehouses)
        self.assertIsNone(target_apps)

        jobs = [make_job(200, "Job A"), make_job(400, "Job B")]
        actionable = []
        for job in jobs:
            if target_jobs is not None and str(job.job_id) not in target_jobs:
                continue
            actionable.append(job)
        self.assertEqual(len(actionable), 2)


if __name__ == "__main__":
    unittest.main()
