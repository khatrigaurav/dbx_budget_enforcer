# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Budget Check
# MAGIC Checks current monthly spend against the configured threshold and outputs the budget status.

# COMMAND ----------

dbutils.widgets.text("budget_threshold", "50000", "Budget Threshold ($)")
budget_threshold = dbutils.widgets.get("budget_threshold")

# COMMAND ----------

result = spark.sql(f"""
SELECT
  CASE
    WHEN SUM(u.usage_quantity * lp.pricing.effective_list.default) > {budget_threshold}
    THEN 'OVER_BUDGET'
    ELSE 'WITHIN_BUDGET'
  END AS budget_status,
  SUM(u.usage_quantity * lp.pricing.effective_list.default) AS current_spend
FROM system.billing.usage u
JOIN system.billing.list_prices lp
  ON lp.sku_name = u.sku_name
  AND u.usage_end_time >= lp.price_start_time
  AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.usage_date >= date_trunc('month', current_date())
""").first()

budget_status = result.budget_status
current_spend = result.current_spend

print(f"Current spend: ${current_spend:,.2f}")
print(f"Threshold:     ${float(budget_threshold):,.2f}")
print(f"Status:        {budget_status}")

dbutils.jobs.taskValues.set(key="budget_status", value=budget_status)
dbutils.jobs.taskValues.set(key="current_spend", value=float(current_spend))
