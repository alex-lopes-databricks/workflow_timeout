# Databricks notebook source
# MAGIC %md
# MAGIC # Set maximum timeout for workflows in the workspace

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("workspace_id", "-")
dbutils.widgets.text("token_secret_scope", "databricks-cost-savings")
dbutils.widgets.text("token_secret_key", "SP_OAUTH_TOKEN")
dbutils.widgets.text("client_id", "XXXX")
dbutils.widgets.text("exception_tags", "job_timeout, job_failure")
dbutils.widgets.text("exception_policies_id", "")
dbutils.widgets.text("timeout_to_set_min", "60")
dbutils.widgets.text("event_log_table", "workflow_timeout_events")

# COMMAND ----------

for k,v in dbutils.widgets.getAll().items(): globals()[k] = v 
exception_tags = set(exception_tags.split(","))
exception_policies_id = set(exception_policies_id.split(","))

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import *
from pyspark.sql.functions import *
from pprint import pprint

# COMMAND ----------

# w = WorkspaceClient(host=workspace_host, 
#                     client_id=client_id,
#                     client_secret=dbutils.secrets.get(scope=token_secret_scope, key=token_secret_key))

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

jobs =  w.jobs.list(limit=100)

# COMMAND ----------

health = JobsHealthRules(rules = [JobsHealthRule(metric = JobsHealthMetric.RUN_DURATION_SECONDS, 
                                        op = JobsHealthOperator.GREATER_THAN, 
                                        value = int(timeout_to_set_min) * 60 / 2)])

# COMMAND ----------

updated_jobs = []
for job in jobs:  
  tags = set(job.settings.tags) if job.settings.tags else set()
  tag_intersection = tags.intersection(exception_tags)
  if (len(tag_intersection) > 0 and not job.settings.timeout_seconds):
    print("Job id:", job.job_id, "Job name:", job.settings.name, "Job tags:", tags, 
          "Job timeout:", job.settings.timeout_seconds, "timeout_to_set_min", timeout_to_set_min)    
    timeout_to_set_min = int(timeout_to_set_min)
    w.jobs.update(job.job_id, new_settings = JobSettings(timeout_seconds=timeout_to_set_min * 60, health = health))
    updated_jobs.append((job.job_id, job.settings.name, timeout_to_set_min))
                  

# COMMAND ----------

(spark.createDataFrame(updated_jobs, "job_id string, job_name string, timeout_to_set_min integer")
    .withColumn("created_at", current_timestamp())
    .write
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable(event_log_table))

# COMMAND ----------

