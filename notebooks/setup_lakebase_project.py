# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Lakebase Autoscaling Project for HLS Agent V3
# MAGIC
# MAGIC Provisions a Lakebase Autoscaling project for short-term (LangGraph checkpoints)
# MAGIC and long-term (user memories via embedding search) agent memory.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PROJECT_NAME = "hls-agent-v3"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Autoscaling Project

# COMMAND ----------

existing_projects = list(w.postgres.list_projects())
project = None
for p in existing_projects:
    if p.name == PROJECT_NAME:
        project = p
        print(f"Project '{PROJECT_NAME}' already exists: {p.project_id}")
        break

if project is None:
    project = w.postgres.create_project(
        name=PROJECT_NAME,
        pg_version="17",
    )
    print(f"Created project '{PROJECT_NAME}': {project.project_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Connection Details

# COMMAND ----------

project_details = w.postgres.get_project(project.project_id)
print(f"Project ID:  {project_details.project_id}")
print(f"Project:     {project_details.name}")
print(f"PG Version:  {project_details.pg_version}")
print(f"State:       {project_details.state}")

branches = list(w.postgres.list_branches(project.project_id))
for b in branches:
    print(f"\nBranch: {b.name}")
    print(f"  Branch ID: {b.branch_id}")
    endpoints = list(w.postgres.list_endpoints(project.project_id, b.branch_id))
    for ep in endpoints:
        print(f"  Endpoint: {ep.name} (ID: {ep.endpoint_id})")
        print(f"  Host: {ep.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Environment Variables
# MAGIC
# MAGIC Copy the endpoint name to your `.env` file:
# MAGIC ```
# MAGIC LAKEBASE_AUTOSCALING_ENDPOINT=<endpoint-name-from-above>
# MAGIC ```
# MAGIC
# MAGIC Or for DABs deployment, update `databricks.yml` with the postgres resource.
