# Databricks notebook source
# MAGIC %pip install mlflow databricks-agents databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC # Deploy HLS Agent V3 to Model Serving
# MAGIC
# MAGIC Logs the LangGraph agent as an MLflow ResponsesAgent model,
# MAGIC registers it in Unity Catalog, and deploys via `agents.deploy()`.

# COMMAND ----------

import mlflow
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

CATALOG = "sean_zhang_catalog"
SCHEMA = "gsk_india_hls"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.hls_supervisor_v3"
EXPERIMENT_ID = ""  # TODO: fill in your experiment ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Log Model to MLflow

# COMMAND ----------

if EXPERIMENT_ID:
    mlflow.set_experiment(experiment_id=EXPERIMENT_ID)

code_paths = [
    "../agent_server/agent.py",
    "../agent_server/prompts.py",
    "../agent_server/tools_hls.py",
    "../agent_server/utils.py",
    "../agent_server/utils_memory.py",
    "../agent_server/start_server.py",
    "../agent_server/__init__.py",
]

with mlflow.start_run(run_name="hls-supervisor-v3-deploy"):
    model_info = mlflow.langchain.log_model(
        lc_model="agent_server.agent",
        artifact_path="agent",
        code_paths=code_paths,
        registered_model_name=MODEL_NAME,
        pip_requirements=[
            "databricks-langchain[memory]>=0.17.0",
            "databricks-ai-bridge[agent-server]>=0.18.0",
            "databricks-sdk>=0.79.0",
            "mlflow>=3.10.1",
            "langgraph>=1.0.9",
            "langchain-mcp-adapters>=0.2.1",
            "databricks-vectorsearch",
            "python-dotenv>=1.2.1",
        ],
    )
    print(f"Model logged: {model_info.model_uri}")
    print(f"Registered as: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deploy to Model Serving Endpoint

# COMMAND ----------

import databricks.agents as agents

deployment = agents.deploy(
    model_name=MODEL_NAME,
    model_version=1,  # Update if re-deploying
    environment_vars={
        "LLM_ENDPOINT_NAME": "databricks-claude-sonnet-4-6",
        "ZINC_VS_INDEX": "sean_zhang_catalog.gsk_india_hls.zinc_vs",
        "DATABRICKS_EMBEDDING_ENDPOINT": "databricks-gte-large-en",
        # Lakebase config will be set via the serving endpoint env vars
    },
)

print(f"Deployment: {deployment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test the Endpoint

# COMMAND ----------

import json

endpoint_name = f"{MODEL_NAME.replace('.', '-')}-endpoint"

response = w.serving_endpoints.query(
    name=endpoint_name,
    input={
        "input": [
            {"role": "user", "content": "What are the latest immunotherapy trials for NSCLC?"}
        ],
        "custom_inputs": {
            "user_id": "test-user",
        },
    },
)

print(json.dumps(response.as_dict(), indent=2))
