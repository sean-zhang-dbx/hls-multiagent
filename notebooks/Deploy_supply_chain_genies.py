# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Genie Spaces
# MAGIC
# MAGIC Deploys the two vaccine supply chain Genie Spaces from the source Azure
# MAGIC workspace to the FEVM workspace. Uses SDK-based auth (no hardcoded tokens).
# MAGIC
# MAGIC **Source:** `adb-984752964297111.11.azuredatabricks.net`
# MAGIC **Target:** `fevm-sean-zhang.cloud.databricks.com`

# COMMAND ----------

import json
import requests
from typing import Optional, Dict, Any
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Tokens are injected via `dbutils.secrets` or notebook widgets.
# MAGIC Never hardcode PATs in source code.

# COMMAND ----------

dbutils.widgets.text("source_token", "", "Source Workspace PAT")
dbutils.widgets.text("target_token", "", "Target Workspace PAT")

SOURCE_HOST = "https://adb-984752964297111.11.azuredatabricks.net"
TARGET_HOST = "https://fevm-sean-zhang.cloud.databricks.com"
TARGET_WAREHOUSE_ID = "16ad82bead3f55b5"

source_token = dbutils.widgets.get("source_token")
target_token = dbutils.widgets.get("target_token")

if not source_token or not target_token:
    raise ValueError(
        "Provide source_token and target_token via notebook widgets or "
        "dbutils.widgets.text() before running."
    )

VACCINE_MANUFACTURING_LOGISTICS = "01f0f611091d1e188a815692868831e2"
VACCINE_COUNTRY_IMMUNIZATION = "01f0f61201bb15adba15def0e3fe4584"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployer

# COMMAND ----------

@dataclass
class WorkspaceConfig:
    workspace_url: str
    token: str
    warehouse_id: str

    def __post_init__(self):
        if not self.workspace_url.startswith("https://"):
            raise ValueError("workspace_url must start with https://")
        if not self.token:
            raise ValueError("token cannot be empty")
        if not self.warehouse_id:
            raise ValueError("warehouse_id cannot be empty")


@dataclass
class GenieSpaceDeploymentConfig:
    space_id: str
    source_workspace: WorkspaceConfig
    target_workspace: WorkspaceConfig
    new_title: Optional[str] = None
    new_description: Optional[str] = None
    parent_path: Optional[str] = None


class GenieSpaceDeployer:
    """Deploy Genie spaces between Databricks workspaces."""

    def __init__(self):
        self.session = requests.Session()

    def get_serialized_space(
        self, workspace_config: WorkspaceConfig, space_id: str,
    ) -> Dict[str, Any]:
        url = f"{workspace_config.workspace_url}/api/2.0/genie/spaces/{space_id}"
        params = {"include_serialized_space": "true"}
        headers = {"Authorization": f"Bearer {workspace_config.token}"}

        response = self.session.get(url, headers=headers, params=params)
        response.raise_for_status()

        space_data = response.json()
        print(f"  Retrieved space '{space_data.get('title')}' (ID: {space_id})")
        return space_data

    def _prepare_create_payload(
        self,
        space_data: Dict[str, Any],
        target_config: WorkspaceConfig,
        new_title: Optional[str] = None,
        new_description: Optional[str] = None,
        parent_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "title": new_title if new_title is not None else space_data.get("title"),
            "warehouse_id": target_config.warehouse_id,
            "serialized_space": space_data.get("serialized_space"),
        }

        if space_data.get("description"):
            payload["description"] = (
                new_description if new_description is not None
                else space_data.get("description")
            )

        if parent_path:
            payload["parent_path"] = parent_path

        if not payload["serialized_space"]:
            raise ValueError("Source space has no serialized_space configuration")
        if not isinstance(payload["serialized_space"], str):
            raise ValueError("serialized_space must be a JSON string, not an object")

        return payload

    def create_space_in_target(
        self, payload: Dict[str, Any], workspace_config: WorkspaceConfig,
    ) -> Dict[str, Any]:
        url = f"{workspace_config.workspace_url}/api/2.0/genie/spaces"
        headers = {
            "Authorization": f"Bearer {workspace_config.token}",
            "Content-Type": "application/json",
        }

        response = self.session.post(url, headers=headers, json=payload)
        response.raise_for_status()

        created_space = response.json()
        print(f"  Created space '{created_space.get('title')}' -> ID: {created_space.get('space_id')}")
        return created_space

    def deploy_space(self, config: GenieSpaceDeploymentConfig) -> Dict[str, Any]:
        print(f"\n{'='*60}")
        print(f"Deploying: {config.space_id}")
        print(f"{'='*60}")

        print("[1/3] Retrieving from source...")
        source_space = self.get_serialized_space(config.source_workspace, config.space_id)

        print("[2/3] Preparing payload...")
        create_payload = self._prepare_create_payload(
            source_space, config.target_workspace,
            new_title=config.new_title, new_description=config.new_description,
            parent_path=config.parent_path,
        )
        print(f"  Title: {create_payload['title']}")
        print(f"  Warehouse: {create_payload['warehouse_id']}")

        print("[3/3] Creating in target...")
        new_space = self.create_space_in_target(create_payload, config.target_workspace)

        print(f"Deployment complete.")
        return new_space

    def deploy_multiple_spaces(
        self, configs: list[GenieSpaceDeploymentConfig],
    ) -> list[Dict[str, Any]]:
        results = []
        for config in configs:
            try:
                result = self.deploy_space(config)
                results.append(result)
            except Exception as e:
                print(f"\n  Error deploying space {config.space_id}: {str(e)}")
                results.append({"error": str(e), "space_id": config.space_id})
        return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy

# COMMAND ----------

source_ws = WorkspaceConfig(
    workspace_url=SOURCE_HOST,
    token=source_token,
    warehouse_id="862f1d757f0424f7",
)

target_ws = WorkspaceConfig(
    workspace_url=TARGET_HOST,
    token=target_token,
    warehouse_id=TARGET_WAREHOUSE_ID,
)

deployment_configs = [
    GenieSpaceDeploymentConfig(
        space_id=VACCINE_MANUFACTURING_LOGISTICS,
        source_workspace=source_ws,
        target_workspace=target_ws,
        new_title="HLS - Vaccine Manufacturing Logistics",
        new_description="Vaccine supply chain manufacturing & logistics Genie Space",
    ),
    GenieSpaceDeploymentConfig(
        space_id=VACCINE_COUNTRY_IMMUNIZATION,
        source_workspace=source_ws,
        target_workspace=target_ws,
        new_title="HLS - Vaccine Country Immunization",
        new_description="Vaccine supply chain country immunization & procurement Genie Space",
    ),
]

deployer = GenieSpaceDeployer()
results = deployer.deploy_multiple_spaces(deployment_configs)

print("\n" + "=" * 60)
print("DEPLOYMENT SUMMARY")
print("=" * 60)
for i, result in enumerate(results, 1):
    if "error" in result:
        print(f"{i}. FAIL - Space {result['space_id']}: {result['error']}")
    else:
        print(f"{i}. OK   - {result.get('title')} (ID: {result.get('space_id')})")
print("=" * 60)
