# Databricks notebook source
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

VACCINE_MANUFACTURING_LOGISTICS = "01f0f611091d1e188a815692868831e2"
VACCINE_COUNTRY_IMMUNIZATION = "01f0f61201bb15adba15def0e3fe4584"

def get_serialized_space(space_id):
  return w.genie._api.do("GET", f"/api/2.0/genie/spaces/{space_id}?include_serialized_space=true")

manufacturing_space = get_serialized_space(VACCINE_MANUFACTURING_LOGISTICS)
immunization_space = get_serialized_space(VACCINE_COUNTRY_IMMUNIZATION)

# COMMAND ----------

# DBTITLE 1,Cell 3
# Configure target workspace credentials
target_host = "https://e2-demo-field-eng.cloud.databricks.com/"  # Replace with target AWS workspace URL
target_token = "<REDACTED>"  # Replace with target workspace PAT

# Create client for target workspace
target_workspace = WorkspaceClient(host=target_host, token=target_token)

def deploy_space(space, target_client):
    """Deploy a serialized Genie space to the target workspace"""
    target_client.genie._api.do("POST", "/api/2.0/genie/spaces", json=space)
    print(f"Deployed space: {space.get('display_name', 'Unknown')}")

# Deploy both spaces to target workspace
deploy_space(manufacturing_space, target_workspace)
deploy_space(immunization_space, target_workspace)

# COMMAND ----------

import json
import requests
from typing import Optional, Dict, Any
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient


@dataclass
class WorkspaceConfig:
    """Configuration for a Databricks workspace."""
    workspace_url: str
    token: str
    warehouse_id: str
    
    def __post_init__(self):
        """Validate workspace configuration."""
        if not self.workspace_url.startswith("https://"):
            raise ValueError("workspace_url must start with https://")
        if not self.token:
            raise ValueError("token cannot be empty")
        if not self.warehouse_id:
            raise ValueError("warehouse_id cannot be empty")


@dataclass
class GenieSpaceDeploymentConfig:
    """Configuration for deploying a Genie space."""
    space_id: str
    source_workspace: WorkspaceConfig
    target_workspace: WorkspaceConfig
    new_title: Optional[str] = None  # Optional: overrides source title
    new_description: Optional[str] = None  # Optional: overrides source description
    parent_path: Optional[str] = None  # Optional: folder where space should be created


class GenieSpaceDeployer:
    """Deploy Genie spaces between Databricks workspaces."""
    
    def __init__(self):
        self.session = requests.Session()
    
    def get_serialized_space(
        self,
        workspace_config: WorkspaceConfig,
        space_id: str,
    ) -> Dict[str, Any]:
        """
        Retrieve the serialized configuration of a Genie space from source workspace.
        
        Args:
            workspace_config: Source workspace configuration
            space_id: ID of the space to export
            
        Returns:
            Dictionary containing space metadata and serialized_space JSON string
        """
        url = f"{workspace_config.workspace_url}/api/2.0/genie/spaces/{space_id}"
        params = {"include_serialized_space": "true"}
        headers = {"Authorization": f"Bearer {workspace_config.token}"}
        
        response = self.session.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        space_data = response.json()
        print(f"✓ Retrieved space '{space_data.get('title')}' (ID: {space_id})")
        
        return space_data
    
    def _prepare_create_payload(
        self,
        space_data: Dict[str, Any],
        target_config: WorkspaceConfig,
        new_title: Optional[str] = None,
        new_description: Optional[str] = None,
        parent_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Transform the get_space response into a valid create_space request payload.
        
        Args:
            space_data: Response from get_space API call
            target_config: Target workspace configuration
            new_title: Override title (optional, defaults to source title)
            new_description: Override description (optional, defaults to source description)
            parent_path: Parent folder path where space should be created (optional)
            
        Returns:
            Formatted payload for create_space API
        """
        # Create the payload with required fields
        # Use source title/description by default, only override if explicitly provided
        payload = {
            "title": new_title if new_title is not None else space_data.get("title"),
            "warehouse_id": target_config.warehouse_id,
            "serialized_space": space_data.get("serialized_space"),
        }
        
        # Add description if present in source
        if space_data.get("description"):
            payload["description"] = new_description if new_description is not None else space_data.get("description")
        
        # Add optional parent_path only if provided
        if parent_path:
            payload["parent_path"] = parent_path
        
        # Validate that serialized_space exists and is a string
        if not payload["serialized_space"]:
            raise ValueError("Source space has no serialized_space configuration")
        
        if not isinstance(payload["serialized_space"], str):
            raise ValueError("serialized_space must be a JSON string, not an object")
        
        return payload
    
    def create_space_in_target(
        self,
        payload: Dict[str, Any],
        workspace_config: WorkspaceConfig,
    ) -> Dict[str, Any]:
        """
        Create a new Genie space in the target workspace.
        
        Args:
            payload: Formatted payload for create_space API
            workspace_config: Target workspace configuration
            
        Returns:
            Response from create_space API call
        """
        url = f"{workspace_config.workspace_url}/api/2.0/genie/spaces"
        headers = {
            "Authorization": f"Bearer {workspace_config.token}",
            "Content-Type": "application/json",
        }
        
        response = self.session.post(url, headers=headers, json=payload)
        response.raise_for_status()
        
        created_space = response.json()
        print(f"✓ Created space '{created_space.get('title')}' in target workspace")
        print(f"  New space ID: {created_space.get('space_id')}")
        
        return created_space
    
    def deploy_space(
        self,
        config: GenieSpaceDeploymentConfig,
    ) -> Dict[str, Any]:
        """
        Deploy a single Genie space from source to target workspace.
        
        Args:
            config: Deployment configuration
            
        Returns:
            Response from create_space API with new space details
        """
        print(f"\n{'='*60}")
        print(f"Deploying Genie Space: {config.space_id}")
        print(f"{'='*60}")
        
        # Step 1: Get the space from source workspace
        print(f"\n[1/3] Retrieving space from source workspace...")
        source_space = self.get_serialized_space(
            config.source_workspace,
            config.space_id,
        )
        
        # Step 2: Prepare the create payload
        print(f"\n[2/3] Preparing create payload...")
        create_payload = self._prepare_create_payload(
            source_space,
            config.target_workspace,
            new_title=config.new_title,
            new_description=config.new_description,
            parent_path=config.parent_path,
        )
        print(f"✓ Payload prepared")
        print(f"  Title: {create_payload['title']}")
        print(f"  Description: {create_payload.get('description', '(none)')}")
        print(f"  Warehouse ID: {create_payload['warehouse_id']}")
        if "parent_path" in create_payload:
            print(f"  Parent path: {create_payload['parent_path']}")
        print(f"  Serialized space size: {len(create_payload['serialized_space'])} chars")
        
        # Step 3: Create the space in target workspace
        print(f"\n[3/3] Creating space in target workspace...")
        new_space = self.create_space_in_target(
            create_payload,
            config.target_workspace,
        )
        
        print(f"\n{'='*60}")
        print(f"✓ Deployment completed successfully!")
        print(f"{'='*60}\n")
        
        return new_space
    
    def deploy_multiple_spaces(
        self,
        configs: list[GenieSpaceDeploymentConfig],
    ) -> list[Dict[str, Any]]:
        """
        Deploy multiple Genie spaces.
        
        Args:
            configs: List of deployment configurations
            
        Returns:
            List of created space responses
        """
        results = []
        for config in configs:
            try:
                result = self.deploy_space(config)
                results.append(result)
            except Exception as e:
                print(f"\n✗ Error deploying space {config.space_id}: {str(e)}")
                results.append({"error": str(e), "space_id": config.space_id})
        
        return results

# COMMAND ----------

# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    # Define source and target workspace configurations
    SOURCE_WORKSPACE = WorkspaceConfig(
        workspace_url="https://adb-984752964297111.11.azuredatabricks.net",
        token="<REDACTED>",
        warehouse_id="862f1d757f0424f7",  # Not used for get, but kept for consistency
    )
    
    TARGET_WORKSPACE = WorkspaceConfig(
        workspace_url="https://e2-demo-field-eng.cloud.databricks.com",
        token="<REDACTED>",
        warehouse_id="862f1d757f0424f7",  # Required for creating the space
    )
    
    # Space IDs to deploy
    VACCINE_MANUFACTURING_LOGISTICS = "01f0f611091d1e188a815692868831e2"
    VACCINE_COUNTRY_IMMUNIZATION = "01f0f61201bb15adba15def0e3fe4584"
    
    # Configure deployments
    deployment_configs = [
        GenieSpaceDeploymentConfig(
            space_id=VACCINE_MANUFACTURING_LOGISTICS,
            source_workspace=SOURCE_WORKSPACE,
            target_workspace=TARGET_WORKSPACE,
            new_title="HLS - Vaccine Manufacturing Logistics",
            new_description="Imported from production workspace"
        ),
        GenieSpaceDeploymentConfig(
            space_id=VACCINE_COUNTRY_IMMUNIZATION,
            source_workspace=SOURCE_WORKSPACE,
            target_workspace=TARGET_WORKSPACE,
            new_title="[Imported] Vaccine Country Immunization",
            new_description="Imported from production workspace"
        ),
    ]
    
    # Deploy spaces
    deployer = GenieSpaceDeployer()
    results = deployer.deploy_multiple_spaces(deployment_configs)
    
    # Print summary
    print("\n" + "="*60)
    print("DEPLOYMENT SUMMARY")
    print("="*60)
    for i, result in enumerate(results, 1):
        if "error" in result:
            print(f"{i}. ✗ Space {result['space_id']}: {result['error']}")
        else:
            print(f"{i}. ✓ Space created: {result.get('title')} (ID: {result.get('space_id')})")
    print("="*60 + "\n")


# COMMAND ----------

print(manufacturing_space)

# COMMAND ----------

print(immunization_space)