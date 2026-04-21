# HLS Multi-Agent V3

LangGraph-based HLS Research Supervisor with Lakebase Autoscaling memory and ZINC chemical similarity search.

Built on the [agent-langgraph-advanced](https://github.com/databricks/app-templates/tree/main/agent-langgraph-advanced) template.

## Architecture

**LLM**: Claude Sonnet 4.6 (via `databricks-claude-sonnet-4-6`). Swap to Opus 4.6 by changing `LLM_ENDPOINT_NAME`.

**Tools** (wired as individual MCP servers + custom tools):
- Cancer Research Literature (Knowledge Assistant)
- Cancer Incidence Analytics (Genie Space)
- Clinical Trials Analytics (Genie Space)
- Clinical Alerts (UC Function: `send_clinical_alert`)
- Web Search (UC Function: `tavily_web_search`)
- Chemical Fingerprint (UC Function: `get_embedding` - SMILES to ECFP4 bitstring)
- Chemical Similarity Search (custom `@tool`: bitstring to ZINC Vector Search)
- Memory (get/save/delete user memories via Lakebase)

**Memory** (Lakebase Autoscaling):
- Short-term: `AsyncCheckpointSaver` (LangGraph conversation state)
- Long-term: `AsyncDatabricksStore` (user preferences, research context)

**Deployment**:
- Databricks App (primary): Chat UI + REST API via `LongRunningAgentServer`
- Model Serving (secondary): `ResponsesAgent` endpoint via `agents.deploy()`

## Setup

### 1. Provision Infrastructure (run notebooks on Databricks)

```bash
# In order:
notebooks/setup_lakebase_project.py    # Creates Lakebase Autoscaling project
notebooks/setup_uc_function.py         # Registers get_embedding UC function
notebooks/setup_zinc_vector_search.py  # Creates ZINC15 Delta table + VS index
```

### 2. Configure Environment

```bash
cp .env.example .env
# Fill in: LAKEBASE_AUTOSCALING_ENDPOINT, MLFLOW_EXPERIMENT_ID
```

### 3. Run Locally

```bash
uv run start-server
```

### 4. Deploy as Databricks App

```bash
databricks bundle deploy --target dev
databricks bundle run hls_multiagent --target dev
```

### 5. Deploy as Model Serving Endpoint

Run `notebooks/deploy_model_serving.py` on Databricks.

## Chemical Search Workflow

Two-step pattern (following [AiChemy](https://github.com/databricks-industry-solutions/aichemy)):

1. Agent calls `get_embedding(smiles)` UC function -> returns 1024-char ECFP4 bitstring
2. Agent calls `search_similar_chemicals(bitstring)` -> queries ZINC Vector Search index

Example: "Find molecules similar to aspirin" triggers:
- `get_embedding("CC(=O)Oc1ccccc1C(=O)O")` -> `"101100...001"`
- `search_similar_chemicals("101100...001")` -> top-5 similar compounds from ZINC
