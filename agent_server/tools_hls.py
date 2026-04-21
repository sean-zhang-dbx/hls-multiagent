"""HLS-specific tools: chemical similarity search via ZINC vector index (AiChemy pattern)."""

import logging
import os
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks_langchain import DatabricksEmbeddings, VectorSearchRetrieverTool
from langchain_core.tools import tool

logger = logging.getLogger(__name__)

VS_INDEX_NAME = os.getenv(
    "ZINC_VS_INDEX", "sean_zhang_catalog.gsk_india_hls.zinc_vs"
)
VS_COLUMNS = ["zinc_id", "smiles", "mwt", "logp", "purchasable"]
VS_TEXT_COLUMN = "smiles"
VS_NUM_RESULTS = 5

_retriever_tool: Optional[VectorSearchRetrieverTool] = None


def _get_retriever_tool(workspace_client: Optional[WorkspaceClient] = None) -> VectorSearchRetrieverTool:
    """Lazy-init the VectorSearchRetrieverTool for ZINC."""
    global _retriever_tool
    if _retriever_tool is None:
        ws = workspace_client or WorkspaceClient()
        _retriever_tool = VectorSearchRetrieverTool(
            index_name=VS_INDEX_NAME,
            num_results=VS_NUM_RESULTS,
            columns=VS_COLUMNS,
            text_column=VS_TEXT_COLUMN,
            tool_name="zinc_molecular_search",
            tool_description="Search ZINC database for drug-like molecules by ECFP4 fingerprint",
            embedding=DatabricksEmbeddings(endpoint="databricks-bge-large-en"),
            workspace_client=ws,
        )
    return _retriever_tool


def hls_tools(workspace_client: Optional[WorkspaceClient] = None):
    """Return list of HLS-specific tools (chemical search)."""

    retriever = _get_retriever_tool(workspace_client)

    @tool
    def search_similar_chemicals(bitstring: str, num_results: int = 5) -> list[dict]:
        """Search for similar molecules in ZINC based on ECFP4 fingerprint.

        Required input is a 1024-char bitstring (e.g. 1011...00) from get_embedding.
        Returns matching compounds with zinc_id, smiles, mwt, logp, and purchasable status.
        """
        if len(bitstring) != 1024 or not all(c in "01" for c in bitstring):
            return [{"error": f"Invalid bitstring: expected 1024 chars of 0/1, got {len(bitstring)} chars"}]

        query_vector = [int(c) for c in bitstring]
        docs = retriever._vector_store.similarity_search_by_vector(
            query_vector, k=num_results
        )
        return [doc.metadata | {VS_TEXT_COLUMN: doc.page_content} for doc in docs]

    return [search_similar_chemicals]
