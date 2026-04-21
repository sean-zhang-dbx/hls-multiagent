"""HLS-specific tools: ECFP4 fingerprint computation + ZINC chemical similarity search.

Follows the AiChemy bitstring pattern:
  1. get_embedding(smiles) -> 1024-char bitstring  (runs RDKit locally in the agent process)
  2. search_similar_chemicals(bitstring) -> ZINC vector search results

RDKit is not available in the UC Python UDF sandbox, so get_embedding runs in-process
rather than as a UC function. Both tools are exposed to the LLM which chains them naturally.
"""

import logging
import os
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks_langchain import DatabricksEmbeddings, VectorSearchRetrieverTool
from langchain_core.tools import tool
from rdkit.Chem import AllChem, MolFromSmiles

logger = logging.getLogger(__name__)

VS_INDEX_NAME = os.getenv(
    "ZINC_VS_INDEX", "sean_zhang_catalog.gsk_india_hls.zinc_vs"
)
VS_COLUMNS = ["zinc_id", "smiles", "mwt", "logp", "purchasable"]
VS_TEXT_COLUMN = "smiles"
VS_NUM_RESULTS = 5

_fpgen = AllChem.GetMorganGenerator(radius=2, fpSize=1024)
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
    """Return list of HLS-specific tools (fingerprint computation + chemical search)."""

    retriever = _get_retriever_tool(workspace_client)

    @tool
    def get_embedding(smiles: str) -> str:
        """Compute ECFP4 molecular fingerprint as a 1024-char bitstring from a SMILES string.

        Use this to convert a chemical structure (SMILES notation) into a fingerprint
        that can be passed to search_similar_chemicals. Also useful standalone for
        comparing two molecules' fingerprints directly.

        Args:
            smiles: A valid SMILES string (e.g. 'CC(=O)Oc1ccccc1C(=O)O' for aspirin)

        Returns:
            A 1024-character string of 0s and 1s representing the ECFP4 fingerprint,
            or an error message if the SMILES is invalid.
        """
        mol = MolFromSmiles(smiles)
        if mol is None:
            return f"Error: Invalid SMILES string: {smiles}"
        fp = _fpgen.GetFingerprintAsNumPy(mol)
        return "".join(str(int(x)) for x in fp)

    @tool
    def search_similar_chemicals(bitstring: str, num_results: int = 5) -> list[dict]:
        """Search for similar molecules in ZINC based on ECFP4 fingerprint.

        Required input is a 1024-char bitstring (e.g. '1011...00') from get_embedding.
        Returns matching compounds with zinc_id, smiles, mwt, logp, and purchasable status.

        Two-step workflow: call get_embedding(smiles) first, then pass the result here.
        """
        if len(bitstring) != 1024 or not all(c in "01" for c in bitstring):
            return [{"error": f"Invalid bitstring: expected 1024 chars of 0/1, got {len(bitstring)} chars"}]

        query_vector = [int(c) for c in bitstring]
        docs = retriever._vector_store.similarity_search_by_vector(
            query_vector, k=num_results
        )
        return [doc.metadata | {VS_TEXT_COLUMN: doc.page_content} for doc in docs]

    return [get_embedding, search_similar_chemicals]
