"""HLS-specific tools: ECFP4 fingerprint computation + ZINC chemical similarity search.

Follows the AiChemy bitstring pattern:
  1. get_embedding(smiles) -> 1024-char bitstring  (runs RDKit locally in the agent process)
  2. search_similar_chemicals(bitstring) -> ZINC vector search results via VS SDK

RDKit is not available in the UC Python UDF sandbox, so get_embedding runs in-process
rather than as a UC function. Both tools are exposed to the LLM which chains them naturally.
"""

import logging
import os
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
from langchain_core.tools import tool
from rdkit.Chem import AllChem, MolFromSmiles

logger = logging.getLogger(__name__)

VS_INDEX_NAME = os.getenv(
    "ZINC_VS_INDEX", "sean_zhang_catalog.gsk_india_hls.zinc_vs"
)
VS_ENDPOINT_NAME = os.getenv("ZINC_VS_ENDPOINT", "hls-agent-vs-endpoint")
VS_COLUMNS = ["zinc_id", "smiles", "mwt", "logp", "purchasable"]
VS_NUM_RESULTS = 5

_fpgen = AllChem.GetMorganGenerator(radius=2, fpSize=1024)
_vs_index = None
_ws_client = None


def _get_vs_index():
    """Lazy-init the Vector Search index object using the workspace client for auth."""
    global _vs_index
    if _vs_index is None:
        ws = _ws_client or WorkspaceClient()
        vs_kwargs = {"workspace_url": ws.config.host}
        sp_id = os.getenv("DATABRICKS_CLIENT_ID")
        sp_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
        pat = os.getenv("DATABRICKS_TOKEN")
        if sp_id and sp_secret:
            vs_kwargs["service_principal_client_id"] = sp_id
            vs_kwargs["service_principal_client_secret"] = sp_secret
        elif pat:
            vs_kwargs["personal_access_token"] = pat
        client = VectorSearchClient(**vs_kwargs)
        _vs_index = client.get_index(
            endpoint_name=VS_ENDPOINT_NAME,
            index_name=VS_INDEX_NAME,
        )
        logger.info("Connected to VS index: %s on endpoint %s", VS_INDEX_NAME, VS_ENDPOINT_NAME)
    return _vs_index


def hls_tools(workspace_client: Optional[WorkspaceClient] = None):
    """Return list of HLS-specific tools (fingerprint computation + chemical search)."""
    global _ws_client
    if workspace_client:
        _ws_client = workspace_client

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

        query_vector = [float(c) for c in bitstring]
        try:
            index = _get_vs_index()
            results = index.similarity_search(
                query_vector=query_vector,
                columns=VS_COLUMNS,
                num_results=num_results,
            )
            rows = results.get("result", {}).get("data_array", [])
            return [dict(zip(VS_COLUMNS, row)) for row in rows]
        except Exception as e:
            logger.error("ZINC vector search failed: %s", e)
            return [{"error": f"Vector search failed: {e}"}]

    return [get_embedding, search_similar_chemicals]
