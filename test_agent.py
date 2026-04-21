"""End-to-end agent test: verify each tool works, then run multi-tool conversations."""

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).parent / ".env", override=True)

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("test_agent")


async def test_1_rdkit_fingerprint():
    """Test: get_embedding tool computes ECFP4 from SMILES."""
    from rdkit.Chem import AllChem, MolFromSmiles
    fpgen = AllChem.GetMorganGenerator(radius=2, fpSize=1024)
    mol = MolFromSmiles("CC(=O)Oc1ccccc1C(=O)O")  # aspirin
    assert mol is not None, "Failed to parse aspirin SMILES"
    fp = fpgen.GetFingerprintAsNumPy(mol)
    bitstring = "".join(str(int(x)) for x in fp)
    assert len(bitstring) == 1024, f"Expected 1024 bits, got {len(bitstring)}"
    assert set(bitstring) <= {"0", "1"}, "Bitstring contains non-binary chars"
    logger.info("PASS: RDKit fingerprint (aspirin -> %d bits, %d set)", len(bitstring), bitstring.count("1"))
    return bitstring


async def test_2_vector_search(aspirin_bitstring: str):
    """Test: search_similar_chemicals queries ZINC VS index."""
    from databricks.vector_search.client import VectorSearchClient
    client = VectorSearchClient()
    index = client.get_index(
        endpoint_name=os.getenv("ZINC_VS_ENDPOINT", "hls-agent-vs-endpoint"),
        index_name=os.getenv("ZINC_VS_INDEX", "sean_zhang_catalog.gsk_india_hls.zinc_vs"),
    )
    query_vector = [float(c) for c in aspirin_bitstring]
    results = index.similarity_search(
        query_vector=query_vector,
        columns=["zinc_id", "smiles", "mwt", "logp", "purchasable"],
        num_results=3,
    )
    rows = results.get("result", {}).get("data_array", [])
    assert len(rows) > 0, "Vector search returned no results"
    logger.info("PASS: Vector search returned %d results:", len(rows))
    for row in rows:
        logger.info("  %s", row)


async def test_3_lakebase_connection():
    """Test: Lakebase Autoscaling checkpoint/store can connect."""
    from agent_server.utils_memory import init_lakebase_config, lakebase_context
    config = init_lakebase_config()
    logger.info("Lakebase config: endpoint=%s", config.autoscaling_endpoint)
    async with lakebase_context(config) as (checkpointer, store):
        await checkpointer.setup()
        await store.setup()
        logger.info("PASS: Lakebase connection + setup succeeded")


async def test_4_llm_endpoint():
    """Test: LLM endpoint responds."""
    from databricks_langchain import ChatDatabricks
    model = ChatDatabricks(endpoint=os.getenv("LLM_ENDPOINT_NAME", "databricks-claude-sonnet-4-6"))
    resp = await model.ainvoke("Say 'hello' and nothing else.")
    text = resp.content
    assert "hello" in text.lower(), f"Unexpected LLM response: {text}"
    logger.info("PASS: LLM endpoint responded: %s", text[:100])


async def test_5_mcp_tools():
    """Test: MCP client can discover tools from UC functions and Genie spaces."""
    from databricks.sdk import WorkspaceClient
    from agent_server.utils import init_mcp_client
    ws = WorkspaceClient()
    mcp_client = init_mcp_client(ws)
    tools = await mcp_client.get_tools()
    tool_names = [t.name for t in tools]
    logger.info("PASS: MCP discovered %d tools: %s", len(tools), tool_names)
    return tool_names


async def test_6_full_agent_invoke():
    """Test: full agent can handle a simple query end-to-end."""
    from agent_server.utils_memory import init_lakebase_config, lakebase_context
    from agent_server.agent import init_agent

    config = init_lakebase_config()
    async with lakebase_context(config) as (checkpointer, store):
        agent = await init_agent(store=store, checkpointer=checkpointer)
        result = await agent.ainvoke(
            {"messages": [{"role": "user", "content": "What is the current time?"}]},
            config={"configurable": {"thread_id": "test-time-1", "user_id": "test-user", "store": store}},
        )
        msgs = result.get("messages", [])
        last_msg = msgs[-1].content if msgs else "NO RESPONSE"
        logger.info("PASS: Agent responded to 'What is the current time?': %s", last_msg[:200])


async def test_7_chemical_search_conversation():
    """Test: agent chains get_embedding -> search_similar_chemicals for drug discovery."""
    from agent_server.utils_memory import init_lakebase_config, lakebase_context
    from agent_server.agent import init_agent

    config = init_lakebase_config()
    async with lakebase_context(config) as (checkpointer, store):
        agent = await init_agent(store=store, checkpointer=checkpointer)
        result = await agent.ainvoke(
            {"messages": [{"role": "user", "content": "Find molecules similar to aspirin (SMILES: CC(=O)Oc1ccccc1C(=O)O) in the ZINC database."}]},
            config={"configurable": {"thread_id": "test-chem-1", "user_id": "test-user", "store": store}},
        )
        msgs = result.get("messages", [])
        tool_calls = [m for m in msgs if hasattr(m, "tool_calls") and m.tool_calls]
        logger.info("PASS: Chemical search conversation completed. %d tool-call messages, final:", len(tool_calls))
        if msgs:
            logger.info("  %s", msgs[-1].content[:300] if hasattr(msgs[-1], "content") else str(msgs[-1])[:300])


async def test_8_memory_conversation():
    """Test: agent saves and retrieves user memory via Lakebase."""
    from agent_server.utils_memory import init_lakebase_config, lakebase_context
    from agent_server.agent import init_agent

    config = init_lakebase_config()
    async with lakebase_context(config) as (checkpointer, store):
        agent = await init_agent(store=store, checkpointer=checkpointer)

        result = await agent.ainvoke(
            {"messages": [{"role": "user", "content": "Remember that my primary research focus is NSCLC immunotherapy with checkpoint inhibitors."}]},
            config={"configurable": {"thread_id": "test-mem-1", "user_id": "test-memory-user", "store": store}},
        )
        logger.info("Save memory response: %s", result["messages"][-1].content[:200] if result.get("messages") else "NO RESPONSE")

        result2 = await agent.ainvoke(
            {"messages": [{"role": "user", "content": "What do you remember about my research interests?"}]},
            config={"configurable": {"thread_id": "test-mem-2", "user_id": "test-memory-user", "store": store}},
        )
        logger.info("PASS: Memory recall response: %s", result2["messages"][-1].content[:300] if result2.get("messages") else "NO RESPONSE")


async def main():
    tests = [
        ("1. RDKit Fingerprint", test_1_rdkit_fingerprint),
        ("2. Vector Search", None),
        ("3. Lakebase Connection", test_3_lakebase_connection),
        ("4. LLM Endpoint", test_4_llm_endpoint),
        ("5. MCP Tools Discovery", test_5_mcp_tools),
        ("6. Full Agent - Time Query", test_6_full_agent_invoke),
        ("7. Full Agent - Chemical Search", test_7_chemical_search_conversation),
        ("8. Full Agent - Memory Save/Recall", test_8_memory_conversation),
    ]

    aspirin_bits = None
    for name, test_fn in tests:
        if name == "2. Vector Search":
            if aspirin_bits is None:
                logger.warning("SKIP: %s (no fingerprint from test 1)", name)
                continue
            test_fn = lambda: test_2_vector_search(aspirin_bits)

        logger.info("=" * 60)
        logger.info("TEST: %s", name)
        logger.info("=" * 60)
        try:
            result = await test_fn()
            if name == "1. RDKit Fingerprint":
                aspirin_bits = result
        except Exception:
            logger.exception("FAIL: %s", name)

    logger.info("=" * 60)
    logger.info("ALL TESTS COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
