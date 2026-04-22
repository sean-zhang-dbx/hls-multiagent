# HLS Multi-Agent V3 - Demo Walkthrough Guide

## App URL

**Live App**: https://hls-multiagent-v3-7474654216647582.aws.databricksapps.com

**Workspace**: https://fevm-sean-zhang.cloud.databricks.com

**GitHub**: https://github.com/sean-zhang-dbx/hls-multiagent

---

## Architecture at a Glance

```
User (Chat UI or API)
   |
   v
LangGraph Agent (Claude Sonnet 4.6)
   |
   +-- get_current_time          (built-in)
   +-- get_embedding             (local RDKit, ECFP4 fingerprints)
   +-- search_similar_chemicals  (Vector Search SDK, ZINC 250K)
   +-- get/save/delete_user_memory (Lakebase Autoscaling, long-term)
   +-- send_clinical_alert       (UC function, emails via MCP)
   +-- tavily_web_search         (UC function, live web via MCP)
   +-- query Genie Space #1      (cancer incidence analytics via MCP)
   +-- query Genie Space #2      (clinical trials analytics via MCP)
```

**Memory**: Lakebase Autoscaling (project `hls-agent-v3`, branch `production`)
- Short-term: LangGraph checkpoints (conversation state across turns)
- Long-term: Embedding-based store (user preferences persisted across sessions)

---

## Demo Script (5 Scenarios)

### Scenario 1: Simple Tool Use — Time Query
**Goal**: Show the agent uses basic tools and responds naturally.

> **Ask**: "What time is it?"

**Expected**: Agent calls `get_current_time` and responds with a formatted date/time.

**Talking point**: "The agent has access to a set of tools and decides which one to use based on the query. This is a simple example — let's see something more interesting."

---

### Scenario 2: Chemical Similarity Search — Two-Step Tool Chaining
**Goal**: Show the agent autonomously chains two tools (fingerprint -> vector search).

> **Ask**: "Find molecules similar to aspirin (SMILES: CC(=O)Oc1ccccc1C(=O)O) in the ZINC database."

**Expected**: Agent:
1. Calls `get_embedding` with the aspirin SMILES → gets a 1024-bit ECFP4 fingerprint
2. Calls `search_similar_chemicals` with that bitstring → queries the ZINC Vector Search index
3. Returns a table of structurally similar compounds with MW, LogP, purchasability

**Talking points**:
- "The agent understands that to search for similar chemicals, it first needs to compute a molecular fingerprint using RDKit."
- "The ECFP4 fingerprint is a 1024-dimensional vector that captures molecular structure — it's the same representation used in drug discovery pipelines."
- "The Vector Search index has 250K drug-like molecules from ZINC15, indexed by their ECFP4 fingerprints for similarity matching."
- "This two-step tool chaining happens automatically — the LLM reads the tool descriptions and figures out the workflow."

**Variations to try**:
- "What about caffeine? (SMILES: CN1C=NC2=C1C(=O)N(C(=O)N2C)C)"
- "Find drugs similar to ibuprofen"

---

### Scenario 3: Genie Space Analytics — Natural Language SQL
**Goal**: Show the agent routes analytical questions to the right Genie Space.

> **Ask**: "What are the top 5 countries by lung cancer incidence rate per 100K?"

**Expected**: Agent routes to the Cancer Incidence Genie Space, which generates SQL, queries the data, and returns a ranked table.

> **Follow-up**: "Now show me the latest Phase III clinical trials for NSCLC with pembrolizumab."

**Expected**: Agent routes to the Clinical Trials Genie Space (different data source).

**Talking points**:
- "The agent has two Genie Spaces available — one for cancer epidemiology data and one for clinical trial data."
- "It decides which Genie Space to query based on the nature of the question — incidence/mortality goes to one, trial data goes to the other."
- "Under the hood, Genie translates the natural language query into SQL against curated Unity Catalog tables."

---

### Scenario 4: Live Web Search — Current Events
**Goal**: Show the agent can search the live web for real-time information.

> **Ask**: "What are the latest FDA approvals for lung cancer treatment in 2026?"

**Expected**: Agent calls `tavily_web_search` via MCP/UC function and returns current FDA approval data with specific drug names, dates, and indications.

**Talking points**:
- "This uses a UC function that calls the Tavily web search API — so the agent can access real-time information beyond its training data."
- "The results include specific approval dates, drug names, and indications — all sourced from the live web."

---

### Scenario 5: Long-Term Memory — Lakebase Autoscaling
**Goal**: Show the agent remembers user preferences across sessions.

> **Ask** (in Thread 1): "Remember that my primary research focus is HER2-positive breast cancer with novel ADC therapies."

**Expected**: Agent calls `save_user_memory` and confirms it saved the preference.

> **Ask** (in Thread 2 — new conversation): "What do you know about my research interests?"

**Expected**: Agent calls `get_user_memory`, retrieves the saved preference from Lakebase, and responds with the HER2/ADC context.

**Talking points**:
- "The agent has both short-term memory (conversation history within a session) and long-term memory (user preferences across sessions)."
- "Long-term memories are stored in Lakebase Autoscaling — Databricks' managed PostgreSQL with auto-scaling and scale-to-zero."
- "Memories are stored as embeddings, so the agent can semantically search for relevant context — not just exact string matching."
- "This enables personalized experiences: the agent can proactively use your research focus to provide more relevant answers."

---

## Advanced Demo Ideas

### Multi-Tool Query
> "I'm researching molecules similar to erlotinib (SMILES: C=Cc1cc2ncnc(Nc3ccc(OC)c(OC)c3)c2cc1OC) for NSCLC. Find similar compounds in ZINC, then tell me about the latest clinical trials for EGFR-targeted therapies."

This triggers: `get_embedding` → `search_similar_chemicals` → Genie Space (clinical trials).

### Clinical Alert
> "Send a clinical alert to sean.zhang@databricks.com about the new HER2 mutations found in today's analysis."

This triggers `send_clinical_alert` UC function (sends a real email).

### Combined Research Workflow
> "Remember that I'm tracking ADC therapies for HER2-positive breast cancer. Now search the web for the latest ASCO 2026 presentations on trastuzumab deruxtecan."

This triggers: `save_user_memory` → `tavily_web_search`.

---

## Infrastructure Behind the Scenes

| Component | Resource | Purpose |
|-----------|----------|---------|
| **LLM** | `databricks-claude-sonnet-4-6` | Agent reasoning (FMAPI) |
| **Short-term Memory** | Lakebase `hls-agent-v3/production` | LangGraph checkpoints |
| **Long-term Memory** | Lakebase `hls-agent-v3/production` | User preferences (embedding store) |
| **Chemical Search** | VS endpoint `hls-agent-vs-endpoint` | ZINC 250K ECFP4 index |
| **Fingerprints** | RDKit (in-process) | SMILES → ECFP4 bitstring |
| **Cancer Incidence** | Genie Space `01f117bb5fff...` | Epidemiology SQL analytics |
| **Clinical Trials** | Genie Space `01f117bb603f...` | Trial data SQL analytics |
| **Web Search** | UC function `tavily_web_search` | Live web via Tavily API |
| **Clinical Alerts** | UC function `send_clinical_alert` | Email notifications |
| **Embeddings** | `databricks-gte-large-en` | Memory vector embeddings |

---

## Accessing the Agent Programmatically

The agent also exposes a REST API at the app URL:

```bash
# Invoke (non-streaming)
curl -X POST https://hls-multiagent-v3-7474654216647582.aws.databricksapps.com/responses \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -d '{
    "input": [{"role": "user", "content": "What time is it?"}],
    "custom_inputs": {"thread_id": "demo-1", "user_id": "demo@example.com"}
  }'
```

---

## Troubleshooting

- **App not loading**: Check compute status in Databricks UI → Apps. It may need a minute to warm up from scale-to-zero.
- **Memory not working**: Ensure Lakebase endpoint is active. Check `databricks postgres list-branches projects/hls-agent-v3`.
- **Chemical search errors**: Ensure VS endpoint is active: `databricks vector-search-endpoints list-endpoints`.
- **MCP tool errors**: If Genie or UC function calls fail, check that the Genie Spaces and UC functions are accessible in the FEVM workspace.
