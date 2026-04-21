# Databricks notebook source
# MAGIC %md
# MAGIC # Chemical Fingerprint Setup Notes
# MAGIC
# MAGIC The `get_embedding` tool computes ECFP4 molecular fingerprints using RDKit.
# MAGIC
# MAGIC **Important**: RDKit is NOT available in the UC Python UDF sandbox, so `get_embedding`
# MAGIC runs as a local `@tool` in the agent process (see `agent_server/tools_hls.py`)
# MAGIC rather than as a UC function.
# MAGIC
# MAGIC This notebook verifies the fingerprint computation works on a cluster with RDKit installed.

# COMMAND ----------

# MAGIC %pip install rdkit
# MAGIC %restart_python

# COMMAND ----------

from rdkit.Chem import AllChem, MolFromSmiles
import numpy as np

fpgen = AllChem.GetMorganGenerator(radius=2, fpSize=1024)

def get_embedding(smiles: str) -> str:
    mol = MolFromSmiles(smiles)
    if mol is None:
        return None
    fp = fpgen.GetFingerprintAsNumPy(mol)
    return "".join(str(int(x)) for x in fp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Fingerprints

# COMMAND ----------

test_molecules = {
    "aspirin": "CC(=O)Oc1ccccc1C(=O)O",
    "caffeine": "Cn1c(=O)c2c(ncn2C)n(C)c1=O",
    "ibuprofen": "CC(C)Cc1ccc(cc1)C(C)C(=O)O",
    "ethanol": "CCO",
}

for name, smiles in test_molecules.items():
    fp = get_embedding(smiles)
    ones = fp.count("1") if fp else 0
    print(f"{name:12s}  SMILES={smiles:45s}  len={len(fp)}  ones={ones}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Fingerprint Properties

# COMMAND ----------

fp_aspirin = get_embedding("CC(=O)Oc1ccccc1C(=O)O")
assert len(fp_aspirin) == 1024, f"Expected 1024, got {len(fp_aspirin)}"
assert all(c in "01" for c in fp_aspirin), "Fingerprint should be binary"
print(f"Aspirin ECFP4: {fp_aspirin[:80]}... ({fp_aspirin.count('1')} bits set)")
print("All checks passed.")
