# Databricks notebook source
# MAGIC %md
# MAGIC # Register get_embedding UC Function
# MAGIC
# MAGIC Creates the `get_embedding` UC function in `sean_zhang_catalog.gsk_india_hls`
# MAGIC that computes ECFP4 molecular fingerprints from SMILES strings.
# MAGIC
# MAGIC This function is callable by the agent via MCP (as a UC function tool) and also
# MAGIC directly via SQL: `SELECT sean_zhang_catalog.gsk_india_hls.get_embedding('CCO')`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION sean_zhang_catalog.gsk_india_hls.get_embedding(smiles STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Compute ECFP4 molecular fingerprint as a 1024-char bitstring from a SMILES string. Uses Morgan fingerprints (radius=2, 1024 bits) via RDKit.'
# MAGIC AS $$
# MAGIC from rdkit.Chem import MolFromSmiles, AllChem
# MAGIC fpgen = AllChem.GetMorganGenerator(radius=2, fpSize=1024)
# MAGIC mol = MolFromSmiles(smiles)
# MAGIC if mol is None:
# MAGIC     return None
# MAGIC fp = fpgen.GetFingerprintAsNumPy(mol)
# MAGIC return ''.join(str(int(x)) for x in fp)
# MAGIC $$

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with aspirin SMILES
# MAGIC SELECT sean_zhang_catalog.gsk_india_hls.get_embedding('CC(=O)Oc1ccccc1C(=O)O') AS aspirin_ecfp4

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test with caffeine
# MAGIC SELECT sean_zhang_catalog.gsk_india_hls.get_embedding('Cn1c(=O)c2c(ncn2C)n(C)c1=O') AS caffeine_ecfp4

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify length is 1024
# MAGIC SELECT LENGTH(sean_zhang_catalog.gsk_india_hls.get_embedding('CCO')) AS bitstring_length

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED sean_zhang_catalog.gsk_india_hls.get_embedding
