# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch rdkit
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup ZINC15 Vector Search Index
# MAGIC
# MAGIC Following the [AiChemy](https://github.com/databricks-industry-solutions/aichemy) pattern:
# MAGIC 1. Load ZINC15 250K dataset
# MAGIC 2. Compute 1024-bit ECFP4 fingerprints using RDKit
# MAGIC 3. Save as Delta table
# MAGIC 4. Create Vector Search endpoint + Delta Sync index

# COMMAND ----------

CATALOG = "sean_zhang_catalog"
SCHEMA = "gsk_india_hls"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.zinc15_250k_full"
VS_ENDPOINT = "hls-agent-vs-endpoint"
VS_INDEX = f"{CATALOG}.{SCHEMA}.zinc_vs"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load ZINC15 Data
# MAGIC
# MAGIC Download the ZINC15 250K "drug-like" subset. Adjust the URL/path as needed.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType
from typing import Iterator

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compute ECFP4 Fingerprints

# COMMAND ----------

from rdkit.Chem import AllChem, MolFromSmiles

@pandas_udf(ArrayType(FloatType()))
def udf_smiles_to_ecfp(smiles: Iterator[pd.Series]) -> Iterator[pd.Series]:
    fpgen = AllChem.GetMorganGenerator(radius=2, fpSize=1024)
    for batch in smiles:
        results = []
        for smi in batch:
            try:
                mol = MolFromSmiles(smi)
                if mol is not None:
                    results.append(fpgen.GetFingerprintAsNumPy(mol).tolist())
                else:
                    results.append(None)
            except Exception:
                results.append(None)
        yield pd.Series(results)

# COMMAND ----------

# MAGIC %md
# MAGIC If you already have a ZINC table loaded, read it. Otherwise load from source.

# COMMAND ----------

try:
    df = spark.table(TABLE_NAME)
    print(f"Table {TABLE_NAME} already exists with {df.count()} rows")
except Exception:
    print(f"Table {TABLE_NAME} not found. Please load ZINC15 data first.")
    print("Example: download from https://zinc15.docking.org/tranches/download")
    print("Then: spark.read.csv(...).write.saveAsTable(TABLE_NAME)")
    dbutils.notebook.exit("ZINC table not found - load data first")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Add ECFP Column and Save

# COMMAND ----------

if "ecfp" not in df.columns:
    df_with_ecfp = df.repartition(32).withColumn("ecfp", udf_smiles_to_ecfp("smiles"))
    df_with_ecfp.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TABLE_NAME)
    print(f"Saved {TABLE_NAME} with ECFP column")
else:
    print(f"ECFP column already exists in {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Vector Search Endpoint

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

client = VectorSearchClient()

try:
    client.create_endpoint(name=VS_ENDPOINT)
    print(f"Created VS endpoint: {VS_ENDPOINT}")
except Exception as e:
    if "already exists" in str(e):
        print(f"Endpoint {VS_ENDPOINT} already exists")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Delta Sync Index

# COMMAND ----------

try:
    index = client.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT,
        source_table_name=TABLE_NAME,
        index_name=VS_INDEX,
        columns_to_sync=[
            "smiles", "zinc_id", "mwt", "logp",
            "reactive", "purchasable", "tranche_name",
            "SPS", "FractionCSP3", "RingCount", "ecfp",
        ],
        pipeline_type="TRIGGERED",
        primary_key="zinc_id",
        embedding_dimension=1024,
        embedding_vector_column="ecfp",
    )
    print(f"Created VS index: {VS_INDEX}")
except Exception as e:
    if "already exists" in str(e):
        print(f"Index {VS_INDEX} already exists")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Index
# MAGIC
# MAGIC Test a query with a sample ECFP fingerprint.

# COMMAND ----------

from descriptors import smiles_to_ecfp

aspirin_smiles = "CC(=O)Oc1ccccc1C(=O)O"
aspirin_ecfp = smiles_to_ecfp(aspirin_smiles).tolist()

index = client.get_index(VS_INDEX)
results = index.similarity_search(
    query_vector=aspirin_ecfp,
    columns=["zinc_id", "smiles", "mwt", "logp", "purchasable"],
    num_results=5,
)
display(results)
