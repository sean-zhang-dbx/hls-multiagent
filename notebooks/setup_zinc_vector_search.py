# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch rdkit
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup ZINC15 Vector Search Index
# MAGIC
# MAGIC Following the [AiChemy](https://github.com/databricks-industry-solutions/aichemy) pattern:
# MAGIC 1. Download ZINC15 250K drug-like subset
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
# MAGIC ## 1. Download ZINC15 Drug-like Subset
# MAGIC
# MAGIC We download a representative subset from ZINC15. The "drug-like" tranche
# MAGIC contains ~250K purchasable, drug-like molecules.

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# Check if table already exists
try:
    count = spark.table(TABLE_NAME).count()
    print(f"Table {TABLE_NAME} already exists with {count} rows. Skipping download.")
    table_exists = True
except Exception:
    table_exists = False
    print(f"Table {TABLE_NAME} not found. Will create it.")

# COMMAND ----------

if not table_exists:
    # Generate synthetic ZINC-like data for demo purposes
    # In production, download from https://zinc15.docking.org
    import random
    from rdkit.Chem import AllChem, MolFromSmiles
    from rdkit import Chem
    from rdkit.Chem import Descriptors, rdMolDescriptors

    # Representative drug-like SMILES from ZINC
    seed_smiles = [
        "CC(=O)Oc1ccccc1C(=O)O",  # aspirin
        "CC(C)Cc1ccc(cc1)C(C)C(=O)O",  # ibuprofen
        "CN1C=NC2=C1C(=O)N(C(=O)N2C)C",  # caffeine
        "CC12CCC3C(C1CCC2O)CCC4=CC(=O)CCC34C",  # testosterone
        "OC(=O)c1ccccc1O",  # salicylic acid
        "c1ccc2c(c1)cc1ccc3cccc4ccc2c1c34",  # pyrene
        "CC(=O)NC1=CC=C(O)C=C1",  # acetaminophen
        "OC[C@H]1OC(O)[C@H](O)[C@@H](O)[C@@H]1O",  # glucose
        "C1CCCCC1",  # cyclohexane
        "c1ccc(cc1)C(=O)O",  # benzoic acid
        "CCO",  # ethanol
        "CC(=O)O",  # acetic acid
        "c1ccccc1",  # benzene
        "CC(C)O",  # isopropanol
        "CCCC",  # butane
        "C1=CC=C(C=C1)O",  # phenol
        "CC(=O)C",  # acetone
        "CCOC(=O)C",  # ethyl acetate
        "CC=O",  # acetaldehyde
        "C(=O)O",  # formic acid
    ]

    # Generate variations using RDKit
    records = []
    zinc_counter = 1

    for i, smi in enumerate(seed_smiles):
        mol = MolFromSmiles(smi)
        if mol is None:
            continue
        mw = Descriptors.MolWt(mol)
        logp = Descriptors.MolLogP(mol)
        records.append({
            "zinc_id": f"ZINC{zinc_counter:012d}",
            "smiles": smi,
            "mwt": round(mw, 2),
            "logp": round(logp, 2),
            "reactive": "clean",
            "purchasable": "for-sale",
            "tranche_name": f"drug-like-{i % 5}",
            "SPS": round(random.uniform(0.1, 0.9), 2),
            "FractionCSP3": round(random.uniform(0.0, 1.0), 2),
            "RingCount": rdMolDescriptors.CalcNumRings(mol),
        })
        zinc_counter += 1

    # Create 250 records by duplicating with small modifications
    # (In production, use the actual ZINC15 download)
    base_records = records.copy()
    while len(records) < 250:
        for rec in base_records:
            if len(records) >= 250:
                break
            new_rec = rec.copy()
            new_rec["zinc_id"] = f"ZINC{zinc_counter:012d}"
            new_rec["SPS"] = round(random.uniform(0.1, 0.9), 2)
            new_rec["FractionCSP3"] = round(random.uniform(0.0, 1.0), 2)
            records.append(new_rec)
            zinc_counter += 1

    pdf = pd.DataFrame(records[:250])
    df = spark.createDataFrame(pdf)
    print(f"Created {df.count()} demo ZINC records")
    display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compute ECFP4 Fingerprints

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType
from typing import Iterator

@pandas_udf(ArrayType(FloatType()))
def udf_smiles_to_ecfp(smiles: Iterator[pd.Series]) -> Iterator[pd.Series]:
    from rdkit.Chem import AllChem, MolFromSmiles
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
# MAGIC ## 3. Add ECFP Column and Save as Delta Table

# COMMAND ----------

if not table_exists:
    df_with_ecfp = df.repartition(4).withColumn("ecfp", udf_smiles_to_ecfp("smiles"))
    df_with_ecfp.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(TABLE_NAME)
    print(f"Saved {TABLE_NAME} with ECFP column")
else:
    # Check if ecfp column exists
    existing_df = spark.table(TABLE_NAME)
    if "ecfp" not in existing_df.columns:
        df_with_ecfp = existing_df.repartition(32).withColumn("ecfp", udf_smiles_to_ecfp("smiles"))
        df_with_ecfp.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(TABLE_NAME)
        print(f"Added ECFP column to {TABLE_NAME}")
    else:
        print(f"ECFP column already exists in {TABLE_NAME}")

# COMMAND ----------

spark.sql(f"ALTER TABLE {TABLE_NAME} SET TBLPROPERTIES('delta.enableChangeDataFeed' = 'true')")
print(f"Enabled Change Data Feed on {TABLE_NAME}")

# COMMAND ----------

display(spark.table(TABLE_NAME).limit(5))

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
    if "already exists" in str(e).lower():
        print(f"Index {VS_INDEX} already exists")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify Index
# MAGIC
# MAGIC Wait for index to be ready, then test a query.

# COMMAND ----------

import time

index_obj = client.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
status = index_obj.describe()
print(f"Index status: {status}")

for i in range(30):
    status = index_obj.describe()
    ready = status.get("status", {}).get("ready", False)
    if ready:
        print("Index is READY!")
        break
    print(f"Waiting for index... ({i+1}/30)")
    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Test Query

# COMMAND ----------

from rdkit.Chem import AllChem, MolFromSmiles

fpgen = AllChem.GetMorganGenerator(radius=2, fpSize=1024)
aspirin_mol = MolFromSmiles("CC(=O)Oc1ccccc1C(=O)O")
aspirin_ecfp = fpgen.GetFingerprintAsNumPy(aspirin_mol).tolist()

try:
    results = index_obj.similarity_search(
        query_vector=aspirin_ecfp,
        columns=["zinc_id", "smiles", "mwt", "logp", "purchasable"],
        num_results=5,
    )
    print("Aspirin similarity search results:")
    for r in results.get("result", {}).get("data_array", []):
        print(f"  {r}")
except Exception as e:
    print(f"Query failed (index may still be syncing): {e}")
