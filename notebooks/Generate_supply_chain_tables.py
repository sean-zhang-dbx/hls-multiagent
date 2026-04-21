# Databricks notebook source
# MAGIC %md
# MAGIC # Vaccine Supply Chain - Data Generation
# MAGIC
# MAGIC Generates 6 Delta tables + seeds Lakebase Autoscaling (PostgreSQL) with
# MAGIC orders/stores/products for the demand-forecasting notebook.
# MAGIC
# MAGIC **Target catalog:** `sean_zhang_catalog.gsk_vaccine_sc_v2`
# MAGIC **Lakebase project:** `projects/gsk-vaccine-sc` (Autoscaling, PG 17)

# COMMAND ----------

# MAGIC %pip install -U "databricks-sdk>=0.81.0" "psycopg[binary]>=3.0"
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG sean_zhang_catalog;
# MAGIC USE SCHEMA gsk_vaccine_sc_v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ==============================================================================
# MAGIC -- GENIE ROOM 1: MANUFACTURING & LOGISTICS (3 Tables)
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS fact_production_shipment (
# MAGIC   record_id STRING NOT NULL,
# MAGIC   product STRING NOT NULL,
# MAGIC   site STRING NOT NULL,
# MAGIC   customer STRING NOT NULL,
# MAGIC   planned_volume LONG,
# MAGIC   actual_volume DOUBLE,
# MAGIC   yield_pct DOUBLE COMMENT 'Manufacturing yield percentage',
# MAGIC   ship_date DATE NOT NULL,
# MAGIC   delivery_date DATE NOT NULL,
# MAGIC   lead_time_days INT COMMENT 'Days from shipment to delivery',
# MAGIC   is_otif BOOLEAN COMMENT 'On-time in-full indicator',
# MAGIC   temp_excursion BOOLEAN COMMENT 'Temperature excursion during transit',
# MAGIC   otif_flag INT COMMENT '1=on-time, 0=late'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Combined manufacturing and logistics metrics'
# MAGIC TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '3');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS fact_current_inventory (
# MAGIC   inventory_id STRING NOT NULL,
# MAGIC   product STRING NOT NULL,
# MAGIC   site STRING NOT NULL,
# MAGIC   on_hand_doses DOUBLE COMMENT 'Doses currently in stock',
# MAGIC   daily_usage DOUBLE COMMENT 'Average daily consumption',
# MAGIC   days_of_supply DOUBLE COMMENT 'Calculated as on_hand / daily_usage',
# MAGIC   snapshot_date DATE NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Current inventory snapshot by product and site'
# MAGIC TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '2');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS genie_room1_kpi (
# MAGIC   product STRING NOT NULL,
# MAGIC   site STRING NOT NULL,
# MAGIC   total_shipments LONG COMMENT 'Number of shipments',
# MAGIC   avg_yield_pct DOUBLE COMMENT 'Average manufacturing yield',
# MAGIC   otif_pct DOUBLE COMMENT 'On-time in-full percentage',
# MAGIC   avg_lead_time_days DOUBLE COMMENT 'Average shipment lead time',
# MAGIC   excursion_rate_pct DOUBLE COMMENT 'Temperature excursion rate',
# MAGIC   avg_days_of_supply DOUBLE COMMENT 'Average inventory coverage',
# MAGIC   stockout_risk_pct DOUBLE COMMENT 'Percentage time out of stock',
# MAGIC   kpi_date DATE NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Pre-aggregated KPIs ready for Genie queries'
# MAGIC TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '2');
# MAGIC
# MAGIC -- ==============================================================================
# MAGIC -- GENIE ROOM 2: IMMUNIZATION & PROCUREMENT (2 Tables)
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS fact_country_supply (
# MAGIC   supply_id STRING NOT NULL,
# MAGIC   country_code STRING NOT NULL,
# MAGIC   country_name STRING NOT NULL,
# MAGIC   income_level STRING COMMENT 'LIC, LMIC, UMIC, HIC',
# MAGIC   antigen STRING NOT NULL,
# MAGIC   supplier STRING,
# MAGIC   channel STRING COMMENT 'UNICEF, GAVI, DIRECT, WHO',
# MAGIC   price_per_dose DOUBLE COMMENT 'Cost per vaccine dose',
# MAGIC   volume_doses DOUBLE COMMENT 'Purchase volume',
# MAGIC   target_coverage_pct DOUBLE COMMENT 'National vaccination target',
# MAGIC   actual_coverage_pct DOUBLE COMMENT 'Actual immunization coverage',
# MAGIC   has_stockout BOOLEAN COMMENT 'Whether stockout occurred',
# MAGIC   stockout_days INT COMMENT 'Total days out of stock',
# MAGIC   vaccine_availability_pct DOUBLE COMMENT '100 - (stockout_days/365*100)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Combined procurement and availability metrics by country'
# MAGIC TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '4');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS genie_room2_kpi (
# MAGIC   country_name STRING NOT NULL,
# MAGIC   country_code STRING NOT NULL,
# MAGIC   income_level STRING NOT NULL,
# MAGIC   antigen STRING NOT NULL,
# MAGIC   avg_price_per_dose DOUBLE,
# MAGIC   avg_coverage_pct DOUBLE,
# MAGIC   target_coverage_pct DOUBLE,
# MAGIC   coverage_gap_pct DOUBLE,
# MAGIC   vaccine_availability_pct DOUBLE,
# MAGIC   stockout_count BIGINT,
# MAGIC   avg_stockout_days DOUBLE,
# MAGIC   supplier_count BIGINT,
# MAGIC   kpi_date DATE NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Pre-aggregated country-level KPIs ready for Genie queries'
# MAGIC TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '3');
# MAGIC
# MAGIC -- ==============================================================================
# MAGIC -- INTEGRATION TABLE (for Agent Orchestrator)
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS integrated_risk_view (
# MAGIC   product STRING,
# MAGIC   site STRING,
# MAGIC   otif_pct DOUBLE,
# MAGIC   avg_yield_pct DOUBLE,
# MAGIC   avg_days_of_supply DOUBLE,
# MAGIC   country_name STRING,
# MAGIC   antigen STRING,
# MAGIC   actual_coverage_pct DOUBLE,
# MAGIC   vaccine_availability_pct DOUBLE,
# MAGIC   risk_status STRING COMMENT 'HIGH_RISK, MEDIUM_RISK, LOW_RISK',
# MAGIC   integration_date DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Integrated view joining manufacturing and immunization domains'
# MAGIC TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '4');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ==============================================================================
# MAGIC -- GENIE ROOM 1: Pre-built Views
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_manufacturing_status AS
# MAGIC SELECT
# MAGIC   product, site, avg_yield_pct, otif_pct, avg_lead_time_days, excursion_rate_pct,
# MAGIC   CASE WHEN avg_yield_pct >= 96 THEN 'EXCELLENT'
# MAGIC        WHEN avg_yield_pct >= 94 THEN 'GOOD'
# MAGIC        WHEN avg_yield_pct >= 92 THEN 'ACCEPTABLE'
# MAGIC        ELSE 'NEEDS_ATTENTION' END as yield_status,
# MAGIC   CASE WHEN otif_pct >= 95 THEN 'ON_TRACK'
# MAGIC        WHEN otif_pct >= 90 THEN 'ACCEPTABLE'
# MAGIC        ELSE 'AT_RISK' END as otif_status
# MAGIC FROM genie_room1_kpi ORDER BY avg_yield_pct DESC;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_inventory_health AS
# MAGIC SELECT
# MAGIC   product, site, avg_days_of_supply, stockout_risk_pct,
# MAGIC   CASE WHEN avg_days_of_supply = 0 THEN 'OUT_OF_STOCK'
# MAGIC        WHEN avg_days_of_supply < 5 THEN 'CRITICAL'
# MAGIC        WHEN avg_days_of_supply < 15 THEN 'LOW'
# MAGIC        ELSE 'HEALTHY' END as inventory_status
# MAGIC FROM genie_room1_kpi ORDER BY avg_days_of_supply ASC;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_logistics_performance AS
# MAGIC SELECT
# MAGIC   product, site, otif_pct, avg_lead_time_days, excursion_rate_pct,
# MAGIC   CASE WHEN otif_pct >= 95 AND excursion_rate_pct < 1 THEN 'EXCELLENT'
# MAGIC        WHEN otif_pct >= 90 AND excursion_rate_pct < 2 THEN 'GOOD'
# MAGIC        ELSE 'NEEDS_ATTENTION' END as logistics_grade
# MAGIC FROM genie_room1_kpi ORDER BY otif_pct DESC;
# MAGIC
# MAGIC -- ==============================================================================
# MAGIC -- GENIE ROOM 2: Pre-built Views
# MAGIC -- ==============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_country_coverage AS
# MAGIC SELECT
# MAGIC   country_name, country_code, income_level, antigen,
# MAGIC   avg_coverage_pct, target_coverage_pct, coverage_gap_pct,
# MAGIC   CASE WHEN coverage_gap_pct <= -5 THEN 'EXCEEDING_TARGET'
# MAGIC        WHEN coverage_gap_pct >= 5 THEN 'BELOW_TARGET'
# MAGIC        ELSE 'ON_TARGET' END as coverage_status,
# MAGIC   CASE WHEN coverage_gap_pct > 10 THEN 'CRITICAL'
# MAGIC        WHEN coverage_gap_pct > 5 THEN 'WARNING'
# MAGIC        ELSE 'ACCEPTABLE' END as gap_severity
# MAGIC FROM genie_room2_kpi ORDER BY coverage_gap_pct DESC;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_vaccine_availability AS
# MAGIC SELECT
# MAGIC   country_name, country_code, income_level, antigen,
# MAGIC   vaccine_availability_pct, avg_stockout_days, stockout_count,
# MAGIC   CASE WHEN vaccine_availability_pct >= 95 THEN 'RELIABLE'
# MAGIC        WHEN vaccine_availability_pct >= 90 THEN 'ACCEPTABLE'
# MAGIC        WHEN vaccine_availability_pct >= 80 THEN 'AT_RISK'
# MAGIC        ELSE 'CRITICAL' END as availability_status
# MAGIC FROM genie_room2_kpi ORDER BY vaccine_availability_pct ASC;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_pricing_analysis AS
# MAGIC SELECT
# MAGIC   country_name, income_level, antigen,
# MAGIC   ROUND(avg_price_per_dose, 2) as price_per_dose, supplier_count,
# MAGIC   CASE WHEN income_level = 'HIC' AND avg_price_per_dose > 15 THEN 'PREMIUM'
# MAGIC        WHEN income_level IN ('UMIC', 'LMIC') AND avg_price_per_dose < 5 THEN 'AFFORDABLE'
# MAGIC        WHEN income_level = 'LIC' AND avg_price_per_dose < 3 THEN 'AFFORDABLE'
# MAGIC        ELSE 'MODERATE' END as price_tier
# MAGIC FROM genie_room2_kpi ORDER BY avg_price_per_dose DESC;

# COMMAND ----------

import pandas as pd
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession.builder.getOrCreate()

CATALOG = "sean_zhang_catalog"
SCHEMA = "gsk_vaccine_sc_v2"
TABLE_PREFIX = f"{CATALOG}.{SCHEMA}"

# =============================================================================
# SCHEMAS
# =============================================================================

schema_fact_production_shipment = StructType([
    StructField("record_id", StringType(), False),
    StructField("product", StringType(), False),
    StructField("site", StringType(), False),
    StructField("customer", StringType(), False),
    StructField("planned_volume", LongType(), False),
    StructField("actual_volume", DoubleType(), False),
    StructField("yield_pct", DoubleType(), False),
    StructField("ship_date", DateType(), False),
    StructField("delivery_date", DateType(), False),
    StructField("lead_time_days", IntegerType(), False),
    StructField("is_otif", BooleanType(), False),
    StructField("temp_excursion", BooleanType(), False),
    StructField("otif_flag", IntegerType(), False),
])

schema_fact_current_inventory = StructType([
    StructField("inventory_id", StringType(), False),
    StructField("product", StringType(), False),
    StructField("site", StringType(), False),
    StructField("on_hand_doses", DoubleType(), False),
    StructField("daily_usage", DoubleType(), False),
    StructField("days_of_supply", DoubleType(), False),
    StructField("snapshot_date", DateType(), False),
])

schema_fact_country_supply = StructType([
    StructField("supply_id", StringType(), False),
    StructField("country_code", StringType(), False),
    StructField("country_name", StringType(), False),
    StructField("income_level", StringType(), False),
    StructField("antigen", StringType(), False),
    StructField("supplier", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("price_per_dose", DoubleType(), False),
    StructField("volume_doses", DoubleType(), False),
    StructField("target_coverage_pct", DoubleType(), False),
    StructField("actual_coverage_pct", DoubleType(), False),
    StructField("has_stockout", BooleanType(), False),
    StructField("stockout_days", IntegerType(), False),
    StructField("vaccine_availability_pct", DoubleType(), False),
])

# =============================================================================
# PANDAS BUILDERS
# =============================================================================

def build_pdf_fact_production_shipment(num_records=1000) -> pd.DataFrame:
    products = ["PCV13", "HPV9", "DTP3", "Influenza", "Rotavirus"]
    sites = ["Wavre-Belgium", "Singapore-Tuas", "Hamilton-USA", "Amsterdam-DC", "Miami-DC"]
    customers = ["UK-NHS", "Nigeria-MOH", "Kenya-MOH", "India-MOH", "Brazil-MOH"]

    base_date = datetime(2024, 1, 1)
    rows = []

    for i in range(num_records):
        product = random.choice(products)
        site = random.choice(sites)
        customer = random.choice(customers)

        planned_volume = random.choice([100_000, 500_000, 1_000_000])
        yield_frac = random.uniform(0.92, 0.998)
        actual_volume = float(int(planned_volume * yield_frac))
        yield_pct = round(yield_frac * 100.0, 2)

        ship_date = (base_date + timedelta(days=i)).date()
        lead_time_days = random.randint(3, 30)
        delivery_date = (ship_date + timedelta(days=lead_time_days))

        is_otif = random.random() < 0.95
        temp_excursion = random.random() < 0.02

        rows.append({
            "record_id": f"REC-{i:05d}",
            "product": product,
            "site": site,
            "customer": customer,
            "planned_volume": int(planned_volume),
            "actual_volume": actual_volume,
            "yield_pct": yield_pct,
            "ship_date": ship_date,
            "delivery_date": delivery_date,
            "lead_time_days": int(lead_time_days),
            "is_otif": bool(is_otif),
            "temp_excursion": bool(temp_excursion),
            "otif_flag": 1 if is_otif else 0,
        })

    return pd.DataFrame(rows)


def build_pdf_fact_current_inventory(num_records=500) -> pd.DataFrame:
    products = ["PCV13", "HPV9", "DTP3", "Influenza", "Rotavirus"]
    sites = ["Amsterdam-DC", "Miami-DC", "Singapore-HUB", "Johannesburg-HUB"]

    today = datetime.now().date()
    rows = []

    for i in range(num_records):
        product = random.choice(products)
        site = random.choice(sites)

        on_hand = random.randint(1000, 500_000)
        daily_usage = random.randint(100, 5_000)
        days_of_supply = on_hand / daily_usage if daily_usage > 0 else 0.0

        if random.random() < 0.03:
            on_hand = 0
            days_of_supply = 0.0

        rows.append({
            "inventory_id": f"INV-{i:05d}",
            "product": product,
            "site": site,
            "on_hand_doses": float(on_hand),
            "daily_usage": float(daily_usage),
            "days_of_supply": float(round(days_of_supply, 1)),
            "snapshot_date": today,
        })

    return pd.DataFrame(rows)


def build_pdf_fact_country_supply(num_records=500) -> pd.DataFrame:
    countries = [
        ("NGA", "Nigeria", "LIC"), ("KEN", "Kenya", "LMIC"), ("ETH", "Ethiopia", "LIC"),
        ("IND", "India", "LMIC"), ("IDN", "Indonesia", "LMIC"), ("BGD", "Bangladesh", "LMIC"),
        ("ZAF", "South Africa", "UMIC"), ("BRA", "Brazil", "UMIC"), ("MEX", "Mexico", "UMIC"),
        ("GBR", "UK", "HIC"), ("FRA", "France", "HIC"), ("DEU", "Germany", "HIC"),
    ]
    antigens = ["DTP3", "PCV13", "HPV9", "Measles", "Influenza"]
    suppliers = ["GSK", "Pfizer", "Merck", "Sanofi", "UNICEF"]
    channels = ["UNICEF", "GAVI", "DIRECT", "WHO"]

    rows = []

    for i in range(num_records):
        country_code, country_name, income = random.choice(countries)
        antigen = random.choice(antigens)
        supplier = random.choice(suppliers)
        channel = random.choice(channels)

        price_per_dose = round(random.uniform(0.50, 30.00), 2)
        volume = float(random.choice([100_000, 500_000, 1_000_000, 5_000_000]))

        target_coverage = 90.0 if income in ["HIC", "UMIC"] else 80.0
        actual_coverage = round(random.uniform(max(40.0, target_coverage - 20.0), 99.0), 1)

        has_stockout = random.random() < 0.40
        stockout_days = int(random.randint(7, 180) if has_stockout else 0)
        vaccine_availability = round(100.0 - (stockout_days / 365.0 * 100.0), 1)

        rows.append({
            "supply_id": f"SUP-{i:05d}",
            "country_code": country_code,
            "country_name": country_name,
            "income_level": income,
            "antigen": antigen,
            "supplier": supplier,
            "channel": channel,
            "price_per_dose": float(price_per_dose),
            "volume_doses": volume,
            "target_coverage_pct": float(target_coverage),
            "actual_coverage_pct": float(actual_coverage),
            "has_stockout": bool(has_stockout),
            "stockout_days": stockout_days,
            "vaccine_availability_pct": float(vaccine_availability),
        })

    return pd.DataFrame(rows)

# =============================================================================
# WRITE HELPER
# =============================================================================

def write_df(df, table_name: str, mode: str = "overwrite", overwrite_schema: bool = True):
    full_name = f"{TABLE_PREFIX}.{table_name}"
    writer = df.write.format("delta").mode(mode)
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(full_name)
    print(f"  wrote {full_name} ({df.count()} rows)")

# =============================================================================
# MAIN: Generate Delta tables
# =============================================================================

def main():
    print("=" * 80)
    print(f"Building supply chain tables -> {TABLE_PREFIX}.*")
    print("=" * 80)

    for t in [
        "fact_production_shipment",
        "fact_current_inventory",
        "genie_room1_kpi",
        "fact_country_supply",
        "genie_room2_kpi",
        "integrated_risk_view",
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_PREFIX}.{t}")

    pdf_prod_ship = build_pdf_fact_production_shipment()
    pdf_inventory = build_pdf_fact_current_inventory()
    pdf_country_supply = build_pdf_fact_country_supply()

    df_prod_ship = spark.createDataFrame(pdf_prod_ship, schema_fact_production_shipment)
    df_inventory = spark.createDataFrame(pdf_inventory, schema_fact_current_inventory)
    df_country_supply = spark.createDataFrame(pdf_country_supply, schema_fact_country_supply)

    # Room 1 KPIs
    df_room1_kpi = (
        df_prod_ship.groupBy("product", "site")
        .agg(
            f.count("record_id").alias("total_shipments"),
            f.round(f.avg("yield_pct"), 2).alias("avg_yield_pct"),
            f.round(f.sum("otif_flag") / f.count("*") * 100.0, 2).alias("otif_pct"),
            f.round(f.avg("lead_time_days"), 1).alias("avg_lead_time_days"),
            f.round(
                f.sum(f.when(f.col("temp_excursion") == True, 1).otherwise(0))
                / f.count("*") * 100.0,
                2,
            ).alias("excursion_rate_pct"),
        )
        .join(
            df_inventory.groupBy("product", "site").agg(
                f.round(f.avg("days_of_supply"), 1).alias("avg_days_of_supply"),
                f.round(
                    f.sum(f.when(f.col("on_hand_doses") == 0, 1).otherwise(0))
                    / f.count("*") * 100.0,
                    2,
                ).alias("stockout_risk_pct"),
            ),
            ["product", "site"],
            "left",
        )
        .withColumn("kpi_date", f.current_date())
    )

    # Room 2 KPIs
    df_room2_kpi = (
        df_country_supply.groupBy("country_name", "country_code", "income_level", "antigen")
        .agg(
            f.round(f.avg("price_per_dose"), 2).alias("avg_price_per_dose"),
            f.round(f.avg("actual_coverage_pct"), 1).alias("avg_coverage_pct"),
            f.round(f.avg("target_coverage_pct"), 1).alias("target_coverage_pct"),
            f.round(f.avg("vaccine_availability_pct"), 1).alias("vaccine_availability_pct"),
            f.sum(f.when(f.col("has_stockout") == True, 1).otherwise(0)).cast("bigint").alias("stockout_count"),
            f.round(f.avg("stockout_days"), 0).alias("avg_stockout_days"),
            f.countDistinct("supplier").cast("bigint").alias("supplier_count"),
        )
        .withColumn("coverage_gap_pct", f.col("target_coverage_pct") - f.col("avg_coverage_pct"))
        .withColumn("kpi_date", f.current_date())
    )

    write_df(df_prod_ship, "fact_production_shipment")
    write_df(df_inventory, "fact_current_inventory")
    write_df(df_room1_kpi, "genie_room1_kpi")
    write_df(df_country_supply, "fact_country_supply")
    write_df(df_room2_kpi, "genie_room2_kpi")

    # Integrated risk view (cross-domain join)
    integrated = spark.sql(f"""
        SELECT
            r1.product, r1.site, r1.otif_pct, r1.avg_yield_pct, r1.avg_days_of_supply,
            r2.country_name, r2.antigen,
            r2.avg_coverage_pct AS actual_coverage_pct,
            r2.vaccine_availability_pct,
            CASE
                WHEN r2.avg_coverage_pct < r2.target_coverage_pct - 10
                     AND r1.avg_days_of_supply < 10 THEN 'HIGH_RISK'
                WHEN r2.avg_coverage_pct < r2.target_coverage_pct - 5
                     OR r1.avg_days_of_supply < 5 THEN 'MEDIUM_RISK'
                ELSE 'LOW_RISK'
            END as risk_status,
            CURRENT_DATE() as integration_date
        FROM {TABLE_PREFIX}.genie_room1_kpi r1
        CROSS JOIN {TABLE_PREFIX}.genie_room2_kpi r2
        WHERE r1.product LIKE CONCAT('%', r2.antigen, '%')
           OR r1.product = r2.antigen
        LIMIT 100
    """)
    write_df(integrated, "integrated_risk_view")

    print(f"\nDelta tables created under {TABLE_PREFIX}:")
    for t in [
        "fact_production_shipment", "fact_current_inventory", "genie_room1_kpi",
        "fact_country_supply", "genie_room2_kpi", "integrated_risk_view",
    ]:
        print(f"  - {TABLE_PREFIX}.{t}")

main()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed Lakebase Autoscaling with orders/stores/products
# MAGIC
# MAGIC Creates tables in the `gsk-vaccine-sc` Lakebase Autoscaling project that the
# MAGIC `analyze_data_lakebase` notebook reads for demand forecasting.

# COMMAND ----------

import psycopg
from databricks.sdk import WorkspaceClient

PROJECT_ID = "gsk-vaccine-sc"
BRANCH = "production"
ENDPOINT_NAME = f"projects/{PROJECT_ID}/branches/{BRANCH}/endpoints/primary"

w = WorkspaceClient()

endpoint = w.postgres.get_endpoint(name=ENDPOINT_NAME)
host = endpoint.status.hosts.host
cred = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
username = w.current_user.me().user_name

conn_string = (
    f"host={host} "
    f"dbname=databricks_postgres "
    f"user={username} "
    f"password={cred.token} "
    f"sslmode=require"
)

print(f"Connecting to Lakebase Autoscaling: {host}")

with psycopg.connect(conn_string) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        print(f"Connected: {cur.fetchone()[0]}")

        # Create tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id SERIAL PRIMARY KEY,
                product_name TEXT NOT NULL,
                category TEXT,
                unit_price NUMERIC(10,2) NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stores (
                store_id SERIAL PRIMARY KEY,
                store_name TEXT NOT NULL,
                region TEXT,
                country TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id SERIAL PRIMARY KEY,
                product_id INT REFERENCES products(product_id),
                to_store_id INT REFERENCES stores(store_id),
                order_date DATE NOT NULL,
                quantity_cases INT NOT NULL
            )
        """)
        conn.commit()

        # Seed products
        cur.execute("SELECT COUNT(*) FROM products")
        if cur.fetchone()[0] == 0:
            products = [
                ("PCV13 Vaccine", "Pneumococcal", 45.00),
                ("HPV9 Vaccine", "HPV", 65.00),
                ("DTP3 Vaccine", "Combined", 12.50),
                ("Influenza Vaccine", "Seasonal", 18.75),
                ("Rotavirus Vaccine", "Rotavirus", 22.00),
            ]
            cur.executemany(
                "INSERT INTO products (product_name, category, unit_price) VALUES (%s, %s, %s)",
                products,
            )

        # Seed stores
        cur.execute("SELECT COUNT(*) FROM stores")
        if cur.fetchone()[0] == 0:
            stores = [
                ("NHS Central", "Europe", "UK"),
                ("Nigeria Federal MOH", "Africa", "Nigeria"),
                ("India National MOH", "Asia", "India"),
                ("Brazil SUS", "Americas", "Brazil"),
                ("Kenya MOH", "Africa", "Kenya"),
                ("France Sante", "Europe", "France"),
                ("Indonesia MOH", "Asia", "Indonesia"),
                ("South Africa DOH", "Africa", "South Africa"),
            ]
            cur.executemany(
                "INSERT INTO stores (store_name, region, country) VALUES (%s, %s, %s)",
                stores,
            )

        # Seed orders (90 days of synthetic data)
        cur.execute("SELECT COUNT(*) FROM orders")
        if cur.fetchone()[0] == 0:
            from datetime import date, timedelta as td
            import random as rng

            base = date(2025, 12, 1)
            order_rows = []
            for day_offset in range(90):
                order_date = base + td(days=day_offset)
                n_orders = rng.randint(3, 12)
                for _ in range(n_orders):
                    order_rows.append((
                        rng.randint(1, 5),
                        rng.randint(1, 8),
                        order_date,
                        rng.randint(10, 500),
                    ))
            cur.executemany(
                "INSERT INTO orders (product_id, to_store_id, order_date, quantity_cases) VALUES (%s, %s, %s, %s)",
                order_rows,
            )

        conn.commit()

        cur.execute("SELECT COUNT(*) FROM orders")
        print(f"Lakebase seeded: {cur.fetchone()[0]} orders")

print("Done - Lakebase Autoscaling tables ready.")
