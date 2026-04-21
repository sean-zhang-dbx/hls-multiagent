# GSK Multiagent Traceability - Vaccine Supply Chain

Vaccine supply chain demo with Genie Spaces, Lakebase Autoscaling, and demand forecasting on Databricks.

## Workspaces

| Environment | URL | Profile |
|-------------|-----|---------|
| Source (Azure) | `adb-984752964297111.11.azuredatabricks.net` | `adb-984752964297111` |
| Target (AWS FEVM) | `fevm-sean-zhang.cloud.databricks.com` | `fe-vm-sean-zhang` |

## Notebooks

| Notebook | Purpose |
|----------|---------|
| `notebooks/Generate_supply_chain_tables.py` | Creates 6 Delta tables + seeds Lakebase Autoscaling with orders/stores/products |
| `notebooks/Deploy_supply_chain_genies.py` | Deploys Genie Spaces from source to target workspace |
| `notebooks/analyze_data_lakebase.py` | Reads from Lakebase Autoscaling, generates demand forecasts, writes to Delta |

## Architecture

```
Unity Catalog: sean_zhang_catalog.gsk_vaccine_sc_v2
  Delta Tables:
  ├── fact_production_shipment
  ├── fact_current_inventory
  ├── genie_room1_kpi
  ├── fact_country_supply
  ├── genie_room2_kpi
  ├── integrated_risk_view
  └── demand_forecast_analytics   (output of forecast pipeline)
  Views:
  ├── v_manufacturing_status
  ├── v_inventory_health
  ├── v_logistics_performance
  ├── v_country_coverage
  ├── v_vaccine_availability
  └── v_pricing_analysis

Lakebase Autoscaling: projects/gsk-vaccine-sc
  Branch: production
  Endpoint: primary (ep-curly-lake-d87rvm8m)
  Database: databricks_postgres
  Tables:
  ├── products    (5 vaccine products)
  ├── stores      (8 global distribution points)
  └── orders      (90 days of synthetic order data)

Genie Spaces:
  ├── Manufacturing & Logistics (Room 1)
  └── Immunization & Procurement (Room 2)
```

## Migration: Lakebase Provisioned -> Autoscaling

| Aspect | Before (Provisioned) | After (Autoscaling) |
|--------|---------------------|---------------------|
| SDK module | `w.database` | `w.postgres` |
| Resource model | Instance (flat) | Project > Branch > Compute |
| Connector | `psycopg2` (v2) | `psycopg` (v3) |
| Auth | `generate_database_credential(instance_names=[...])` | `generate_database_credential(endpoint=...)` |
| Capacity | Fixed CU_1-CU_8 | Autoscaling 0.5-112 CU |
| Scale-to-zero | Not supported | Configurable timeout |
| Branching | Not supported | Full branching (dev/test/prod) |

## Setup

1. Run `Generate_supply_chain_tables.py` on the FEVM workspace to create Delta tables and seed Lakebase
2. Run `Deploy_supply_chain_genies.py` to deploy the two Genie Spaces (requires PATs via widgets)
3. Run `analyze_data_lakebase.py` to generate demand forecasts from Lakebase data
