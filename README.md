# GSK Multiagent Traceability - Vaccine Supply Chain

Vaccine supply chain demo with Genie Spaces, Lakebase, and demand forecasting on Databricks.

## Workspace

- **Source (Azure):** `adb-984752964297111.11.azuredatabricks.net`
- **Target (AWS FEVM):** `fevm-sean-zhang.cloud.databricks.com`

## Notebooks

| Notebook | Purpose |
|----------|---------|
| `notebooks/Generate_supply_chain_tables.py` | Creates 6 Delta tables with synthetic vaccine supply chain data |
| `notebooks/Deploy_supply_chain_genies.py` | Deploys Genie Spaces between workspaces |
| `notebooks/analyze_data_lakebase.py` | Reads from Lakebase, generates demand forecasts, writes to Delta |

## Architecture

```
Unity Catalog (Delta Tables)
  ├── fact_production_shipment
  ├── fact_current_inventory
  ├── genie_room1_kpi
  ├── fact_country_supply
  ├── genie_room2_kpi
  └── integrated_risk_view

Lakebase (PostgreSQL)
  └── orders, stores, products → demand forecast pipeline

Genie Spaces
  ├── Manufacturing & Logistics (Room 1)
  └── Immunization & Procurement (Room 2)
```
