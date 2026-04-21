# Databricks notebook source
# MAGIC %md
# MAGIC # Demand Forecasting from Lakebase Autoscaling
# MAGIC
# MAGIC 1. Read order data from **Lakebase Autoscaling** (`projects/gsk-vaccine-sc`)
# MAGIC 2. Generate demand forecast (7-day moving average + seasonality)
# MAGIC 3. Write results to Delta for dashboards
# MAGIC
# MAGIC **Migration notes (Provisioned -> Autoscaling):**
# MAGIC - `psycopg2` replaced with `psycopg` (v3)
# MAGIC - `w.database` API replaced with `w.postgres` API
# MAGIC - Hierarchical resource names: `projects/{id}/branches/{id}/endpoints/{id}`
# MAGIC - OAuth token scoped to endpoint (1-hour expiry)

# COMMAND ----------

# MAGIC %pip install -U "databricks-sdk>=0.81.0" "psycopg[binary]>=3.0"
# MAGIC %restart_python

# COMMAND ----------

import psycopg
import pandas as pd
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Lakebase Autoscaling

# COMMAND ----------

PROJECT_ID = "gsk-vaccine-sc"
BRANCH = "production"
ENDPOINT_NAME = f"projects/{PROJECT_ID}/branches/{BRANCH}/endpoints/primary"

CATALOG = "sean_zhang_catalog"
SCHEMA = "gsk_vaccine_sc_v2"
SAVE_TABLE = f"{CATALOG}.{SCHEMA}.demand_forecast_analytics"

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

with psycopg.connect(conn_string) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        print(f"Connected to Lakebase Autoscaling: {cur.fetchone()[0]}")

        cur.execute("SELECT COUNT(*) FROM orders")
        print(f"Orders in Lakebase: {cur.fetchone()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read historical data via Spark SQL over psycopg3

# COMMAND ----------

def read_lakebase_as_spark(query: str) -> "pyspark.sql.DataFrame":
    """Read from Lakebase Autoscaling into a Spark DataFrame via psycopg3."""
    cred_fresh = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
    fresh_conn = (
        f"host={host} dbname=databricks_postgres "
        f"user={username} password={cred_fresh.token} sslmode=require"
    )
    with psycopg.connect(fresh_conn) as conn:
        pdf = pd.read_sql(query, conn)
    return spark.createDataFrame(pdf)

historical_query = """
    SELECT
        o.order_date::date as order_date,
        COUNT(*) as order_count,
        SUM(o.quantity_cases) as total_cases,
        SUM(o.quantity_cases * p.unit_price) as total_value,
        AVG(o.quantity_cases) as avg_order_size
    FROM orders o
    JOIN stores s ON o.to_store_id = s.store_id
    JOIN products p ON o.product_id = p.product_id
    GROUP BY o.order_date::date
    ORDER BY order_date
"""

historical_sdf = read_lakebase_as_spark(historical_query)
historical_df = historical_sdf.toPandas()
historical_df['forecast_ind'] = False
historical_df = historical_df[
    ['order_date', 'order_count', 'total_cases', 'total_value', 'forecast_ind']
]

print(f"Historical data: {len(historical_df)} days")
display(historical_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Forecast

# COMMAND ----------

def generate_forecast_data(historical_df, days_forward):
    """Generate forecast data based on historical trends and weekly seasonality."""
    recent_orders = historical_df['order_count'].tail(7)
    avg_orders = recent_orders.mean()
    recent_cases = historical_df['total_cases'].tail(7)
    avg_cases = recent_cases.mean()
    recent_values = historical_df['total_value'].tail(7)
    avg_value = recent_values.mean()

    if len(historical_df) >= 14:
        prev_week_orders = historical_df['order_count'].iloc[-14:-7].mean()
        growth_rate = (avg_orders - prev_week_orders) / prev_week_orders
        growth_rate = max(-0.2, min(0.2, growth_rate))
    else:
        growth_rate = 0

    from datetime import timedelta
    last_date = pd.to_datetime(historical_df['order_date'].iloc[-1])
    forecast_data = []
    for i in range(1, days_forward + 1):
        forecast_date = last_date + timedelta(days=i)
        day_of_week = forecast_date.weekday()

        seasonal_factor = 1.0
        if day_of_week == 6:
            seasonal_factor = 0.7
        elif day_of_week == 5:
            seasonal_factor = 0.8
        elif day_of_week in [1, 2, 3]:
            seasonal_factor = 1.1

        trend_factor = 1 + (growth_rate * i / 7)

        forecast_orders = max(1, int(avg_orders * seasonal_factor * trend_factor))
        forecast_cases = max(1, int(avg_cases * seasonal_factor * trend_factor))
        forecast_value = max(1, avg_value * seasonal_factor * trend_factor)

        forecast_data.append({
            "order_date": forecast_date,
            "order_count": forecast_orders,
            "total_cases": forecast_cases,
            "total_value": forecast_value,
        })

    return pd.DataFrame(forecast_data)


def plot_forecast(historical_df, forecast_df):
    """Plot historical and forecasted order data."""
    import matplotlib.pyplot as plt

    plt.figure(figsize=(12, 6))
    plt.plot(
        historical_df['order_date'],
        historical_df['order_count'],
        label='Historical',
        color='green',
        marker='o',
    )
    plt.plot(
        forecast_df['order_date'],
        forecast_df['order_count'],
        label='Forecast',
        color='purple',
        marker='o',
    )
    plt.xlabel('Order Date')
    plt.ylabel('Order Count')
    plt.title('Order Count Time Series Forecast')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Forecast & Save to Delta

# COMMAND ----------

forecast_df = generate_forecast_data(historical_df, days_forward=30)
forecast_df['forecast_ind'] = True
forecast_df = forecast_df[
    ['order_date', 'order_count', 'total_cases', 'total_value', 'forecast_ind']
]

historical_sdf = spark.createDataFrame(historical_df)
forecast_sdf = spark.createDataFrame(forecast_df)
sdf_union = historical_sdf.union(forecast_sdf)

sdf_union.write.format("delta").mode("overwrite").saveAsTable(SAVE_TABLE)
print(f"Saved {sdf_union.count()} rows to {SAVE_TABLE}")

plot_forecast(historical_df, forecast_df)

# COMMAND ----------

display(spark.table(SAVE_TABLE))
