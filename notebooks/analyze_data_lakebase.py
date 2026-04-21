# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read Data from Lakebase
# MAGIC 2. Demand forecasting
# MAGIC 3. Serve it to dashboard

# COMMAND ----------

# DBTITLE 1,Read data from Lakebase
# Based on database/demo_setup.py
%pip install psycopg2
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd

# COMMAND ----------

CATALOG = "sz_lakebase_pg"
SCHEMA = "public"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

def generate_forecast_data(historical_df, days_forward):
    """Generate forecast data based on historical data and trends."""
    # 2. Calculate 7-day moving averages
    recent_orders = historical_df['order_count'].tail(7)
    avg_orders = recent_orders.mean()
    recent_cases = historical_df['total_cases'].tail(7)
    avg_cases = recent_cases.mean()
    recent_values = historical_df['total_value'].tail(7)
    avg_value = recent_values.mean()

    # 3. Calculate growth trend (week-over-week comparison)
    if len(historical_df) >= 14:
        prev_week_orders = historical_df['order_count'].iloc[-14:-7].mean()
        growth_rate = (avg_orders - prev_week_orders) / prev_week_orders
        growth_rate = max(-0.2, min(0.2, growth_rate))  # Cap at ±20%
    else:
        growth_rate = 0

    # 4. Generate forecasts with seasonality patterns
    from datetime import timedelta
    last_date = pd.to_datetime(historical_df['order_date'].iloc[-1])
    forecast_data = []
    for i in range(1, days_forward + 1):
        forecast_date = last_date + timedelta(days=i)
        day_of_week = forecast_date.weekday()

        # Weekly seasonality factors
        seasonal_factor = 1.0
        if day_of_week == 6:      # Sunday: 70% of normal
            seasonal_factor = 0.7
        elif day_of_week == 5:    # Saturday: 80% of normal  
            seasonal_factor = 0.8
        elif day_of_week in [1, 2, 3]:  # Tue-Thu: 110% of normal
            seasonal_factor = 1.1

        # Apply growth trend over time
        trend_factor = 1 + (growth_rate * i / 7)

        # Calculate forecasted values
        forecast_orders = max(1, int(avg_orders * seasonal_factor * trend_factor))
        forecast_cases = max(1, int(avg_cases * seasonal_factor * trend_factor))
        forecast_value = max(1, avg_value * seasonal_factor * trend_factor)

        forecast_data.append({
            "order_date": forecast_date,
            "order_count": forecast_orders,
            "total_cases": forecast_cases,
            "total_value": forecast_value
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
        marker='o'
    )
    plt.plot(
        forecast_df['order_date'], 
        forecast_df['order_count'], 
        label='Forecast', 
        color='purple', 
        marker='o'
    )
    plt.xlabel('Order Date')
    plt.ylabel('Order Count')
    plt.title('Order Count Time Series Forecast')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def get_demand_forecast(
    days_back=90, 
    days_forward=30, 
    region=None,
    save_location=f"users.sean_zhang.brickhouse_lakebase_analytics"
):
    """Advanced demand forecasting using historical patterns"""

    # 1. Get historical order data from Unity Catalog
    historical_query = f"""
        SELECT 
            DATE(o.order_date) as order_date,
            COUNT(*) as order_count,
            SUM(o.quantity_cases) as total_cases,
            SUM(o.quantity_cases * p.unit_price) as total_value,
            AVG(o.quantity_cases) as avg_order_size
        FROM {CATALOG}.{SCHEMA}.orders o
        JOIN {CATALOG}.{SCHEMA}.stores s ON o.to_store_id = s.store_id
        JOIN {CATALOG}.{SCHEMA}.products p ON o.product_id = p.product_id
        GROUP BY DATE(o.order_date)
        ORDER BY order_date
    """

    # Use Spark DataFrame and convert to Pandas DataFrame
    historical_sdf = spark.sql(historical_query)
    historical_df = historical_sdf.toPandas()
    historical_df['forecast_ind'] = False
    historical_df = historical_df[
        ['order_date', 'order_count', 'total_cases', 'total_value', 'forecast_ind']
    ]

    historial_sdf = spark.createDataFrame(historical_df)
    print(historial_sdf.columns)

    # Generate forecast data
    forecast_df = generate_forecast_data(historical_df, days_forward)

    # Forecast indicator
    forecast_df['forecast_ind'] = True
    forecast_df = forecast_df[
        ['order_date', 'order_count', 'total_cases', 'total_value', 'forecast_ind']
    ]

    # Convert forecast DataFrame to Spark DataFrame
    forecast_sdf = spark.createDataFrame(forecast_df)
    print(forecast_sdf.columns)

    # Union spark dataframes
    sdf_union = historial_sdf.union(forecast_sdf)

    # Save the forecast data to Unity Catalog
    sdf_union.write.format("delta").mode("overwrite").saveAsTable(save_location)

    # Plot the forecast
    plot_forecast(historical_df, forecast_df)

# Rerun the function and save it into users.sean_zhang.brickhouse_lakebase_analytics
get_demand_forecast()

# COMMAND ----------

spark.table('users.sean_zhang.brickhouse_lakebase_analytics').display()