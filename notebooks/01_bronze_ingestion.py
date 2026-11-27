# Databricks notebook source

# Set your workspace catalog name
# Replace this with the actual name shown in Catalog Explorer

catalog = "workspace"

# Create our Bronze schema
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.bronze
""")

# Create a volume for our raw CSV files
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS
    {catalog}.default.raw_data
""")

print("Bronze schema and raw data volume created!")


# COMMAND ----------


# Project: E-Commerce Lakehouse
# Notebook: 01_bronze_ingestion

from pyspark.sql import functions as F

print("E-Commerce Lakehouse Started")


# COMMAND ----------


catalog = "workspace"

customers_path = (
    f"/Volumes/{catalog}/default/raw_data/"
    "customers.csv"
)

orders_path = (
    f"/Volumes/{catalog}/default/raw_data/"
    "orders.csv"
)

print("Dataset paths configured!")


# COMMAND ----------


customers_df = (
    spark.read
    .option("header", True)
    .csv(customers_path)
)

display(customers_df)


# COMMAND ----------


# Display column names
print("Columns:")
print(customers_df.columns)

# Display data types
print("Schema:")
customers_df.printSchema()

# Count records
print("Total customers:", customers_df.count())


# COMMAND ----------


orders_df = (
    spark.read
    .option("header", True)
    .csv(orders_path)
)

display(orders_df)


# COMMAND ----------


print("Total orders:", orders_df.count())

orders_df.printSchema()



# COMMAND ----------


# Save customers
customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.bronze.customers")

# Save orders
orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.bronze.orders")

print("Bronze ingestion completed!")


# COMMAND ----------


# Verify customer table
spark.sql(f"""
    SELECT *
    FROM {catalog}.bronze.customers
    LIMIT 10
""").show()

# Verify order table
spark.sql(f"""
    SELECT *
    FROM {catalog}.bronze.orders
    LIMIT 10
""").show()

print("Bronze tables verified!")


# COMMAND ----------

