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


from pyspark.sql import functions as F

# Catalog configuration
catalog = "workspace"

# Source data directory
raw_path = f"/Volumes/{catalog}/default/raw_data/"

# List of datasets
datasets = {
    "customers": "customers.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "products": "products.csv",
    "payments": "order_payments.csv",
    "reviews": "order_reviews.csv",
    "sellers": "sellers.csv",
    "geolocation": "geolocation.csv",
    "category_translation": "product_category_name_translation.csv"
}

print("Dataset configuration completed!")


# COMMAND ----------


from pyspark.sql import functions as F

def ingest_to_bronze(table_name, file_name):

    file_path = raw_path + file_name

    print(f"Starting ingestion: {table_name}")

    # Read CSV without automatic type inference
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(file_path)
    )

    # Preserve postal codes as strings
    if table_name == "customers":
        df = df.withColumn(
            "customer_zip_code_prefix",
            F.col("customer_zip_code_prefix").cast("string")
        )

    # Write to Delta
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.bronze.{table_name}")
    )

    record_count = df.count()

    print(f"Completed: {table_name}")
    print(f"Records loaded: {record_count}")

    return record_count



# COMMAND ----------


ingestion_results = []

for table_name, file_name in datasets.items():

    record_count = ingest_to_bronze(
        table_name,
        file_name
    )

    ingestion_results.append(
        (table_name, record_count)
    )

print("All datasets processed!")


# COMMAND ----------


results_df = spark.createDataFrame(
    ingestion_results,
    ["table_name", "record_count"]
)

display(results_df)



# COMMAND ----------

# SHOW TABLES IN workspace.bronze;
display(spark.sql("SHOW TABLES IN workspace.bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.bronze.customers
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) 
# MAGIC FROM workspace.bronze.orders;