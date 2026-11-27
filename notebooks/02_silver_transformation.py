# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "workspace"

# Create Silver schema
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.silver
""")

print("Silver schema created successfully!")


# COMMAND ----------

# Read Bronze customer data

customers = spark.table(
    f"{catalog}.bronze.customers"
)

# Check original record count

print("Original records:", customers.count())

# Remove records without customer ID

customers_clean = customers.filter(
    F.col("customer_id").isNotNull()
)

# Remove duplicate customer IDs

customers_clean = customers_clean.dropDuplicates(
    ["customer_id"]
)

# Preserve postal code as a string

customers_clean = customers_clean.withColumn(
    "customer_zip_code_prefix",
    F.col("customer_zip_code_prefix").cast("string")
)

# Save cleaned data

customers_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.customers")

print("Cleaned records:", customers_clean.count())

print("Customer Silver transformation completed!")

# COMMAND ----------


from pyspark.sql import functions as F

catalog = "workspace"

# Read Bronze orders table
orders = spark.table(f"{catalog}.bronze.orders")

print("Original order records:", orders.count())

# Remove records without order_id
orders_clean = orders.filter(
    F.col("order_id").isNotNull()
)

# Remove duplicate order IDs
orders_clean = orders_clean.dropDuplicates(["order_id"])

# Clean order status
orders_clean = orders_clean.withColumn(
    "order_status",
    F.trim(F.col("order_status"))
)

# Convert date columns from strings to timestamps
timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column in timestamp_columns:
    orders_clean = orders_clean.withColumn(
        column,
        F.to_timestamp(F.col(column), "yyyy-MM-dd HH:mm:ss")
    )

# Create a separate purchase date column
orders_clean = orders_clean.withColumn(
    "order_purchase_date",
    F.to_date("order_purchase_timestamp")
)

# Save the cleaned table in Silver
orders_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.orders")

print("Silver orders transformation completed!")
print("Cleaned order records:", orders_clean.count())


# COMMAND ----------


# Check record count
silver_orders = spark.table("workspace.silver.orders")

print("Silver orders count:", silver_orders.count())

# View sample records
display(silver_orders.limit(10))

# Check for duplicate order IDs
duplicate_orders = (
    silver_orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate order IDs:", duplicate_orders.count())

# Check missing order IDs
print(
    "Missing order IDs:",
    silver_orders.filter(F.col("order_id").isNull()).count()
)

# Check table schema
silver_orders.printSchema()


# COMMAND ----------

