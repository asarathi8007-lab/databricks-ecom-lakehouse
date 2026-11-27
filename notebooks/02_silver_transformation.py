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

### For creating silver layer orders table
from pyspark.sql import functions as F

catalog = "workspace"

# Read Bronze order_items
items = spark.table(f"{catalog}.bronze.order_items")

print("Original records:", items.count())

# Remove records missing essential identifiers
items_clean = items.filter(
    F.col("order_id").isNotNull() &
    F.col("order_item_id").isNotNull()
)

# Convert data types
items_clean = (
    items_clean
    .withColumn("order_item_id", F.col("order_item_id").cast("int"))
    .withColumn("price", F.col("price").cast("decimal(10,2)"))
    .withColumn("freight_value", F.col("freight_value").cast("decimal(10,2)"))
    .withColumn(
        "shipping_limit_date",
        F.to_timestamp("shipping_limit_date", "yyyy-MM-dd HH:mm:ss")
    )
)

# Remove duplicate order-item combinations
items_clean = items_clean.dropDuplicates(
    ["order_id", "order_item_id"]
)

# Save Silver table
items_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.order_items")

print("Silver order_items transformation completed!")
print("Cleaned records:", items_clean.count())


# COMMAND ----------


silver_items = spark.table("workspace.silver.order_items")

print("Total records:", silver_items.count())

# View sample data
display(silver_items.limit(10))

# Check duplicate order-item combinations
duplicates = (
    silver_items
    .groupBy("order_id", "order_item_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate order-item combinations:", duplicates.count())

# Check schema
silver_items.printSchema()


# COMMAND ----------

### For creating Silver layer payments table
from pyspark.sql import functions as F

catalog = "workspace"

# Read Bronze payments table
payments = spark.table(f"{catalog}.bronze.payments")

print("Original payment records:", payments.count())

# Remove records without order_id
payments_clean = payments.filter(
    F.col("order_id").isNotNull()
)

# Clean columns and convert data types
payments_clean = (
    payments_clean
    .withColumn(
        "payment_sequential",
        F.col("payment_sequential").cast("int")
    )
    .withColumn(
        "payment_installments",
        F.col("payment_installments").cast("int")
    )
    .withColumn(
        "payment_value",
        F.col("payment_value").cast("decimal(10,2)")
    )
    .withColumn(
        "payment_type",
        F.lower(F.trim(F.col("payment_type")))
    )
)

# Save Silver table
payments_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.payments")

print("Silver payments transformation completed!")
print("Cleaned payment records:", payments_clean.count())


# COMMAND ----------


silver_payments = spark.table("workspace.silver.payments")

print("Total payment records:", silver_payments.count())

# View sample records
display(silver_payments.limit(10))

# Analyse payment methods
display(
    silver_payments
    .groupBy("payment_type")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("payment_value").alias("total_payment_value")
    )
    .orderBy(F.desc("total_payment_value"))
)

# Check payment types
display(
    silver_payments
    .select("payment_type")
    .distinct()
)


# COMMAND ----------

### Silver layer code for products table
from pyspark.sql import functions as F

catalog = "workspace"

# Read Bronze products
products = spark.table(f"{catalog}.bronze.products")

print("Original product records:", products.count())

# Remove records without product_id
products_clean = products.filter(
    F.col("product_id").isNotNull()
)

# Remove duplicate product IDs
products_clean = products_clean.dropDuplicates(["product_id"])

# Clean category name
products_clean = products_clean.withColumn(
    "product_category_name",
    F.trim(F.col("product_category_name"))
)

# Convert numeric columns
integer_columns = [
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]

for column in integer_columns:
    products_clean = products_clean.withColumn(
        column,
        F.col(column).cast("int")
    )

# Save Silver products table
products_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.products")

print("Silver products transformation completed!")
print("Cleaned product records:", products_clean.count())


# COMMAND ----------


silver_products = spark.table("workspace.silver.products")

print("Total products:", silver_products.count())

# View sample records
display(silver_products.limit(10))

# Check duplicate product IDs
duplicates = (
    silver_products
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate product IDs:", duplicates.count())

# Analyse product categories
display(
    silver_products
    .groupBy("product_category_name")
    .count()
    .orderBy(F.desc("count"))
    .limit(15)
)

# Check schema
silver_products.printSchema()


# COMMAND ----------

### Inspect the records in review before changing the bronze layer file


from pyspark.sql import functions as F

reviews = spark.table("workspace.bronze.reviews")

# Inspect examples that don't match the expected ID format
invalid_reviews = reviews.filter(
    F.col("review_id").isNotNull() &
    ~F.upper(F.col("review_id")).rlike("^[0-9A-F]{32}$")
)

display(invalid_reviews.limit(15))

# Check the lengths of review IDs
display(
    invalid_reviews
    .withColumn("id_length", F.length("review_id"))
    .groupBy("id_length")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------


### For corrected CSV read

reviews_test = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .load("/Volumes/workspace/default/raw_data/order_reviews.csv")
)

print("Fresh CSV record count:", reviews_test.count())

print("Column count:", len(reviews_test.columns))
print("Columns:", reviews_test.columns)

# Check review ID quality
invalid_reviews_test = reviews_test.filter(
    F.col("review_id").isNull() |
    ~F.upper(F.col("review_id")).rlike("^[0-9A-F]{32}$")
)

print("Invalid review IDs:", invalid_reviews_test.count())

# Inspect correctly parsed records
display(reviews_test.limit(10))


# COMMAND ----------


reviews_test.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.bronze.reviews")

print("Bronze reviews table successfully repaired!")


# COMMAND ----------

### For creating Silver layer review table


from pyspark.sql import functions as F

catalog = "workspace"

reviews = spark.table(f"{catalog}.bronze.reviews")

# Remove records without review ID
reviews_clean = reviews.filter(
    F.col("review_id").isNotNull()
)

# Remove duplicate review IDs
reviews_clean = reviews_clean.dropDuplicates(["review_id"])

# Convert review score
reviews_clean = reviews_clean.withColumn(
    "review_score",
    F.expr("try_cast(review_score AS INT)")
)

# Clean comment columns
reviews_clean = (
    reviews_clean
    .withColumn("review_comment_title", F.trim("review_comment_title"))
    .withColumn("review_comment_message", F.trim("review_comment_message"))
)

# Convert date columns safely
reviews_clean = reviews_clean.withColumn(
    "review_creation_date",
    F.expr("try_cast(review_creation_date AS DATE)")
)

reviews_clean = reviews_clean.withColumn(
    "review_answer_timestamp",
    F.expr("try_cast(review_answer_timestamp AS TIMESTAMP)")
)

# Save Silver table
reviews_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.reviews")

print("Silver reviews transformation completed!")
print("Silver review records:", reviews_clean.count())


# COMMAND ----------


silver_reviews = spark.table("workspace.silver.reviews")

print("Total reviews:", silver_reviews.count())

print(
    "Duplicate review IDs:",
    silver_reviews.groupBy("review_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Invalid review scores:",
    silver_reviews.filter(
        F.col("review_score").isNull() |
        ~F.col("review_score").between(1, 5)
    ).count()
)

silver_reviews.printSchema()

display(silver_reviews.limit(10))


# COMMAND ----------

### To inspect bronze layer review table schema

reviews = spark.table("workspace.bronze.reviews")

# Display column names and data types
reviews.printSchema()

# Display sample records
display(reviews.limit(10))

# COMMAND ----------

### Check for malformed records in review table after transformation


reviews = spark.table("workspace.bronze.reviews")

# Count rows with missing review IDs
print(
    "Rows with missing review_id:",
    reviews.filter(F.col("review_id").isNull()).count()
)

# Count rows with invalid review IDs
print(
    "Rows with unexpected review_id:",
    reviews.filter(
        ~F.col("review_id").rlike("^[a-f0-9]{32}$") &
        F.col("review_id").isNotNull()
    ).count()
)

# Inspect malformed records
display(
    reviews.filter(
        F.col("review_id").isNull() |
        ~F.col("review_id").rlike("^[a-f0-9]{32}$")
    )
)


# COMMAND ----------

### Code for transforming and saving Silver layer seller table
from pyspark.sql import functions as F

catalog = "workspace"

# Read Bronze sellers
sellers = spark.table(f"{catalog}.bronze.sellers")

print("Original seller records:", sellers.count())

# Remove records without seller_id
sellers_clean = sellers.filter(
    F.col("seller_id").isNotNull()
)

# Remove duplicate seller IDs
sellers_clean = sellers_clean.dropDuplicates(["seller_id"])

# Clean seller information
sellers_clean = (
    sellers_clean
    .withColumn(
        "seller_zip_code_prefix",
        F.col("seller_zip_code_prefix").cast("string")
    )
    .withColumn(
        "seller_city",
        F.lower(F.trim(F.col("seller_city")))
    )
    .withColumn(
        "seller_state",
        F.upper(F.trim(F.col("seller_state")))
    )
)

# Save Silver sellers table
sellers_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.sellers")

print("Silver sellers transformation completed!")
print("Cleaned seller records:", sellers_clean.count())


# COMMAND ----------


silver_sellers = spark.table("workspace.silver.sellers")

print("Total sellers:", silver_sellers.count())

# Sample records
display(silver_sellers.limit(10))

# Sellers by state
display(
    silver_sellers
    .groupBy("seller_state")
    .count()
    .orderBy(F.desc("count"))
)

# Check duplicate seller IDs
print(
    "Duplicate seller IDs:",
    silver_sellers.groupBy("seller_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

silver_sellers.printSchema()


# COMMAND ----------

### code for transforming and saving Silver layer geolocation table
from pyspark.sql import functions as F

catalog = "workspace"

# Read Bronze geolocation
geo = spark.table(f"{catalog}.bronze.geolocation")

print("Original geolocation records:", geo.count())

# Remove records without ZIP code prefix
geo_clean = geo.filter(
    F.col("geolocation_zip_code_prefix").isNotNull()
)

# Preserve ZIP code prefix as string
geo_clean = geo_clean.withColumn(
    "geolocation_zip_code_prefix",
    F.col("geolocation_zip_code_prefix").cast("string")
)

# Convert latitude and longitude
geo_clean = (
    geo_clean
    .withColumn(
        "geolocation_lat",
        F.col("geolocation_lat").cast("double")
    )
    .withColumn(
        "geolocation_lng",
        F.col("geolocation_lng").cast("double")
    )
)

# Standardise city and state
geo_clean = (
    geo_clean
    .withColumn(
        "geolocation_city",
        F.lower(F.trim(F.col("geolocation_city")))
    )
    .withColumn(
        "geolocation_state",
        F.upper(F.trim(F.col("geolocation_state")))
    )
)

# Remove exact duplicate records
geo_clean = geo_clean.dropDuplicates()

# Save Silver geolocation
geo_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.geolocation")

print("Silver geolocation transformation completed!")
print("Cleaned records:", geo_clean.count())


# COMMAND ----------


silver_geo = spark.table("workspace.silver.geolocation")

print("Total geolocation records:", silver_geo.count())

# Sample records
display(silver_geo.limit(10))

# Check multiple records for the same ZIP prefix
display(
    silver_geo
    .groupBy("geolocation_zip_code_prefix")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
    .limit(10)
)

# Check null coordinates
print(
    "Records with missing coordinates:",
    silver_geo.filter(
        F.col("geolocation_lat").isNull() |
        F.col("geolocation_lng").isNull()
    ).count()
)

silver_geo.printSchema()


# COMMAND ----------

### code for transforming and saving Silver layer category translation table


from pyspark.sql import functions as F

catalog = "workspace"

# Read Bronze category translation
categories = spark.table(
    f"{catalog}.bronze.category_translation"
)

print("Original category records:", categories.count())

# Remove records without category names
categories_clean = categories.filter(
    F.col("product_category_name").isNotNull() &
    F.col("product_category_name_english").isNotNull()
)

# Standardise category names
categories_clean = (
    categories_clean
    .withColumn(
        "product_category_name",
        F.lower(F.trim(F.col("product_category_name")))
    )
    .withColumn(
        "product_category_name_english",
        F.lower(F.trim(F.col("product_category_name_english")))
    )
)

# Remove exact duplicate mappings
categories_clean = categories_clean.dropDuplicates()

# Save Silver category translation
categories_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.silver.category_translation")

print("Silver category translation completed!")
print("Cleaned category records:", categories_clean.count())


# COMMAND ----------


silver_categories = spark.table(
    "workspace.silver.category_translation"
)

print("Total category mappings:", silver_categories.count())

# View translations
display(silver_categories.limit(20))

# Check duplicate mappings
print(
    "Duplicate mappings:",
    silver_categories.groupBy(
        "product_category_name",
        "product_category_name_english"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

silver_categories.printSchema()


# COMMAND ----------

### Sliver layer final verification
catalog = "workspace"

silver_tables = [
    "customers",
    "orders",
    "order_items",
    "payments",
    "products",
    "reviews",
    "sellers",
    "geolocation",
    "category_translation"
]

for table_name in silver_tables:

    table_path = f"{catalog}.silver.{table_name}"

    df = spark.table(table_path)

    print(
        f"{table_name}: {df.count()} records"
    )


# COMMAND ----------

