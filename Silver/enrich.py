# Databricks notebook source
# ====================================================================================
# Author: Saurabh Chakraborty
# The purpose of this notebook is to do data cleansing, create enrich layer dataframes
# =====================================================================================

from pyspark.sql.functions import col, year, round, broadcast, to_date, trim, when, initcap, regexp_replace, sum

# Reading raw data again as new spark session
CATALOG = "workspace"
SCHEMA = "default"

orders_df = spark.read.table(f"{CATALOG}.{SCHEMA}.orders")
customers_df = spark.read.table(f"{CATALOG}.{SCHEMA}.customer")
products_df = spark.read.table(f"{CATALOG}.{SCHEMA}.products")


orders_df = orders_df.dropDuplicates()
# I avoided blindly deduplicating based on business keys because profiling revealed scenarios where the same customer, order, and product combination appeared with different quantities and prices. Instead of risking transactional data loss, I treated these as potential anomalies in test case Order ID	Product ID	CA-2017-118017	TEC-AC-10002006

products_df = products_df.dropDuplicates()
# I avoided deduplicating based on product id and state which I think is the grain of the data. But dropping would lose which product name is correct one , may lose correct data


# COMMAND ----------

# Applying basic data cleansing on top of raw data and selecting required columns only

# cleaned orders_df
enrich_orders_df = orders_df.select(
    col("`Order ID`").alias("order_id"),
    col("`Customer ID`").alias("customer_id"),
    col("`Product ID`").alias("product_id"),
    to_date(col("`Order Date`"), "d/M/yyyy").alias("order_date"),
    col("Quantity").alias("quantity"),
    col("Price").alias("price"),
    col("Profit").alias("profit")
).filter(
    col("`Customer ID`").isNotNull() &
    col("`Order Date`").isNotNull() &
    col("`Product ID`").isNotNull() &
    col("`Order ID`").isNotNull() &
    col("Profit").isNotNull() &
    col("Price").isNotNull()
)
# display(enrich_orders_df)

# cleaned products_df
enrich_products_df = products_df.select(
    col("`Product ID`").alias("product_id"),
    col("State").alias("state"),
    col("Category").alias("category"),
    col("Sub-Category").alias("sub_category"),
    col("Product Name").alias("product_name")
).filter(
    col("`Product ID`").isNotNull() &
    col("State").isNotNull() &
    col("Category").isNotNull() &
    col("Sub-Category").isNotNull() &
    col("`Product Name`").isNotNull()
)
# display(enrich_products_df)

# cleaned customers_df 
# cleansing like some names had space in between or trailing spaces and special characters in name and some null values proper name casing and dropping duplicates
enrich_customers_df = customers_df.select(
    trim(col("`Customer ID`")).alias("customer_id"),
    when(
        col("`Customer Name`").isNull(),
        "Unknown"
    ).otherwise(
        initcap(
            regexp_replace(
                regexp_replace(
                    trim(col("`Customer Name`")),
                    r"[^a-zA-Z\s]",
                    ""
                ),
                r"\s+",
                " "
            )
        )
    ).alias("customer_name"),
    initcap(trim(col("Country"))).alias("country"),
    initcap(trim(col("City"))).alias("city")
).filter(
    col("customer_id").isNotNull() &
    col("country").isNotNull() &
    col("city").isNotNull()
).dropDuplicates(["customer_id"])

# display(enrich_customers_df)

# master table answer to question number 3
# Assuming product dimension is small enough to fit in memory, broadcast join is used

enrich_master_df = enrich_orders_df.alias("o") \
    .join(
        enrich_customers_df.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    ) \
    .join(
        broadcast(enrich_products_df).alias("p"),
        col("o.product_id") == col("p.product_id"),
        "left"
    ) \
    .select(
        col("o.order_id"),
        col("o.order_date"),
        year(col("o.order_date")).alias("order_year"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.country"),
        col("o.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.sub_category"),
        col("o.quantity"),
        round(col("o.profit"), 2).alias("profit")
    )

# display(enrich_master_df)
# overwrite used for assignment simplicity
# production would typically use merge/incremental loads
enrich_master_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.enrich_master_df")


# COMMAND ----------

# enrich_orders_df.alias("o").join(
#     enrich_products_df.alias("p"),
#     col("o.product_id") == col("p.product_id"),
#     "left"
# ).filter(
#     col("p.product_id").isNull()
# ).select(
#     "o.product_id"
# ).distinct().display()

# There are some Product Exists in Orders but Missing in Product Table 

# COMMAND ----------

# orders_df.groupBy(
#         "`Order ID`",
#         "`Product ID`"
#     ).count().filter(
#         col("count") > 1
#     ).show()

# COMMAND ----------

# customers_df.groupBy(
#         "`Customer ID`"
#     ).count().filter(
#         col("count") > 1
#     ).show()

# COMMAND ----------

# products_df.groupBy(
#         "`Product ID`",
#          "State"
#     ).count().filter(
#         col("count") > 1
#     ).show()

# COMMAND ----------

# enrich_master_df.filter(
#     col("category").isNull()
# ).select(
#     "product_id"
# ).distinct().display()