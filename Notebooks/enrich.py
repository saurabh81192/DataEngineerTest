# Databricks notebook source
# =========================================================
# Author: Saurabh Chakraborty
# The purpose of this notebook is to do data cleansing, create enrich layer dataframes
# =========================================================

from pyspark.sql.functions import col, year, round, broadcast,to_date, trim, when, initcap, regexp_replace, sum

# Reading raw data again as new spark session 
orders_df = spark.read.table("workspace.default.orders")
customers_df = spark.read.table("workspace.default.customer")
products_df = spark.read.table("workspace.default.products")

# Applying basic data cleansing on top of raw data and selecting required columns only

# cleaned orders_df
enrich_orders_df = orders_df.select(
    col("`Customer ID`").alias("customer_id"),
    to_date(col("`Order Date`"), "d/M/yyyy").alias("order_date"),
    col("Profit").alias("profit"),
    col("`Product ID`").alias("product_id"),
    col("`Order ID`").alias("order_id"),
    col("Quantity").alias("quantity"),
    col("Price").alias("price")
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
    col("Category").alias("category"),
    col("Sub-Category").alias("sub_category"),
    col("Product Name").alias("product_name")
).filter(
    col("`Product ID`").isNotNull() &
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
).dropDuplicates()

# display(enrich_customers_df)


# master table answer to question number 3
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

display(enrich_master_df)
# enrich_master_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("workspace.default.enrich_master_df")


# COMMAND ----------

enrich_orders_df.alias("o").join(
    enrich_products_df.alias("p"),
    col("o.product_id") == col("p.product_id"),
    "left"
).filter(
    col("p.product_id").isNull()
).select(
    "o.product_id"
).distinct().display()

# There are some Product Exists in Orders but Missing in Product Table 

# COMMAND ----------

# Profit aggregate table answer to question number 4
profit_aggregate_df = enrich_master_df.groupBy(
    "order_year",
    "category",
    "sub_category",
    "customer_name"
).agg(
    round(sum("profit"), 2).alias("total_profit")
)

# display(profit_aggregate_df)

# COMMAND ----------

# for sql outputs saved the dataframe as view
profit_aggregate_df.createOrReplaceTempView("master_view")

# COMMAND ----------

# Answer to question no 5 a
profit_by_year = spark.sql("""
SELECT
    order_year,
    ROUND(SUM(total_profit), 2) AS yearly_total_profit
FROM master_view
GROUP BY order_year
ORDER BY order_year
""")

# display(profit_by_year)

# COMMAND ----------

# Answer to question no 5 b
profit_by_year_category = spark.sql("""
SELECT
    order_year,
    category,
    ROUND(SUM(total_profit), 2) AS yearly_total_profit
FROM master_view
GROUP BY
    order_year,
    category
ORDER BY
    order_year,
    category
""")

# display(profit_by_year_category)

# COMMAND ----------

# Answer to question no 5 c
profit_by_customer = spark.sql("""
SELECT
    customer_name,
    ROUND(SUM(total_profit), 2) AS yearly_total_profit
FROM master_view
GROUP BY customer_name
ORDER BY yearly_total_profit DESC
""")

# display(profit_by_customer)

# COMMAND ----------

# Answer to question no 5 d
profit_by_customer_year = spark.sql("""
SELECT
    customer_name,
    order_year,
    ROUND(SUM(total_profit), 2) AS yearly_total_profit
FROM master_view
GROUP BY
    customer_name,
    order_year
ORDER BY
    customer_name,
    order_year
""")

# display(profit_by_customer_year)