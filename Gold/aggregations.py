# Databricks notebook source
# =========================================================
# Author: Saurabh Chakraborty
# The purpose of this notebook is to perform aggregations on top of enrich layer
# =========================================================

from pyspark.sql.functions import round, sum
CATALOG = "workspace"
SCHEMA = "default"


# COMMAND ----------

# Read the enrich table
enrich_master_df = spark.read.table("enrich_master_df")

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
profit_aggregate_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.profit_aggregate_master")

# COMMAND ----------

# Creating view for SQL outputs
profit_aggregate_df.createOrReplaceTempView("master_view")

# COMMAND ----------

# Answer to question no 5 a
profit_by_year_df = spark.sql("""
SELECT
    order_year,
    ROUND(SUM(total_profit), 2) AS yearly_total_profit
FROM master_view
GROUP BY order_year
ORDER BY order_year
""")

# display(profit_by_year)
profit_by_year_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.profit_by_year_curated")

# COMMAND ----------

# Answer to question no 5 b
profit_by_year_category_df = spark.sql("""
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
profit_by_year_category_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.profit_by_year_category_curated")

# COMMAND ----------

# Answer to question no 5 c
profit_by_customer_df = spark.sql("""
SELECT
    customer_name,
    ROUND(SUM(total_profit), 2) AS yearly_total_profit
FROM master_view
GROUP BY customer_name
ORDER BY yearly_total_profit DESC
""")

# display(profit_by_customer)
profit_by_customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.profit_by_customer_curated")

# COMMAND ----------

# Answer to question no 5 d
profit_by_customer_year_df = spark.sql("""
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
profit_by_customer_year_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.profit_by_customer_year_curated")