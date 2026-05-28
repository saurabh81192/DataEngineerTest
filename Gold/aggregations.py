# Databricks notebook source
# =========================================================
# Author: Saurabh Chakraborty
# The purpose of this notebook is to perform aggregations on top of enrich layer
# =========================================================

from pyspark.sql.functions import col, year, round, broadcast,to_date, trim, when, initcap, regexp_replace, sum


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