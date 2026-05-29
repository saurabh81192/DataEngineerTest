# Databricks notebook source
# =========================================================
# Author: Saurabh Chakraborty
# The purpose of this notebook is to load the data from the tables into Spark DataFrames (Raw Data Ingestion)
# =========================================================


# Reading the raw data as tables as its UC-enabled cluster (Serverless Compute)
# Added the data as tables in the UC-enabled cluster (Serverless Compute)

# Reading the data as tables

CATALOG = "workspace"
SCHEMA = "default"

orders_df = spark.read.table(f"{CATALOG}.{SCHEMA}.orders")
# display(orders_df)

customers_df = spark.read.table(f"{CATALOG}.{SCHEMA}.customer")
# display(customers_df)

products_df = spark.read.table(f"{CATALOG}.{SCHEMA}.products")
# display(products_df)


# ===============================================================

# Another way of reading the data is to load the raw files in volumes as then read it as Spark DataFrames
# Added the files in Managed Volumes /Volumes/workspace/default/rawfiles
# ===============================================================

# Reading the raw data as files
products_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/workspace/default/rawfiles/Products.csv")
# display(products_df)

orders_df = spark.read.option("multiline", "true").json("/Volumes/workspace/default/rawfiles/Orders.json")
# display(orders_df)



# COMMAND ----------

# As customers data is in .xlsx format and free edition does not support adding libraries in cluster settings below is only psuedo code 

# Reading the raw data as files
# customers_df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load("/Volumes/workspace/default/rawfiles/Customers.xlsx")
# display(customers_df)