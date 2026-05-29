# Databricks notebook source
# =========================================================
# Author: Saurabh Chakraborty
# PySpark Pytest Framework for enrich layer
# =========================================================

import pytest
from pyspark.sql.functions import *
from pyspark.sql.types import *

CATALOG = "workspace"
SCHEMA = "default"

# Load Tables
orders_df = spark.table(f"{CATALOG}.{SCHEMA}.orders")
customers_df = spark.table(f"{CATALOG}.{SCHEMA}.customer")
products_df = spark.table(f"{CATALOG}.{SCHEMA}.products")
master_df = spark.table(f"{CATALOG}.{SCHEMA}.enrich_master_df")


# COMMAND ----------

# Test result tracker function 
test_results = []
def log_result(test_name, status, message=""):
    test_results.append({
        "test_name": test_name,
        "status": status,
        "message": message
    })

# COMMAND ----------


# =========================================================
# TEST 1 - MASTER DATAFRAME NOT EMPTY
# =========================================================
def test_master_not_empty():
    assert master_df.count() > 0

# =================================================================================================
# TEST 2 - SOURCE TARGET COUNT VALIDATION (This ensures no orders were lost during cleansing/joins)
# ==================================================================================================

def test_source_target_count():
    assert master_df.select("order_id").distinct().count() == orders_df.select("`Order ID`").distinct().count()

# =================================================================================================
# TEST 3 - REQUIRED COLUMNS PRESENT IN ENRICH LAYER
# ==================================================================================================

def test_required_columns():

    expected_columns = [
        "order_id",
        "order_date",
        "order_year",
        "customer_id",
        "customer_name",
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "quantity",
        "profit"
    ]

    for c in expected_columns:
        assert c in master_df.columns

# =========================================================
# TEST 4 - YEAR DERIVATION VALIDATION
# =========================================================

def test_order_year():

    invalid = master_df.filter(
        year(col("order_date")) != col("order_year")
    ).count()

    assert invalid == 0

# =========================================================
# TEST 5 - PROFIT DATATYPE VALIDATION 
# =========================================================

def test_profit_datatype():

    dtype = dict(master_df.dtypes)["profit"]

    assert dtype in ["double", "float", "decimal"]

# =========================================================
# TEST 6 - COUNTRY STANDARDIZATION
# =========================================================

def test_country_case():

    invalid = master_df.filter(
        col("country") != initcap(col("country"))).count()
    assert invalid == 0

# =========================================================
# TEST 7 - PROFIT ROUNDING CHECK
# =========================================================

def test_profit_rounding():
    invalid = master_df.filter(
        col("profit").cast("string").rlike(r"\d+\.\d{3,}")
    ).count()

    assert invalid == 0
# =========================================================
# TEST 8 - RECONCILLATION CHECK
# =========================================================

def test_all_orders_present():

    missing_orders = orders_df.select(
        "`Order ID`"
    ).distinct().subtract(
        master_df.select("order_id").distinct()
    ).count()

    assert missing_orders == 0, f"{missing_orders} order_ids missing in master_df"

def test_join_explosion():

    source_count = orders_df.count()
    target_count = master_df.count()

    assert target_count <= source_count, f"Join explosion detected. Orders count = {source_count}, Master count = {target_count}"

# =========================================================
# TEST 9 - ANOMALIES FLAG
# =========================================================

def test_missing_products_in_orders():

    missing_products_df = orders_df.alias("o").join(
        products_df.alias("p"),
        col("o.`Product ID`") == col("p.`Product ID`"),
        "left"
    ).filter(
        col("p.`Product ID`").isNull()
    ).select(
        col("o.`Product ID`")
    ).distinct()

    missing_count = missing_products_df.count()

    assert missing_count == 0, f"{missing_count} product_ids from orders missing in products table"



def test_inconsistent_product_price_within_order():

    inconsistent_price_df = orders_df.groupBy(
        "`Order ID`",
        "`Customer ID`",
        "`Product ID`"
    ).agg(
        countDistinct("Price").alias("distinct_price_count")
    ).filter(
        col("distinct_price_count") > 1
    )

    inconsistent_count = inconsistent_price_df.count()

    assert inconsistent_count == 0, "Same product has multiple prices within same order"

def test_duplicate_customer_order_product():

    duplicate_df = master_df.groupBy(
        "customer_id",
        "order_id",
        "product_id"
    ).count().filter(
        col("count") > 1
    )

    duplicate_count = duplicate_df.count()

    assert duplicate_count == 0, f"{duplicate_count} duplicate customer_id + order_id + product_id combinations found"

def test_product_id_maps_to_single_product():

    inconsistent_products = products_df.groupBy(
        "`Product ID`",
        "State"
    ).agg(
        countDistinct("`Product Name`").alias("distinct_product_count")
    ).filter(
        col("distinct_product_count") > 1
    )

    inconsistent_count = inconsistent_products.count()

    assert inconsistent_count == 0, "Same product_id maps to multiple product names"

# COMMAND ----------

# =========================================================
# Executing tests
# =========================================================

# putting all test functions in a list
all_tests = [
   test_master_not_empty
    ,test_source_target_count
    ,test_required_columns
    ,test_order_year
    ,test_profit_datatype
    ,test_country_case
    ,test_profit_rounding
    ,test_all_orders_present
    ,test_join_explosion
    ,test_missing_products_in_orders
    ,test_inconsistent_product_price_within_order
    ,test_duplicate_customer_order_product
    ,test_product_id_maps_to_single_product
 
]

for test in all_tests:
    try:
        test()
        log_result(
            test.__name__,
            "PASS"
        )
    except Exception as ex:
        log_result(
            test.__name__,
            "FAIL",
            str(ex)
        )

# COMMAND ----------

# =========================================================
# collating all results
# =========================================================

results_df = spark.createDataFrame(test_results)

display(results_df)

# COMMAND ----------

