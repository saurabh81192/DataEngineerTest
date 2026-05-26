# Databricks notebook source
# =========================================================
# Author: Saurabh Chakraborty
# PySpark Pytest Framework for ingestion and enrich layer
# =========================================================

import pytest
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load Tables
orders_df = spark.table("workspace.default.orders")
customers_df = spark.table("workspace.default.customer")
products_df = spark.table("workspace.default.products")
master_df = spark.table("workspace.default.enrich_master_df")


# Test result tracker function 
test_results = []
def log_result(test_name, status, message=""):
    test_results.append({
        "test_name": test_name,
        "status": status,
        "message": message
    })



dataframes = [
    ("orders_df", orders_df),
    ("customers_df", customers_df),
    ("products_df", products_df),
    ("master_df", master_df)
]

@pytest.mark.parametrize(
    "df_name,df",
    dataframes
)

# =========================================================
# TEST 1 - DATA NOT EMPTY
# =========================================================
def test_dataframe_not_empty(df_name, df):

    assert df.count() > 0, f"{df_name} is empty"

# =========================================================
# TEST 2 - REQUIRED COLUMNS EXIST
# =========================================================

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

null_check_columns = [
    "order_id",
    "customer_id",
    "product_id",
    "profit"
]

@pytest.mark.parametrize(
    "column_name",
    null_check_columns
)
# =========================================================
# TEST 3 - COLUMNS NULL CHECK
# =========================================================
def test_null_validation(column_name):

    null_count = master_df.filter(
        col(column_name).isNull()
    ).count()

    assert null_count == 0, f"{column_name} contains null values"

# =========================================================
# TEST 4 - DUPLICATE CHECK
# =========================================================

def test_no_duplicates():

    assert master_df.count() == master_df.distinct().count()

# =========================================================
# TEST 5 - PROFIT ROUNDING CHECK
# =========================================================

def test_profit_rounding():

    invalid = master_df.filter(
        col("profit").cast("string").rlike(r"\d+\.\d{3,}")
    ).count()

    assert invalid == 0

# =========================================================
# TEST 6 - NEGATIVE QUANTITY CHECK
# =========================================================

def test_negative_quantity():

    assert master_df.filter(
        col("quantity") < 0
    ).count() == 0

# =========================================================
# TEST 7 - ORDER DATE VALIDATION
# =========================================================

def test_order_date_not_null():

    assert master_df.filter(
        col("order_date").isNull()
    ).count() == 0

# =========================================================
# TEST 8 - PRODUCT JOIN VALIDATION
# =========================================================

def test_product_join():

    unmatched = master_df.filter(
        col("product_name").isNull()
    ).count()

    assert unmatched == 0

# =========================================================
# TEST 9 - CUSTOMER JOIN VALIDATION
# =========================================================

def test_customer_join():

    unmatched = master_df.filter(
        col("customer_name").isNull()
    ).count()

    assert unmatched == 0

# =========================================================
# TEST 10 - SOURCE TARGET COUNT VALIDATION
# =========================================================

def test_source_target_count():

    assert master_df.count() <= orders_df.count()

# =========================================================
# TEST 11 - YEAR DERIVATION VALIDATION
# =========================================================

def test_order_year():

    invalid = master_df.filter(
        year(col("order_date")) != col("order_year")
    ).count()

    assert invalid == 0

# =========================================================
# TEST 12 - PROFIT DATATYPE VALIDATION (Can be parameterized and reused for other columns)
# =========================================================

def test_profit_datatype():

    dtype = dict(master_df.dtypes)["profit"]

    assert dtype in ["double", "float", "decimal"]

# =========================================================
# TEST 13 - COUNTRY STANDARDIZATION (Can be parameterized and reused for other columns)
# =========================================================

def test_country_case():

    invalid = master_df.filter(
        col("country") != initcap(col("country"))
    ).count()

    assert invalid == 0

# =========================================================
# TEST 14 - CUSTOMER NAME EXTRA SPACE CHECK (Can be parameterized and reused for other columns)
# =========================================================

def test_customer_name_spaces():

    invalid = master_df.filter(
        col("customer_name").rlike(r"\s{2,}")
    ).count()

    assert invalid == 0

# =========================================================
# TEST 15 - PROFIT NOT NULL
# =========================================================

def test_profit_not_null():

    assert master_df.filter(
        col("profit").isNull()
    ).count() == 0

# =========================================================
# Executing tests
# =========================================================

# putting all test functions in a list
all_tests = [

    test_orders_not_empty,
    test_customers_not_empty,
    test_products_not_empty,
    test_master_not_empty,
    test_required_columns,
    test_order_id_null,
    test_customer_id_null,
    test_product_id_null,
    test_no_duplicates,
    test_profit_rounding,
    test_negative_quantity,
    test_order_date_not_null,
    test_product_join,
    test_customer_join,
    test_source_target_count,
    test_order_year,
    test_profit_datatype,
    test_country_case,
    test_customer_name_spaces,
    test_profit_not_null
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

# =========================================================
# collating all results
# =========================================================

results_df = spark.createDataFrame(test_results)

display(results_df)


# COMMAND ----------

