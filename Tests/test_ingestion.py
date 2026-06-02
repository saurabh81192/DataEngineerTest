# Databricks notebook source
# =========================================================
# Author: Saurabh Chakraborty
# PySpark Pytest Framework for ingestion layer
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


# =============================================================================
# TEST 1 - TABLES EXIST (Source Exists) 
# Incase its volume based ingestion we will add tests like below to check Source Exists
# ==============================================================================

def test_orders_file_exists():

    files = dbutils.fs.ls(
        "/Volumes/workspace/default/rawfiles/"
    )

    file_names = [f.name for f in files]

    assert "Orders.json" in file_names

def test_products_file_exists():

    files = dbutils.fs.ls(
        "/Volumes/workspace/default/rawfiles/"
    )

    file_names = [f.name for f in files]

    assert "Products.csv" in file_names

def test_customers_file_exists():

    files = dbutils.fs.ls(
        "/Volumes/workspace/default/rawfiles/"
    )

    file_names = [f.name for f in files]

    assert "Customers.xlsx" in file_names

# =============================================================================
# TEST 1 - TABLES EXIST (Source Exists)
# ==============================================================================
@pytest.mark.parametrize(
    "table_name",
    ["orders", "customer", "products"]
)
def test_source_tables_exist(table_name):

    tables = [table.name for table in spark.catalog.listTables("workspace.default")]

    assert table_name in tables

# =============================================================================
# TEST 2 - DATA NOT EMPTY (This test will ensure that raw data has been loaded)
# ==============================================================================

dataframes = [
    ("orders_df", orders_df),
    ("customers_df", customers_df),
    ("products_df", products_df)
]

@pytest.mark.parametrize(
    "df_name,df",
    dataframes
)

def test_dataframe_not_empty(df_name, df):

    assert df.count() > 0, f"{df_name} is empty"

# =================================================================================================================
# TEST 3 - MANDATORY COLUMNS PRESENT (This test will ensure that mandatory columns required for populating gold 
# layer data is present or not)
# ==================================================================================================================
ORDERS_COLUMNS = set(orders_df.columns)
PRODUCTS_COLUMNS = set(products_df.columns)
CUSTOMERS_COLUMNS = set(customers_df.columns)

def test_orders_schema():

    expected_columns = {
        "Order ID",
        "Order Date",
        "Customer ID",
        "Product ID",
        "Quantity",
        "Discount",
        "Profit"
    }

    assert expected_columns.issubset(ORDERS_COLUMNS)

def test_customers_schema():

    expected_columns = {
        "Customer ID",
        "Customer Name",
        "email",
        "Segment",
        "Country",
        "City",
        "State"  
    }

    assert expected_columns.issubset(CUSTOMERS_COLUMNS)

def test_products_schema():

    expected_columns = {
        "Product ID",
        "Category",
        "Sub-Category",
        "Product Name",
        "State"    
    }

    assert expected_columns.issubset(PRODUCTS_COLUMNS)

# =================================================================================================================
# TEST 4 - DATATYPES CHECK
# ==================================================================================================================

def test_orders_datatypes():

    expected_schema = {
        "Order ID": "string",
        "Customer ID": "string",
        "Product ID": "string"
    }

    actual_schema = dict(orders_df.dtypes)

    for column_name, expected_type in expected_schema.items():
        assert actual_schema[column_name] == expected_type

def test_products_datatypes():

    expected_schema = {
        "Product ID": "string",
        "Category": "string",
        "Sub-Category": "string"
    }

    actual_schema = dict(products_df.dtypes)

    for column_name, expected_type in expected_schema.items():
        assert actual_schema[column_name] == expected_type

def test_customers_datatypes():

    expected_schema = {
        "Customer ID": "string",
        "Customer Name": "string",
        "email": "string",
        "Segment": "string",
        "Country": "string",
        "City": "string",  
        "State": "string"
    }

    actual_schema = dict(customers_df.dtypes)

    for column_name, expected_type in expected_schema.items():
        assert actual_schema[column_name] == expected_type

#Below all test basically deals with basic data quality checks

# =================================================================================================================
# TEST 5 - BLANK NULLS CHECK
# ==================================================================================================================

null_blank_cases = [
    (orders_df, "`Customer ID`"),
    (orders_df, "`Product ID`"),
    (orders_df, "`Order ID`"),
    (customers_df, "`Customer Name`"),
    (products_df, "`Product Name`"),
    (products_df, "`Category`"),
    (products_df, "`Sub-Category`"),
    (products_df, "`Product ID`"),

]

@pytest.mark.parametrize(
    "df,column_name",
    null_blank_cases
)
def test_null_or_blank_validation(df, column_name):

    invalid_count = df.filter(
        col(column_name).isNull() |
        (trim(col(column_name)) == "")
    ).count()

    assert invalid_count == 0, f"{column_name} contains null or blank values"

# ============================================================================
# TEST 6 - INVALID ORDER DATE (if any wrong format it will impact aggregation)
# =============================================================================

def test_invalid_order_dates():

    invalid = orders_df.filter(
        to_date(
            col("`Order Date`"),
            "d/M/yyyy"
        ).isNull()
    ).count()

    assert invalid == 0

# =========================================================
# TEST 7 - NEGATIVE QUANTITY CHECK
# =========================================================

def test_extreme_quantity():

    invalid_count = orders_df.filter(
        (col("Quantity") <= 0) |
        (col("Quantity") > 1000)
    ).count()

    assert invalid_count == 0, "Quantity contains invalid values (<=0 or >1000)"

# =========================================================
# TEST 8 - CUSTOMER NAME CHECK
# =========================================================

def test_customer_name_special_chars():

    invalid = customers_df.filter(
        col("`Customer Name`").rlike(
            r"[#@$%^&*()_+=\[\]{}|\\:;\"<>?/]"
        )
    ).count()

    assert invalid == 0, "Customer Name contains invalid special characters"

def test_customer_name_spaces():

    invalid = customers_df.filter(
        col("`Customer Name`").rlike(r"\s{2,}")
    ).count()

    assert invalid == 0

# =========================================================
# TEST 9 - DATA FORMAT CHECK
# =========================================================

def test_product_id_format():

    invalid = products_df.filter(
        col("`Product ID`").rlike(r"^P\d+$") == False
    ).count()

    assert invalid == 0


# COMMAND ----------

# =========================================================
# Executing tests in here (will use pytest ingestion.py in production)
# =========================================================

# putting all test functions in a list
all_tests = [
    test_orders_file_exists
    ,test_products_file_exists
    ,test_customers_file_exists
    ,test_orders_schema
    ,test_customers_schema
    ,test_products_schema
    ,test_orders_datatypes
    ,test_products_datatypes
    ,test_customers_datatypes
    ,test_invalid_order_dates
    ,test_extreme_quantity
    ,test_customer_name_special_chars
    ,test_customer_name_spaces
    ,test_product_id_format
 
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

for table_name in ["orders", "customer", "products"]:
    try:
        test_source_tables_exist(table_name)
        log_result(
            f"test_source_tables_exist[{table_name}]",
            "PASS"
        )
    except Exception as ex:
        log_result(
            f"test_source_tables_exist[{table_name}]",
            "FAIL",
            str(ex)
        )

for df_name, df in dataframes:
    try:
        test_dataframe_not_empty(df_name, df)

        log_result(
            f"test_dataframe_not_empty[{df_name}]",
            "PASS"
        )
    except Exception as ex:
        log_result(
            f"test_dataframe_not_empty[{df_name}]",
            "FAIL",
            str(ex)
        )

for df,column_name in null_blank_cases:
    try:
        test_null_or_blank_validation(df, column_name)
        log_result(
            f"test_null_or_blank_validation[{column_name}]",
            "PASS"
        )
    except Exception as ex:
        log_result(
            f"test_null_or_blank_validation[{column_name}]",
            "FAIL",
            str(ex)
        )

# COMMAND ----------

# =========================================================
# collating all results
# =========================================================

results_df = spark.createDataFrame(test_results)

display(results_df)
