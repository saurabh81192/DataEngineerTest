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

# =========================================================
# TEST 1 - DATA NOT EMPTY
# =========================================================

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

# =========================================================
# TEST 3 - COLUMNS NULL CHECK
# =========================================================
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

def test_null_validation(column_name):

    null_count = master_df.filter(
        col(column_name).isNull()
    ).count()

    assert null_count == 0, f"{column_name} contains null values"

# =========================================================
# TEST 4 - RECONCILATION CHECK (anti join validation as checked in case of there are some product exists in orders but missing in product Table )
# =========================================================

def test_all_customers_present():

    missing_customers = customers_df.select(
        "`Customer ID`"
    ).distinct().subtract(
        master_df.select("customer_id").distinct()
    ).count()

    assert missing_customers == 0, \
        f"{missing_customers} customer_ids missing in master_df"

def test_all_products_present():

    missing_products = products_df.select(
        "`Product ID`"
    ).distinct().subtract(
        master_df.select("product_id").distinct()
    ).count()

    assert missing_products == 0, \
        f"{missing_products} product_ids missing in master_df"

def test_all_orders_present():

    missing_orders = orders_df.select(
        "`Order ID`"
    ).distinct().subtract(
        master_df.select("order_id").distinct()
    ).count()

    assert missing_orders == 0, \
        f"{missing_orders} order_ids missing in master_df"

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

    assert missing_count == 0, \
        f"{missing_count} product_ids from orders missing in products table"


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

def test_order_date_format():
    invalid_count = master_df.filter(
        col("order_date").isNull() |
        (
            date_format(
                col("order_date"),
                "yyyy-MM-dd"
            ).isNull()
        )
    ).count()
    assert invalid_count == 0, "order_date contains nulls or invalid yyyy-MM-dd format"


# =========================================================
# TEST 8 - CUSTOMER JOIN VALIDATION
# =========================================================

def test_customer_join():

    unmatched = master_df.filter(
        col("customer_name").isNull()
    ).count()

    assert unmatched == 0

# =========================================================
# TEST 9 - SOURCE TARGET COUNT VALIDATION
# =========================================================

def test_source_target_count():

    assert master_df.select("order_id").distinct().count() == orders_df.select("`Order ID`").distinct().count()
    
# =========================================================
# TEST 10 - YEAR DERIVATION VALIDATION
# =========================================================

def test_order_year():

    invalid = master_df.filter(
        year(col("order_date")) != col("order_year")
    ).count()

    assert invalid == 0

# =========================================================
# TEST 11 - PROFIT DATATYPE VALIDATION (Can be parameterized and reused for other columns)
# =========================================================

def test_profit_datatype():

    dtype = dict(master_df.dtypes)["profit"]

    assert dtype in ["double", "float", "decimal"]

# =========================================================
# TEST 12 - COUNTRY STANDARDIZATION (Can be parameterized and reused for other columns)
# =========================================================

def test_country_case():

    invalid = master_df.filter(
        col("country") != initcap(col("country"))
    ).count()

    assert invalid == 0

# =========================================================
# TEST 13 - CUSTOMER NAME EXTRA SPACE CHECK (Can be parameterized and reused for other columns)
# =========================================================

def test_customer_name_spaces():

    invalid = master_df.filter(
        col("customer_name").rlike(r"\s{2,}")
    ).count()

    assert invalid == 0



# =========================================================
# Executing tests
# =========================================================

# putting all test functions in a list
all_tests = [
    test_profit_rounding,
    test_all_customers_present,
    test_all_products_present,
    test_all_orders_present,
    test_missing_products_in_orders,
    test_negative_quantity,
    test_order_date_format,
    test_customer_join,
    test_source_target_count,
    test_order_year,
    test_profit_datatype,
    test_country_case,
    test_customer_name_spaces,
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

for column_name in null_check_columns:
    try:
        test_null_validation(column_name)
        log_result(
            f"test_null_validation[{column_name}]",
            "PASS"
        )
    except Exception as ex:
        log_result(
            f"test_null_validation[{column_name}]",
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

# =========================================================
# collating all results
# =========================================================

results_df = spark.createDataFrame(test_results)

display(results_df)


# COMMAND ----------

