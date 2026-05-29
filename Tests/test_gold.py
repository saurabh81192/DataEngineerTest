# Databricks notebook source
# =========================================================
# Author: Saurabh Chakraborty
# PySpark Pytest Framework for gold/aggregate layer
# =========================================================

import pytest
from pyspark.sql.functions import col, year, round, broadcast, to_date, trim, when, initcap, regexp_replace, sum, current_date
from pyspark.sql.types import *

CATALOG = "workspace"
SCHEMA = "default"

master_df = spark.table(f"{CATALOG}.{SCHEMA}.enrich_master_df")
profit_aggregate_df = spark.table(f"{CATALOG}.{SCHEMA}.profit_aggregate_master")
profit_by_year = spark.table(f"{CATALOG}.{SCHEMA}.profit_by_year_curated")
profit_by_year_category = spark.table(f"{CATALOG}.{SCHEMA}.profit_by_year_category_curated")
profit_by_customer = spark.table(f"{CATALOG}.{SCHEMA}.profit_by_customer_curated")
profit_by_customer_year = spark.table(f"{CATALOG}.{SCHEMA}.profit_by_customer_year_curated")




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


def test_profit_reconciliation():

    master_profit = master_df.agg(
        sum("profit")
    ).collect()[0][0]

    aggregate_profit = profit_aggregate_df.agg(
        sum("total_profit")
    ).collect()[0][0]

    assert abs(master_profit - aggregate_profit) < 0.01, "Profit reconciliation failed"

def test_no_future_dates():

    invalid = master_df.filter(
        col("order_date") > current_date()
    ).count()

    assert invalid == 0

def test_extreme_negative_profit():

    invalid = master_df.filter(
        col("profit") < -10000
    ).count()

    assert invalid == 0, "Extreme negative profit values found"

def test_profit_by_year_reconciliation():

    source_profit = master_df.agg(
        round(sum("profit"), 2).alias("total_profit")
    ).collect()[0]["total_profit"]

    aggregate_profit = profit_by_year.agg(
        round(sum("yearly_total_profit"), 2).alias("total_profit")
    ).collect()[0]["total_profit"]

    assert source_profit == aggregate_profit, "Profit mismatch between source and yearly aggregate"


def test_order_year_unique():

    duplicate_count = profit_by_year.groupBy(
        "order_year"
    ).count().filter(
        col("count") > 1
    ).count()

    assert duplicate_count == 0, "Duplicate order_year values found"


def test_profit_by_year_category_reconciliation():

    yearly_total = profit_by_year.agg(
        round(sum("yearly_total_profit"), 2)
    ).collect()[0][0]

    category_total = profit_by_year_category.agg(
        round(sum("yearly_total_profit"), 2)
    ).collect()[0][0]

    assert yearly_total == category_total, "Mismatch between yearly and yearly-category aggregates"

def test_order_year_category_unique():

    duplicate_count = profit_by_year_category.groupBy(
        "order_year",
        "category"
    ).count().filter(
        col("count") > 1
    ).count()

    assert duplicate_count == 0, "Duplicate order_year + category combinations found"

def test_customer_uniqueness():

    duplicate_count = profit_by_customer.groupBy(
        "customer_name"
    ).count().filter(
        col("count") > 1
    ).count()

    assert duplicate_count == 0, "Duplicate customer_name values found"

def test_profit_by_customer_reconciliation():

    source_profit = master_df.agg(
        round(sum("profit"), 2)
    ).collect()[0][0]

    customer_profit = profit_by_customer.agg(
        round(sum("yearly_total_profit"), 2)
    ).collect()[0][0]

    assert source_profit == customer_profit, "Mismatch between source and customer aggregate profits"
def test_customer_year_uniqueness():

    duplicate_count = profit_by_customer_year.groupBy(
        "customer_name",
        "order_year"
    ).count().filter(
        col("count") > 1
    ).count()

    assert duplicate_count == 0, "Duplicate customer_name + order_year combinations found"


def test_customer_year_reconciliation():

    source_profit = master_df.agg(
        round(sum("profit"), 2)
    ).collect()[0][0]

    aggregate_profit = profit_by_customer_year.agg(
        round(sum("yearly_total_profit"), 2)
    ).collect()[0][0]

    assert source_profit == aggregate_profit, "Mismatch between source and customer-year aggregate profits"

null_check_cases = [
    (profit_by_year, "yearly_total_profit"),
    (profit_by_year_category, "yearly_total_profit"),
    (profit_by_customer, "yearly_total_profit"),
    (profit_by_customer_year, "yearly_total_profit")
]

@pytest.mark.parametrize(
    "df,column_name",
    null_check_cases
)
def test_gold_null_checks(df, column_name):

    invalid = df.filter(
        col(column_name).isNull()
    ).count()

    assert invalid == 0

# COMMAND ----------

# =========================================================
# Executing tests
# =========================================================

# putting all test functions in a list
all_tests = [
    test_profit_reconciliation
    ,test_no_future_dates
    ,test_extreme_negative_profit
    ,test_profit_by_year_reconciliation
    ,test_order_year_unique
    ,test_profit_by_year_category_reconciliation
    ,test_order_year_category_unique
    ,test_customer_uniqueness
    ,test_profit_by_customer_reconciliation
    ,test_customer_year_uniqueness
    ,test_customer_year_reconciliation
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

for df, column_name in null_check_cases:
    try:
        test_gold_null_checks(df, column_name)
        log_result(
            f"test_gold_null_checks[{column_name}]",
            "PASS"
        )
    except Exception as ex:
        log_result(
            f"test_gold_null_checks[{column_name}]",
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

