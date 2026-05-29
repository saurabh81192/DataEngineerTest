## Architecture

This solution follows Medallion Architecture:

- Bronze Layer:
  Raw ingestion of source datasets from Unity Catalog / Volumes.

- Silver Layer:
  Data cleansing, standardization, enrichment and referential validations.

- Gold Layer:
  Aggregated business-ready datasets for analytics and reporting.

## Data Modeling

- enrich_orders_df → Orders fact
- enrich_customers_df → Customer Dimension
- enrich_products_df → Product Dimension
- enrich_master_df → Sales Fact Table
- profit_aggregate_df → Reporting Aggregate Table

- order grain is - order line item level data
- product id can be duplicate as grain is country level (product_id, state)


## Test Cases

- ingestion layer -> 24 test cases
- enrich layer layer -> 14 test cases
- gold layer -> 20 test cases
