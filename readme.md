## Architecture

This solution follows Medallion Architecture:

- Bronze Layer:
  Raw ingestion of source datasets from Unity Catalog / Volumes.

- Silver Layer:
  Data cleansing, standardization, enrichment and referential validations.

- Gold Layer:
  Aggregated business-ready datasets for analytics and reporting.

## Data Modeling

- enrich_customers_df → Customer Dimension
- enrich_products_df → Product Dimension
- enrich_master_df → Sales Fact Table
- profit_aggregate_df → Reporting Aggregate Table
