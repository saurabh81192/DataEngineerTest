## Architecture

This solution follows Medallion Architecture:

- Bronze Layer:
  Raw ingestion of source datasets from Unity Catalog / Volumes.

- Silver Layer:
  Data cleansing, standardization, enrichment and referential validations.

- Gold Layer:
  Aggregated business-ready datasets for analytics and reporting.
