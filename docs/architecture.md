# Architecture Notes

## Default local path

The standard runner uses DuckDB and Parquet so a reviewer can reproduce the full project with a normal Python environment.

1. **Ingest:** Download the public January 2024 NYC TLC Parquet file.
2. **Bronze:** Copy the source data unchanged into the local warehouse.
3. **Silver:** Keep only trips with present, ordered timestamps, a positive distance, a positive total amount, and a pickup date in the configured reporting month.
4. **Gold:** Aggregate validated trips into daily revenue, trip volume, average distance, and average tip metrics.
5. **Reporting:** Emit a quality report and separate dashboard-ready analytics summaries.

## Optional Spark and Delta path

`run_spark_pipeline.py` applies the same validation rules using local PySpark and writes Delta tables for Bronze, Silver, and Gold. This provides a portable Spark and Delta implementation for learning and demonstration. It is deliberately optional: the repository does not require a Databricks account and does not represent a cloud deployment.

## Quality controls

The pipeline rejects records with missing pickup or dropoff timestamps, a dropoff at or before pickup, nonpositive trip distance, nonpositive total amount, or a pickup outside the configured reporting month. Rejected-row counts remain visible in `quality_report.json` so data loss is observable.

## Metric definition

The column named `total_revenue` is the sum of the source file’s `total_amount` field. It includes recorded tips and fees and may include entries from payment types that are not settled collections. It is a gross recorded trip-total metric, not a claim of net revenue.
