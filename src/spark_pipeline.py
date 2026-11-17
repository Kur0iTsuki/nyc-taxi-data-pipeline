"""Optional Spark and Delta Lake implementation of the NYC taxi pipeline."""

import json
from datetime import date
from pathlib import Path


def spark_dependency_message() -> str:
    """Return the install guidance used when optional Spark packages are absent."""
    return "PySpark and Delta Lake are optional. Install them with `python3 -m pip install -r requirements-spark.txt`."


def _spark_session():
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError(spark_dependency_message()) from error

    builder = (
        SparkSession.builder.appName("nyc-taxi-medallion-pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _month_bounds(target_month: str) -> tuple[date, date]:
    try:
        year, month = (int(value) for value in target_month.split("-"))
        start = date(year, month, 1)
    except (TypeError, ValueError) as error:
        raise ValueError("target_month must use YYYY-MM format") from error
    end = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    return start, end


def run_spark_pipeline(
    raw_path: Path, warehouse_path: Path, reports_path: Path, target_month: str = "2024-01"
) -> dict:
    """Build Delta Bronze, Silver, and Gold outputs from the NYC taxi Parquet input."""
    raw_path = Path(raw_path)
    warehouse_path = Path(warehouse_path)
    reports_path = Path(reports_path)
    if not raw_path.is_file():
        raise FileNotFoundError(f"Dataset not found: {raw_path}")
    month_start, month_end = _month_bounds(target_month)

    spark = _spark_session()
    try:
        from pyspark.sql import functions as functions

        bronze_path = warehouse_path / "bronze" / "yellow_trips_delta"
        silver_path = warehouse_path / "silver" / "valid_trips_delta"
        gold_path = warehouse_path / "gold" / "daily_revenue_delta"
        reports_path.mkdir(parents=True, exist_ok=True)

        bronze = spark.read.parquet(str(raw_path))
        bronze.write.format("delta").mode("overwrite").save(str(bronze_path))

        silver = bronze.where(
            functions.col("tpep_pickup_datetime").isNotNull()
            & functions.col("tpep_dropoff_datetime").isNotNull()
            & (functions.col("tpep_dropoff_datetime") > functions.col("tpep_pickup_datetime"))
            & (functions.col("trip_distance") > 0)
            & (functions.col("total_amount") > 0)
            & (functions.to_date("tpep_pickup_datetime") >= functions.lit(month_start.isoformat()))
            & (functions.to_date("tpep_pickup_datetime") < functions.lit(month_end.isoformat()))
        )
        silver.write.format("delta").mode("overwrite").save(str(silver_path))

        gold = (
            silver.withColumn("pickup_date", functions.to_date("tpep_pickup_datetime"))
            .groupBy("pickup_date")
            .agg(
                functions.count("*").alias("trip_count"),
                functions.round(functions.sum("total_amount"), 2).alias("total_revenue"),
                functions.round(functions.avg("trip_distance"), 2).alias("avg_trip_distance"),
                functions.round(functions.avg("tip_amount"), 2).alias("avg_tip_amount"),
            )
            .orderBy("pickup_date")
        )
        gold.write.format("delta").mode("overwrite").save(str(gold_path))

        raw_rows = bronze.count()
        valid_rows = silver.count()
        report = {
            "raw_rows": raw_rows,
            "valid_rows": valid_rows,
            "rejected_rows": raw_rows - valid_rows,
            "daily_aggregate_rows": gold.count(),
            "target_month": target_month,
            "quality_status": "passed" if raw_rows == valid_rows else "needs_attention",
        }
        (reports_path / "spark_quality_report.json").write_text(json.dumps(report, indent=2) + "\n")
        return report
    finally:
        spark.stop()
