import json
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from pipeline import run_pipeline
from analytics import generate_analytics
from spark_pipeline import spark_dependency_message


class TaxiPipelineTests(unittest.TestCase):
    def test_spark_runner_explains_optional_dependency_installation(self):
        message = spark_dependency_message()

        self.assertIn("requirements-spark.txt", message)
        self.assertIn("PySpark", message)

    def test_pipeline_writes_empty_gold_output_when_all_rows_are_invalid(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            raw_path = root / "invalid_trips.parquet"
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT * FROM (
                        VALUES (1, TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-01-01 09:45:00', 2.5, 10.0, 1.0, 0.5, 12.0)
                    ) AS trips(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount, tip_amount, tolls_amount, total_amount)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(raw_path)],
            )
            connection.close()

            report = run_pipeline(raw_path, root / "warehouse", root / "reports")

            gold_path = root / "warehouse" / "gold" / "daily_revenue.parquet"
            self.assertEqual(report["valid_rows"], 0)
            self.assertTrue(gold_path.is_file())
            self.assertEqual(duckdb.sql("SELECT COUNT(*) FROM read_parquet(?)", params=[str(gold_path)]).fetchone()[0], 0)

    def test_pipeline_rejects_pickups_outside_the_configured_month(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            raw_path = root / "mixed_month_trips.parquet"
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT * FROM (
                        VALUES
                            (1, TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-01-01 10:15:00', 2.5, 10.0, 1.0, 0.5, 12.0),
                            (1, TIMESTAMP '2023-12-31 10:00:00', TIMESTAMP '2023-12-31 10:15:00', 2.5, 10.0, 1.0, 0.5, 12.0)
                    ) AS trips(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount, tip_amount, tolls_amount, total_amount)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(raw_path)],
            )
            connection.close()

            report = run_pipeline(raw_path, root / "warehouse", root / "reports")

            self.assertEqual(report["raw_rows"], 2)
            self.assertEqual(report["valid_rows"], 1)
            self.assertEqual(report["rejected_rows"], 1)

    def test_pipeline_filters_invalid_trips_and_writes_quality_report(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            raw_path = root / "yellow_tripdata.parquet"
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT * FROM (
                        VALUES
                            (1, TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-01-01 10:15:00', 2.5, 10.0, 1.0, 0.5, 12.0),
                            (2, TIMESTAMP '2024-01-01 11:00:00', TIMESTAMP '2024-01-01 10:45:00', 1.0, 5.0, 1.0, 0.0, 6.0),
                            (1, TIMESTAMP '2024-01-02 09:00:00', TIMESTAMP '2024-01-02 09:10:00', -1.0, 4.0, 1.0, 0.0, 5.0)
                    ) AS trips(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount, tip_amount, tolls_amount, total_amount)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(raw_path)],
            )
            connection.close()

            report = run_pipeline(raw_path, root / "warehouse", root / "reports")

            self.assertEqual(report["raw_rows"], 3)
            self.assertEqual(report["valid_rows"], 1)
            self.assertEqual(report["rejected_rows"], 2)
            self.assertTrue((root / "warehouse" / "gold" / "daily_revenue.parquet").is_file())
            self.assertEqual(json.loads((root / "reports" / "quality_report.json").read_text())["valid_rows"], 1)

            analytics_report = generate_analytics(
                root / "warehouse" / "silver" / "valid_trips.parquet",
                root / "reports" / "analytics",
            )

            self.assertEqual(analytics_report["total_valid_trips"], 1)
            self.assertTrue((root / "reports" / "analytics" / "daily_metrics.parquet").is_file())
            self.assertTrue((root / "reports" / "analytics" / "peak_hours.parquet").is_file())
            self.assertTrue((root / "reports" / "analytics" / "payment_types.parquet").is_file())
            self.assertTrue((root / "reports" / "analytics" / "pickup_zones.parquet").is_file())
            self.assertEqual(
                duckdb.sql(
                    "SELECT trip_count, total_revenue, avg_trip_distance, avg_tip_amount FROM read_parquet(?)",
                    params=[str(root / "reports" / "analytics" / "daily_metrics.parquet")],
                ).fetchone(),
                (1, 12.0, 2.5, 1.0),
            )
            self.assertEqual(
                duckdb.sql(
                    "SELECT pickup_hour, trip_count FROM read_parquet(?)",
                    params=[str(root / "reports" / "analytics" / "peak_hours.parquet")],
                ).fetchone(),
                (10, 1),
            )
            self.assertEqual(
                duckdb.sql(
                    "SELECT pickup_zone, trip_count FROM read_parquet(?)",
                    params=[str(root / "reports" / "analytics" / "pickup_zones.parquet")],
                ).fetchone(),
                ("unknown", 1),
            )

    @unittest.skipUnless(
        importlib.util.find_spec("pyspark") and importlib.util.find_spec("delta"),
        "requires optional PySpark and Delta Lake dependencies",
    )
    def test_spark_pipeline_writes_delta_layers_for_validated_trips(self):
        from spark_pipeline import run_spark_pipeline

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            raw_path = root / "yellow_tripdata.parquet"
            connection = duckdb.connect()
            connection.execute(
                """
                COPY (
                    SELECT * FROM (
                        VALUES
                            (1, TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-01-01 10:15:00', 2.5, 10.0, 1.0, 0.5, 12.0),
                            (1, TIMESTAMP '2024-01-01 11:00:00', TIMESTAMP '2024-01-01 10:45:00', 1.0, 5.0, 1.0, 0.0, 6.0)
                    ) AS trips(VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount, tip_amount, tolls_amount, total_amount)
                ) TO ? (FORMAT PARQUET)
                """,
                [str(raw_path)],
            )
            connection.close()

            report = run_spark_pipeline(raw_path, root / "warehouse-spark", root / "reports")

            self.assertEqual(report["raw_rows"], 2)
            self.assertEqual(report["valid_rows"], 1)
            self.assertTrue((root / "warehouse-spark" / "gold" / "daily_revenue_delta").is_dir())


if __name__ == "__main__":
    unittest.main()
