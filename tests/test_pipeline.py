import json
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from pipeline import run_pipeline


class TaxiPipelineTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
