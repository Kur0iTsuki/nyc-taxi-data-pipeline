"""Run the optional local Spark and Delta Lake pipeline."""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent / "src"))

from spark_pipeline import run_spark_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/raw/yellow_tripdata_2024-01.parquet"))
    parser.add_argument("--target-month", default="2024-01", help="Expected pickup month in YYYY-MM format.")
    arguments = parser.parse_args()
    if not arguments.input.is_file():
        parser.error("Dataset not found. Run `python3 scripts/download_data.py` first or pass --input.")
    try:
        report = run_spark_pipeline(
            arguments.input, Path("warehouse-spark"), Path("reports"), arguments.target_month
        )
    except RuntimeError as error:
        parser.error(str(error))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
