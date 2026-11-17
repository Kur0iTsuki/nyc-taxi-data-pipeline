"""Command-line entry point for the NYC Taxi Data Pipeline."""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent / "src"))

from pipeline import run_pipeline
from analytics import generate_analytics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/raw/yellow_tripdata_2024-01.parquet"))
    parser.add_argument("--target-month", default="2024-01", help="Expected pickup month in YYYY-MM format.")
    arguments = parser.parse_args()
    if not arguments.input.is_file():
        parser.error("Dataset not found. Run `python3 scripts/download_data.py` first or pass --input.")
    report = run_pipeline(arguments.input, Path("warehouse"), Path("reports"), arguments.target_month)
    analytics_report = generate_analytics(
        Path("warehouse") / "silver" / "valid_trips.parquet",
        Path("reports") / "analytics",
    )
    print(json.dumps({"quality": report, "analytics": analytics_report}, indent=2))


if __name__ == "__main__":
    main()
