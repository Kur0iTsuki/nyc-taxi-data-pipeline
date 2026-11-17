"""Command-line entry point for the NYC Taxi Data Pipeline."""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent / "src"))

from pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/raw/yellow_tripdata_2024-01.parquet"))
    arguments = parser.parse_args()
    if not arguments.input.is_file():
        parser.error("Dataset not found. Run `python3 scripts/download_data.py` first or pass --input.")
    print(json.dumps(run_pipeline(arguments.input, Path("warehouse"), Path("reports")), indent=2))


if __name__ == "__main__":
    main()
