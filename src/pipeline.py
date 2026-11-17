"""Bronze, Silver, and Gold transformations for NYC yellow taxi trip data."""

import json
from datetime import date
from pathlib import Path

import duckdb


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _month_bounds(target_month: str) -> tuple[date, date]:
    try:
        year, month = (int(value) for value in target_month.split("-"))
        start = date(year, month, 1)
    except (TypeError, ValueError) as error:
        raise ValueError("target_month must use YYYY-MM format") from error
    end = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    return start, end


def run_pipeline(raw_path: Path, warehouse_path: Path, reports_path: Path, target_month: str = "2024-01") -> dict:
    """Build layered Parquet outputs and a data-quality report from a raw trip file."""
    raw_path = Path(raw_path)
    warehouse_path = Path(warehouse_path)
    reports_path = Path(reports_path)
    month_start, month_end = _month_bounds(target_month)
    for layer in ("bronze", "silver", "gold"):
        (warehouse_path / layer).mkdir(parents=True, exist_ok=True)
    reports_path.mkdir(parents=True, exist_ok=True)

    bronze_path = warehouse_path / "bronze" / "yellow_trips.parquet"
    silver_path = warehouse_path / "silver" / "valid_trips.parquet"
    gold_path = warehouse_path / "gold" / "daily_revenue.parquet"
    connection = duckdb.connect()
    raw = _sql_path(raw_path)
    bronze = _sql_path(bronze_path)
    silver = _sql_path(silver_path)
    gold = _sql_path(gold_path)

    connection.execute(f"COPY (SELECT * FROM read_parquet('{raw}')) TO '{bronze}' (FORMAT PARQUET)")
    connection.execute(
        f"""
        COPY (
            SELECT *
            FROM read_parquet('{bronze}')
            WHERE tpep_pickup_datetime IS NOT NULL
              AND tpep_dropoff_datetime IS NOT NULL
              AND tpep_dropoff_datetime > tpep_pickup_datetime
              AND trip_distance > 0
              AND total_amount > 0
              AND CAST(tpep_pickup_datetime AS DATE) >= DATE '{month_start.isoformat()}'
              AND CAST(tpep_pickup_datetime AS DATE) < DATE '{month_end.isoformat()}'
        ) TO '{silver}' (FORMAT PARQUET)
        """
    )
    connection.execute(
        f"""
        COPY (
            SELECT
                CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
                COUNT(*) AS trip_count,
                ROUND(SUM(total_amount), 2) AS total_revenue,
                ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
                ROUND(AVG(tip_amount), 2) AS avg_tip_amount
            FROM read_parquet('{silver}')
            GROUP BY 1
            ORDER BY 1
        ) TO '{gold}' (FORMAT PARQUET)
        """
    )
    raw_rows = connection.execute(f"SELECT COUNT(*) FROM read_parquet('{bronze}')").fetchone()[0]
    valid_rows = connection.execute(f"SELECT COUNT(*) FROM read_parquet('{silver}')").fetchone()[0]
    gold_rows = connection.execute(f"SELECT COUNT(*) FROM read_parquet('{gold}')").fetchone()[0]
    connection.close()

    report = {
        "raw_rows": raw_rows,
        "valid_rows": valid_rows,
        "rejected_rows": raw_rows - valid_rows,
        "daily_aggregate_rows": gold_rows,
        "target_month": target_month,
        "quality_status": "passed" if raw_rows == valid_rows else "needs_attention",
    }
    (reports_path / "quality_report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report
