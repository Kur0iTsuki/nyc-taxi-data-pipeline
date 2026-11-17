"""Analytics outputs derived from validated NYC taxi trips."""

import json
from pathlib import Path

import duckdb


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("'", "''")


def _has_column(connection: duckdb.DuckDBPyConnection, parquet_path: str, column_name: str) -> bool:
    columns = connection.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')").fetchall()
    return column_name in {column[0] for column in columns}


def generate_analytics(silver_path: Path, output_path: Path) -> dict:
    """Write reusable analytics summaries from a validated trip Parquet file."""
    silver_path = Path(silver_path)
    output_path = Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    silver = _sql_path(silver_path)
    daily_path = _sql_path(output_path / "daily_metrics.parquet")
    peak_hour_path = _sql_path(output_path / "peak_hours.parquet")
    payment_type_path = _sql_path(output_path / "payment_types.parquet")
    pickup_zone_path = _sql_path(output_path / "pickup_zones.parquet")
    connection = duckdb.connect()

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
        ) TO '{daily_path}' (FORMAT PARQUET)
        """
    )
    pickup_zone_expression = "CAST(PULocationID AS VARCHAR)" if _has_column(connection, silver, "PULocationID") else "'unknown'"
    connection.execute(
        f"""
        COPY (
            SELECT
                {pickup_zone_expression} AS pickup_zone,
                COUNT(*) AS trip_count,
                ROUND(SUM(total_amount), 2) AS total_revenue
            FROM read_parquet('{silver}')
            GROUP BY 1
            ORDER BY trip_count DESC, pickup_zone
        ) TO '{pickup_zone_path}' (FORMAT PARQUET)
        """
    )
    connection.execute(
        f"""
        COPY (
            SELECT
                EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
                COUNT(*) AS trip_count,
                ROUND(SUM(total_amount), 2) AS total_revenue
            FROM read_parquet('{silver}')
            GROUP BY 1
            ORDER BY trip_count DESC, pickup_hour
        ) TO '{peak_hour_path}' (FORMAT PARQUET)
        """
    )
    payment_expression = "CAST(payment_type AS VARCHAR)" if _has_column(connection, silver, "payment_type") else "'unknown'"
    connection.execute(
        f"""
        COPY (
            SELECT
                {payment_expression} AS payment_type,
                COUNT(*) AS trip_count,
                ROUND(SUM(total_amount), 2) AS total_revenue
            FROM read_parquet('{silver}')
            GROUP BY 1
            ORDER BY trip_count DESC, payment_type
        ) TO '{payment_type_path}' (FORMAT PARQUET)
        """
    )

    total_valid_trips = connection.execute(f"SELECT COUNT(*) FROM read_parquet('{silver}')").fetchone()[0]
    top_hour = connection.execute(
        f"SELECT pickup_hour, trip_count FROM read_parquet('{peak_hour_path}') ORDER BY trip_count DESC, pickup_hour LIMIT 1"
    ).fetchone()
    top_pickup_zone = connection.execute(
        f"SELECT pickup_zone, trip_count FROM read_parquet('{pickup_zone_path}') ORDER BY trip_count DESC, pickup_zone LIMIT 1"
    ).fetchone()
    connection.close()

    report = {
        "total_valid_trips": total_valid_trips,
        "top_pickup_hour": top_hour[0] if top_hour else None,
        "top_pickup_hour_trip_count": top_hour[1] if top_hour else 0,
        "top_pickup_zone": top_pickup_zone[0] if top_pickup_zone else None,
        "top_pickup_zone_trip_count": top_pickup_zone[1] if top_pickup_zone else 0,
    }
    (output_path / "analytics_report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report
