# NYC Taxi Data Pipeline

An end-to-end, reproducible data pipeline built with official NYC Taxi & Limousine Commission trip records. It downloads real Parquet data, applies Bronze–Silver–Gold transformations, records quality metrics, and creates an analytics-ready daily revenue table.

## Architecture

```text
NYC TLC Parquet -> Bronze: source copy -> Silver: validated trips -> Gold: daily revenue metrics
                                      \
                                       -> quality_report.json
```

## What it demonstrates

- **Ingestion:** downloads an official public Parquet dataset.
- **Quality gates:** rejects null timestamps, backwards trips, zero/negative distances, and nonpositive totals.
- **Layered storage:** writes durable Bronze, Silver, and Gold Parquet outputs.
- **Analytics:** produces daily trip volume, revenue, average trip distance, and average tip metrics.
- **Verification:** includes an automated test that proves invalid trips are rejected and outputs are written.

## Run

```bash
python3 -m pip install -r requirements.txt
python3 scripts/download_data.py
python3 run_pipeline.py
python3 -m unittest discover -s tests -v
```

The raw input, derived warehouse files, and reports are intentionally ignored by Git. Anyone can reproduce the full pipeline locally from the documented public source.

## Dataset

The project uses the January 2024 Yellow Taxi trip-record file published by the NYC Taxi & Limousine Commission. The agency publishes millions of public trip records and documents the dataset format on its [Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Scope

This is a portfolio project built with public data only. It is not affiliated with the NYC TLC and contains no employer or customer data.
