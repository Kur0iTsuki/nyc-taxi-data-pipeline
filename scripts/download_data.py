"""Download one month of official NYC TLC yellow taxi data."""

from pathlib import Path
from urllib.request import urlretrieve


URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
DESTINATION = Path("data/raw/yellow_tripdata_2024-01.parquet")


def main() -> None:
    DESTINATION.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {URL}")
    urlretrieve(URL, DESTINATION)
    print(f"Dataset ready: {DESTINATION}")


if __name__ == "__main__":
    main()
