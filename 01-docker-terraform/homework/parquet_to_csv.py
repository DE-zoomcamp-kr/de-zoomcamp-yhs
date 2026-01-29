from pathlib import Path
import pandas as pd


BASE_DIR = Path("/app")
DATA_DIR = BASE_DIR / "data"

PARQUET_FILE = DATA_DIR / "green_tripdata_2025-11.parquet"
CSV_FILE = DATA_DIR / "green_tripdata_2025-11.csv"


def main() -> None:
    if not PARQUET_FILE.exists():
        raise FileNotFoundError(f"{PARQUET_FILE} not found")

    df = pd.read_parquet(PARQUET_FILE)
    df.to_csv(CSV_FILE, index=False)

    print(f"Converted parquet → csv: {CSV_FILE}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
