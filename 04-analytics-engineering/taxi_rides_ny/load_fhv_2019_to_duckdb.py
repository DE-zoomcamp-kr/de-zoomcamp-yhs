import duckdb
import requests
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
TAXI_TYPE = "fhv"
YEAR = 2019

DATA_DIR = Path("data") / TAXI_TYPE
DATA_DIR.mkdir(parents=True, exist_ok=True)

DUCKDB_PATH = Path("taxi_rides_ny.duckdb")
DUCKDB_SCHEMA = "prod"
DUCKDB_TABLE = f"{DUCKDB_SCHEMA}.fhv_tripdata"

def download_csv_gz(taxi_type: str, year: int, month: int) -> Path:
    csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
    csv_gz_path = DATA_DIR / csv_gz_filename
    if csv_gz_path.exists():
        print(f"Skipping {csv_gz_filename} (already exists)")
        return csv_gz_path

    url = f"{BASE_URL}/{taxi_type}/{csv_gz_filename}"
    print(f"Downloading {url}")
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()

    with open(csv_gz_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    return csv_gz_path

def convert_to_parquet(csv_gz_path: Path) -> Path:
    parquet_filename = csv_gz_path.name.replace(".csv.gz", ".parquet")
    parquet_path = DATA_DIR / parquet_filename

    if parquet_path.exists():
        print(f"Skipping {parquet_filename} (already exists)")
        # 공간 아끼려면 csv.gz 삭제
        try:
            csv_gz_path.unlink()
        except FileNotFoundError:
            pass
        return parquet_path

    print(f"Converting {csv_gz_path.name} -> {parquet_filename}")
    con = duckdb.connect()  # in-memory
    con.execute(f"""
        COPY (
            SELECT * FROM read_csv_auto('{csv_gz_path}', sample_size=100000)
        )
        TO '{parquet_path}' (FORMAT PARQUET);
    """)
    con.close()

    # 변환 후 원본 삭제(디스크 절약)
    csv_gz_path.unlink()
    return parquet_path

def load_parquets_to_duckdb():
    print(f"Loading Parquet files into DuckDB: {DUCKDB_PATH} -> {DUCKDB_TABLE}")
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DUCKDB_SCHEMA};")

    # union_by_name=true로 월별 컬럼 미세 차이에도 안전하게 합칩니다.
    con.execute(f"""
        CREATE OR REPLACE TABLE {DUCKDB_TABLE} AS
        SELECT *
        FROM read_parquet('{DATA_DIR}/*.parquet', union_by_name=true);
    """)
    cnt = con.execute(f"SELECT COUNT(*) FROM {DUCKDB_TABLE};").fetchone()[0]
    con.close()
    print(f"Loaded rows: {cnt:,}")

def main():
    # 2019년 1~12월
    for month in range(1, 13):
        csv_gz_path = download_csv_gz(TAXI_TYPE, YEAR, month)
        convert_to_parquet(csv_gz_path)

    load_parquets_to_duckdb()

if __name__ == "__main__":
    main()