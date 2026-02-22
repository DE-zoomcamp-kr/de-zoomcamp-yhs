import os
from pathlib import Path
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BUCKET = "kestra-zoomcamp-will-demo-485315"
LOCAL_PATH = Path("data")

LOCAL_PATH.mkdir(parents=True, exist_ok=True)

def download_file(service: str, year: int, month: str) -> str:
    file_name = f"{service}_tripdata_{year}-{month}.parquet"
    url = f"{BASE_URL}/{file_name}"
    print(f"Downloading {file_name} …")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    (LOCAL_PATH / file_name).write_bytes(r.content)
    return str(LOCAL_PATH / file_name)

def upload_to_gcs(local_file: str, service: str, year: int):
    # 예: gs://.../yellow/2019/...
    dst = f"gs://{BUCKET}/{service}/{year}/"
    print(f"Uploading {local_file} to {dst} …")
    os.system(f"gsutil cp {local_file} {dst}")

def main():
    service = "yellow"
    years = [2019, 2020]
    months = [f"{m:02d}" for m in range(1, 13)]

    for year in years:
        for month in months:
            local_file = download_file(service, year, month)
            upload_to_gcs(local_file, service, year)

if __name__ == "__main__":
    main()