import os
from pathlib import Path
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
YEAR = 2024
BUCKET = "kestra-zoomcamp-will-demo-485315"
LOCAL_PATH = Path("data")

LOCAL_PATH.mkdir(parents=True, exist_ok=True)

def download_file(year: int, month: str):
    file_name = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"{BASE_URL}/{file_name}"
    print(f"Downloading {file_name} …")
    response = requests.get(url)
    (LOCAL_PATH / file_name).write_bytes(response.content)
    return str(LOCAL_PATH / file_name)

def upload_to_gcs(local_file: str):
    print(f"Uploading {local_file} to GCS …")
    os.system(f"gsutil cp {local_file} gs://{BUCKET}/yellow/2024/")

def main():
    months = [f"{m:02d}" for m in range(1, 13)]
    for month in months:
        local_file = download_file(YEAR, month)
        upload_to_gcs(local_file)

if __name__ == "__main__":
    main()