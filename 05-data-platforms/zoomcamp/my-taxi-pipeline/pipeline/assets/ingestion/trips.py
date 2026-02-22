"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
materialization:
  type: table
  strategy: append
columns:
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: fare_amount
    type: float
  - name: payment_type
    type: integer
  - name: taxi_type
    type: string
@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def generate_months(start, end):
    current = start.replace(day=1)
    months = []
    while current < end:
        months.append(current)
        current += relativedelta(months=1)
    return months

def materialize():
    start_date = datetime.fromisoformat(os.environ["BRUIN_START_DATE"].replace("Z",""))
    end_date = datetime.fromisoformat(os.environ["BRUIN_END_DATE"].replace("Z",""))
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    months = generate_months(start_date, end_date)
    all_dfs = []

    for taxi_type in taxi_types:
        for month in months:
            year = month.year
            month_str = f"{month.month:02d}"
            file_url = f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month_str}.parquet"
            print(f"Downloading {file_url}")

            try:
                df = pd.read_parquet(file_url)

                # Rename pickup/dropoff columns
                if taxi_type == "yellow":
                    df = df.rename(columns={
                        "tpep_pickup_datetime": "pickup_datetime",
                        "tpep_dropoff_datetime": "dropoff_datetime"
                    })
                elif taxi_type == "green":
                    df = df.rename(columns={
                        "lpep_pickup_datetime": "pickup_datetime",
                        "lpep_dropoff_datetime": "dropoff_datetime"
                    })

                df["taxi_type"] = taxi_type

                # Keep required columns including payment_type
                df = df[[
                    "pickup_datetime",
                    "dropoff_datetime",
                    "PULocationID",
                    "DOLocationID",
                    "fare_amount",
                    "payment_type",
                    "taxi_type"
                ]].rename(columns={
                    "PULocationID": "pickup_location_id",
                    "DOLocationID": "dropoff_location_id"
                })

                all_dfs.append(df)

            except Exception as e:
                print(f"Failed to load {file_url}: {e}")

    return pd.concat(all_dfs, ignore_index=True)