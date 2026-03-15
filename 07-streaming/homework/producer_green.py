# producer_green.py

import pandas as pd
import json
from kafka import KafkaProducer
from time import time

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

columns = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
    "total_amount"
]

print("Loading dataset...")

df = pd.read_parquet(url, columns=columns)

# datetime → string
df["lpep_pickup_datetime"] = df["lpep_pickup_datetime"].astype(str)
df["lpep_dropoff_datetime"] = df["lpep_dropoff_datetime"].astype(str)

# NaN
df["passenger_count"] = df["passenger_count"].fillna(0)
df["trip_distance"] = df["trip_distance"].fillna(0)
df["tip_amount"] = df["tip_amount"].fillna(0)
df["total_amount"] = df["total_amount"].fillna(0)

print("Dataset size:", len(df))

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "green-trips"

print("Sending data to Kafka...")

t0 = time()

for _, row in df.iterrows():
    producer.send(topic, value=row.to_dict())

producer.flush()

t1 = time()

print(f"took {(t1 - t0):.2f} seconds")