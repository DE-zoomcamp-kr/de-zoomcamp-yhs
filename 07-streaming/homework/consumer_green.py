from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "green-trips",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    group_id="green-trips-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

count = 0
total = 0

print("Reading messages from Kafka...")

for message in consumer:
    trip = message.value
    total += 1

    if trip["trip_distance"] > 5:
        count += 1

    # dataset 크기만큼 읽으면 종료
    if total >= 49416:
        break

consumer.close()

print("Total trips:", total)
print("Trips with distance > 5:", count)