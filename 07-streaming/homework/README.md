# Data Engineering Zoomcamp - Module 7 Homework (Streaming)

This homework practices **stream processing using Kafka (Redpanda) and PyFlink**.

The pipeline built in this homework is:

Producer → Kafka (Redpanda) → PyFlink → PostgreSQL

Dataset used:

Green Taxi Trip Data – October 2025  
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet

---

# Environment Setup

The infrastructure from the workshop was reused.

Start the services:

```bash
cd 07-streaming/workshop
docker compose build
docker compose up -d
````

This starts the following services:

* Redpanda (Kafka-compatible broker)
* Flink JobManager
* Flink TaskManager
* PostgreSQL

Services:

| Service          | Address                                        |
| ---------------- | ---------------------------------------------- |
| Kafka / Redpanda | localhost:9092                                 |
| Flink UI         | [http://localhost:8081](http://localhost:8081) |
| PostgreSQL       | localhost:5432                                 |

---

# Question 1. Redpanda Version

Command used:

```bash
docker exec -it workshop-redpanda-1 rpk version
```

Result:

```
rpk version: v25.3.9
Git ref:     ********
Build date:  2026 Feb 26 07 48 21 Thu
OS/Arch:     linux/amd64
Go version:  go1.24.3
```

Answer: **v25.3.9**

---

# Question 2. Sending Data to Redpanda

Topic created:

```bash
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Producer execution:

```bash
uv run python producer_green.py
```

Result:

```
Loading dataset...
Dataset size: 49416
Sending data to Kafka...
took 10.91 seconds
```

Answer: **10 seconds**

---

# Question 3. Kafka Consumer – Trip Distance

Kafka consumer execution:

```bash
uv run python consumer_green.py
```

The consumer reads all records and counts trips where:

```
trip_distance > 5
```

Result:

```
Reading messages from Kafka...
Total trips: 49416
Trips with distance > 5: 8506
```

Answer: **8506**

---

# Question 4. Tumbling Window – Pickup Location
- 07-streaming/workshop/src/job/q4.py

```
CREATE TABLE green_trips_window (
    window_start TIMESTAMP,
    PULocationID INTEGER,
    num_trips BIGINT,
    PRIMARY KEY (window_start, PULocationID)
);
```

```
docker exec -it workshop-jobmanager-1 flink run -py /opt/src/job/q4.py
```

Query:

```sql
SELECT PULocationID, num_trips
FROM green_trips_window
ORDER BY num_trips DESC
LIMIT 3;

Result:
```
```
 pulocationid | num_trips 
--------------+-----------
           74 |        15
           74 |        14
           74 |        13
(3 rows)
```

Answer: **74**

---

# Question 5. Session Window – Longest Streak
- 07-streaming/workshop/src/job/q5.py

```
CREATE TABLE green_trips_session (
    PULocationID INTEGER,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    num_trips BIGINT,
    PRIMARY KEY (PULocationID, window_start, window_end)
);
```

```
docker exec -it workshop-jobmanager-1 flink run \
-py /opt/src/job/q5.py
```

Query:

```sql
SELECT MAX(num_trips)
FROM green_trips_session;
```

```
 max 
-----
  81
(1 row)
```

Answer: **81**

---

# Question 6. Tumbling Window – Largest Tip
- 07-streaming/workshop/src/job/q6.py

```sql
CREATE TABLE green_trips_tip (
    window_start TIMESTAMP,
    total_tip DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);
```

Result query:

```sql
SELECT *
FROM green_trips_tip
ORDER BY total_tip DESC
LIMIT 1;
```

```
    window_start     |     total_tip     
---------------------+-------------------
 2025-10-16 18:00:00 | 524.9599999999998
(1 row)
```

Answer:

```
2025-10-16 18:00:00
```
