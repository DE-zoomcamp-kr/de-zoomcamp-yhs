# Module 6 – Batch Processing with Spark (Homework)

This homework covers installing Spark, processing NYC Yellow Taxi data with PySpark, repartitioning Parquet files, performing aggregations, and analyzing data using Spark UI.

---

## Environment

* Spark Version: **4.1.1**
* Python: 3.12
* Java: 17
* Execution Mode: `local[*]`

---

## Question 1 – Spark Version

After creating a Spark session:

```python
spark.version
```

**Output:**

```
4.1.1
```

---

## Question 2 – Repartition & Parquet File Size

Steps:

* Read `yellow_tripdata_2025-11.parquet`
* Repartition into 4 partitions
* Save to Parquet
* Calculate average file size

**Average Parquet file size:**

```
~75 MB
```

Correct answer: **75MB**

---

## Question 3 – Trips on November 15, 2025

Filtered trips where:

```
to_date(tpep_pickup_datetime) = '2025-11-15'
```

**Result:**

```
162,604 trips
```

Correct answer: **162,604**

---

## Question 4 – Longest Trip Duration

Calculated trip duration in hours:

```
(unix_timestamp(dropoff) - unix_timestamp(pickup)) / 3600
```

**Longest trip duration:**

```
90.6 hours
```

Correct answer: **90.6**

---

## Question 5 – Spark UI Port

Retrieved dynamically from SparkContext:

```python
spark.sparkContext.uiWebUrl
```

**Spark UI runs on:**

```
4040
```

Correct answer: **4040**

---

## Question 6 – Least Frequent Pickup Zone

Joined Yellow Taxi data with `taxi_zone_lookup.csv`
Grouped by pickup Zone and ordered ascending by count.

**Least frequent pickup zone:**

```
Governor's Island/Ellis Island/Liberty Island
```

Correct answer:

**Governor's Island/Ellis Island/Liberty Island**


