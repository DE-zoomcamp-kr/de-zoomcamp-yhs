-- BigQuery Setup
CREATE OR REPLACE EXTERNAL TABLE zoomcamp.yellow_2024_ext
  OPTIONS (
    format = 'PARQUET',
    uris =
      [
        'gs://kestra-zoomcamp-will-demo-485315/yellow/2024/yellow_tripdata_2024-01.parquet',
        'gs://kestra-zoomcamp-will-demo-485315/yellow/2024/yellow_tripdata_2024-02.parquet',
        'gs://kestra-zoomcamp-will-demo-485315/yellow/2024/yellow_tripdata_2024-03.parquet',
        'gs://kestra-zoomcamp-will-demo-485315/yellow/2024/yellow_tripdata_2024-04.parquet',
        'gs://kestra-zoomcamp-will-demo-485315/yellow/2024/yellow_tripdata_2024-05.parquet',
        'gs://kestra-zoomcamp-will-demo-485315/yellow/2024/yellow_tripdata_2024-06.parquet']);

CREATE OR REPLACE TABLE zoomcamp.yellow_2024
AS
SELECT * FROM zoomcamp.yellow_2024_ext;

-- Q1. Count of records
-- 20,332,093
SELECT COUNT(*) AS total_rows
FROM zoomcamp.yellow_2024_ext;

-- Q2. Estimated data read (External vs Table)
-- 0 MB for the External Table and 155.12 MB for the Materialized Table
SELECT COUNT(DISTINCT PULocationID)
FROM zoomcamp.yellow_2024_ext;

SELECT COUNT(DISTINCT PULocationID)
FROM zoomcamp.yellow_2024;

-- Q3.  Columnar storage
SELECT PULocationID FROM zoomcamp.yellow_2024;

SELECT PULocationID, DOLocationID FROM zoomcamp.yellow_2024;

-- Q4. Zero fare trips
-- 8,333
SELECT COUNT(*) FROM zoomcamp.yellow_2024 WHERE fare_amount = 0;

-- Q5. Partitioning & clustering strategy
-- Partition by tpep_dropoff_datetime and Cluster on VendorID
CREATE OR REPLACE TABLE zoomcamp.yellow_2024_part
  PARTITION BY DATE(tpep_dropoff_datetime)
  CLUSTER BY VendorID
AS
SELECT * FROM zoomcamp.yellow_2024;

-- Q6. Partition benefits
SELECT DISTINCT VendorID
FROM zoomcamp.yellow_2024
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
-- 실행 시 이 쿼리가 310.24MB를 처리합니다.

SELECT DISTINCT VendorID
FROM zoomcamp.yellow_2024_part
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
-- 실행 시 이 쿼리가 26.84MB를 처리합니다.

-- Q9. Table scan explanation
SELECT COUNT(*) FROM zoomcamp.yellow_2024;
