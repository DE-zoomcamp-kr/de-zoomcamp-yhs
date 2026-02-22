/* @bruin
name: staging.trips
type: duckdb.sql
depends:
  - ingestion.trips
  - ingestion.payment_lookup
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp
columns:
  - name: pickup_datetime
    type: timestamp
@bruin */

SELECT
  t.pickup_datetime,
  t.dropoff_datetime,
  t.pickup_location_id,
  t.dropoff_location_id,
  t.fare_amount,
  t.taxi_type,
  p.payment_type_name
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup p
  ON t.payment_type = p.payment_type_id
WHERE t.pickup_datetime >= '{{ start_datetime }}'
  AND t.pickup_datetime < '{{ end_datetime }}'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY t.pickup_datetime,
               t.dropoff_datetime,
               t.pickup_location_id,
               t.dropoff_location_id,
               t.fare_amount
  ORDER BY t.pickup_datetime
) = 1;