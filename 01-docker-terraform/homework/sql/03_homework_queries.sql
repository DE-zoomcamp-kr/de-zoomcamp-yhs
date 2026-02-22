-- Q3
SELECT COUNT(*) AS trips_le_1_mile
FROM green_taxi_trips_2025_11
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01'
  AND trip_distance <= 1;

-- Q4
SELECT
  DATE(lpep_pickup_datetime) AS pickup_day,
  MAX(trip_distance) AS max_trip_distance
FROM green_taxi_trips_2025_11
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01'
  AND trip_distance < 100
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

-- Q5
SELECT
  z.Zone AS pickup_zone,
  SUM(t.total_amount) AS total_amount_sum
FROM green_taxi_trips_2025_11 t
JOIN taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
WHERE lpep_pickup_datetime >= '2025-11-18'
  AND lpep_pickup_datetime <  '2025-11-19'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

-- Q6
SELECT
  dz.Zone AS dropoff_zone,
  MAX(t.tip_amount) AS max_tip
FROM green_taxi_trips_2025_11 t
JOIN taxi_zone_lookup pz
  ON t.PULocationID = pz.LocationID
JOIN taxi_zone_lookup dz
  ON t.DOLocationID = dz.LocationID
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01'
  AND pz.Zone = 'East Harlem North'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;
