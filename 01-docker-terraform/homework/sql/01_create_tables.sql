DROP TABLE IF EXISTS green_taxi_trips_2025_11;
DROP TABLE IF EXISTS taxi_zone_lookup;

CREATE TABLE green_taxi_trips_2025_11 (
  VendorID DOUBLE PRECISION,
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime TIMESTAMP,
  store_and_fwd_flag TEXT,
  RatecodeID DOUBLE PRECISION,
  PULocationID INTEGER,
  DOLocationID INTEGER,
  passenger_count DOUBLE PRECISION,
  trip_distance DOUBLE PRECISION,
  fare_amount DOUBLE PRECISION,
  extra DOUBLE PRECISION,
  mta_tax DOUBLE PRECISION,
  tip_amount DOUBLE PRECISION,
  tolls_amount DOUBLE PRECISION,
  ehail_fee DOUBLE PRECISION,
  improvement_surcharge DOUBLE PRECISION,
  total_amount DOUBLE PRECISION,
  payment_type DOUBLE PRECISION,
  trip_type DOUBLE PRECISION,
  congestion_surcharge DOUBLE PRECISION,
  cbd_congestion_fee DOUBLE PRECISION
);

CREATE TABLE taxi_zone_lookup (
  LocationID INTEGER,
  Borough TEXT,
  Zone TEXT,
  service_zone TEXT
);
