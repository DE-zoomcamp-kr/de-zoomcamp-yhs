\copy green_taxi_trips_2025_11 FROM '/data/green_tripdata_2025-11.csv' DELIMITER ',' CSV HEADER NULL '';

\copy taxi_zone_lookup FROM '/data/taxi_zone_lookup.csv' DELIMITER ',' CSV HEADER;
