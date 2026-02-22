# Homework 4 Analytics Engineering with dbt (Module 4)

## Q1. dbt Lineage and Execution

**Answer:** `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)

- `dbt run --select int_trips_unioned` builds the **selected node**.
- dbt will also build any **required upstream dependencies** to make the selected node runnable.
- It will **not** build downstream models unless you explicitly include them (e.g., `+int_trips_unioned+`).

---

## Q2. dbt Tests (accepted_values)

**Answer:** dbt will fail the test, returning a non-zero exit code

- `accepted_values` enforces that `payment_type` must be in `[1,2,3,4,5]`.
- When value `6` appears, the test query returns failing rows, so dbt reports a test failure and exits non-zero (typical CI-breaking behavior for tests).

---

## Q3. Count of records in `fct_monthly_zone_revenue`

**Answer:** **12,998**
```
SELECT COUNT(*) AS cnt
FROM `{{ target.project }}.{{ target.dataset }}.fct_monthly_zone_revenue`;
```

---

## Q4. Best performing pickup zone for Green taxis in 2020

**Answer:** **East Harlem North**

```
SELECT
  pickup_zone,
  SUM(revenue_monthly_total_amount) AS total_revenue
FROM `{{ target.project }}.{{ target.dataset }}.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND year = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
```

---

## Q5. Green taxi total trips in October 2019

**Answer:** **384,624**

```
SELECT
  SUM(total_monthly_trips) AS total_trips
FROM `{{ target.project }}.{{ target.dataset }}.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND year = 2019
  AND month = 10;
```

---

## Q6. Build `stg_fhv_tripdata` (2019) and count records

**Answer:** **42,084,899**

```
WITH source AS (
  SELECT * 
  FROM {{ source('raw_nyc_tripdata', 'fhv_tripdata_2019') }}
),
filtered AS (
  SELECT
    dispatching_base_num,
    -- rename to project naming conventions
    CAST(PUlocationID AS INT64) AS pickup_location_id,
    CAST(DOlocationID AS INT64) AS dropoff_location_id,
    CAST(SR_Flag AS INT64) AS sr_flag,

    -- standardize timestamps
    CAST(pickup_datetime AS TIMESTAMP)  AS pickup_datetime,
    CAST(dropOff_datetime AS TIMESTAMP) AS dropoff_datetime
  FROM source
  WHERE dispatching_base_num IS NOT NULL
)
SELECT * FROM filtered;

SELECT COUNT(*) AS cnt
FROM `{{ target.project }}.{{ target.dataset }}.stg_fhv_tripdata`;
```
