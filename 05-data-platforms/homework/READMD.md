# Module 5 Homework - Data Platforms with Bruin

This homework demonstrates understanding of Bruin’s project structure, materialization strategies, pipeline execution, variables, quality checks, and lineage features.

---

## Question 1. Bruin Pipeline Structure

**Question:**  
In a Bruin project, what are the required files/directories?

**Answer:**  
`.bruin.yml` and `pipeline/` with `pipeline.yml` and `assets/`

**Explanation:**  
A valid Bruin project must contain:

- `.bruin.yml` (environment and connection configuration)
- `pipeline/`
  - `pipeline.yml` (pipeline definition)
  - `assets/` (SQL, Python, seed assets)

Without this structure, Bruin cannot detect and execute assets properly.

---

## Question 2. Materialization Strategies

**Question:**  
Which incremental strategy is best for processing a specific time interval by deleting and inserting data for that period?

**Answer:**  
`time_interval`

**Explanation:**  
The `time_interval` strategy:

- Deletes rows in the specified time range
- Re-inserts data for that interval
- Is ideal for time-partitioned datasets such as NYC taxi data based on `pickup_datetime`

---

## Question 3. Pipeline Variables

**Question:**  
How do you override the default taxi types to process only yellow taxis?

**Answer:**

```bash
bruin run --var 'taxi_types=["yellow"]'
````

**Explanation:**
Pipeline variables defined as arrays must be overridden using valid JSON format.
The value must be passed as a JSON array string.

---

## Question 4. Running with Dependencies

**Question:**
How do you run `ingestion/trips.py` and all downstream assets?

**Answer:**

```bash
bruin run ingestion/trips.py --downstream
```

**Explanation:**
The `--downstream` flag ensures that:

* The selected asset runs
* All dependent assets are executed in order

---

## Question 5. Quality Checks

**Question:**
Which quality check ensures `pickup_datetime` never has NULL values?

**Answer:**
`not_null: true`

**Explanation:**
The `not_null` check validates that a column does not contain NULL values.

---

## Question 6. Lineage and Dependencies

**Question:**
Which command visualizes the dependency graph between assets?

**Answer:**

```bash
bruin lineage
```

**Explanation:**
The `bruin lineage` command shows upstream and downstream relationships between assets, helping visualize the DAG structure.

---

## Question 7. First-Time Run

**Question:**
Which flag ensures tables are created from scratch on a new DuckDB database?

**Answer:**
`--full-refresh`

**Explanation:**
The `--full-refresh` flag:

* Drops existing tables
* Recreates them from scratch
* Ensures a clean pipeline run

Example:

```bash
bruin run ./pipeline/pipeline.yml --full-refresh
```

---

# Summary

Through this homework, the following concepts were demonstrated:

* Understanding of Bruin project structure
* Proper use of incremental materialization strategies
* Overriding pipeline variables
* Executing assets with dependencies
* Implementing column-level data quality checks
* Inspecting lineage and pipeline DAG
* Running full refresh builds

The NYC Taxi ELT pipeline successfully ingests, transforms, validates, and aggregates data using Bruin with DuckDB.

---

