# Module 2 Homework – Workflow Orchestration (Kestra)

This README describes how each homework question was solved using **Kestra flows**.

---

## Q1. Uncompressed file size (Yellow Taxi, 2020-12)

**Flow used**
```
homework/flows/q1_yellow_2020_12_file_size.yaml
```

**What the flow does**
- Downloads `yellow_tripdata_2020-12.csv.gz`
- Unzips the file
- Prints the uncompressed file size in the execution logs
```
2026-01-31 00:05:35.541 -rw-r--r-- 1 root root 129M Jul 14  2022 yellow_tripdata_2020-12.csv
```

---

## Q2. Rendered value of the `file` variable

**Flow used**
```
homework/flows/q2_file_render.yaml
```

**Inputs**
- `taxi = green`
- `year = 2020`
- `month = 04`

**What the flow does**
- Renders the `file` variable using Kestra templating
- Prints the rendered value in the logs
```
2026-01-31 00:14:15.424 Rendered file name is: green_tripdata_2020-04.csv
```

---

## Q3. Total rows for Yellow Taxi data in 2020

**Flow used**
```
homework/flows/q3_yellow_2020.yaml
```

**What the flow does**
- Loads all Yellow Taxi data for year 2020 into BigQuery
- Counts total rows across all 2020 CSV files
```
2026-01-30T15:33:13.001174Z INFO TOTAL ROWS FOR yellow 2020: 24648499
```


---

## Q4. Total rows for Green Taxi data in 2020

**Flow used**
```
homework/flows/q4_green_2020.yaml
```

**What the flow does**
- Loads all Green Taxi data for year 2020 into BigQuery
- Counts total rows across all 2020 CSV files
```
2026-01-31 00:31:18.517 TOTAL ROWS FOR green 2020: 1734051
```

---

## Q5. Rows for Yellow Taxi data (March 2021)

**Flow used**
```

homework/flows/q5_yellow_2021_03_exact.yaml

```

**What the flow does**
- Loads `yellow_tripdata_2021-03.csv` into BigQuery
- Counts rows for that specific file
```
SELECT COUNT(*) FROM `~.zoomcamp.yellow_tripdata_2021_03`;
-- 1925152
```

---

## Q6. Configure timezone for New York in a Schedule trigger

- Add a `timezone` property set to **`America/New_York`**
