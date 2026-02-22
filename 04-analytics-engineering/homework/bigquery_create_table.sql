CREATE SCHEMA IF NOT EXISTS `terraform-demo-485315.nytaxi`
OPTIONS(location="EU");

-- Yellow 2019
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.yellow_2019_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-will-demo-485315/yellow/2019/yellow_tripdata_2019-*.parquet']
);

-- Yellow 2020
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.yellow_2020_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-will-demo-485315/yellow/2020/yellow_tripdata_2020-*.parquet']
);

-- Green 2019
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.green_2019_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-will-demo-485315/green/2019/green_tripdata_2019-*.parquet']
);

-- Green 2020
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.green_2020_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-will-demo-485315/green/2020/green_tripdata_2020-*.parquet']
);