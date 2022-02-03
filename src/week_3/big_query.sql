-- Create external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `atomic-voice-338721.nytaxi.external_yellow_tripdata`
OPTIONS (
    format = 'parquet',
    uris = ['gs://dtc_data_lake_atomic-voice-338721/parquet/yellow_tripdata_2019-*.parquet',
            'gs://dtc_data_lake_atomic-voice-338721/parquet/yellow_tripdata_2020-*.parquet']
);

-- Check yellow trip data
SELECT * FROM atomic-voice-338721.nytaxi.external_yellow_tripdata limit 100;

SELECT count(*) FROM atomic-voice-338721.nytaxi.external_yellow_tripdata;

-- Create a non partitioned table from external data
CREATE OR REPLACE TABLE atomic-voice-338721.nytaxi.yellow_tripdata AS 
SELECT * FROM atomic-voice-338721.nytaxi.external_yellow_tripdata;

-- Create a partitioned table from table
CREATE OR REPLACE TABLE atomic-voice-338721.nytaxi.yello_tripdata_partioned_by_date
PARTITION BY 
    DATE(tpep_pickup_datetime) AS
SELECT * FROM atomic-voice-338721.nytaxi.yellow_tripdata;

-- Impact of parition
-- Scans 1.6 GB data in 2.2 seconds
SELECT DISTINCT(VendorID)
FROM `atomic-voice-338721.nytaxi.yellow_tripdata`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- With parition
-- Scans 105.9 MB in 1.1 seconds
SELECT DISTINCT(VendorID)
FROM `atomic-voice-338721.nytaxi.yello_tripdata_partioned_by_date` 
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look in to the paritions
SELECT table_name, partition_id, total_rows
    FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name='yello_tripdata_partioned_by_date'
ORDER BY total_rows DESC;


-- Clustering

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `nytaxi.yello_tripdata_paritioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS 
SELECT * FROM `atomic-voice-338721.nytaxi.external_yellow_tripdata`;


-- Query scans with only parition
-- Scans 1.1 GB Data in 0.7 seconds
SELECT COUNT(*) as trips
FROM `atomic-voice-338721.nytaxi.yello_tripdata_partioned_by_date`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
    AND VendorID=1;

-- Query scans with parition and clustering
-- Scans 862.3 MB Data in 0.7 seconds
SELECT COUNT(*) as trips
FROM `atomic-voice-338721.nytaxi.yello_tripdata_paritioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
    AND VendorID=1;
