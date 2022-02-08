--- Create an external table with fhv data
CREATE OR REPLACE EXTERNAL TABLE `atomic-voice-338721.nytaxi.external_fhv_data`
OPTIONS (
    format="parquet",
    uris =["gs://dtc_data_lake_atomic-voice-338721/parquet/fhv_tripdata_*.parquet"]
);


--- Check external table
SELECT * FROM `atomic-voice-338721.nytaxi.external_fhv_data` limit 100;

SELECT COUNT(*) FROM `atomic-voice-338721.nytaxi.external_fhv_data`;


--- Create a non-partitioned table from extrernal data
CREATE OR REPLACE TABLE `atomic-voice-338721.nytaxi.fhv_data` AS
SELECT * FROM `atomic-voice-338721..nytaxi.external_fhv_data`;

--- Question 1: Count of FHV vehicles for the year 2019
SELECT COUNT(*) FROM `atomic-voice-338721.nytaxi.fhv_data` 
    WHERE  DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31';

--- Question 2: Distinct dispatching base numbers
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `atomic-voice-338721.nytaxi.fhv_data`;

--- Question 4: What is the count, estimated and actual data processed for query which 
--- counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279 *

SELECT COUNT(*) FROM `atomic-voice-338721.nytaxi.fhv_data` 
    WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31' 
    AND dispatching_base_num in ('B00987', 'B02060', 'B02279');

--- Question 5 Best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag

SELECT DISTINCT(dispatching_base_num) FROM `atomic-voice-338721.nytaxi.fhv_data`;
SELECT DISTINCT(SR_Flag) FROM `atomic-voice-338721.nytaxi.fhv_data`;
-- SR_Flags are sequential, partitioning them based on this allows deprecation policy, metadata
-- says it can be either 1 or null, but rather the values are ranging from null, 1-42. 
-- Partioning by dispatching_base_num may result in frequent updates to partitions which degrades
-- the performance. Clustering of these columns makes more sense to avoid frequent updates to paritions
