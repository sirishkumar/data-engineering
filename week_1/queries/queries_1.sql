SELECT
       tpep_pickup_datetime,
       tpep_dropoff_datetime,
       total_amount,
       CONCAT(puz."Borough", ' / ', puz."Zone") AS "pickup_loc",
       CONCAT(doz."Borough", ' / ', doz."Zone" ) AS "dropoff_loc"
FROM
    yellow_taxi_data as t,
    taxi_zones as puz,
    taxi_zones as doz

WHERE
    t."PULocationID" = puz."LocationID" AND
    t."DOLocationID" = doz."LocationID"
LIMIT 100;

-- query using joins

SELECT
       tpep_pickup_datetime,
       tpep_dropoff_datetime,
       total_amount,
       CONCAT(puz."Borough", ' / ', puz."Zone") AS "pickup_loc",
       CONCAT(doz."Borough", ' / ', doz."Zone" ) AS "dropoff_loc"
FROM
    yellow_taxi_data as t INNER JOIN taxi_zones puz
        ON t."PULocationID" =  puz."LocationID"
    INNER JOIN taxi_zones doz on t."DOLocationID" = doz."LocationID"
LIMIT 101;

-- Check if whether PULocationID or DOLocationID is null
SELECT
    *
FROM
    yellow_taxi_data
WHERE
    "PULocationID" = NULL;

SELECT * FROM yellow_taxi_data WHERE "DOLocationID" IS NULL LIMIT 100;

-- Select all trips with undefined pickup or dropoff locations
SELECT
    *
FROM
    yellow_taxi_data
WHERE
    "PULocationID" NOT IN (SELECT "LocationID" FROM taxi_zones)
LIMIT 100;

SELECT
    *
FROM
    yellow_taxi_data
WHERE
    "DOLocationID" NOT IN (SELECT "LocationID" FROM taxi_zones);


-- Number of trips by day
SELECT
    DATE_TRUNC('DAY', tpep_dropoff_datetime),
    total_amount
FROM
    yellow_taxi_data;

SELECT
    CAST(tpep_dropoff_datetime AS DATE) as "day",
    COUNT(1) as "count",
    MAX(total_amount)
FROM
    yellow_taxi_data AS t
GROUP BY
         CAST(tpep_dropoff_datetime AS DATE)
ORDER BY "count" DESC;

-- Group BY multiple fields

SELECT
    CAST(tpep_pickup_datetime AS DATE) as "day",
    "DOLocationID",
    COUNT(1) AS "count",
    MAX(total_amount)
FROM yellow_taxi_data
GROUP BY
    1,2
ORDER BY
    "day" ASC,
    "DOLocationID" ASC;

-- Number of trips on Jan 15
SELECT
    CAST(tpep_pickup_datetime as DATE) as day,
    MAX(tip_amount) as max_tip_on_day,
    COUNT(1)
FROM
    yellow_taxi_data
GROUP BY
    CAST(tpep_pickup_datetime as DATE)
ORDER BY max_tip_on_day DESC;

-- Most popular destination on Jan 14th
SELECT
    CAST(tpep_pickup_datetime as DATE) as day,
    "DOLocationID" as destination,
    COUNT(1) as total_trips
FROM
     yellow_taxi_data as t INNER JOIN taxi_zones tz
         ON t."DOLocationID" = tz."LocationID"
GROUP BY
    1, 2
ORDER BY total_trips DESC;

SELECT * from taxi_zones where "LocationID"=236;


--

SELECT
    CONCAT(tz."Borough", ' / ', tz."Zone"),
    result.day,
    result.total_trips
FROM taxi_zones as tz INNER JOIN
(SELECT
    CAST(tpep_pickup_datetime as DATE) as day,
    "DOLocationID" as destination,
    COUNT(1) as total_trips
FROM
     yellow_taxi_data
GROUP BY
    1, 2
ORDER BY total_trips DESC) as result
 ON tz."LocationID"=result.destination;

-- Get PICK-DROPOFF pair with maximum average price

SELECT
       avg_amount,
       start_end_loc_id,
       CONCAT(puz."Zone", ' / ', doz."Zone")
FROM
(SELECT
    COUNT(1) as total_trips,
    MAX(t."PULocationID") as "PLID",
    MAX(t."DOLocationID") as "DLID",
    CONCAT(t."PULocationID", '-', t."DOLocationID") as "start_end_loc_id",
    AVG(t.total_amount) as avg_amount
FROM
    yellow_taxi_data as t
GROUP BY start_end_loc_id
ORDER BY avg_amount DESC) as result INNER JOIN taxi_zones puz
    ON result."PLID"=puz."LocationID"
INNER JOIN taxi_zones  doz
    ON result."DLID"=doz."LocationID"
ORDER BY avg_amount DESC;