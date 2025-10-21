DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

CREATE TABLE IF NOT EXISTS bronze.taxi (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count FLOAT,
    trip_distance FLOAT,
    RatecodeID FLOAT,
    store_and_fwd_flag VARCHAR(5),
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount FLOAT, 
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT
);

COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-01.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-02.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-03.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-04.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-05.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-06.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-07.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-08.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-09.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-10.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-11.csv' WITH (FORMAT csv, HEADER true);
COPY bronze.taxi FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-12.csv' WITH (FORMAT csv, HEADER true);


/*------------------------------------------------------------------------------------------------------------

			SILVER LAYER
------------------------------------------------------------------------------------------------------------*/

CREATE TABLE silver.taxi AS
SELECT DISTINCT
	vendorID,
    CASE vendorID
        WHEN 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN 2 THEN 'Curb Mobility, LLC'
        WHEN 6 THEN 'Myle Technologies Inc'
        WHEN 7 THEN 'Helix'
        ELSE 'Unknown'
    END AS vendor_name,
    CASE  RatecodeID
        WHEN  1 THEN 'Standard rate'
        WHEN  2 THEN 'JFK'
        WHEN  3 THEN 'Newark'
        WHEN  4 THEN 'Nassau or Westchester'
        WHEN  5 THEN 'Negotiated fare'
        WHEN  6 THEN 'Group ride'
        ELSE  'Null/unknown'
    END AS rate_description,
    CASE
        WHEN payment_type = 0 THEN 'Flex Fare trip'
        WHEN payment_type = 1 THEN 'Credit card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No charge'
        WHEN payment_type = 4 THEN 'Dispute'
        WHEN payment_type = 5 THEN 'Unknown'
        WHEN payment_type = 6 THEN 'Voided trip'
    END AS payment_description,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60 AS tripduration,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    ABS(fare_amount) AS fare_amount,
    ABS(extra) AS extra,
    ABS(mta_tax) AS mta_tax,
    ABS(tip_amount) AS tip_amount,
    ABS(tolls_amount) AS tolls_amount,
    ABS(improvement_surcharge) AS improvement_surcharge,
    ABS(fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge 
        + congestion_surcharge + airport_fee) AS total_amount,
    ABS(congestion_surcharge) AS congestion_surcharge,
    ABS(airport_fee) AS airport_fee
FROM bronze.taxi
WHERE trip_distance > 0
  AND tpep_pickup_datetime >= '2024-01-01'
  AND tpep_pickup_datetime < '2025-01-01';

/*------------------------------------------------------------------------------------------------------------

							GOLD LAYER
------------------------------------------------------------------------------------------------------------*/
CREATE TABLE gold.daily_summary AS
SELECT 
    DATE(tpep_pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    SUM(trip_distance) AS total_distance_miles,
    SUM(total_amount) AS total_revenue,
    SUM(tip_amount) AS total_tips,
    AVG(total_amount) AS avg_fare,
    AVG(trip_distance) AS avg_trip_distance
FROM silver.taxi
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY trip_date;

CREATE TABLE gold.monthly_summary AS
SELECT 
    DATE_TRUNC('month', tpep_pickup_datetime) AS month,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    SUM(tip_amount) AS total_tips,
    SUM(trip_distance) AS total_distance
FROM silver.taxi
GROUP BY DATE_TRUNC('month', tpep_pickup_datetime)
ORDER BY month;

CREATE TABLE gold.payment_summary AS
SELECT 
    payment_description,
    COUNT(*) AS trip_count,
    SUM(total_amount) AS total_revenue,
    SUM(tip_amount) AS total_tips,
    ROUND(AVG((tip_amount / NULLIF(total_amount, 0))::numeric) * 100, 2) AS avg_tip_percent
FROM silver.taxi
GROUP BY payment_description
ORDER BY total_revenue DESC;

CREATE TABLE gold.vendor_summary AS
SELECT 
    vendor_name,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(total_amount) AS avg_fare
FROM silver.taxi
GROUP BY vendor_name
ORDER BY total_revenue DESC;


CREATE TABLE gold.zone_summary AS
SELECT 
    PULocationID,
    COUNT(*) AS pickups,
    SUM(total_amount) AS revenue_from_pickups,
    AVG(DIS) AS total_tips
FROM silver.taxi
GROUP BY PULocationID
ORDER BY pickups DESC;










