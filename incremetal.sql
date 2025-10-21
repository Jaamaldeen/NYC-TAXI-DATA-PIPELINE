DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;
DROP SCHEMA IF EXISTS metadata CASCADE;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
CREATE SCHEMA metadata;

CREATE TABLE metadata.last_load_period(
    last_load_time TIMESTAMP,
    schema_type VARCHAR PRIMARY KEY
);

INSERT INTO metadata.last_load_period(schema_type, last_load_time)
VALUES
    ('bronze', NULL),
    ('silver', NULL),
    ('gold', NULL);

CREATE TABLE bronze.taxi (
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
    Airport_fee FLOAT,
    time_uploaded TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_trip UNIQUE (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID)
);

CREATE TABLE silver.taxi (
    vendorID INT,
    vendor_name VARCHAR,
    rate_description VARCHAR,
    payment_description VARCHAR,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    tripduration FLOAT,
    passenger_count FLOAT,
    trip_distance FLOAT,
    store_and_fwd_flag VARCHAR(5),
    PULocationID INT,
    DOLocationID INT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT
);

CREATE TABLE gold.daily_summary (
    trip_date DATE PRIMARY KEY,
    total_trips BIGINT,
    total_passengers FLOAT,
    total_distance_miles FLOAT,
    total_revenue FLOAT,
    total_tips FLOAT,
    avg_fare FLOAT,
    avg_trip_distance FLOAT
);

CREATE TABLE gold.monthly_summary (
    month DATE PRIMARY KEY,
    total_trips BIGINT,
    total_revenue FLOAT,
    total_tips FLOAT,
    total_distance FLOAT
);

CREATE TABLE gold.payment_summary (
    payment_description VARCHAR PRIMARY KEY,
    trip_count BIGINT,
    total_revenue FLOAT,
    total_tips FLOAT,
    avg_tip_percent NUMERIC
);

CREATE TABLE gold.vendor_summary (
    vendor_name VARCHAR PRIMARY KEY,
    total_trips BIGINT,
    total_revenue FLOAT,
    total_distance FLOAT,
    avg_trip_distance FLOAT,
    avg_fare FLOAT
);

CREATE TABLE gold.zone_summary (
    PULocationID INT PRIMARY KEY,
    pickups BIGINT,
    revenue_from_pickups FLOAT,
    total_tips FLOAT
);

CREATE OR REPLACE FUNCTION metadata.update_last_load()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE metadata.last_load_period
    SET last_load_time = CURRENT_TIMESTAMP
    WHERE schema_type = TG_TABLE_SCHEMA;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_metadata_bronze
AFTER INSERT ON bronze.taxi
FOR EACH STATEMENT
EXECUTE FUNCTION metadata.update_last_load();

CREATE TRIGGER trg_update_metadata_silver
AFTER INSERT ON silver.taxi
FOR EACH STATEMENT
EXECUTE FUNCTION metadata.update_last_load();

CREATE OR REPLACE FUNCTION silver_load()
RETURNS TRIGGER AS $$
DECLARE
    m_last_load_time TIMESTAMP;
BEGIN
    SELECT last_load_time
    INTO m_last_load_time
    FROM metadata.last_load_period
    WHERE schema_type = 'bronze';

    INSERT INTO silver.taxi
    SELECT DISTINCT
        vendorID,
        CASE vendorID
            WHEN 1 THEN 'Creative Mobile Technologies, LLC'
            WHEN 2 THEN 'Curb Mobility, LLC'
            WHEN 6 THEN 'Myle Technologies Inc'
            WHEN 7 THEN 'Helix'
            ELSE 'Unknown'
        END AS vendor_name,
        CASE RatecodeID
            WHEN 1 THEN 'Standard rate'
            WHEN 2 THEN 'JFK'
            WHEN 3 THEN 'Newark'
            WHEN 4 THEN 'Nassau or Westchester'
            WHEN 5 THEN 'Negotiated fare'
            WHEN 6 THEN 'Group ride'
            ELSE 'Null/unknown'
        END AS rate_description,
        CASE payment_type
            WHEN 0 THEN 'Flex Fare trip'
            WHEN 1 THEN 'Credit card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            WHEN 6 THEN 'Voided trip'
        END AS payment_description,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        ROUND(EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60, 2) AS tripduration,
        passenger_count,
        trip_distance,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        ABS(fare_amount),
        ABS(extra),
        ABS(mta_tax),
        ABS(tip_amount),
        ABS(tolls_amount),
        ABS(improvement_surcharge),
        ABS(fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge 
            + congestion_surcharge + airport_fee),
        ABS(congestion_surcharge),
        ABS(airport_fee)
    FROM bronze.taxi
    WHERE (m_last_load_time IS NULL OR time_uploaded > m_last_load_time)
      AND trip_distance > 0
	  AND tpep_dropoff_datetime > '2023-12-31' 
	  AND tpep_pickup_datetime > '2023-12-31';
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_silver_load
AFTER INSERT ON bronze.taxi
FOR EACH STATEMENT
EXECUTE FUNCTION silver_load();

CREATE OR REPLACE FUNCTION gold_load()
RETURNS TRIGGER AS $$
BEGIN
    WITH new_data AS (
        SELECT 
            DATE(tpep_pickup_datetime) AS trip_date,
            COUNT(*) AS total_trips,
            SUM(passenger_count) AS total_passengers,
            SUM(trip_distance) AS total_distance_miles,
            SUM(total_amount) AS total_revenue,
            SUM(tip_amount) AS total_tips
        FROM new_silver_rows
        GROUP BY DATE(tpep_pickup_datetime)
    )
    INSERT INTO gold.daily_summary (
        trip_date, total_trips, total_passengers, total_distance_miles,
        total_revenue, total_tips, avg_fare, avg_trip_distance
    )
    SELECT
        trip_date, total_trips, total_passengers, total_distance_miles,
        total_revenue, total_tips,
        total_revenue / total_trips,
        total_distance_miles / total_trips
    FROM new_data
    ON CONFLICT (trip_date)
    DO UPDATE SET
        total_trips = gold.daily_summary.total_trips + EXCLUDED.total_trips,
        total_passengers = gold.daily_summary.total_passengers + EXCLUDED.total_passengers,
        total_distance_miles = gold.daily_summary.total_distance_miles + EXCLUDED.total_distance_miles,
        total_revenue = gold.daily_summary.total_revenue + EXCLUDED.total_revenue,
        total_tips = gold.daily_summary.total_tips + EXCLUDED.total_tips,
        avg_fare = (gold.daily_summary.total_revenue + EXCLUDED.total_revenue) / 
                   (gold.daily_summary.total_trips + EXCLUDED.total_trips),
        avg_trip_distance = (gold.daily_summary.total_distance_miles + EXCLUDED.total_distance_miles) / 
                            (gold.daily_summary.total_trips + EXCLUDED.total_trips);

    WITH new_data AS (
        SELECT 
            DATE_TRUNC('month', tpep_pickup_datetime)::DATE AS month,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            SUM(tip_amount) AS total_tips,
            SUM(trip_distance) AS total_distance
        FROM new_silver_rows
        GROUP BY month
    )
    INSERT INTO gold.monthly_summary (month, total_trips, total_revenue, total_tips, total_distance)
    SELECT month, total_trips, total_revenue, total_tips, total_distance FROM new_data
    ON CONFLICT (month)
    DO UPDATE SET
        total_trips = gold.monthly_summary.total_trips + EXCLUDED.total_trips,
        total_revenue = gold.monthly_summary.total_revenue + EXCLUDED.total_revenue,
        total_tips = gold.monthly_summary.total_tips + EXCLUDED.total_tips,
        total_distance = gold.monthly_summary.total_distance + EXCLUDED.total_distance;

    WITH new_data AS (
        SELECT 
            payment_description,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS total_revenue,
            SUM(tip_amount) AS total_tips
        FROM new_silver_rows
        GROUP BY payment_description
    )
    INSERT INTO gold.payment_summary (payment_description, trip_count, total_revenue, total_tips, avg_tip_percent)
    SELECT 
        payment_description, trip_count, total_revenue, total_tips,
        ROUND((total_tips / NULLIF(total_revenue, 0))::numeric * 100, 2)
    FROM new_data
    ON CONFLICT (payment_description)
    DO UPDATE SET
        trip_count = gold.payment_summary.trip_count + EXCLUDED.trip_count,
        total_revenue = gold.payment_summary.total_revenue + EXCLUDED.total_revenue,
        total_tips = gold.payment_summary.total_tips + EXCLUDED.total_tips,
        avg_tip_percent = ROUND(
            ((gold.payment_summary.total_tips + EXCLUDED.total_tips) /
             NULLIF(gold.payment_summary.total_revenue + EXCLUDED.total_revenue, 0))::numeric * 100, 2
        );

    WITH new_data AS (
        SELECT 
            vendor_name,
            COUNT(*) AS total_trips,
            SUM(total_amount) AS total_revenue,
            SUM(trip_distance) AS total_distance
        FROM new_silver_rows
        GROUP BY vendor_name
    )
    INSERT INTO gold.vendor_summary (vendor_name, total_trips, total_revenue, total_distance, avg_trip_distance, avg_fare)
    SELECT
        vendor_name, total_trips, total_revenue, total_distance,
        total_distance / total_trips,
        total_revenue / total_trips
    FROM new_data
    ON CONFLICT (vendor_name)
    DO UPDATE SET
        total_trips = gold.vendor_summary.total_trips + EXCLUDED.total_trips,
        total_revenue = gold.vendor_summary.total_revenue + EXCLUDED.total_revenue,
        total_distance = gold.vendor_summary.total_distance + EXCLUDED.total_distance,
        avg_trip_distance = (gold.vendor_summary.total_distance + EXCLUDED.total_distance) / 
                            (gold.vendor_summary.total_trips + EXCLUDED.total_trips),
        avg_fare = (gold.vendor_summary.total_revenue + EXCLUDED.total_revenue) /
                   (gold.vendor_summary.total_trips + EXCLUDED.total_trips);

    WITH new_data AS (
        SELECT 
            PULocationID,
            COUNT(*) AS pickups,
            SUM(total_amount) AS revenue_from_pickups,
            SUM(tip_amount) AS total_tips
        FROM new_silver_rows
        GROUP BY PULocationID
    )
    INSERT INTO gold.zone_summary (PULocationID, pickups, revenue_from_pickups, total_tips)
    SELECT PULocationID, pickups, revenue_from_pickups, total_tips FROM new_data
    ON CONFLICT (PULocationID)
    DO UPDATE SET
        pickups = gold.zone_summary.pickups + EXCLUDED.pickups,
        revenue_from_pickups = gold.zone_summary.revenue_from_pickups + EXCLUDED.revenue_from_pickups,
        total_tips = gold.zone_summary.total_tips + EXCLUDED.total_tips;

    UPDATE metadata.last_load_period
    SET last_load_time = CURRENT_TIMESTAMP
    WHERE schema_type = 'gold';

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_gold_load
AFTER INSERT ON silver.taxi
REFERENCING NEW TABLE AS new_silver_rows
FOR EACH STATEMENT
EXECUTE FUNCTION gold_load();


DROP TABLE IF EXISTS tmp_taxi;
CREATE TEMP TABLE tmp_taxi (LIKE bronze.taxi INCLUDING DEFAULTS INCLUDING CONSTRAINTS EXCLUDING INDEXES);
copy tmp_taxi (
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
)
FROM 'C:\Program Files\PostgreSQL\17\data\yellow taxi\yellow_tripdata_2024-02.csv'
WITH (FORMAT csv, HEADER true);



INSERT INTO bronze.taxi
SELECT * FROM tmp_taxi
ON CONFLICT DO NOTHING;
