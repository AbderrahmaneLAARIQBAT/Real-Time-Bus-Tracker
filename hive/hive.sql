CREATE EXTERNAL TABLE IF NOT EXISTS eta_predictions (
    bus_id STRING,
    line_id STRING,
    station_name STRING,
    distance_km DOUBLE,
    speed DOUBLE,
    eta_minutes DOUBLE
)
STORED AS PARQUET
LOCATION '/smartbus/eta_results';

SELECT * FROM eta_predictions LIMIT 10;
