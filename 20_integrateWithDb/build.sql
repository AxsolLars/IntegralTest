USE ROLE ROLE_AXSOL_ETL;
USE DATABASE AXSOL_DB_DEV;
USE SCHEMA BOK;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE STREAM powermeter_stream
ON TABLE JANITZA_POWERMETER
SHOW_INITIAL_ROWS = TRUE;
CREATE OR REPLACE STREAM powermeter_stream
ON TABLE JANITZA_POWERMETER
SHOW_INITIAL_ROWS = FALSE;
CREATE OR REPLACE STREAM power_production_stream
ON TABLE METEOCONTROL_CTRL
SHOW_INITIAL_ROWS = TRUE;
CREATE OR REPLACE TABLE stream_consumed_marker (
    processed_at TIMESTAMP,
    row_count NUMBER
);

CREATE OR REPLACE TABLE power_export_sum (
    timestamp TIMESTAMP_TZ(9),
    cumulative_sum DOUBLE,
    integral FLOAT,
    delta FLOAT,
    value FLOAT
);

CREATE OR REPLACE TABLE power_production_sum (
    timestamp TIMESTAMP_TZ(9),
    cumulative_sum DOUBLE,
    integral FLOAT,
    delta FLOAT,
    value FLOAT
);

ALTER TABLE power_export_sum CLUSTER BY (timestamp);
ALTER TABLE power_production_sum CLUSTER BY (timestamp);