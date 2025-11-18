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
INSERT INTO power_export_sum (
    timestamp,
    cumulative_sum,
    integral,
    delta,
    value
)
VALUES (
    '2025-10-20 09:48:00 +0000',
    0.0,
    0.0,
    0.0,
    0.0
);
GRANT SELECT ON TABLE AXSOL_DB_DEV.BOK.power_export_sum TO ROLE BOK_READER;
CREATE OR REPLACE TABLE power_production_sum (
    timestamp TIMESTAMP_TZ(9),
    cumulative_sum DOUBLE,
    integral FLOAT,
    delta FLOAT,
    value FLOAT
);
-- base value, important:

INSERT INTO power_production_sum (
    timestamp,
    cumulative_sum,
    integral,
    delta,
    value
)
VALUES (
    '2025-10-20 09:48:00 +0000',
    0.0,
    0.0,
    0.0,
    0.0
);

GRANT SELECT ON TABLE AXSOL_DB_DEV.BOK.power_production_sum TO ROLE BOK_READER;
ALTER TABLE power_export_sum CLUSTER BY (timestamp);
ALTER TABLE power_production_sum CLUSTER BY (timestamp);

CREATE OR REPLACE TABLE sum_history (
    date DATE NOT NULL,
    production_sum DOUBLE,
    export_sum DOUBLE
);
GRANT SELECT ON TABLE AXSOL_DB_DEV.BOK.sum_history TO ROLE BOK_READER;