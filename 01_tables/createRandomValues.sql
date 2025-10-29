USE DATABASE testing_db;
USE SCHEMA integral_test;

USE ROLE ROLE_TEST_ETL;

CREATE OR REPLACE TABLE random_values (
    id INT AUTOINCREMENT,
    timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    value FLOAT,
    CONSTRAINT pk_random_values PRIMARY KEY (id)
);

COMMENT ON TABLE random_values IS 'Holds live random values inserted by the generator script.';

CREATE OR REPLACE TABLE random_values_mirror (
    id INT,
    timestamp TIMESTAMP_NTZ,
    value FLOAT,
    CONSTRAINT pk_random_values_mirror PRIMARY KEY (id)
);

COMMENT ON TABLE random_values_mirror IS 'Mirror table populated periodically by ETL tasks';

